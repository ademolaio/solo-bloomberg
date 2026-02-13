import uuid
from datetime import datetime, timezone, date

import pandas as pd
import yfinance as yf

# optional:
# from symbols.etfs import ETFS


from .venue.venues import resolve_venue, Venue


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "default"

SOURCE = "yfinance"


def classify_asset(info: dict) -> str:
    qt = (info.get("quoteType") or "").lower()
    if qt == "etf":
        return "etf"
    return "equity"


def venue_from_yfinance_info(default_venue, info: dict):
    ex = (info.get("exchange") or "").upper()

    if ex in ("NMS", "NAS"):
        return ("NASDAQ", "XNAS")
    if ex in ("NYQ", "NYS"):
        return ("NYSE", "XNYS")
    if ex in ("PNK", "OTC"):
        return ("OTC", "OTCM")

    return (default_venue.exchange, default_venue.mic)


def get_existing_instrument_id(client, asset_class: str, symbol: str, mic: str) -> str | None:
    rows = client.query(
        """
        SELECT instrument_id
        FROM meta_data.instruments
        WHERE asset_class = %(asset_class)s
          AND mic = %(mic)s
          AND symbol = %(symbol)s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        parameters={"asset_class": asset_class, "mic": mic, "symbol": symbol},
    ).result_rows
    return rows[0][0] if rows else None


def get_or_create_instrument_id(client, asset_class: str, symbol: str, mic: str, exchange: str, short_name: str) -> str:
    existing = get_existing_instrument_id(client, asset_class, symbol, mic)
    if existing:
        return existing

    client.command(
        """
        INSERT INTO meta_data.instruments
        (asset_class, symbol, mic, exchange, short_name, is_active, updated_at, source)
        VALUES
        (%(asset_class)s, %(symbol)s, %(mic)s, %(exchange)s, %(short_name)s, 1, now64(3), %(source)s)
        """,
        parameters={
            "asset_class": asset_class,
            "symbol": symbol,
            "mic": mic,
            "exchange": exchange,
            "short_name": short_name,
            "source": SOURCE,
        },
    )

    created = get_existing_instrument_id(client, asset_class, symbol, mic)
    if not created:
        raise RuntimeError(f"Failed to create instrument_id for {asset_class=} {symbol=} {mic=}")
    return created


def upsert_equity_etf_meta(client, instrument_id: str, meta: dict):
    client.command(
        """
        INSERT INTO meta_data.equities_etfs
        (instrument_id, isin, figi, currency, country, sector, industry, updated_at, source)
        VALUES
        (%(instrument_id)s, %(isin)s, %(figi)s, %(currency)s, %(country)s, %(sector)s, %(industry)s, now64(3), %(source)s)
        """,
        parameters={
            "instrument_id": instrument_id,
            "isin": meta.get("isin", ""),
            "figi": meta.get("figi", ""),
            "currency": meta.get("currency", ""),
            "country": meta.get("country", ""),
            "sector": meta.get("sector", ""),
            "industry": meta.get("industry", ""),
            "source": SOURCE,
        },
    )


def get_max_loaded_date(client, instrument_id: str) -> date | None:
    rows = client.query(
        """
        SELECT max(date)
        FROM market_data.daily_prices
        WHERE instrument_id = %(instrument_id)s
          AND source = %(source)s
        """,
        parameters={"instrument_id": instrument_id, "source": SOURCE},
    ).result_rows
    return rows[0][0] if rows and rows[0][0] is not None else None


def ingest_symbol(client, symbol: str, mode: str = "daily"):
    t = yf.Ticker(symbol)
    try:
        info = t.info or {}
    except Exception:
        info = {}

    base_venue = resolve_venue(symbol)
    exchange, mic = venue_from_yfinance_info(base_venue, info)

    asset_class = classify_asset(info)

    short_name = info.get("shortName") or info.get("longName") or symbol

    meta = {
        "currency": info.get("currency") or "",
        "country": info.get("country") or "",
        "sector": info.get("sector") or "",
        "industry": info.get("industry") or "",
        "isin": info.get("isin") or "",
        "figi": info.get("figi") or "",
    }

    instrument_id = get_or_create_instrument_id(
        client=client,
        asset_class=asset_class,
        symbol=symbol,
        mic=mic,
        exchange=exchange,
        short_name=short_name,
    )

    upsert_equity_etf_meta(client, instrument_id, meta)

    # incremental pull
    if mode == "daily":
        max_date = get_max_loaded_date(client, instrument_id)

        if max_date:
            start = (pd.Timestamp(max_date) + pd.Timedelta(days=1)).date()
            today = datetime.now(timezone.utc).date()

            if start > today:
                print(f"[skip] {symbol}: up to date ({max_date})")
                return

            hist = t.history(start=str(start), auto_adjust=False)
        else:
            hist = t.history(period="max", auto_adjust=False)

    elif mode == "backfill":
        hist = t.history(period="max", auto_adjust=False)

    else:
        raise ValueError("mode must be 'daily' or 'backfill'")

    if hist.empty:
        print(f"[skip] {symbol}: no new history")
        return


    hist = hist.reset_index()
    idx_col = None
    for c in ("Date", "Datetime"):
        if c in hist.columns:
            idx_col = c
            break
    if idx_col is None:
        idx_col = hist.columns[0]

    hist["date"] = pd.to_datetime(hist[idx_col], utc=True, errors="coerce").dt.date
    hist = hist.dropna(subset=["date"])
    hist = hist[(hist["date"] >= date(1970, 1, 1)) & (hist["date"] <= date(2149, 6, 6))]

    if hist.empty:
        print(f"[skip] {symbol}: no valid dated rows after cleanup")
        return

    batch_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    rows = []
    for _, r in hist.iterrows():
        rows.append((
            instrument_id,
            r["date"],
            float(r.get("Open", 0.0)),
            float(r.get("High", 0.0)),
            float(r.get("Low", 0.0)),
            float(r.get("Close", 0.0)),
            float(r.get("Adj Close", r.get("Close", 0.0))),
            int(r.get("Volume", 0)) if pd.notna(r.get("Volume", 0)) else 0,
            SOURCE,
            ingested_at,
            batch_id,
        ))

    cols = [
        "instrument_id", "date", "open", "high", "low", "close", "adj_close", "volume",
        "source", "ingested_at", "batch_id"
    ]

    df = pd.DataFrame(rows, columns=cols)

    # Add YYYYMM to align with PARTITION BY toYYYYMM(date)
    df["yyyymm"] = pd.to_datetime(df["date"]).dt.strftime("%Y%m")

    total = 0
    df = df.sort_values(["instrument_id", "date", "ingested_at"])

    for yyyymm, g in df.groupby("yyyymm", sort=True):
        g = g.drop_duplicates(subset=["instrument_id", "date"], keep="last")

        client.insert(
            "market_data.daily_prices",
            list(g[cols].itertuples(index=False, name=None)),
            column_names=cols,
        )
        total += len(g)

    print(f"[ok] {symbol} ({asset_class}) {exchange}/{mic}: +{total} rows (partitioned by month)")

