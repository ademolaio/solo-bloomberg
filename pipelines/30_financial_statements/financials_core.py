# pipelines/30_financial_statements/financials_core.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Literal, Iterable

import clickhouse_connect
import pandas as pd
import yfinance as yf


StatementKind = Literal["income_statement", "balance_sheet", "cashflow_statement"]
PeriodKind = Literal["annual", "quarterly"]


@dataclass(frozen=True)
class CHConfig:
    host: str = "clickhouse"
    port: int = 8123
    username: str = "default"
    password: str = "default"
    database: Optional[str] = None  # keep None; use explicit DB.table


def get_client(cfg: CHConfig | None = None):
    cfg = cfg or CHConfig()
    return clickhouse_connect.get_client(
        host=cfg.host,
        port=cfg.port,
        username=cfg.username,
        password=cfg.password,
        database=cfg.database,
    )


def exec_sql(client, sql: str):
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        client.command(stmt)


# ---------------------------
# Universe (Mode A)
# ---------------------------

def get_universe_symbols(
    client,
    *,
    asset_class: str = "equity",
    is_active: int = 1,
) -> list[str]:
    rows = client.query(
        """
        SELECT symbol
        FROM meta_data.v_instruments_current
        WHERE asset_class = %(asset_class)s
          AND is_active = %(is_active)s
        ORDER BY symbol
        """,
        parameters={"asset_class": asset_class, "is_active": is_active},
    ).result_rows
    return [r[0] for r in rows]


# ---------------------------
# YFinance extract helpers
# ---------------------------

def _yf_df_for(statement: StatementKind, period: PeriodKind, t: yf.Ticker) -> pd.DataFrame:
    if statement == "income_statement":
        return t.financials if period == "annual" else t.quarterly_financials
    if statement == "balance_sheet":
        return t.balance_sheet if period == "annual" else t.quarterly_balance_sheet
    if statement == "cashflow_statement":
        return t.cashflow if period == "annual" else t.quarterly_cashflow
    raise ValueError(f"Unknown statement={statement}")


def fetch_statement(symbol: str, statement: StatementKind, period: PeriodKind) -> pd.DataFrame:
    t = yf.Ticker(symbol)
    try:
        df = _yf_df_for(statement, period, t)

        # Coerce Series -> DataFrame (yfinance sometimes returns Series)
        if isinstance(df, pd.Series):
            df = df.to_frame()

        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            return pd.DataFrame()

        return df
    except Exception:
        return pd.DataFrame()


def uuid4_str() -> str:
    import uuid
    return str(uuid.uuid4())


def wide_to_long(
    df_wide: pd.DataFrame,
    *,
    symbol: str,
    period: PeriodKind,
    currency: str = "",
    source: str = "yfinance",
    loaded_at: datetime | None = None,
    batch_id: str | None = None,
) -> pd.DataFrame:
    if df_wide is None or df_wide.empty:
        return pd.DataFrame()

    # Coerce Series -> DataFrame defensively (again)
    if isinstance(df_wide, pd.Series):
        df_wide = df_wide.to_frame()

    if not isinstance(df_wide, pd.DataFrame):
        return pd.DataFrame()

    loaded_at = loaded_at or datetime.now(timezone.utc)
    batch_id = batch_id or uuid4_str()

    df = df_wide.copy()

    # Ensure metric names are strings
    df.index = df.index.astype(str)

    # Stack: (metric x fiscal_date) -> rows
    long = (
        df.stack()  # default behavior drops NaNs; fine for storage sanity
          .reset_index()
          .rename(columns={"level_0": "metric", "level_1": "fiscal_date", 0: "value"})
    )

    # Normalize fiscal_date and value
    long["fiscal_date"] = pd.to_datetime(long["fiscal_date"], errors="coerce").dt.date
    long["value"] = pd.to_numeric(long["value"], errors="coerce")

    long = long.dropna(subset=["fiscal_date", "metric", "value"])
    if long.empty:
        return pd.DataFrame()

    out = pd.DataFrame({
        "ticker": symbol,
        "fiscal_date": long["fiscal_date"],
        "period": period,
        "metric": long["metric"].astype(str),
        "value": long["value"].astype("float64"),
        "currency": currency or "",
        "source": source,
        "loaded_at": loaded_at,
        "_batch_id": batch_id,
    })

    # de-dupe within batch
    out = out.sort_values(["ticker", "fiscal_date", "period", "metric", "loaded_at"])
    out = out.drop_duplicates(subset=["ticker", "fiscal_date", "period", "metric"], keep="last")
    return out


def insert_long_rows(client, *, table_fq: str, df_long: pd.DataFrame) -> int:
    if df_long is None or df_long.empty:
        return 0

    cols = ["ticker", "fiscal_date", "period", "metric", "value", "currency", "source", "loaded_at", "_batch_id"]
    client.insert(
        table_fq,
        list(df_long[cols].itertuples(index=False, name=None)),
        column_names=cols,
    )
    return len(df_long)


def ingest_one_symbol(
    client,
    *,
    table_fq: str,
    symbol: str,
    statement: StatementKind,
    period: PeriodKind,
    source: str = "yfinance",
) -> int:
    df_wide = fetch_statement(symbol, statement, period)
    if df_wide.empty:
        print(f"[skip] {symbol} {statement}/{period}: empty")
        return 0

    df_long = wide_to_long(
        df_wide,
        symbol=symbol,
        period=period,
        currency="",          # optionally fetch from yf info later
        source=source,
        loaded_at=datetime.now(timezone.utc),
        batch_id=uuid4_str(),
    )

    if df_long.empty:
        print(f"[skip] {symbol} {statement}/{period}: no rows after normalize")
        return 0

    n = insert_long_rows(client, table_fq=table_fq, df_long=df_long)
    print(f"[ok] {symbol} {statement}/{period}: +{n} rows")
    return n


def backfill_all_symbols(
    client,
    *,
    table_fq: str,
    statement: StatementKind,
    period: PeriodKind,
    symbols: list[str],
    source: str = "yfinance",
) -> int:
    total = 0
    for s in symbols:
        total += ingest_one_symbol(
            client,
            table_fq=table_fq,
            symbol=s,
            statement=statement,
            period=period,
            source=source,
        )
    return total