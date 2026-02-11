from __future__ import annotations

import pandas as pd
import yfinance as yf


def _safe_short_name(t: yf.Ticker, symbol: str) -> str:
    try:
        info = getattr(t, "info", None)
        if isinstance(info, dict):
            for k in ("shortName", "short_name", "name", "displayName", "longName"):
                v = info.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
    except Exception:
        pass
    return symbol


def fetch_daily_history_max(symbol: str) -> tuple[str, pd.DataFrame]:
    """
    Fetch max-available daily OHLCV for `symbol` via yfinance.

    Returns:
      (short_name, df) where df has columns:
        date, open, high, low, close, adj_close, volume
    """
    t = yf.Ticker(symbol)
    short_name = _safe_short_name(t, symbol)

    df = t.history(
        period="max",
        interval="1d",
        auto_adjust=False,
        actions=False,
    )

    if df is None or df.empty:
        empty = pd.DataFrame(columns=["date", "open", "high", "low", "close", "adj_close", "volume"])
        return short_name, empty

    df = df.reset_index()

    date_col = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else None)
    if not date_col:
        raise ValueError(f"{symbol}: missing Date/Datetime column from yfinance history() output")

    df.rename(
        columns={
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        },
        inplace=True,
    )

    if "adj_close" not in df.columns:
        df["adj_close"] = df["close"]

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["volume"] = df["volume"].fillna(0).astype("int64")

    df = df[["date", "open", "high", "low", "close", "adj_close", "volume"]]
    return short_name, df