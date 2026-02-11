from __future__ import annotations

import pandas as pd


def clean_daily_bars(df: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning for daily bars:
    - drop rows with missing date
    - sort by date
    - enforce non-negative volume
    """
    if df is None or df.empty:
        return df

    out = df.copy()
    out = out.dropna(subset=["date"])
    out = out.sort_values("date")

    # Volume should never be negative
    if "volume" in out.columns:
        out["volume"] = out["volume"].fillna(0).astype("int64")
        out.loc[out["volume"] < 0, "volume"] = 0

    return out