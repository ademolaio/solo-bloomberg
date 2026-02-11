from __future__ import annotations

import datetime as dt
import pandas as pd
import clickhouse_connect

from pipeline.settings import settings


def get_ch_client():
    return clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_HTTP_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database="market_data",
    )


def insert_daily_prices(
    *,
    symbol: str,
    short_name: str,
    bars: pd.DataFrame,
    batch_id: str,
    source: str = "yfinance",
) -> int:
    """
    Insert normalized daily bars into market_data.daily_prices.
    """
    if bars is None or bars.empty:
        return 0

    df = bars.copy()
    df["ticker"] = symbol
    df["short_name"] = short_name
    df["ingested_at"] = dt.datetime.utcnow()
    df["batch_id"] = batch_id
    df["source"] = source

    df = df[
        [
            "ticker",
            "short_name",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "ingested_at",
            "batch_id",
            "source",
        ]
    ]

    client = get_ch_client()
    client.insert_df("daily_prices", df)
    return len(df)