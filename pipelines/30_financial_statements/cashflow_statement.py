# pipelines/30_financial_statements/cashflow_statement.py
from __future__ import annotations

from .financials_core import (
    exec_sql,
    get_client,
    get_universe_symbols,
    backfill_all_symbols,
)

DB = "fundamental_data"
TABLE = f"{DB}.cashflow_statement"
SOURCE = "yfinance"


DDL = f"""
CREATE DATABASE IF NOT EXISTS {DB};

DROP TABLE IF EXISTS {TABLE};

CREATE TABLE {TABLE}
(
    ticker      String,
    fiscal_date Date,
    period      LowCardinality(String),  -- annual | quarterly
    metric      LowCardinality(String),
    value       Float64,
    currency    LowCardinality(String),
    source      LowCardinality(String) default '{SOURCE}',
    loaded_at   DateTime64(3, 'UTC') default now64(3, 'UTC'),
    _batch_id   String
)
ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (ticker, fiscal_date, period, metric)
SETTINGS index_granularity = 8192;
"""


def main(asset_class: str = "equity", is_active: int = 1):
    client = get_client()
    exec_sql(client, DDL)

    symbols = get_universe_symbols(client, asset_class=asset_class, is_active=is_active)

    backfill_all_symbols(client, table_fq=TABLE, statement="cashflow_statement", period="annual", symbols=symbols, source=SOURCE)
    backfill_all_symbols(client, table_fq=TABLE, statement="cashflow_statement", period="quarterly", symbols=symbols, source=SOURCE)

    print("[ok] cashflow_statement: DDL + backfill completed")


if __name__ == "__main__":
    main()