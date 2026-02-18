from __future__ import annotations

from .aggregation_core import (
    exec_sql,
    get_client,
    year_ranges,
    backfill_by_ranges,
)

DB = "market_data"
SOURCE_FILTER = ""  # optionally: "AND source = 'yfinance'"


DDL = f"""
DROP TABLE IF EXISTS {DB}.quarterly_prices;

CREATE TABLE {DB}.quarterly_prices
(
  instrument_id UUID,
  source LowCardinality(String),

  quarter_start Date,
  quarter_end   Date,

  open_state      AggregateFunction(argMin, Float64, Date),
  close_state     AggregateFunction(argMax, Float64, Date),
  high_state      AggregateFunction(max, Float64),
  low_state       AggregateFunction(min, Float64),
  adj_close_state AggregateFunction(argMax, Float64, Date),
  volume_state    AggregateFunction(sum, UInt64),

  built_at DateTime64(3, 'UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(quarter_start)
ORDER BY (instrument_id, source, quarter_start);

DROP VIEW IF EXISTS {DB}.mv_quarterly_prices;

CREATE MATERIALIZED VIEW {DB}.mv_quarterly_prices
TO {DB}.quarterly_prices
AS
SELECT
  instrument_id,
  source,
  toStartOfQuarter(date) AS quarter_start,
  max(date)              AS quarter_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM {DB}.daily_prices
GROUP BY instrument_id, source, quarter_start;

CREATE OR REPLACE VIEW {DB}.v_quarterly_prices AS
SELECT
  instrument_id,
  source,
  quarter_start,
  quarter_end,
  argMinMerge(open_state)      AS open,
  argMaxMerge(close_state)     AS close,
  maxMerge(high_state)         AS high,
  minMerge(low_state)          AS low,
  argMaxMerge(adj_close_state) AS adj_close,
  sumMerge(volume_state)       AS volume,
  max(built_at)                AS built_at
FROM {DB}.quarterly_prices
GROUP BY instrument_id, source, quarter_start, quarter_end;
"""


BACKFILL_TEMPLATE = f"""
INSERT INTO {DB}.quarterly_prices
SELECT
  instrument_id,
  source,
  toStartOfQuarter(date) AS quarter_start,
  max(date)              AS quarter_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM {DB}.daily_prices
WHERE date >= toDate('{{start}}')
  AND date <  toDate('{{end}}')
  {SOURCE_FILTER}
GROUP BY instrument_id, source, quarter_start;
"""


def main(min_year: int = 1970, max_year: int = 2026):
    client = get_client()
    exec_sql(client, DDL)

    # Backfill year-by-year to avoid "too many partitions per insert block"
    backfill_by_ranges(
        client,
        insert_sql_template=BACKFILL_TEMPLATE,
        ranges=year_ranges(min_year, max_year),
        label="quarterly",
    )

    print("[ok] quarterly: DDL + MV + backfill completed")


if __name__ == "__main__":
    main()