-- 1) Monthly table (states)
DROP TABLE IF EXISTS market_data.monthly_prices;
CREATE TABLE market_data.monthly_prices
(
  instrument_id UUID,
  source LowCardinality(String),

  month_start Date,
  month_end   Date,

  open_state      AggregateFunction(argMin, Float64, Date),
  close_state     AggregateFunction(argMax, Float64, Date),
  high_state      AggregateFunction(max, Float64),
  low_state       AggregateFunction(min, Float64),
  adj_close_state AggregateFunction(argMax, Float64, Date),
  volume_state    AggregateFunction(sum, UInt64),

  built_at DateTime64(3, 'UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(month_start)
ORDER BY (instrument_id, source, month_start);


-- 2) Monthly materialized view (keeps it updated going forward)
DROP VIEW IF EXISTS market_data.mv_monthly_prices;
CREATE MATERIALIZED VIEW market_data.mv_monthly_prices
TO market_data.monthly_prices
AS
SELECT
  instrument_id,
  source,
  toStartOfMonth(date) AS month_start,
  max(date)            AS month_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM market_data.daily_prices
GROUP BY
  instrument_id, source, month_start;


-- 3) Backfill (IMPORTANT: do it in time chunks to avoid “too many partitions”)
INSERT INTO market_data.monthly_prices
SELECT
  instrument_id,
  source,
  toStartOfMonth(date) AS month_start,
  max(date)            AS month_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM market_data.daily_prices
WHERE date >= toDate('1970-01-01')
  AND date <  toDate('1971-01-01')
GROUP BY
  instrument_id, source, month_start;




