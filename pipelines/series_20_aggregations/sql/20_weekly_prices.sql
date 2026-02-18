-- 1) Weekly table DDL good, but I would tweak ordering
DROP TABLE IF EXISTS market_data.weekly_prices;
CREATE TABLE market_data.weekly_prices
(
  instrument_id UUID,
  source LowCardinality(String),

  week_start Date,
  week_end   Date,

  open_state      AggregateFunction(argMin, Float64, Date),
  close_state     AggregateFunction(argMax, Float64, Date),
  high_state      AggregateFunction(max, Float64),
  low_state       AggregateFunction(min, Float64),
  adj_close_state AggregateFunction(argMax, Float64, Date),
  volume_state    AggregateFunction(sum, UInt64),

  built_at DateTime64(3, 'UTC')
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(week_start)
ORDER BY (instrument_id, source, week_start);


-- 2) Materialized view — week_end = toMonday(date)+4 is not always true
DROP VIEW IF EXISTS market_data.mv_weekly_prices;
CREATE MATERIALIZED VIEW IF NOT EXISTS market_data.mv_weekly_prices
TO market_data.weekly_prices
AS
SELECT
  instrument_id,
  source,
  toMonday(date) AS week_start,
  max(date)      AS week_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM market_data.daily_prices
GROUP BY
  instrument_id, source, week_start;



-- 3) Backfill INSERT — logic good, but match the MV exactly
INSERT INTO market_data.weekly_prices
SELECT
  instrument_id,
  source,
  toMonday(date) AS week_start,
  max(date)      AS week_end,

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
  instrument_id, source, week_start;


INSERT INTO market_data.weekly_prices
SELECT
  instrument_id,
  source,
  toMonday(date) AS week_start,
  toMonday(date) + 4 AS week_end,

  argMinState(open, date)      AS open_state,
  argMaxState(close, date)     AS close_state,
  maxState(high)               AS high_state,
  minState(low)                AS low_state,
  argMaxState(adj_close, date) AS adj_close_state,
  sumState(volume)             AS volume_state,

  now64(3, 'UTC')              AS built_at
FROM market_data.daily_prices
GROUP BY
  instrument_id, source, week_start, week_end;


-- 4) How to query the finalized weekly OHLC
SELECT
  instrument_id,
  source,
  week_start,
  week_end,
  argMinMerge(open_state)        AS open,
  argMaxMerge(close_state)       AS close,
  maxMerge(high_state)           AS high,
  minMerge(low_state)            AS low,
  argMaxMerge(adj_close_state)   AS adj_close,
  sumMerge(volume_state)         AS volume
FROM market_data.weekly_prices
GROUP BY instrument_id, source, week_start, week_end
ORDER BY instrument_id, week_start;



CREATE OR REPLACE VIEW market_data.v_weekly_prices AS
SELECT
  instrument_id,
  source,
  week_start,
  week_end,
  argMinMerge(open_state)      AS open,
  argMaxMerge(close_state)     AS close,
  maxMerge(high_state)         AS high,
  minMerge(low_state)          AS low,
  argMaxMerge(adj_close_state) AS adj_close,
  sumMerge(volume_state)       AS volume,
  max(built_at)                AS built_at
FROM market_data.weekly_prices
GROUP BY instrument_id, source, week_start, week_end;