-- 4) Futures continuous series subtype table (synthetic instruments)
DROP TABLE IF EXISTS meta_data.futures_continuous;
CREATE TABLE IF NOT EXISTS meta_data.futures_continuous
(
    instrument_id     UUID,

    root_symbol       LowCardinality(String),   -- ES, CL, FDAX, etc.
    roll_rule         LowCardinality(String),   -- e.g. "front_month", "volume_roll", "time_roll"
    roll_offset_days  Int16,                    -- e.g. 0, 2, 5
    price_adjustment  LowCardinality(String),   -- "none", "back_adjusted", "ratio_adjusted"

    currency          LowCardinality(String),

    updated_at        DateTime64(3) DEFAULT now64(3),
    source            LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (root_symbol, roll_rule, roll_offset_days, price_adjustment, instrument_id);