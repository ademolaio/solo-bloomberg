CREATE DATABASE IF NOT EXISTS economic_data;

DROP TABLE IF EXISTS economic_data.fred_series_universe;

CREATE TABLE economic_data.fred_series_universe
(
    series_id        String,                     -- FRED series id (CPIAUCSL)
    is_active        UInt8 DEFAULT 1,

    -- Optional routing
    macro_series_id  UUID,                       -- when you’ve mapped it (nullable ok)
    priority         UInt8 DEFAULT 5,            -- 1 highest, 9 lowest

    created_at       DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at       DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source           LowCardinality(String)      -- "manual", "discovery_seed", etc.
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (is_active, priority, series_id)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS economic_data.v_fred_series_universe_current;

CREATE VIEW economic_data.v_fred_series_universe_current AS
SELECT *
FROM economic_data.fred_series_universe
FINAL;