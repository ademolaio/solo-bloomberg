DROP TABLE IF EXISTS economic_data.fred_observations;

CREATE TABLE economic_data.fred_observations
(
    series_id        String,
    date             Date,

    value            Float64,                    -- numeric value
    is_missing       UInt8 DEFAULT 0,           -- 1 if "." or missing from API
    realtime_start   Date,
    realtime_end     Date,

    source           LowCardinality(String) DEFAULT 'fred_api',
    ingested_at      DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    batch_id         String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(date)
ORDER BY (series_id, date, realtime_start, ingested_at)
SETTINGS index_granularity = 8192;

-- Latest-value view per series (Metabase-friendly)
DROP VIEW IF EXISTS economic_data.v_fred_latest;

CREATE VIEW economic_data.v_fred_latest AS
SELECT
    series_id,
    argMax(value, (date, ingested_at)) AS value,
    argMax(date,  (date, ingested_at)) AS date,
    max(ingested_at) AS ingested_at
FROM economic_data.fred_observations
WHERE is_missing = 0
GROUP BY series_id;