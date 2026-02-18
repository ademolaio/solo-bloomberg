DROP TABLE IF EXISTS economic_data.fred_series_meta;

CREATE TABLE economic_data.fred_series_meta
(
    series_id              String,

    title                  String,
    units                  LowCardinality(String),
    units_short            LowCardinality(String),
    frequency              LowCardinality(String),
    frequency_short        LowCardinality(String),
    seasonal_adjustment    LowCardinality(String),
    seasonal_adjustment_short LowCardinality(String),

    observation_start      Date,
    observation_end        Date,

    last_updated           DateTime64(3,'UTC'),
    popularity             Int32,
    notes                  String,

    source                 LowCardinality(String) DEFAULT 'fred_api',
    built_at               DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    batch_id               String
)
ENGINE = ReplacingMergeTree(built_at)
ORDER BY (series_id, built_at)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS economic_data.v_fred_series_meta_latest;

CREATE VIEW economic_data.v_fred_series_meta_latest AS
SELECT
    series_id,
    argMax(title, built_at) AS title,
    argMax(units, built_at) AS units,
    argMax(units_short, built_at) AS units_short,
    argMax(frequency, built_at) AS frequency,
    argMax(frequency_short, built_at) AS frequency_short,
    argMax(seasonal_adjustment, built_at) AS seasonal_adjustment,
    argMax(seasonal_adjustment_short, built_at) AS seasonal_adjustment_short,
    argMax(observation_start, built_at) AS observation_start,
    argMax(observation_end, built_at) AS observation_end,
    argMax(last_updated, built_at) AS last_updated,
    argMax(popularity, built_at) AS popularity,
    argMax(notes, built_at) AS notes,
    argMax(source, built_at) AS source,
    max(built_at) AS built_at
FROM economic_data.fred_series_meta
GROUP BY series_id;