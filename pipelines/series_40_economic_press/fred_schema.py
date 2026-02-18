from __future__ import annotations

from pipelines.common.clickhouse_core import get_client, exec_sql

DB = "economic_data"

DDL = f"""
CREATE DATABASE IF NOT EXISTS {DB};

-- Universe: which series IDs do we ingest?
DROP TABLE IF EXISTS {DB}.fred_series_universe;

CREATE TABLE {DB}.fred_series_universe
(
    series_id        String,
    is_active        UInt8 DEFAULT 1,

    macro_series_id  Nullable(UUID),
    priority         UInt8 DEFAULT 5,

    created_at       DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at       DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source           LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (is_active, priority, series_id);

DROP VIEW IF EXISTS {DB}.v_fred_series_universe_current;
CREATE VIEW {DB}.v_fred_series_universe_current AS
SELECT *
FROM {DB}.fred_series_universe
FINAL;

-- Series metadata (versioned by built_at)
DROP TABLE IF EXISTS {DB}.fred_series_meta;

CREATE TABLE {DB}.fred_series_meta
(
    series_id                 String,

    title                     String,
    units                     LowCardinality(String),
    units_short               LowCardinality(String),
    frequency                 LowCardinality(String),
    frequency_short           LowCardinality(String),
    seasonal_adjustment       LowCardinality(String),
    seasonal_adjustment_short LowCardinality(String),

    observation_start         Nullable(Date32),
    observation_end           Nullable(Date32),

    last_updated              DateTime64(3,'UTC'),
    popularity                Int32,
    notes                     String,

    source                    LowCardinality(String) DEFAULT 'fred_api',
    built_at                  DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    batch_id                  String
)
ENGINE = ReplacingMergeTree(built_at)
ORDER BY (series_id, built_at);

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
  max(built_at) AS built_at_max
FROM economic_data.fred_series_meta
GROUP BY series_id;

-- Observations
DROP TABLE IF EXISTS {DB}.fred_observations;

CREATE TABLE {DB}.fred_observations
(
    series_id        String,
    date             Date,

    value            Float64,
    is_missing       UInt8 DEFAULT 0,

    realtime_start   Date,
    realtime_end     Date,

    source           LowCardinality(String) DEFAULT 'fred_api',
    ingested_at      DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    batch_id         String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(date)
ORDER BY (series_id, date, realtime_start, ingested_at);

-- Latest numeric observation per series
DROP VIEW IF EXISTS economic_data.v_fred_latest;

CREATE VIEW economic_data.v_fred_latest AS
SELECT
  series_id,
  argMax(value, tuple(date, ingested_at)) AS value,
  argMax(date,  tuple(date, ingested_at)) AS obs_date,
  max(ingested_at) AS ingested_at_max
FROM economic_data.fred_observations
WHERE is_missing = 0
GROUP BY series_id;

-- Dashboard-friendly enriched latest view
DROP VIEW IF EXISTS economic_data.v_fred_latest_enriched;

CREATE VIEW economic_data.v_fred_latest_enriched AS
SELECT
  m.series_id,
  m.title,
  m.units_short,
  m.frequency_short,
  l.value,
  l.obs_date,
  l.ingested_at_max,
  m.built_at_max
FROM economic_data.v_fred_series_meta_latest m
LEFT JOIN economic_data.v_fred_latest l USING (series_id);
"""


def main():
    client = get_client()
    exec_sql(client, DDL)
    print("[ok] fred provider schema built")


if __name__ == "__main__":
    main()