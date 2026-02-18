from __future__ import annotations

from pipelines.common.clickhouse_core import get_client, exec_sql

DDL = """
CREATE DATABASE IF NOT EXISTS meta_data;

-- canonical registry (your internal semantic IDs)
DROP TABLE IF EXISTS meta_data.macro_series_registry;

CREATE TABLE meta_data.macro_series_registry
(
    macro_series_id      UUID DEFAULT generateUUIDv4(),

    canonical_code       LowCardinality(String),
    canonical_name       String,
    description          String,

    region               LowCardinality(String),
    currency             LowCardinality(String),
    unit                 LowCardinality(String),
    scale                Float64 DEFAULT 1.0,
    frequency            LowCardinality(String),
    seasonal_adjustment  LowCardinality(String),
    transformation       LowCardinality(String),
    topic                LowCardinality(String),
    subtopic             LowCardinality(String),

    tags                 Array(LowCardinality(String)),
    is_active            UInt8 DEFAULT 1,

    created_at           DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at           DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source               LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (region, topic, canonical_code, macro_series_id);

DROP VIEW IF EXISTS meta_data.v_macro_series_current;
CREATE VIEW meta_data.v_macro_series_current AS
SELECT *
FROM meta_data.macro_series_registry
FINAL;

-- provider mapping
DROP TABLE IF EXISTS meta_data.macro_series_provider_map;

CREATE TABLE meta_data.macro_series_provider_map
(
    macro_series_id        UUID,

    provider               LowCardinality(String),
    provider_series_id     String,
    provider_series_name   String,
    provider_region        LowCardinality(String),
    provider_unit          LowCardinality(String),
    provider_frequency     LowCardinality(String),

    is_primary             UInt8 DEFAULT 1,
    is_active              UInt8 DEFAULT 1,

    created_at             DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at             DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source                 LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (provider, provider_series_id, macro_series_id);

DROP VIEW IF EXISTS meta_data.v_macro_series_provider_map_current;
CREATE VIEW meta_data.v_macro_series_provider_map_current AS
SELECT *
FROM meta_data.macro_series_provider_map
FINAL;
"""


def main():
    client = get_client()
    exec_sql(client, DDL)
    print("[ok] macro registry schema built")


if __name__ == "__main__":
    main()