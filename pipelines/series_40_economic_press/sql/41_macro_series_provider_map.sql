DROP TABLE IF EXISTS meta_data.macro_series_provider_map;

CREATE TABLE meta_data.macro_series_provider_map
(
    macro_series_id        UUID,

    provider               LowCardinality(String),     -- "fred", "ecb", "imf", ...
    provider_series_id     String,                     -- e.g. "CPIAUCSL"
    provider_series_name   String,
    provider_region        LowCardinality(String),
    provider_unit          LowCardinality(String),
    provider_frequency     LowCardinality(String),

    is_primary             UInt8 DEFAULT 1,            -- if multiple providers represent same concept
    is_active              UInt8 DEFAULT 1,

    created_at             DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at             DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source                 LowCardinality(String)      -- "manual", "fred_discovery", etc.
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (provider, provider_series_id, macro_series_id)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS meta_data.v_macro_series_provider_map_current;

CREATE VIEW meta_data.v_macro_series_provider_map_current AS
SELECT *
FROM meta_data.macro_series_provider_map
FINAL;