CREATE DATABASE IF NOT EXISTS meta_data;

DROP TABLE IF EXISTS meta_data.macro_series_registry;

CREATE TABLE meta_data.macro_series_registry
(
    macro_series_id      UUID DEFAULT generateUUIDv4(),

    -- Canonical identity
    canonical_code       LowCardinality(String),   -- e.g. "US_CPI_HEADLINE_SA"
    canonical_name       String,                   -- e.g. "US CPI (Headline), Seasonally Adjusted"
    description          String,

    -- Semantics
    region               LowCardinality(String),   -- e.g. "US", "EU", "DE"
    currency             LowCardinality(String),   -- e.g. "USD", "" if N/A
    unit                 LowCardinality(String),   -- e.g. "Index", "%", "USD", "bps"
    scale                Float64 DEFAULT 1.0,      -- optional: multiply raw provider value by scale
    frequency            LowCardinality(String),   -- e.g. "D", "W", "M", "Q", "A"
    seasonal_adjustment  LowCardinality(String),   -- e.g. "SA", "NSA", "WDA", ""
    transformation       LowCardinality(String),   -- e.g. "level", "yoy", "mom", "log", "diff"
    topic                LowCardinality(String),   -- e.g. "inflation", "rates", "growth", "labor"
    subtopic             LowCardinality(String),   -- e.g. "cpi", "pce", "policy_rate"

    -- Governance / tags
    tags                 Array(LowCardinality(String)),  -- ["headline","basket","consumption"]
    is_active            UInt8 DEFAULT 1,

    -- Provenance
    created_at           DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    updated_at           DateTime64(3,'UTC') DEFAULT now64(3,'UTC'),
    source               LowCardinality(String)          -- "manual", "import", etc.
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (region, topic, canonical_code, macro_series_id)
SETTINGS index_granularity = 8192;

-- Convenient current view
DROP VIEW IF EXISTS meta_data.v_macro_series_current;

CREATE VIEW meta_data.v_macro_series_current AS
SELECT *
FROM meta_data.macro_series_registry
FINAL;