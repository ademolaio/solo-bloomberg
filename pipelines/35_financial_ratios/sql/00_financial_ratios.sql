CREATE DATABASE IF NOT EXISTS {DB};

DROP TABLE IF EXISTS {DB}.financial_ratios;

CREATE TABLE {DB}.financial_ratios
(
    instrument_id UUID,
    fiscal_date   Date,
    period        LowCardinality(String),          -- annual | quarterly | ttm

    ratio_code    LowCardinality(String),          -- e.g. roe, gross_margin
    value         Float64,                         -- raw numeric (fractions for margins/returns)
    currency      LowCardinality(String),          -- usually '' for ratios

    source        LowCardinality(String) DEFAULT '{SOURCE_DEFAULT}',
    built_at      DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    batch_id      String
)
ENGINE = ReplacingMergeTree(built_at)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (instrument_id, period, fiscal_date, ratio_code, source)
SETTINGS index_granularity = 8192;