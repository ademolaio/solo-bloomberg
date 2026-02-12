-- 2) Equity + ETF subtype table
DROP TABLE IF EXISTS meta_data.equities_etfs;
CREATE TABLE IF NOT EXISTS meta_data.equities_etfs
(
    instrument_id   UUID,

    isin            LowCardinality(String),
    figi            LowCardinality(String),

    currency        LowCardinality(String),
    country         LowCardinality(String),
    sector          LowCardinality(String),
    industry        LowCardinality(String),

    updated_at      DateTime64(3) DEFAULT now64(3),
    source          LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (instrument_id);