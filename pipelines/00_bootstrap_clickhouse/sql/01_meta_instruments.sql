-- 1) Global instrument registry
DROP TABLE IF EXISTS meta_data.instruments;
CREATE TABLE IF NOT EXISTS meta_data.instruments
(
    instrument_id   UUID DEFAULT generateUUIDv4(),

    asset_class     LowCardinality(String),     -- equity, etf, future, fx, index, option (later)
    symbol          String,                     -- canonical display symbol (AAPL, SAP.DE, ESZ5, etc.)
    mic             LowCardinality(String),     -- market identifier code (or placeholder early)
    exchange        LowCardinality(String),     -- human-readable exchange name/code

    short_name      String,

    is_active       UInt8 DEFAULT 1,

    created_at      DateTime64(3) DEFAULT now64(3),
    updated_at      DateTime64(3) DEFAULT now64(3),
    source          LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (asset_class, mic, symbol, instrument_id);