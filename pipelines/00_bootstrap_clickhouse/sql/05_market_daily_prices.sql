-- 5) Market data fact table (daily bars)
DROP TABLE IF EXISTS market_data.daily_prices;
CREATE TABLE IF NOT EXISTS market_data.daily_prices
(
    instrument_id  UUID,
    date           Date,

    open           Float64,
    high           Float64,
    low            Float64,
    close          Float64,
    adj_close      Float64,
    volume         UInt64,

    source         LowCardinality(String),
    ingested_at    DateTime64(3, 'UTC'),
    batch_id       String
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(date)
ORDER BY (instrument_id, date, source)
SETTINGS index_granularity = 8192;