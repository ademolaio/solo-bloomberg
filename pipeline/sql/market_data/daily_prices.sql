CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.daily_prices
(
    ticker        LowCardinality(String),
    short_name    LowCardinality(String),

    date          Date,

    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    adj_close     Float64,
    volume        UInt64,

    ingested_at   DateTime64(3),
    batch_id      String,
    source        LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (ticker, date, source)
SETTINGS index_granularity = 8192;