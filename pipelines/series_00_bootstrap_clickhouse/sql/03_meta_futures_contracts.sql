-- 3) Futures contracts subtype table
DROP TABLE IF EXISTS meta_data.futures_contracts;
CREATE TABLE IF NOT EXISTS meta_data.futures_contracts
(
    instrument_id     UUID,

    root_symbol       LowCardinality(String),   -- ES, CL, FDAX, etc.
    expiration_date   Date,
    contract_month    LowCardinality(String),   -- your convention (e.g., H6, M6, Z5)
    multiplier        Float64,

    currency          LowCardinality(String),

    updated_at        DateTime64(3) DEFAULT now64(3),
    source            LowCardinality(String)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (root_symbol, expiration_date, instrument_id);