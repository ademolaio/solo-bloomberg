DROP TABLE IF EXISTS fundamental_data.cashflow_statements;

CREATE TABLE IF NOT EXISTS fundamental_data.cashflow_statements
(
    instrument_id      UUID,
    source             LowCardinality(String),

    fiscal_period_end  Date,
    period_type        LowCardinality(String),   -- 'annual' | 'quarterly' | 'ttm'
    fiscal_year        UInt16,
    fiscal_quarter     UInt8,                    -- 1..4, 0 if not applicable

    metric             LowCardinality(String),   -- e.g. OperatingCashFlow, CapEx, FreeCashFlow
    value_decimal      Decimal(38, 6),
    value_float        Float64,
    currency           LowCardinality(String),

    ingested_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    batch_id           UUID DEFAULT generateUUIDv4()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(fiscal_period_end)
ORDER BY
(
    instrument_id,
    fiscal_period_end,
    period_type,
    fiscal_year,
    fiscal_quarter,
    metric,
    source
)
SETTINGS index_granularity = 8192;

DROP VIEW IF EXISTS fundamental_data.v_cashflow_statement_current;

CREATE VIEW fundamental_data.v_cashflow_statement_current AS
SELECT *
FROM fundamental_data.cashflow_statement
FINAL;