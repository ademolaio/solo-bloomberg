DROP TABLE IF EXISTS {DB}.ratios_latest_snapshot;

CREATE TABLE {DB}.ratios_latest_snapshot
(
    instrument_id UUID,
    period        LowCardinality(String),

    gross_margin Float64,
    operating_margin Float64,
    ebit_margin Float64,
    net_margin Float64,

    roe Float64,
    roa Float64,
    roic Float64,
    roce Float64,

    current_ratio Float64,
    quick_ratio Float64,
    cash_ratio Float64,

    debt_to_equity Float64,
    net_debt_to_ebitda Float64,
    debt_to_assets Float64,
    interest_coverage Float64,

    asset_turnover Float64,
    inventory_turnover Float64,
    dso_days Float64,
    dio_days Float64,
    dpo_days Float64,
    ccc_days Float64,

    fcf_margin Float64,
    ocf_to_net_income Float64,
    capex_to_revenue Float64,
    fcf_to_net_income Float64,

    max_fiscal_date Date,
    built_at DateTime64(3,'UTC')
)
ENGINE = ReplacingMergeTree(built_at)
ORDER BY (instrument_id, period);