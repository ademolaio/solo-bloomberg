DROP VIEW IF EXISTS {DB}.v_ratios_latest_snapshot_source;

CREATE VIEW {DB}.v_ratios_latest_snapshot_source AS
SELECT
    instrument_id,
    period,

    gross_margin,
    operating_margin,
    ebit_margin,
    net_margin,

    roe,
    roa,
    roic,
    roce,

    current_ratio,
    quick_ratio,
    cash_ratio,

    debt_to_equity,
    net_debt_to_ebitda,
    debt_to_assets,
    interest_coverage,

    asset_turnover,
    inventory_turnover,
    dso_days,
    dio_days,
    dpo_days,
    ccc_days,

    fcf_margin,
    ocf_to_net_income,
    capex_to_revenue,
    fcf_to_net_income,

    max_fiscal_date,
    built_at
FROM {DB}.v_ratios_latest_wide;
