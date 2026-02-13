DROP VIEW IF EXISTS fundamental_data.v_ratios_latest_wide;

CREATE VIEW fundamental_data.v_ratios_latest_wide AS
SELECT
    im.instrument_id AS instrument_id,
    im.symbol        AS symbol,
    im.short_name    AS short_name,
    r.period         AS period,

    maxIf(r.value, r.ratio_code = 'gross_margin')        AS gross_margin,
    maxIf(r.value, r.ratio_code = 'operating_margin')    AS operating_margin,
    maxIf(r.value, r.ratio_code = 'ebit_margin')         AS ebit_margin,
    maxIf(r.value, r.ratio_code = 'net_margin')          AS net_margin,

    maxIf(r.value, r.ratio_code = 'roe')                 AS roe,
    maxIf(r.value, r.ratio_code = 'roa')                 AS roa,
    maxIf(r.value, r.ratio_code = 'roic')                AS roic,
    maxIf(r.value, r.ratio_code = 'roce')                AS roce,

    maxIf(r.value, r.ratio_code = 'current_ratio')       AS current_ratio,
    maxIf(r.value, r.ratio_code = 'quick_ratio')         AS quick_ratio,
    maxIf(r.value, r.ratio_code = 'cash_ratio')          AS cash_ratio,

    maxIf(r.value, r.ratio_code = 'debt_to_equity')      AS debt_to_equity,
    maxIf(r.value, r.ratio_code = 'net_debt_to_ebitda')  AS net_debt_to_ebitda,
    maxIf(r.value, r.ratio_code = 'debt_to_assets')      AS debt_to_assets,
    maxIf(r.value, r.ratio_code = 'interest_coverage')   AS interest_coverage,

    maxIf(r.value, r.ratio_code = 'asset_turnover')      AS asset_turnover,
    maxIf(r.value, r.ratio_code = 'inventory_turnover')  AS inventory_turnover,
    maxIf(r.value, r.ratio_code = 'dso_days')            AS dso_days,
    maxIf(r.value, r.ratio_code = 'dio_days')            AS dio_days,
    maxIf(r.value, r.ratio_code = 'dpo_days')            AS dpo_days,
    maxIf(r.value, r.ratio_code = 'ccc_days')            AS ccc_days,

    maxIf(r.value, r.ratio_code = 'fcf_margin')          AS fcf_margin,
    maxIf(r.value, r.ratio_code = 'ocf_to_net_income')   AS ocf_to_net_income,
    maxIf(r.value, r.ratio_code = 'capex_to_revenue')    AS capex_to_revenue,
    maxIf(r.value, r.ratio_code = 'fcf_to_net_income')   AS fcf_to_net_income,

    max(r.fiscal_date) AS max_fiscal_date,
    max(r.built_at)    AS built_at
FROM fundamental_data.v_financial_ratios_latest AS r
LEFT JOIN meta_data.v_instrument_master AS im
    ON r.instrument_id = im.instrument_id
GROUP BY
    im.instrument_id, im.symbol, im.short_name, r.period;
