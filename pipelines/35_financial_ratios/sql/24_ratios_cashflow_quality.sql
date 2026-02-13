DROP VIEW IF EXISTS {DB}.v_ratios_cashflow_quality;
CREATE VIEW {DB}.v_ratios_cashflow_quality AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('fcf_margin','ocf_to_net_income','capex_to_revenue','fcf_to_net_income');
