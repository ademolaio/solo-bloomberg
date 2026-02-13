DROP VIEW IF EXISTS {DB}.v_ratios_leverage;
CREATE VIEW {DB}.v_ratios_leverage AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('debt_to_equity','net_debt_to_ebitda','debt_to_assets','interest_coverage');
