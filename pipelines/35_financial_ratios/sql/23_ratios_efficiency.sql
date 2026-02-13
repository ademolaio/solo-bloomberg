DROP VIEW IF EXISTS {DB}.v_ratios_efficiency;
CREATE VIEW {DB}.v_ratios_efficiency AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('asset_turnover','inventory_turnover','dso_days','dio_days','dpo_days','ccc_days');