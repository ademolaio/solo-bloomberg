DROP VIEW IF EXISTS {DB}.v_ratios_liquidity;
CREATE VIEW {DB}.v_ratios_liquidity AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('current_ratio','quick_ratio','cash_ratio');