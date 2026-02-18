DROP VIEW IF EXISTS {DB}.v_ratios_profitability;
CREATE VIEW {DB}.v_ratios_profitability AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('gross_margin','operating_margin','ebit_margin','net_margin','roe','roa','roic','roce');