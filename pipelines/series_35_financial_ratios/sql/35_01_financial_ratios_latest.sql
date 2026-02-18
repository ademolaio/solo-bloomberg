CREATE VIEW {DB}.v_financial_ratios_latest AS
WITH latest_dates AS
(
    SELECT
        instrument_id,
        period,
        ratio_code,
        max(fiscal_date) AS fiscal_date
    FROM {DB}.financial_ratios
    GROUP BY instrument_id, period, ratio_code
)
SELECT
    r.instrument_id,
    r.period,
    r.ratio_code,
    argMax(r.value, r.built_at)  AS value,
    any(ld.fiscal_date)          AS fiscal_date,
    argMax(r.source, r.built_at) AS source,
    max(r.built_at)              AS built_at
FROM {DB}.financial_ratios AS r
INNER JOIN latest_dates AS ld
    ON  r.instrument_id = ld.instrument_id
    AND r.period        = ld.period
    AND r.ratio_code    = ld.ratio_code
    AND r.fiscal_date   = ld.fiscal_date
GROUP BY
    r.instrument_id, r.period, r.ratio_code;