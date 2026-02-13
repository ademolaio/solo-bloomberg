from __future__ import annotations

from pipelines.common.clickhouse_core import get_client, exec_sql

DB = "fundamental_data"
SOURCE_DEFAULT = "derived_yfinance"

DDL = f"""
CREATE DATABASE IF NOT EXISTS {DB};

DROP TABLE IF EXISTS {DB}.financial_ratios;

CREATE TABLE {DB}.financial_ratios
(
    instrument_id UUID,
    fiscal_date   Date,
    period        LowCardinality(String),          -- annual | quarterly | ttm

    ratio_code    LowCardinality(String),          -- e.g. roe, gross_margin
    value         Float64,                         -- raw numeric (fractions for margins/returns)
    currency      LowCardinality(String),          -- usually '' for ratios

    source        LowCardinality(String) DEFAULT '{SOURCE_DEFAULT}',
    built_at      DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    batch_id      String
)
ENGINE = ReplacingMergeTree(built_at)
PARTITION BY toYYYYMM(fiscal_date)
ORDER BY (instrument_id, period, fiscal_date, ratio_code, source)
SETTINGS index_granularity = 8192;


DROP VIEW IF EXISTS {DB}.v_financial_ratios_latest;

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


DROP VIEW IF EXISTS {DB}.v_ratios_profitability;
CREATE VIEW {DB}.v_ratios_profitability AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('gross_margin','operating_margin','ebit_margin','net_margin','roe','roa','roic','roce');

DROP VIEW IF EXISTS {DB}.v_ratios_liquidity;
CREATE VIEW {DB}.v_ratios_liquidity AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('current_ratio','quick_ratio','cash_ratio');

DROP VIEW IF EXISTS {DB}.v_ratios_leverage;
CREATE VIEW {DB}.v_ratios_leverage AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('debt_to_equity','net_debt_to_ebitda','debt_to_assets','interest_coverage');

DROP VIEW IF EXISTS {DB}.v_ratios_efficiency;
CREATE VIEW {DB}.v_ratios_efficiency AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('asset_turnover','inventory_turnover','dso_days','dio_days','dpo_days','ccc_days');

DROP VIEW IF EXISTS {DB}.v_ratios_cashflow_quality;
CREATE VIEW {DB}.v_ratios_cashflow_quality AS
SELECT *
FROM {DB}.v_financial_ratios_latest
WHERE ratio_code IN ('fcf_margin','ocf_to_net_income','capex_to_revenue','fcf_to_net_income');


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

-- Optional helper view to (re)fill the snapshot table:
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


INSERT INTO {DB}.financial_ratios
SELECT
  im.instrument_id,
  f.fiscal_date,
  f.period,
  x.1 AS ratio_code,
  x.2 AS value,
  ''  AS currency,
  'derived_yfinance' AS source,
  now64(3,'UTC') AS built_at,
  'manual_sql'  AS batch_id
FROM
(
  SELECT
    i.ticker,
    i.fiscal_date,
    i.period,

    maxIf(toNullable(i.value), i.metric = 'Total Revenue') AS revenue,
    maxIf(toNullable(i.value), i.metric = 'Net Income')    AS net_income,

    maxIf(toNullable(b.value), b.metric = 'Total Assets')  AS total_assets,

    coalesce(
      maxIf(toNullable(b.value), b.metric = 'Stockholders Equity'),
      maxIf(toNullable(b.value), b.metric = 'Stockholders Equity Attributable to Parent'),
      maxIf(toNullable(b.value), b.metric = 'Common Stock Equity'),
      maxIf(toNullable(b.value), b.metric = 'Total Equity Gross Minority Interest')
    ) AS equity
  FROM {DB}.income_statement i
  LEFT JOIN {DB}.balance_sheet b
    ON i.ticker = b.ticker
   AND i.fiscal_date = b.fiscal_date
   AND i.period = b.period
  WHERE i.period = 'annual'
  GROUP BY i.ticker, i.fiscal_date, i.period
) AS f
INNER JOIN meta_data.v_instrument_master AS im
  ON im.symbol = f.ticker
ARRAY JOIN
  [
    ('net_margin', if(f.revenue > 0,      f.net_income / f.revenue,      NULL)),
    ('roa',        if(f.total_assets > 0, f.net_income / f.total_assets, NULL)),
    ('roe',        if(f.equity > 0,       f.net_income / f.equity,       NULL))
  ] AS x
WHERE x.2 IS NOT NULL;
"""


SNAPSHOT_REFRESH_SQL = f"""
-- Rebuild snapshot (idempotent "latest wins" via ReplacingMergeTree(built_at))
INSERT INTO {DB}.ratios_latest_snapshot
SELECT *
FROM {DB}.v_ratios_latest_snapshot_source;
"""


def main(refresh_snapshot: bool = True):
    client = get_client()
    exec_sql(client, DDL)

    if refresh_snapshot:
        client.command(SNAPSHOT_REFRESH_SQL)

    print("[ok] financial_ratios schema/views built"
          + (" + snapshot refreshed" if refresh_snapshot else ""))


if __name__ == "__main__":
    main()