-- Populates fundamental_data.v_ratios_efficiency

INSERT INTO fundamental_data.financial_ratios
WITH
  now64(3,'UTC') AS built_at_now,
  'derived_yfinance' AS src,
  'manual_sql_efficiency_annual_v2' AS bid,

  f AS
  (
    SELECT
      i.ticker      AS ticker,
      i.fiscal_date AS fiscal_date,
      i.period      AS period,

      /* Revenue */
      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Total Revenue'),
        maxIf(toNullable(i.value), i.metric = 'Operating Revenue')
      ) AS revenue,

      /* COGS */
      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Cost Of Revenue'),
        maxIf(toNullable(i.value), i.metric = 'Reconciled Cost Of Revenue')
      ) AS cogs,

      /* Balance sheet */
      maxIf(toNullable(b.value), b.metric = 'Total Assets') AS total_assets,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Inventory'),
        maxIf(toNullable(b.value), b.metric = 'Inventories')
      ) AS inventory,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Accounts Receivable'),
        maxIf(toNullable(b.value), b.metric = 'Net Receivables')
      ) AS accounts_receivable,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Accounts Payable'),
        maxIf(toNullable(b.value), b.metric = 'Trade Payables')
      ) AS accounts_payable

    FROM fundamental_data.income_statement i
    LEFT JOIN fundamental_data.balance_sheet b
      ON i.ticker=b.ticker
     AND i.fiscal_date=b.fiscal_date
     AND i.period=b.period
    WHERE i.period='annual'
    GROUP BY i.ticker, i.fiscal_date, i.period
  ),

  eff AS
  (
    SELECT
      ticker,
      fiscal_date,
      period,

      if(total_assets > 0 AND revenue IS NOT NULL,
         revenue / total_assets, NULL) AS asset_turnover,

      if(inventory > 0 AND cogs IS NOT NULL,
         cogs / inventory, NULL) AS inventory_turnover,

      if(revenue > 0 AND accounts_receivable IS NOT NULL,
         365 * accounts_receivable / revenue, NULL) AS dso_days,

      if(cogs > 0 AND inventory IS NOT NULL,
         365 * inventory / cogs, NULL) AS dio_days,

      if(cogs > 0 AND accounts_payable IS NOT NULL,
         365 * accounts_payable / cogs, NULL) AS dpo_days
    FROM f
  )

SELECT
  im.instrument_id,
  e.fiscal_date,
  e.period,
  x.1 AS ratio_code,
  x.2 AS value,
  '' AS currency,
  src AS source,
  built_at_now AS built_at,
  bid AS batch_id
FROM eff e
INNER JOIN meta_data.v_instrument_master im
  ON im.symbol = e.ticker
ARRAY JOIN
[
  ('asset_turnover',     e.asset_turnover),
  ('inventory_turnover', e.inventory_turnover),
  ('dso_days',           e.dso_days),
  ('dio_days',           e.dio_days),
  ('dpo_days',           e.dpo_days),
  ('ccc_days',
      if(e.dio_days IS NOT NULL
         AND e.dso_days IS NOT NULL
         AND e.dpo_days IS NOT NULL,
         e.dio_days + e.dso_days - e.dpo_days,
         NULL))
] AS x
WHERE x.2 IS NOT NULL;