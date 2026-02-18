/* Populates these Views
-- fundamental_data.v_financial_ratios_latest
-- fundamental_data.v_ratios_profitability
-- fundamental_data.v_ratios_liquidity
-- fundamental_data.v_ratios_leverage
*/



INSERT INTO fundamental_data.financial_ratios
WITH
  now64(3,'UTC') AS built_at_now,
  'derived_yfinance' AS src,
  'manual_sql_core_ratios_annual' AS bid,

  f AS
  (
    SELECT
      i.ticker      AS ticker,
      i.fiscal_date AS fiscal_date,
      i.period      AS period,

      /* Income Statement */
      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Total Revenue'),
        maxIf(toNullable(i.value), i.metric = 'Operating Revenue')
      ) AS revenue,

      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Gross Profit'),
        maxIf(toNullable(i.value), i.metric = 'Gross Profit (Loss)')
      ) AS gross_profit,

      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Operating Income'),
        maxIf(toNullable(i.value), i.metric = 'Operating Income (Loss)'),
        maxIf(toNullable(i.value), i.metric = 'EBIT')
      ) AS operating_income,

      coalesce(
        maxIf(toNullable(i.value), i.metric = 'EBITDA'),
        maxIf(toNullable(i.value), i.metric = 'Normalized EBITDA')
      ) AS ebitda,

      coalesce(
        maxIf(toNullable(i.value), i.metric = 'Net Income'),
        maxIf(toNullable(i.value), i.metric = 'Net Income Common Stockholders')
      ) AS net_income,

      /* Balance Sheet */
      maxIf(toNullable(b.value), b.metric = 'Total Assets') AS total_assets,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Total Current Assets'),
        maxIf(toNullable(b.value), b.metric = 'Current Assets')
      ) AS current_assets,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Total Current Liabilities'),
        maxIf(toNullable(b.value), b.metric = 'Current Liabilities')
      ) AS current_liabilities,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Cash And Cash Equivalents'),
        maxIf(toNullable(b.value), b.metric = 'Cash Cash Equivalents And Short Term Investments')
      ) AS cash_and_eq,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Inventory'),
        maxIf(toNullable(b.value), b.metric = 'Inventories')
      ) AS inventory,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Total Debt'),
        (coalesce(maxIf(toNullable(b.value), b.metric = 'Long Term Debt'), 0)
         + coalesce(maxIf(toNullable(b.value), b.metric = 'Short Long Term Debt'), 0)),
        (coalesce(maxIf(toNullable(b.value), b.metric = 'Long Term Debt And Capital Lease Obligation'), 0)
         + coalesce(maxIf(toNullable(b.value), b.metric = 'Current Debt And Capital Lease Obligation'), 0))
      ) AS total_debt,

      coalesce(
        maxIf(toNullable(b.value), b.metric = 'Stockholders Equity'),
        maxIf(toNullable(b.value), b.metric = 'Stockholders Equity Attributable to Parent'),
        maxIf(toNullable(b.value), b.metric = 'Common Stock Equity'),
        maxIf(toNullable(b.value), b.metric = 'Total Equity Gross Minority Interest')
      ) AS equity,

      /* Cashflow */
      coalesce(
        maxIf(toNullable(c.value), c.metric = 'Operating Cash Flow'),
        maxIf(toNullable(c.value), c.metric = 'Cash Flow From Continuing Operating Activities')
      ) AS ocf,

      coalesce(
        maxIf(toNullable(c.value), c.metric = 'Capital Expenditure'),
        maxIf(toNullable(c.value), c.metric = 'Capital Expenditures')
      ) AS capex

    FROM fundamental_data.income_statement i
    LEFT JOIN fundamental_data.balance_sheet b
      ON i.ticker=b.ticker AND i.fiscal_date=b.fiscal_date AND i.period=b.period
    LEFT JOIN fundamental_data.cashflow_statement c
      ON i.ticker=c.ticker AND i.fiscal_date=c.fiscal_date AND i.period=c.period
    WHERE i.period = 'annual'
    GROUP BY i.ticker, i.fiscal_date, i.period
  )

SELECT
  im.instrument_id,
  f.fiscal_date,
  f.period,
  x.1 AS ratio_code,
  x.2 AS value,
  ''  AS currency,
  src AS source,
  built_at_now AS built_at,
  bid AS batch_id
FROM f
INNER JOIN meta_data.v_instrument_master AS im
  ON im.symbol = f.ticker
ARRAY JOIN
[
  ('gross_margin',       if(f.revenue > 0 AND f.gross_profit     IS NOT NULL, f.gross_profit / f.revenue, NULL)),
  ('operating_margin',   if(f.revenue > 0 AND f.operating_income IS NOT NULL, f.operating_income / f.revenue, NULL)),
  ('ebitda_margin',      if(f.revenue > 0 AND f.ebitda           IS NOT NULL, f.ebitda / f.revenue, NULL)),
  ('net_margin',         if(f.revenue > 0 AND f.net_income       IS NOT NULL, f.net_income / f.revenue, NULL)),
  ('roa',                if(f.total_assets > 0 AND f.net_income  IS NOT NULL, f.net_income / f.total_assets, NULL)),
  ('roe',                if(f.equity > 0 AND f.net_income        IS NOT NULL, f.net_income / f.equity, NULL)),

  ('current_ratio',      if(f.current_liabilities > 0 AND f.current_assets IS NOT NULL, f.current_assets / f.current_liabilities, NULL)),
  ('quick_ratio',        if(f.current_liabilities > 0 AND f.current_assets IS NOT NULL,
                           (f.current_assets - coalesce(f.inventory,0)) / f.current_liabilities, NULL)),
  ('cash_ratio',         if(f.current_liabilities > 0 AND f.cash_and_eq IS NOT NULL, f.cash_and_eq / f.current_liabilities, NULL)),

  ('debt_to_equity',     if(f.equity > 0 AND f.total_debt IS NOT NULL, f.total_debt / f.equity, NULL)),
  ('debt_to_assets',     if(f.total_assets > 0 AND f.total_debt IS NOT NULL, f.total_debt / f.total_assets, NULL)),

  ('ocf_to_revenue',     if(f.revenue > 0 AND f.ocf IS NOT NULL, f.ocf / f.revenue, NULL)),
  ('capex_to_revenue',   if(f.revenue > 0 AND f.capex IS NOT NULL, abs(f.capex) / f.revenue, NULL)),
  ('fcf_margin',         if(f.revenue > 0 AND f.ocf IS NOT NULL AND f.capex IS NOT NULL, (f.ocf - abs(f.capex)) / f.revenue, NULL)),
  ('ocf_to_net_income',  if(f.net_income != 0 AND f.ocf IS NOT NULL AND f.net_income IS NOT NULL, f.ocf / f.net_income, NULL)),
  ('fcf_to_net_income',  if(f.net_income != 0 AND f.ocf IS NOT NULL AND f.capex IS NOT NULL AND f.net_income IS NOT NULL,
                           (f.ocf - abs(f.capex)) / f.net_income, NULL)),

  ('net_debt',           if(f.total_debt IS NOT NULL, (f.total_debt - coalesce(f.cash_and_eq,0)), NULL))
] AS x
WHERE x.2 IS NOT NULL;