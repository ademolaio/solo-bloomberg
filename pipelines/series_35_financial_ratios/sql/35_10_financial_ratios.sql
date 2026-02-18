-- Crates the first data for financial_ratios


INSERT INTO fundamental_data.financial_ratios
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
  FROM fundamental_data.income_statement i
  LEFT JOIN fundamental_data.balance_sheet b
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