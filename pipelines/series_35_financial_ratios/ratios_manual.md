# Ratios Checks

This is where you check if the both backfill and daily ingestion worked.

---

## Ratios Scripts

### Creating the tables for the Fundamental Ratio Data:


```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.35_financial_ratios.compute_ratios"
```
---

## Income Statement Sanity Checks

### 1) Count rows by period + ticker

```sql 
SELECT ticker, countDistinct(fiscal_date) AS periods
FROM fundamental_data.income_statement
GROUP BY ticker
ORDER BY periods DESC
LIMIT 20;
```

```sql 
SELECT
  metric,
  count() AS n
FROM fundamental_data.balance_sheet
WHERE period = 'annual'
  AND metric ILIKE '%equity%'
GROUP BY metric
ORDER BY n DESC
LIMIT 50;
```

```sql 
SELECT
  i.ticker,
  i.fiscal_date AS income_date,
  b.fiscal_date AS bs_date
FROM fundamental_data.income_statement i
LEFT JOIN fundamental_data.balance_sheet b
  ON i.ticker = b.ticker
 AND i.fiscal_date = b.fiscal_date
 AND i.period = b.period
WHERE i.period = 'annual'
  AND i.ticker IN ('AAPL','MSFT','SAP.DE','NESN.SW','ROG.SW','ASML.AS','JNJ')
LIMIT 200;
```


```sql 
INSERT INTO fundamental_data.financial_ratios
(instrument_id, fiscal_date, period, ratio_code, value, currency, source, built_at, batch_id)
SELECT
  instrument_id,
  toDate('2024-12-31'),
  'annual',
  'roe',
  0.25,
  '',
  'manual_test',
  now64(3,'UTC'),
  'test'
FROM meta_data.v_instrument_master
WHERE symbol = 'AAPL'
LIMIT 1;
```

```sql 
SELECT * 
FROM fundamental_data.v_financial_ratios_latest
WHERE ratio_code='roe'
LIMIT 20;
```

```sql 
SELECT *
FROM fundamental_data.v_ratios_latest_wide
WHERE symbol='AAPL'
LIMIT 5;
```


```sql 
SELECT
    i.ticker,
    i.fiscal_date,
    i.period,

    maxIf(toNullable(i.value), i.metric = 'Total Revenue') AS revenue,
    maxIf(toNullable(i.value), i.metric = 'Net Income')    AS net_income,

    maxIf(toNullable(b.value), b.metric = 'Total Assets')  AS total_assets,

    coalesce(
      maxIf(toNullable(b.value), b.metric = 'Stockholders Equity'),
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
ORDER BY i.ticker, i.fiscal_date DESC
LIMIT 50;
```



```sql 
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
```

```sql 
SELECT ratio_code, count() AS n
FROM fundamental_data.financial_ratios
GROUP BY ratio_code
ORDER BY n DESC;
```

```sql 
SELECT *
FROM fundamental_data.v_financial_ratios_latest
WHERE ratio_code IN ('roe','roa','net_margin')
LIMIT 50;
```

```sql 
SELECT metric, count() n
FROM fundamental_data.income_statement
WHERE period='annual'
GROUP BY metric
ORDER BY n DESC
LIMIT 80;
```


```sql 
SELECT metric, count() n
FROM fundamental_data.cashflow_statement
WHERE period='annual'
GROUP BY metric
ORDER BY n DESC
LIMIT 80;
```

```sql 
SELECT metric, count() n
FROM fundamental_data.balance_sheet
WHERE period='annual'
GROUP BY metric
ORDER BY n DESC
LIMIT 80;
```


```sql 
SELECT ratio_code, count() n
FROM fundamental_data.financial_ratios
WHERE batch_id = 'manual_sql_core_ratios_annual'
GROUP BY ratio_code
ORDER BY n DESC;
```

```sql 
SELECT *
FROM fundamental_data.v_financial_ratios_latest
WHERE ratio_code IN ('gross_margin','operating_margin','net_margin','roe','roa','current_ratio','debt_to_equity','fcf_margin')
ORDER BY fiscal_date DESC
LIMIT 50;
```


```text
1.1 Period values

Use these as period consistently:
	•	annual
	•	quarterly
	•	ttm (derived from last 4 quarters; optional but strongly recommended)

1.2 Ratio code naming

Use snake_case, avoid spaces, avoid units in names unless needed.

Recommended conventions:
	•	Margins end with _margin (fraction, e.g. 0.42 = 42%)
	•	Returns start with ro (fraction)
	•	Coverage ends with _coverage (multiple)
	•	Turnover ends with _turnover (multiple)
	•	Days end with _days (days)
	•	Growth ends with _yoy, _cagr_3y, _cagr_5y (fraction)
	•	Per-share ends with _per_share (currency units)
	•	Yields end with _yield (fraction)

Examples:
	•	gross_margin, operating_margin, net_margin
	•	roe, roa, roic
	•	interest_coverage
	•	asset_turnover
	•	dso_days, dio_days, dpo_days, ccc_days
	•	revenue_yoy, revenue_cagr_3y
	•	fcf_per_share
	•	dividend_yield

⸻

2) “Core 25” institutional ratios to compute first (from statements)

These are the most useful and realistically computable from your yfinance statements.

2.1 Profitability / Margins
	1.	gross_margin = gross_profit / revenue
	2.	operating_margin = operating_income / revenue
	3.	ebit_margin = ebit / revenue (fallback: operating_income)
	4.	net_margin = net_income / revenue

2.2 Returns
	5.	roe = net_income / avg_equity
	6.	roa = net_income / avg_assets
	7.	roic = nopat / avg_invested_capital
	8.	roce = ebit / avg_capital_employed

2.3 Liquidity
	9.	current_ratio = current_assets / current_liabilities
	10.	quick_ratio = (current_assets - inventory) / current_liabilities
	11.	cash_ratio = cash_and_equivalents / current_liabilities

2.4 Leverage / Solvency
	12.	debt_to_equity = total_debt / total_equity
	13.	net_debt_to_ebitda = (total_debt - cash) / ebitda
	14.	debt_to_assets = total_debt / total_assets
	15.	interest_coverage = ebit / interest_expense

2.5 Efficiency
	16.	asset_turnover = revenue / avg_assets
	17.	inventory_turnover = cogs / avg_inventory
	18.	dso_days = (avg_receivables / revenue) * 365
	19.	dio_days = (avg_inventory / cogs) * 365
	20.	dpo_days = (avg_payables / cogs) * 365
	21.	ccc_days = dio_days + dso_days - dpo_days

2.6 Cash flow quality
	22.	fcf_margin = fcf / revenue
	23.	ocf_to_net_income = operating_cash_flow / net_income
	24.	capex_to_revenue = capex / revenue (capex usually negative in CF; use abs for ratio if desired)
	25.	fcf_to_net_income = fcf / net_income

Notes:

	•	Averages use (current + prior) / 2 when prior exists; otherwise use current.
	•	fcf = operating_cash_flow - capex (capex often negative; handle carefully).

⸻

3) Required line items (mapping)

Your fundamental_data.* tables are “tall” (metric, value). You need a mapping layer that resolves common metric variants.

3.1 Income statement line items (annual/quarterly)
	•	revenue: Total Revenue / TotalRevenue
	•	gross_profit: Gross Profit
	•	cogs: Cost Of Revenue / CostOfRevenue
	•	operating_income: Operating Income
	•	ebit: EBIT (if missing, use operating_income)
	•	ebitda: EBITDA (sometimes missing)
	•	net_income: Net Income / NetIncome
	•	interest_expense: Interest Expense (often missing in yfinance statements; coverage may be NA)

3.2 Balance sheet line items
	•	total_assets: Total Assets
	•	total_equity: Total Stockholder Equity / Total Equity Gross Minority Interest
	•	current_assets: Total Current Assets
	•	current_liabilities: Total Current Liabilities
	•	inventory: Inventory
	•	cash: Cash And Cash Equivalents / Cash Cash Equivalents And Short Term Investments
	•	receivables: Net Receivables / Accounts Receivable
	•	payables: Accounts Payable
	•	total_debt: Total Debt / (Short Long Term Debt + Long Term Debt)

3.3 Cashflow statement line items
	•	operating_cash_flow: Total Cash From Operating Activities
	•	capex: Capital Expenditures
	•	dividends_paid: Dividends Paid (optional)
```

### 1) Count rows by period + ticker

```sql
SELECT
  ticker,
  period,
  count() AS rows
FROM fundamental_data.income_statement
GROUP BY ticker, period
ORDER BY ticker, period;
```


### 2) Confirm min/max fiscal dates per ticker + period
```sql
SELECT
  ticker,
  period,
  min(fiscal_date) AS min_fiscal_date,
  max(fiscal_date) AS max_fiscal_date,
  uniqExact(metric) AS metrics
FROM fundamental_data.income_statement
GROUP BY ticker, period
ORDER BY ticker, period;
```

### 3) Duplicate check (your “never duplicates” rule)
```sql
SELECT
  ticker, fiscal_date, period, metric,
  count() AS n_versions
FROM fundamental_data.income_statement
GROUP BY ticker, fiscal_date, period, metric
HAVING n_versions > 1
ORDER BY n_versions DESC
LIMIT 50;
```
---


## Balance Sheet Sanity Checks

### 1) Row count + time coverage

```sql
SELECT
  count() AS rows,
  min(fiscal_date) AS min_fiscal_date,
  max(fiscal_date) AS max_fiscal_date,
  uniqExact(ticker) AS tickers,
  uniqExact(period) AS periods,
  uniqExact(metric) AS metrics
FROM fundamental_data.balance_sheet;
```


### 2) Duplicate key check (the important one)
```sql
SELECT
  count() AS rows,
  uniqExact(ticker, fiscal_date, period, metric) AS uniq_keys,
  (rows - uniq_keys) AS duplicate_rows
FROM fundamental_data.balance_sheet;
```

### 3) Show actual duplicate groups (top offenders)
```sql
SELECT
  ticker,
  fiscal_date,
  period,
  metric,
  count() AS c,
  max(loaded_at) AS newest_loaded_at
FROM fundamental_data.balance_sheet
GROUP BY ticker, fiscal_date, period, metric
HAVING c > 1
ORDER BY c DESC, ticker, fiscal_date
LIMIT 50;
```

### 4) “Current view” validation (does FINAL collapse them?)
```sql
SELECT
  count() AS rows_final
FROM fundamental_data.balance_sheet FINAL;
```
---

## Cash Flow Sanity Checks

### 0) Quick smoke test: table exists + sample rows

```sql
SELECT
  count() AS rows,
  min(fiscal_date) AS min_fiscal_date,
  max(fiscal_date) AS max_fiscal_date,
  uniqExact(ticker) AS tickers,
  uniqExact(period) AS periods,
  uniqExact(metric) AS metrics
FROM fundamental_data.balance_sheet;
```


### 1) Row count + coverage + cardinalities
```sql
SELECT
  count() AS rows,
  min(fiscal_date) AS min_fiscal_date,
  max(fiscal_date) AS max_fiscal_date,
  uniqExact(ticker) AS tickers,
  groupUniqArray(period) AS periods,
  uniqExact(metric) AS metrics
FROM fundamental_data.cashflow_statement;
```

### 2) Duplicate-key check (your real “integrity” test)
```sql
SELECT
  count() AS rows,
  uniqExact(ticker, fiscal_date, period, metric) AS uniq_keys,
  (rows - uniq_keys) AS duplicate_rows
FROM fundamental_data.cashflow_statement;
```

### 3) Show the worst duplicate groups (if any)
```sql
SELECT
  ticker, fiscal_date, period, metric,
  count() AS c,
  max(loaded_at) AS newest_loaded_at
FROM fundamental_data.cashflow_statement
GROUP BY ticker, fiscal_date, period, metric
HAVING c > 1
ORDER BY c DESC, ticker, fiscal_date
LIMIT 50;
```

### 4) Verify FINAL collapses versions
```sql
SELECT
  count() AS rows_final
FROM fundamental_data.cashflow_statement FINAL;
```

### 5) Per-ticker “did we actually get both annual + quarterly?”
```sql
SELECT
  ticker,
  count() AS rows,
  groupUniqArray(period) AS periods,
  min(fiscal_date) AS min_date,
  max(fiscal_date) AS max_date
FROM fundamental_data.cashflow_statement
GROUP BY ticker
ORDER BY ticker;
```
---