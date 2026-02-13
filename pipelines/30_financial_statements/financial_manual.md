# Financial Checks

This is where you check if the both backfill and daily ingestion worked.

---

## Financial Scripts

### Weekly

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.30_financial_statements.income_statement"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.30_financial_statements.balance_sheet"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.30_financial_statements.cashflow_statement"
```

---

## Income Statement Sanity Checks

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