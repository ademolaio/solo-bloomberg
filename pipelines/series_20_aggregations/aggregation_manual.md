# Aggrgation Checks

This is where you check if the both backfill and daily ingestion worked.

---

## Aggregations Scripts

### Weekly

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.20_aggregations.weekly"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.20_aggregations.monthly"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.20_aggregations.quarterly"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.20_aggregations.yearly"
```

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.20_aggregations.quinquennial_prices"
```

---

## Weekly Sanity Checks

### 1) Sanity-check row counts + date coverage (weekly)

```sql
SELECT
  count() AS weekly_rows,
  min(week_start) AS first_week,
  max(week_end)   AS last_week
FROM market_data.weekly_prices;
```

```sql
SELECT
  i.symbol,
  count() AS weeks,
  min(w.week_start) AS first_week,
  max(w.week_end)   AS last_week
FROM market_data.weekly_prices w
JOIN meta_data.v_instrument_master i
  ON w.instrument_id = i.instrument_id
GROUP BY i.symbol
ORDER BY i.symbol;
```

### 2) Query weekly “final values” via the view (human-friendly)
```sql
SELECT *
FROM market_data.v_weekly_prices
ORDER BY instrument_id, week_start
LIMIT 200;
```
---


## Monthly Sanity Checks

### 1) Verify monthly table coverage

```sql
SELECT
  count() AS monthly_rows,
  min(month_start) AS first_month,
  max(month_end)   AS last_month
FROM market_data.monthly_prices;
```

```sql
SELECT
  i.symbol,
  count() AS weeks,
  min(w.week_start) AS first_week,
  max(w.week_end)   AS last_week
FROM market_data.weekly_prices w
JOIN meta_data.v_instrument_master i
  ON w.instrument_id = i.instrument_id
GROUP BY i.symbol
ORDER BY i.symbol;
```

### 2) Spot-check correctness vs daily (pick 1 symbol)
```sql
SELECT
  month_start,
  month_end,
  argMinMerge(open_state)      AS open,
  argMaxMerge(close_state)     AS close,
  maxMerge(high_state)         AS high,
  minMerge(low_state)          AS low,
  argMaxMerge(adj_close_state) AS adj_close,
  sumMerge(volume_state)       AS volume
FROM market_data.monthly_prices
WHERE instrument_id = '<UUID>'
  AND source = 'yfinance'
ORDER BY month_start DESC
LIMIT 24;
```

```sql
SELECT
  min(date) AS first_trade,
  max(date) AS last_trade,
  argMin(open, date)  AS open,
  argMax(close, date) AS close,
  max(high) AS high,
  min(low)  AS low,
  argMax(adj_close, date) AS adj_close,
  sum(volume) AS volume
FROM market_data.daily_prices
WHERE instrument_id = '<UUID>'
  AND source = 'yfinance'
  AND date >= toDate('2025-01-01')
  AND date <  toDate('2025-02-01');
```
---


## Quarterly Sanity Checks

### 1) Row counts / date span

```sql
SELECT
  count() AS quarterly_rows,
  min(quarter_start) AS first_q,
  max(quarter_end)   AS last_q
FROM market_data.quarterly_prices;
```


### 2) Read finalized values (use the view)
```sql
SELECT *
FROM market_data.v_quarterly_prices
ORDER BY instrument_id, quarter_start
LIMIT 200;
```
---

## Yearly Sanity Checks

### 1) Row counts / date span

```sql
SELECT count() yearly_rows, 
       min(year_start) first_year, 
       max(year_end) last_year
FROM market_data.yearly_prices;
```
---

## Quinquennial Sanity Checks

### 1) Row counts / date span

```sql
SELECT
  count() AS rows,
  min(quinquennial_start) AS first_bucket,
  max(quinquennial_end) AS last_date
FROM market_data.quinquennial_prices;
```

```sql
SELECT *
FROM market_data.v_quinquennial_prices
ORDER BY instrument_id, quinquennial_start
LIMIT 50;
```
---