# Ingestion Checks

This is where you check if the both backfill and daily ingestion worked.
---

## Backfill & Daily scripts

### 1) Backfill Ingestion

```dockerfile
docker compose exec runtime bash -lc \
"python -m pipelines.10_ingest_yfinance.backfill_ingestion"
```

### 2) Daily Ingestion

```dockerfile
docker compose exec runtime bash -lc \
"python -m pipelines.10_ingest_yfinance.daily_ingestion"
```

---


## Sanity Checks

### 1) Check duplicates (raw, not merged yet)

```sql
SELECT
  instrument_id,
  source,
  date,
  count() AS n
FROM market_data.daily_prices
GROUP BY instrument_id, source, date
HAVING n > 1
ORDER BY n DESC, instrument_id, source, date
LIMIT 200;
```

### 2) Duplicate summary per symbol (join to your instrument master view)
```sql
SELECT
  i.symbol,
  sum(cnt - 1) AS duplicate_rows,
  countIf(cnt > 1) AS duplicate_dates
FROM
(
  SELECT instrument_id, source, date, count() AS cnt
  FROM market_data.daily_prices
  GROUP BY instrument_id, source, date
)
d
JOIN meta_data.v_instrument_master i
  ON d.instrument_id = i.instrument_id
WHERE d.cnt > 1
GROUP BY i.symbol
ORDER BY duplicate_rows DESC, duplicate_dates DESC
LIMIT 200;
```

### 3) “Effective” row count after dedupe (using FINAL)
```sql
SELECT
  i.symbol,
  count() AS rows_final,
  min(p.date) AS first_date,
  max(p.date) AS last_date
FROM market_data.daily_prices  p
JOIN meta_data.v_instrument_master i
  ON p.instrument_id = i.instrument_id
WHERE p.source = 'yfinance'
GROUP BY i.symbol
ORDER BY i.symbol;
```

### 4) Compare raw vs FINAL (fast “did I double insert?” test)
```sql
SELECT
  i.symbol,
  count() AS rows_raw,
  count() FINAL AS rows_final,
  (rows_raw - rows_final) AS extra_physical_rows
FROM market_data.daily_prices p
JOIN meta_data.v_instrument_master i
  ON p.instrument_id = i.instrument_id
WHERE p.source = 'yfinance'
GROUP BY i.symbol
ORDER BY extra_physical_rows DESC, i.symbol;
```

### 5) Coverage + expected trading days gap (rough)
```sql
SELECT
  i.symbol,
  min(p.date) AS first_date,
  max(p.date) AS last_date,
  count() AS rows_final,
  dateDiff('day', first_date, last_date) + 1 AS calendar_days,
  round(rows_final / calendar_days, 4) AS row_density
FROM market_data.daily_prices FINAL p
JOIN meta_data.v_instrument_master i
  ON p.instrument_id = i.instrument_id
WHERE p.source = 'yfinance'
GROUP BY i.symbol
ORDER BY row_density ASC, i.symbol;
```

### 6) Find missing trading dates (best-effort)
```sql
WITH
  per_symbol AS
  (
    SELECT
      instrument_id,
      source,
      min(date) AS first_date,
      max(date) AS last_date
    FROM market_data.daily_prices FINAL
    WHERE source = 'yfinance'
    GROUP BY instrument_id, source
  ),
  calendar AS
  (
    SELECT arrayJoin(
      arrayMap(x -> addDays(first_date, x),
               range(dateDiff('day', first_date, last_date) + 1))
    ) AS d, instrument_id, source
    FROM per_symbol
  )
SELECT
  i.symbol,
  c.d AS missing_date
FROM calendar c
LEFT JOIN market_data.daily_prices FINAL p
  ON p.instrument_id = c.instrument_id
 AND p.source = c.source
 AND p.date = c.d
JOIN meta_data.v_instrument_master i
  ON i.instrument_id = c.instrument_id
WHERE p.instrument_id IS NULL
  AND toDayOfWeek(c.d) NOT IN (6,7)  -- exclude Sat/Sun
ORDER BY i.symbol, missing_date
LIMIT 500;
```


### 7) Data sanity (bad values)
```sql
SELECT
  i.symbol,
  countIf(open < 0 OR high < 0 OR low < 0 OR close < 0 OR adj_close < 0) AS negative_price_rows,
  countIf(high < low) AS high_lt_low_rows,
  countIf(volume < 0) AS negative_volume_rows
FROM market_data.daily_prices FINAL p
JOIN meta_data.v_instrument_master i
  ON p.instrument_id = i.instrument_id
WHERE p.source = 'yfinance'
GROUP BY i.symbol
ORDER BY (negative_price_rows + high_lt_low_rows + negative_volume_rows) DESC, i.symbol;
```

---