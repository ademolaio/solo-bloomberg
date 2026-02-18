# Economic Data Check

This is where you check if the both backfill and daily ingestion worked.

---

## Economic Scripts

### 1) Build schema

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.series_40_economic_press.build_all"
```
### 2) Seed universe

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.series_40_economic_press.fred_seed_universe"
```
### 3) Run FRED discovery

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.series_40_economic_press.fred_discovery"
```

### 4) Pull observations

```dockerfile
docker compose exec runtime bash -lc "python -m pipelines.series_40_economic_press.fred_observations"

```
---



## Economic Press Sanity Checks

```sql
SELECT series_id, is_active, priority 
FROM economic_data.v_fred_series_universe_current 
ORDER BY priority, series_id
```

```sql
SELECT
  count() AS rows,
  uniqExact(series_id) AS series_loaded,
  min(date) AS min_date,
  max(date) AS max_date
FROM economic_data.fred_observations;
```


```sql
SELECT
  series_id,
  max(date) AS last_date,
  anyLast(value) AS last_value
FROM economic_data.fred_observations
GROUP BY series_id
ORDER BY last_date DESC, series_id
LIMIT 200;
```


```sql
SELECT *
FROM economic_data.v_fred_obs_latest_daily
WHERE series_id = 'SOFR'
ORDER BY date DESC
LIMIT 10;
```

### A. Row counts: raw vs latest-per-day
```sql
SELECT
  (SELECT count() FROM economic_data.fred_observations) AS raw_rows,
  (SELECT count() FROM economic_data.v_fred_obs_latest_daily) AS latest_daily_rows;
```

### B. Pick one series and compare duplicates
```sql
SELECT
  series_id,
  date,
  count() AS raw_versions,
  min(ingested_at) AS first_ingest,
  max(ingested_at) AS last_ingest
FROM economic_data.fred_observations
WHERE series_id = 'SOFR'
GROUP BY series_id, date
HAVING raw_versions > 1
ORDER BY date DESC
LIMIT 50;
```


### C. Confirm the view picks the last ingested value
```sql
SELECT
  o.series_id,
  o.date,
  o.value,
  o.ingested_at
FROM economic_data.fred_observations o
INNER JOIN (
  SELECT series_id, date, max(ingested_at) AS max_ingested
  FROM economic_data.fred_observations
  WHERE series_id='SOFR'
  GROUP BY series_id, date
) m
ON o.series_id=m.series_id AND o.date=m.date AND o.ingested_at=m.max_ingested
ORDER BY o.date DESC
LIMIT 20;
```


```sql
SELECT count() FROM economic_data.fred_series_meta;
```
---

### 5) Practical “what next” for your app
```text
A) Add these to Metabase
	•	v_fred_series_latest (macro dashboard tiles)
	•	v_fred_obs_latest_daily (charting)
	•	v_fred_obs_quarterly_avg and/or ..._eoq (quarterly views)
	•	v_fred_obs_yearly_avg and/or ..._eoy (yearly views)
```
