# solo-bloomberg

A self-hosted, containerized analytics platform built with a strict, reproducible runtime.
The system uses Docker as the **only** Python environment—no virtual environments, no host installs.

---

## Runtime (Docker) — Locked

The runtime defines **how all code executes**.  
It is immutable, reproducible, and shared by all tools (Prefect, SQLMesh, etc.).

### Principles

- Docker image = Python environment
- `Dockerfile` = execution contract
- `requirements.txt` = dependency truth
- No `python -m venv`
- No `pip install` on host
- No installing packages in running containers
- Rebuild image to change runtime state

---

## Creating the databases
### market_data
```text
Raw/cleaned time-series: prices, OHLCV, corporate actions (splits/dividends), 
futures continuous series, options chains snapshots (if you go there).
```

```sql
CREATE DATABASE IF NOT EXISTS market_data;
```

### economic_data
```text
Macro series (FRED, ECB, OECD, etc.), calendar/event metadata, releases, revisions (if you model them).
```

```sql
CREATE DATABASE IF NOT EXISTS economic_data;
```

### fundamental_data
```text
Financial statements, ratios, estimates (if you add), corporate profiles, sector/industry mappings.
```

```sql
CREATE DATABASE IF NOT EXISTS fundamental_data;

```

### quant_data
```text
Derived features + signals: returns, vol, z-scores, breadth metrics, factor exposures, 
regime labels, model outputs. (This is your “physics layer”.)
```

```sql
CREATE DATABASE IF NOT EXISTS quant_data;

```

### meta_data
```text
Operational control plane: tickers/master instrument table, source lineage, 
ingestion runs, data quality checks, last_updated timestamps, error logs, 
symbol mapping (ISIN ↔ ticker), etc.
```

```sql
CREATE DATABASE IF NOT EXISTS meta_data;

```

### trade_data
```text
Execution and portfolio layer: orders (submitted/canceled), fills (executed trades), 
position states (live + paper), realized/unrealized PnL, margin usage, cash movements, 
portfolio snapshots, strategy attribution tags, and risk metrics (exposure, Greeks, drawdown).
```

```sql
CREATE DATABASE IF NOT EXISTS trade_data;
```
---


docker compose exec runtime bash -lc \
"python -m pipelines.10_ingest_yfinance.backfill_ingestion"

docker compose exec runtime bash -lc \
"python -m pipelines.10_ingest_yfinance.daily_ingestion"


docker compose exec runtime bash -lc "python -m pipelines.10_ingest_yfinance.daily_ingestion"
