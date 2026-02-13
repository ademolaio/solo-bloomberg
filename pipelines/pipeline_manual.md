# SOLO BLOOMBERG -- FULL DIRECTORY ARCHITECTURE

------------------------------------------------------------------------

## SYSTEM PHILOSOPHY

This project is structured as a layered financial data platform:

-   **Layer 0 --- Infrastructure**
-   **Layer 1 --- Raw Market Data**
-   **Layer 2 --- Aggregated Models**
-   **Layer 3 --- Indicators & Signals**
-   **Layer 4 --- Analytics & Research**
-   **Layer 5 --- Strategy & Execution (Future)**

Each directory prefix (00\_, 10\_, 20\_, etc.) represents increasing
abstraction and refinement.

------------------------------------------------------------------------

# ROOT DIRECTORY STRUCTURE

pipelines/ ├── 00_bootstrap_clickhouse/ ├── 10_ingest_yfinance/ ├──
15_ingest_fundamentals/ ├── 16_ingest_macro/ ├── 20_aggregations/ ├──
30_indicators/ ├── 40_signals/ ├── 50_analytics/ └── 60_strategy/

------------------------------------------------------------------------

# 00_bootstrap_clickhouse

**Purpose:** Infrastructure setup.

Responsibilities: - Create schemas (market_data, meta_data, macro_data,
etc.) - Create base tables (daily_prices) - Define engines
(ReplacingMergeTree, AggregatingMergeTree) - Index and partition
strategy

This layer should be run once per environment.

------------------------------------------------------------------------

# 10_ingest_yfinance

**Purpose:** Raw price ingestion.

Responsibilities: - Pull OHLCV data from Yahoo Finance - Normalize
timestamps - Assign instrument_id - Deduplicate - Insert into
`market_data.daily_prices`

Key file: - ingestion_core.py

This feeds Layer 1.

------------------------------------------------------------------------

# 15_ingest_fundamentals

**Purpose:** Company financial statements and metrics.

Tables might include: - fundamentals_income_statements -
fundamentals_balance_sheets - fundamentals_cash_flows -
fundamentals_ratios

Sources may include: - Yahoo Finance - SEC EDGAR - Financial Modeling
Prep - Polygon

------------------------------------------------------------------------

# 16_ingest_macro

**Purpose:** Economic data ingestion.

Tables might include: - macro_rates (US10Y, DE10Y, etc.) -
macro_inflation - macro_unemployment - macro_money_supply (M1, M2) -
macro_gdp

Sources: - FRED - ECB SDW - SNB - BOJ - IMF

This supports macro-quant analysis.

------------------------------------------------------------------------

# 20_aggregations

**Purpose:** Transform daily data into higher timeframes.

Sub-structure:

20_aggregations/ ├── sql/ │ ├── 00_weekly_prices.sql │ ├──
01_monthly_prices.sql │ ├── 02_quarterly_prices.sql │ ├──
03_yearly_prices.sql │ ├── 04_quinquennial_prices.sql │ └──
05_decennial_prices.sql ├── weekly.py ├── monthly.py ├── quarterly.py
├── yearly.py ├── quinquennial.py └── aggregation_core.py

Uses: - AggregatingMergeTree - Materialized Views - argMinState /
argMaxState - sumState / maxState / minState

Produces: - weekly_prices - monthly_prices - quarterly_prices -
yearly_prices - quinquennial_prices

------------------------------------------------------------------------

# 30_indicators

**Purpose:** Technical indicators.

Examples: - RSI - MACD - ATR - Bollinger Bands - Moving Averages

These consume aggregated or daily price data.

------------------------------------------------------------------------

# 40_signals

**Purpose:** Trading logic.

Examples: - Moving average crossovers - Volatility compression -
Breakout detection - Macro regime overlays

This is where quant logic lives.

------------------------------------------------------------------------

# 50_analytics

**Purpose:** Research dashboards & metrics.

Examples: - Return distributions - Rolling volatility - Sharpe ratios -
Drawdown curves - Correlation matrices

Feeds BI tools (Metabase, Superset).

------------------------------------------------------------------------

# 60_strategy

**Purpose:** Execution framework (future).

Examples: - Portfolio construction - Risk allocation - Position sizing -
Rebalancing logic - Order simulation

This is where the Nano Fund logic lives.

------------------------------------------------------------------------

# DATA FLOW SUMMARY

Infrastructure → Daily Ingestion → Aggregations → Indicators → Signals →
Analytics → Strategy

------------------------------------------------------------------------

# NAMING CONVENTIONS

-   Raw tables: daily_prices
-   Aggregated tables: \*\_prices
-   Views: v\_\*
-   Materialized views: mv\_\*
-   State tables: AggregatingMergeTree
-   Final consumer views: plain SELECT merge views

------------------------------------------------------------------------

# LONG-TERM EXPANSION

Future additions: - 70_machine_learning/ - 80_backtesting/ -
90_live_execution/

------------------------------------------------------------------------

# SYSTEM IDENTITY

This architecture supports:

-   Macro-Quant trading
-   Multi-asset coverage
-   Institutional-grade aggregation logic
-   Zero-duplicate data discipline
-   Expandable signal research
-   Family office analytics

------------------------------------------------------------------------

END OF DOCUMENT


## Directory Tree

```text
pipelines/
├── 00_bootstrap_clickhouse/     # DBs, base tables, views (DDL only)
├── 10_ingest_market_data/       # prices/quotes (daily candles, maybe intraday later)
├── 20_aggregations/             # weekly/monthly/quarterly/yearly/quinquennial built from daily
├── 30_ingest_fundamentals/      # financial statements, ratios, fundamentals
├── 40_ingest_macro/             # FRED/ECB/DESTATIS etc. macro & rates
├── 50_factors/                  # factor exposures built from prices + fundamentals
├── 60_indicators/               # technical indicators built from price aggregates
├── 70_signals/                  # signal rules (entries/exits/conditions)
└── 80_strategies/               # portfolio construction, sizing, execution logic


00_bootstrap_clickhouse — Infrastructure / schema

Contains
	•	CREATE DATABASE
	•	base tables:
	•	meta_data.instruments
	•	market_data.daily_prices
	•	fundamental_data.* (empty schema tables)
	•	economic_data.* (empty schema tables)
	•	“master” views like meta_data.v_instrument_master

Does NOT contain
	•	ingestion logic
	•	backfills


10_ingest_market_data — Raw market prices (canonical raw layer)

Purpose: get daily bars into market_data.daily_prices.

Typical outputs
	•	market_data.daily_prices (ReplacingMergeTree)
	•	optional later:
	•	market_data.intraday_prices (1m/5m)
	•	market_data.corporate_actions (splits/dividends)
	•	market_data.fx_rates (if you add FX)

Key idea: this is the raw truth for prices.

⸻

20_aggregations — Timeframe resampling / rollups

Purpose: transform daily → higher timeframes.

Outputs
	•	market_data.weekly_prices (+ v_weekly_prices)
	•	market_data.monthly_prices (+ v_monthly_prices)
	•	market_data.quarterly_prices (+ v_quarterly_prices)
	•	market_data.yearly_prices (+ v_yearly_prices)
	•	market_data.quinquennial_prices (+ view)

Mechanism
	•	AggregatingMergeTree + MV + “backfill by ranges”
	•	Users query via v_* views (because GUI “SELECT *” on state columns is painful)

⸻

30_ingest_fundamentals — Fundamentals / financial statements

Purpose: ingest company fundamentals (quarterly/annual statements + derived metrics).

Data examples
	•	income statement: revenue, COGS, gross profit, operating income, net income
	•	balance sheet: assets, liabilities, equity, cash, debt
	•	cash flow: CFO/CFI/CFF
	•	shares outstanding, EPS, dividends
	•	derived metrics: margins, ROE/ROA, leverage, FCF yield

Outputs (suggested DB)
	•	fundamental_data.financial_statements (by instrument_id, period_end, period_type)
	•	fundamental_data.key_metrics (PE, EV/EBITDA, ROIC, etc.)
	•	fundamental_data.estimates (later, if you add them)

⸻

40_ingest_macro — Macro / economic series

Purpose: ingest macro series with timestamps (rates, inflation, GDP, PMI, etc.)

Data examples
	•	FRED series (US CPI, Fed Funds, unemployment)
	•	ECB, Bundesbank, Destatis, OECD, IMF (later)
	•	yield curves, policy rates, inflation breakevens

Outputs
	•	economic_data.series (metadata)
	•	economic_data.observations (series_id, date, value)

⸻

50_factors — Factors (cross-sectional + time-series)

Purpose: compute factor exposures and factor returns.

Inputs
	•	prices (daily/weekly/monthly)
	•	fundamentals
	•	macro (optional)

Examples
	•	value: earnings yield, book-to-market
	•	momentum: 12–1 month momentum
	•	quality: ROE, profit margin stability
	•	low vol: rolling vol
	•	size: market cap

Outputs
	•	quant_data.factor_exposures
	•	quant_data.factor_returns
	•	quant_data.factor_scores

⸻

60_indicators — Technical indicators (features)

Purpose: compute technical features used by signals.

Inputs
	•	usually v_daily_prices, v_weekly_prices, etc.

Examples
	•	RSI, ATR, Bollinger Bands, MACD
	•	realized vol, range stats, gaps
	•	trend filters (MA cross, slope)

Outputs
	•	quant_data.indicators_daily
	•	quant_data.indicators_weekly
(whatever granularity you want)

⸻

70_signals — Signal logic (conditions)

Purpose: turn indicators/factors into boolean/score outputs.

Examples
	•	“RSI < 30 AND price above 200DMA” → long candidate
	•	“vol expansion + break of weekly level” → risk-off

Outputs
	•	trade_data.signals (signal_name, instrument_id, date, value/score)

⸻

80_strategies — Portfolio + execution logic

Purpose: combine signals into positions, sizing, risk rules, orders.

Examples
	•	position sizing, volatility targeting
	•	rebalance schedules
	•	order generation scaffolding (later IBKR)

Outputs
	•	trade_data.positions
	•	trade_data.orders
	•	trade_data.performance

```
```text
solo-bloomberg/
│
├── pipelines/
│   │
│   ├── 00_bootstrap_clickhouse/      → Infrastructure (DDL only)
│   │
│   ├── 10_ingest_yfinance/           → Raw market data ingestion
│   │
│   ├── 20_aggregations/              → Timeframe transformations
│   │
│   ├── 25_analytics_views/           → Scan / screening views
│   │
│   ├── 30_financial_statements/      → Fundamental raw financial data
│   │
│   ├── 40_macro_series/              → Economic + rates + liquidity data
│   │
│   ├── 50_quant_factors/             → Cross-sectional factor models
│   │
│   ├── 60_technical_indicators/      → Indicator computations (per instrument)
│   │
│   ├── 70_signal_models/             → Trading logic (decision layer)
│   │
│   ├── 80_portfolio_strategies/      → Position sizing + portfolio construction
│   │
│   └── pipeline_manual.md
```