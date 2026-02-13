# SOLO BLOOMBERG

## Institutional-Grade Personal Market Intelligence System

------------------------------------------------------------------------

# 1. PURPOSE

Solo Bloomberg is a deterministic, multi-layer financial data
architecture designed to:

-   Ingest global market data
-   Build multi-timeframe OHLCV models
-   Integrate fundamental data
-   Integrate macroeconomic data
-   Construct factors
-   Generate signals
-   Support portfolio-level decision logic

------------------------------------------------------------------------

# 2. ARCHITECTURAL PHILOSOPHY

1.  Raw data is immutable\
2.  Aggregations are deterministic\
3.  No duplicate tolerance\
4.  Domain separation is strict\
5.  Views are used for analytics consumption\
6.  Every layer depends only on the layer beneath it

------------------------------------------------------------------------

# 3. LAYERED ARCHITECTURE

## Layer 0 --- Infrastructure

-   ClickHouse
-   Docker / Docker Compose
-   Runtime container
-   Bootstrap DDL

------------------------------------------------------------------------

## Layer 1 --- Raw Data

### market_data.daily_prices

-   instrument_id
-   date
-   open
-   high
-   low
-   close
-   adj_close
-   volume
-   source
-   ingested_at
-   batch_id

------------------------------------------------------------------------

## Fundamentals Schema

Schema: `fundamentals`

-   income_statements
-   balance_sheets
-   cash_flows
-   key_metrics

------------------------------------------------------------------------

## Macro Schema

Schema: `macro`

-   interest_rates
-   cpi
-   gdp
-   unemployment
-   money_supply
-   yield_curves
-   fx_rates

------------------------------------------------------------------------

# 4. LAYER 2 --- AGGREGATED MODELS

Derived strictly from `market_data.daily_prices`

Tables:

-   weekly_prices
-   monthly_prices
-   quarterly_prices
-   yearly_prices
-   quinquennial_prices

All use AggregatingMergeTree.

------------------------------------------------------------------------

# 5. LAYER 3 --- INDICATORS

Schema: `indicators`

Examples:

-   rsi
-   atr
-   macd
-   moving_averages
-   bollinger_bands

------------------------------------------------------------------------

# 6. LAYER 4 --- FACTORS

Schema: `factors`

Examples:

-   value
-   momentum
-   quality
-   carry
-   term_premium

------------------------------------------------------------------------

# 7. LAYER 5 --- SIGNAL ENGINE

Schema: `signals`

Examples:

-   mean_reversion
-   trend_following
-   macro_regime
-   volatility_regime

------------------------------------------------------------------------

# 8. DATA FLOW

Raw → Aggregations → Indicators → Factors → Signals → Portfolio

------------------------------------------------------------------------

# 9. LONG-TERM VISION

A deterministic macro-quant research and execution framework.


```text
solo-bloomberg/
│
├── pipelines/
│   │
│   ├── 00_bootstrap_clickhouse/
│   │   └── sql/
│   │       ├── 00_create_databases.sql
│   │       ├── 01_meta_instruments.sql
│   │       └── 05_market_daily_prices.sql
│   │
│   ├── 10_ingest_yfinance/
│   │   ├── ingestion_core.py
│   │   ├── daily_ingestion.py
│   │   └── backfill_ingestion.py
│   │
│   ├── 20_aggregations/
│   │   ├── aggregation_core.py
│   │   ├── weekly.py
│   │   ├── monthly.py
│   │   ├── quarterly.py
│   │   ├── yearly.py
│   │   └── quinquennial.py
│   │
│   ├── 25_analytics_views/
│   │   ├── analytics_core.py
│   │   ├── build_all.py
│   │   ├── sql/
│   │   │   ├── 00_v_52w_performance.sql
│   │   │   ├── 01_v_top_1d_movers.sql
│   │   │   ├── 02_v_volume_surge.sql
│   │   │   ├── 03_v_gap_scan.sql
│   │   │   ├── 04_v_momentum_20d_60d.sql
│   │   │   ├── 05_v_distance_from_sma.sql
│   │   │   ├── 06_v_breadth_pct_above_sma.sql
│   │   │   ├── 07_v_52w_scanner.sql
│   │   │   ├── 08_v_atr14_pct.sql
│   │   │   ├── 09_v_inside_outside_day.sql
│   │   │   ├── 10_v_rv30_and_rank.sql
│   │   │   ├── 11_v_daily_regime_120_zscore.sql
│   │   │   ├── 12_v_weekly_regime_100_zscore.sql
│   │   │   ├── 13_v_monthly_regime_60_zscore.sql
│   │   │   ├── 14_v_quarterly_regime_20_zscore.sql
│   │   │   ├── 15_v_metrics_5y_weekly.sql
│   │   │   ├── 16_v_metrics_5y_monthly.sql
│   │   │   ├── 17_v_metrics_5y_quarterly.sql
│   │   │   └── 18_vix_weekly_snapshot.sql
│   │   └── README.md
│   │
│   ├── 30_financial_statements/
│   │   ├── income_statement.py
│   │   ├── balance_sheet.py
│   │   ├── cashflow_statement.py
│   │   └── financial_ratios.py
│   │
│   ├── 40_macro_series/
│   │   ├── fred_ingestion.py
│   │   ├── rates.py
│   │   ├── inflation.py
│   │   └── liquidity.py
│   │
│   ├── 50_quant_factors/
│   │   ├── factor_core.py
│   │   ├── value.py
│   │   ├── momentum.py
│   │   ├── quality.py
│   │   ├── low_volatility.py
│   │   └── carry.py
│   │
│   ├── 60_technical_indicators/
│   │   ├── indicators_core.py
│   │   ├── rsi.py
│   │   ├── atr.py
│   │   └── bollinger.py
│   │
│   ├── 70_signal_models/
│   │   ├── signal_core.py
│   │   ├── mean_reversion.py
│   │   ├── breakout.py
│   │   └── macro_filtered_momentum.py
│   │
│   ├── 80_portfolio_strategies/
│   │   ├── strategy_core.py
│   │   ├── iron_condor_strategy.py
│   │   ├── regime_allocation.py
│   │   └── risk_management.py
│   │
│   └── pipeline_manual.md
│
├── runtime/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
│
├── docker-compose.yml
└── README.md
```