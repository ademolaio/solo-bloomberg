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
