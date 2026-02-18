-- 6a) “Current” instrument registry view
DROP VIEW IF EXISTS meta_data.v_instruments_current;
CREATE VIEW IF NOT EXISTS meta_data.v_instruments_current AS
SELECT *
FROM meta_data.instruments
FINAL;


-- 6b) Unified instrument master view
DROP VIEW IF EXISTS meta_data.v_instrument_master;
CREATE VIEW meta_data.v_instrument_master AS
SELECT
    i.instrument_id AS instrument_id,
    i.asset_class   AS asset_class,
    i.symbol        AS symbol,
    i.mic           AS mic,
    i.exchange      AS exchange,
    i.short_name    AS short_name,
    i.is_active     AS is_active,
    i.created_at    AS created_at,
    i.updated_at    AS updated_at,
    i.source        AS source,

    e.isin          AS isin,
    e.figi          AS figi,
    e.currency      AS eq_currency,
    e.country       AS country,
    e.sector        AS sector,
    e.industry      AS industry,

    fc.root_symbol      AS fut_root_symbol,
    fc.expiration_date  AS expiration_date,
    fc.contract_month   AS contract_month,
    fc.multiplier       AS multiplier,
    fc.currency         AS fut_currency,

    fcont.root_symbol       AS cont_root_symbol,
    fcont.roll_rule         AS roll_rule,
    fcont.roll_offset_days  AS roll_offset_days,
    fcont.price_adjustment  AS price_adjustment,
    fcont.currency          AS cont_currency
FROM meta_data.v_instruments_current AS i
LEFT JOIN meta_data.equities_etfs      AS e     ON i.instrument_id = e.instrument_id
LEFT JOIN meta_data.futures_contracts  AS fc    ON i.instrument_id = fc.instrument_id
LEFT JOIN meta_data.futures_continuous AS fcont ON i.instrument_id = fcont.instrument_id;


-- 6c) Daily prices decorated for dashboards
DROP VIEW IF EXISTS market_data.v_daily_prices;
CREATE VIEW market_data.v_daily_prices AS
SELECT
    p.date,
    im.asset_class,
    im.symbol,
    im.short_name,
    p.open, p.high, p.low, p.close, p.adj_close, p.volume,
    p.source, p.ingested_at, p.batch_id
FROM market_data.daily_prices p
LEFT JOIN meta_data.v_instrument_master im
    ON p.instrument_id = im.instrument_id;