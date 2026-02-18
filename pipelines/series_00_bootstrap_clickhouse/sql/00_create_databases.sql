-- 00) Minimal DDL set (manual bootstrap)
DROP DATABASE market_data;
CREATE DATABASE IF NOT EXISTS market_data;

DROP DATABASE economic_data;
CREATE DATABASE IF NOT EXISTS economic_data;

DROP DATABASE fundamental_data;
CREATE DATABASE IF NOT EXISTS fundamental_data;

DROP DATABASE quant_data;
CREATE DATABASE IF NOT EXISTS quant_data;

DROP DATABASE meta_data;
CREATE DATABASE IF NOT EXISTS meta_data;

DROP DATABASE trade_data;
CREATE DATABASE IF NOT EXISTS trade_data;