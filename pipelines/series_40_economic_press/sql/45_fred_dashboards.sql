-- 2) “Latest overall per series” view (one row per series)

CREATE OR REPLACE VIEW economic_data.v_fred_series_latest AS
SELECT
  series_id,
  max(date) AS last_date,
  argMax(value, date) AS last_value
FROM economic_data.v_fred_obs_latest_daily
GROUP BY series_id;

-- Quarterly average
CREATE OR REPLACE VIEW economic_data.v_fred_obs_quarterly_avg AS
SELECT
  series_id,
  toStartOfQuarter(date) AS quarter_start,
  avg(value) AS value_q_avg,
  count() AS n_obs
FROM economic_data.v_fred_obs_latest_daily
GROUP BY series_id, quarter_start;


-- Quarterly end-of-quarter (last observation in quarter)
CREATE OR REPLACE VIEW economic_data.v_fred_obs_quarterly_eoq AS
SELECT
  series_id,
  toStartOfQuarter(date) AS quarter_start,
  argMax(value, date) AS value_q_eoq,
  max(date) AS last_date_in_q
FROM economic_data.v_fred_obs_latest_daily
GROUP BY series_id, quarter_start;


-- 4) Year-by-year rollups
-- Yearly average
CREATE OR REPLACE VIEW economic_data.v_fred_obs_yearly_avg AS
SELECT
  series_id,
  toStartOfYear(date) AS year_start,
  avg(value) AS value_y_avg,
  count() AS n_obs
FROM economic_data.v_fred_obs_latest_daily
GROUP BY series_id, year_start;


-- Year-end (last observation in year)
CREATE OR REPLACE VIEW economic_data.v_fred_obs_yearly_eoy AS
SELECT
  series_id,
  toStartOfYear(date) AS year_start,
  argMax(value, date) AS value_y_eoy,
  max(date) AS last_date_in_y
FROM economic_data.v_fred_obs_latest_daily
GROUP BY series_id, year_start;


