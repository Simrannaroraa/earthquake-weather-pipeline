-- Databricks SQL; set catalog/schema names to match your environment.
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

-- Bronze stores source payloads as JSON text so upstream schema changes are recoverable.
CREATE TABLE IF NOT EXISTS workspace.bronze.ingestion_runs (
  run_id STRING, _ingested_at TIMESTAMP, _source_file STRING, raw_json STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.bronze.earthquake_events (
  run_id STRING, _ingested_at TIMESTAMP, _source_file STRING, event_id STRING, raw_json STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.bronze.weather_snapshots (
  run_id STRING, _ingested_at TIMESTAMP, event_id STRING, requested_at TIMESTAMP, raw_json STRING
) USING DELTA;

-- Normalized 3NF landing target: event facts, referenced location and magnitude type,
-- and time-stamped weather observations. PK/FK constraints are documented; Delta does
-- not enforce relational FK constraints in the same way as an OLTP database.
CREATE TABLE IF NOT EXISTS workspace.silver.location (
  location_id BIGINT, latitude DOUBLE NOT NULL, longitude DOUBLE NOT NULL, depth_km DOUBLE,
  place STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.silver.magnitude_type (
  magnitude_type_id BIGINT, code STRING NOT NULL
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.silver.earthquake (
  event_id STRING NOT NULL, event_time_utc TIMESTAMP NOT NULL, updated_at_utc TIMESTAMP,
  magnitude DOUBLE, location_id BIGINT, magnitude_type_id BIGINT, status STRING,
  significance INT, tsunami INT, network STRING, network_code STRING, source_url STRING,
  CONSTRAINT pk_earthquake PRIMARY KEY (event_id)
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.silver.weather_observation (
  event_id STRING NOT NULL, observation_time_utc TIMESTAMP NOT NULL, requested_at_utc TIMESTAMP,
  grid_latitude DOUBLE, grid_longitude DOUBLE, temperature_c DOUBLE, apparent_temperature_c DOUBLE,
  relative_humidity_pct INT, precipitation_mm DOUBLE, weather_code INT, wind_speed_kmh DOUBLE,
  CONSTRAINT pk_weather PRIMARY KEY (event_id, observation_time_utc)
) USING DELTA;

-- Gold star schema
CREATE TABLE IF NOT EXISTS workspace.gold.dim_date (
  date_key INT, date DATE, year INT, month INT, day INT, day_of_week INT, day_name STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.gold.dim_location (
  location_key BIGINT, latitude DOUBLE, longitude DOUBLE, place STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.gold.dim_magnitude_type (
  magnitude_type_key BIGINT, magnitude_type STRING
) USING DELTA;
CREATE TABLE IF NOT EXISTS workspace.gold.fact_earthquake (
  event_id STRING, date_key INT, location_key BIGINT, magnitude_type_key BIGINT,
  magnitude DOUBLE, depth_km DOUBLE, significance INT, tsunami INT, event_ts_utc TIMESTAMP,
  latitude DOUBLE, longitude DOUBLE, temperature_c DOUBLE, relative_humidity_pct INT,
  precipitation_mm DOUBLE, weather_code INT, wind_speed_kmh DOUBLE, url STRING
) USING DELTA;
