-- Databricks Unity Catalog Delta Lake DDL
-- Matches Bronze, Silver (3NF + curated), and Gold (Star Schema) tables
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE VOLUME IF NOT EXISTS workspace.bronze.landing;

-- Bronze: append-only raw JSON envelopes and extracted objects
CREATE TABLE IF NOT EXISTS workspace.bronze.ingestion_runs (
  run_id STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING,
  raw_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.earthquake_events (
  run_id STRING,
  _ingested_at TIMESTAMP,
  _source_file STRING,
  event_id STRING,
  raw_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.bronze.weather_snapshots (
  run_id STRING,
  _ingested_at TIMESTAMP,
  event_id STRING,
  requested_at TIMESTAMP,
  raw_json STRING
) USING DELTA;

-- Silver: Normalized 3NF tables
CREATE TABLE IF NOT EXISTS workspace.silver.location (
  location_id BIGINT,
  latitude DOUBLE NOT NULL,
  longitude DOUBLE NOT NULL,
  depth_km DOUBLE,
  place STRING,
  CONSTRAINT pk_silver_location PRIMARY KEY (location_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.magnitude_type (
  magnitude_type_id BIGINT,
  code STRING NOT NULL,
  CONSTRAINT pk_silver_mag_type PRIMARY KEY (magnitude_type_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.earthquake (
  event_id STRING NOT NULL,
  event_time_utc TIMESTAMP NOT NULL,
  updated_at_utc TIMESTAMP,
  magnitude DOUBLE,
  location_id BIGINT,
  magnitude_type_id BIGINT,
  status STRING,
  significance INT,
  tsunami INT,
  network STRING,
  network_code STRING,
  source_url STRING,
  _ingested_at TIMESTAMP,
  CONSTRAINT pk_silver_earthquake PRIMARY KEY (event_id)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.silver.weather_observation (
  event_id STRING NOT NULL,
  observation_time_utc TIMESTAMP NOT NULL,
  requested_at_utc TIMESTAMP,
  grid_latitude DOUBLE,
  grid_longitude DOUBLE,
  temperature_c DOUBLE,
  apparent_temperature_c DOUBLE,
  relative_humidity_pct INT,
  precipitation_mm DOUBLE,
  weather_code INT,
  wind_speed_kmh DOUBLE,
  CONSTRAINT pk_silver_weather PRIMARY KEY (event_id, observation_time_utc)
) USING DELTA;

-- Gold: Analytical Star Schema
CREATE TABLE IF NOT EXISTS workspace.gold.dim_date (
  date_key INT NOT NULL,
  date DATE NOT NULL,
  year INT,
  month INT,
  day INT,
  day_of_week INT,
  day_name STRING,
  CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_location (
  location_key BIGINT NOT NULL,
  latitude DOUBLE,
  longitude DOUBLE,
  place STRING,
  CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.dim_magnitude_type (
  magnitude_type_key BIGINT NOT NULL,
  magnitude_type STRING,
  CONSTRAINT pk_dim_mag_type PRIMARY KEY (magnitude_type_key)
) USING DELTA;

CREATE TABLE IF NOT EXISTS workspace.gold.fact_earthquake (
  event_id STRING NOT NULL,
  date_key INT,
  location_key BIGINT,
  magnitude_type_key BIGINT,
  magnitude DOUBLE,
  depth_km DOUBLE,
  significance INT,
  tsunami INT,
  event_ts_utc TIMESTAMP,
  latitude DOUBLE,
  longitude DOUBLE,
  temperature_c DOUBLE,
  relative_humidity_pct INT,
  precipitation_mm DOUBLE,
  weather_code INT,
  wind_speed_kmh DOUBLE,
  url STRING,
  CONSTRAINT pk_fact_earthquake PRIMARY KEY (event_id)
) USING DELTA;
