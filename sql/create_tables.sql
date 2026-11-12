-- ==============================================================================
-- Earthquake + Weather Data Pipeline
-- Week 4: Normalized (3NF) Relational Database Schema
-- File: sql/create_tables.sql
-- Compatible with SQLite, PostgreSQL, DuckDB, MySQL, and Spark SQL
-- ==============================================================================

-- 1. LOCATION Table (Normalized spatial dimension entity)
-- Stores distinct geographical coordinates and place descriptions.
-- Eliminates transitive dependencies between event records and geographic places.
CREATE TABLE IF NOT EXISTS location (
    location_id INTEGER PRIMARY KEY,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    depth_km REAL,
    place TEXT
);

-- 2. MAGNITUDE_TYPE Table (Normalized seismic calculation classification)
-- Stores standard seismological magnitude codes (e.g., md, ml, mb, mww).
-- Eliminates redundant string repetition across million-row event sets.
CREATE TABLE IF NOT EXISTS magnitude_type (
    magnitude_type_id INTEGER PRIMARY KEY,
    code TEXT NOT NULL UNIQUE
);

-- 3. EARTHQUAKE Table (Normalized core seismic event entity)
-- Stores verified seismic events with event-level metrics.
-- Foreign keys link to location and magnitude_type entities.
CREATE TABLE IF NOT EXISTS earthquake (
    event_id TEXT PRIMARY KEY,
    event_time_utc TEXT NOT NULL,
    updated_at_utc TEXT,
    magnitude REAL,
    location_id INTEGER NOT NULL REFERENCES location(location_id),
    magnitude_type_id INTEGER NOT NULL REFERENCES magnitude_type(magnitude_type_id),
    status TEXT,
    significance INTEGER,
    tsunami INTEGER DEFAULT 0,
    network TEXT,
    network_code TEXT,
    source_url TEXT,
    _ingested_at TEXT
);

-- 4. WEATHER_OBSERVATION Table (Normalized meteorological snapshot entity)
-- Stores time-stamped surface weather conditions at the epicenter at observation time.
-- Composite primary key: (event_id, observation_time_utc).
-- Foreign key references parent earthquake event.
CREATE TABLE IF NOT EXISTS weather_observation (
    event_id TEXT NOT NULL REFERENCES earthquake(event_id),
    observation_time_utc TEXT NOT NULL,
    requested_at_utc TEXT,
    grid_latitude REAL,
    grid_longitude REAL,
    temperature_c REAL,
    apparent_temperature_c REAL,
    relative_humidity_pct INTEGER,
    precipitation_mm REAL,
    weather_code INTEGER,
    wind_speed_kmh REAL,
    PRIMARY KEY (event_id, observation_time_utc)
);

-- Performance Indexes for Foreign Key Joins & Filtering
CREATE INDEX IF NOT EXISTS idx_earthquake_location ON earthquake(location_id);
CREATE INDEX IF NOT EXISTS idx_earthquake_magtype ON earthquake(magnitude_type_id);
CREATE INDEX IF NOT EXISTS idx_weather_event ON weather_observation(event_id);
