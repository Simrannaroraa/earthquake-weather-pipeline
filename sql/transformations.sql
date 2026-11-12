-- Example analytics over Gold star schema.
-- Target catalog: workspace (adjust if using custom catalog name)
USE CATALOG workspace;

-- 1. Monthly Trends: Earthquake frequency, average magnitude, and average surface temperature
SELECT
    d.year,
    d.month,
    count(*) AS event_count,
    round(avg(f.magnitude), 2) AS avg_magnitude,
    round(avg(f.depth_km), 2) AS avg_depth_km,
    round(avg(f.temperature_c), 1) AS avg_temp_c
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_date d USING (date_key)
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- 2. Top Active Locations: Event count, maximum magnitude, and average significance
SELECT
    l.place,
    count(*) AS event_count,
    round(max(f.magnitude), 1) AS max_magnitude,
    round(avg(f.significance), 1) AS avg_significance
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_location l USING (location_key)
GROUP BY l.place
ORDER BY event_count DESC, max_magnitude DESC
LIMIT 20;

-- 3. Weather Conditions at Ingestion Time
-- NOTE: Weather is a current/forecast grid-cell snapshot at ingestion time,
-- not a historical reconstruction at the exact earthquake origin time.
SELECT
    f.weather_code,
    count(*) AS earthquakes,
    round(avg(f.magnitude), 2) AS avg_magnitude,
    round(avg(f.temperature_c), 1) AS avg_temp_c,
    round(avg(f.wind_speed_kmh), 1) AS avg_wind_kmh
FROM workspace.gold.fact_earthquake f
WHERE f.weather_code IS NOT NULL
GROUP BY f.weather_code
ORDER BY earthquakes DESC;

-- 4. Ingestion & Enrichment Coverage (Weather Match Rate)
SELECT
    count(*) AS total_earthquakes,
    count(f.temperature_c) AS enriched_with_weather,
    count(*) - count(f.temperature_c) AS without_weather,
    round(100.0 * count(f.temperature_c) / count(*), 1) AS weather_enrichment_pct
FROM workspace.gold.fact_earthquake f;

-- 5. High-Impact Events with Weather & Coordinates
SELECT
    f.event_id,
    f.event_ts_utc,
    l.place,
    f.magnitude,
    m.magnitude_type,
    f.depth_km,
    f.significance,
    f.tsunami,
    f.temperature_c,
    f.wind_speed_kmh,
    f.precipitation_mm,
    f.url
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_location l USING (location_key)
LEFT JOIN workspace.gold.dim_magnitude_type m USING (magnitude_type_key)
ORDER BY f.magnitude DESC, f.significance DESC
LIMIT 10;
