# Databricks AI/BI Lakeview Dashboard Specification

This document specifies the design, layout, and SQL datasets for the **Earthquake & Weather Intelligence Dashboard** built on the Gold layer in Unity Catalog (`workspace.gold`).

---

## 1. Dashboard Overview

- **Target Audience:** Data Engineering Reviewers, Seismological Analysts, Incident Response Teams.
- **Refresh Cadence:** Triggered on pipeline completion or scheduled daily.
- **Primary Catalog:** `workspace`
- **Underlying Tables:**
  - `workspace.gold.fact_earthquake`
  - `workspace.gold.dim_location`
  - `workspace.gold.dim_date`
  - `workspace.gold.dim_magnitude_type`

---

## 2. Layout Grid and Visualizations

```text
+----------------------------------------------------------------------------------------------------+
|                                    GLOBAL FILTERS & DATE SELECTOR                                  |
|   [ Date Range: Last 30 Days ]   [ Minimum Magnitude: 2.5 ]   [ Region/Place Search: All ]         |
+----------------------------------------------------------------------------------------------------+
|  KPI 1: Total Earthquakes  |  KPI 2: Max Magnitude  |  KPI 3: Weather Coverage  |  KPI 4: Tsunami Alerts|
|          36 Events         |          4.5 M         |          72.2 %          |          0 Events   |
+----------------------------------------------------------------------------------------------------+
|  WIDGET 5: Global Earthquake Epicenter Map (Geographic Scatter)                                    |
|  - Latitude vs Longitude                                                                           |
|  - Bubble size: Magnitude                                                                          |
|  - Bubble color: Depth (km) or Significance                                                        |
+---------------------------------------------------------+------------------------------------------+
|  WIDGET 6: Magnitude & Depth Distribution               |  WIDGET 7: Top Active Epicenters         |
|  - Scatter Plot: Magnitude vs Depth (km)                |  - Horizontal Bar Chart                  |
|  - Color: Magnitude Type                                |  - Top 10 locations by event count       |
+---------------------------------------------------------+------------------------------------------+
|  WIDGET 8: Surface Temperature vs Magnitude             |  WIDGET 9: Weather Condition Breakdown   |
|  - Box plot / Scatter: Surface Temp (°C) vs Magnitude   |  - Donut Chart by WMO Weather Code       |
|  - Note: Weather at ingestion time                      |  - Clear vs Rain vs Overcast             |
+---------------------------------------------------------+------------------------------------------+
|  WIDGET 10: Recent Significant Events Detail Table                                                 |
|  - Event ID, Timestamp (UTC), Place, Magnitude, Depth, Weather Temp, Wind Speed, USGS Link         |
+----------------------------------------------------------------------------------------------------+
```

---

## 3. Dataset Queries

### Dataset 1: Summary KPIs (`kpi_summary`)
```sql
SELECT
    count(*) AS total_events,
    max(magnitude) AS max_magnitude,
    avg(magnitude) AS avg_magnitude,
    sum(CASE WHEN tsunami = 1 THEN 1 ELSE 0 END) AS tsunami_events,
    count(temperature_c) AS weather_enriched_events,
    round(100.0 * count(temperature_c) / count(*), 1) AS weather_coverage_pct
FROM workspace.gold.fact_earthquake;
```

### Dataset 2: Geospatial Map Points (`epicenter_map`)
```sql
SELECT
    f.event_id,
    f.latitude,
    f.longitude,
    l.place,
    f.magnitude,
    f.depth_km,
    f.significance,
    f.event_ts_utc,
    f.temperature_c,
    f.wind_speed_kmh,
    f.url
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_location l USING (location_key)
WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL;
```
- **Visualization:** Point Map
  - **Latitude:** `latitude`
  - **Longitude:** `longitude`
  - **Size:** `magnitude`
  - **Color:** `depth_km`
  - **Tooltip:** `place`, `magnitude`, `depth_km`, `temperature_c`

### Dataset 3: Top Active Regions (`top_active_places`)
```sql
SELECT
    l.place,
    count(*) AS event_count,
    round(max(f.magnitude), 1) AS max_magnitude,
    round(avg(f.significance), 1) AS avg_significance
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_location l USING (location_key)
GROUP BY l.place
ORDER BY event_count DESC, max_magnitude DESC
LIMIT 10;
```
- **Visualization:** Horizontal Bar Chart
  - **Y-axis:** `place`
  - **X-axis:** `event_count`

### Dataset 4: Magnitude vs Depth Scatter (`mag_vs_depth`)
```sql
SELECT
    f.event_id,
    f.magnitude,
    f.depth_km,
    f.significance,
    m.magnitude_type
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_magnitude_type m USING (magnitude_type_key);
```
- **Visualization:** Scatter Plot
  - **X-axis:** `magnitude`
  - **Y-axis:** `depth_km` (inverted or standard)
  - **Color by:** `magnitude_type`

### Dataset 5: Weather Condition Distribution (`weather_distribution`)
```sql
SELECT
    CASE f.weather_code
        WHEN 0 THEN 'Clear sky'
        WHEN 1 THEN 'Mainly clear'
        WHEN 2 THEN 'Partly cloudy'
        WHEN 3 THEN 'Overcast'
        WHEN 45 THEN 'Fog'
        WHEN 51 THEN 'Light drizzle'
        WHEN 61 THEN 'Slight rain'
        WHEN 63 THEN 'Moderate rain'
        WHEN 65 THEN 'Heavy rain'
        WHEN 80 THEN 'Rain showers'
        ELSE concat('WMO Code ', coalesce(cast(f.weather_code AS STRING), 'Unknown'))
    END AS weather_condition,
    count(*) AS event_count,
    round(avg(f.temperature_c), 1) AS avg_temperature_c,
    round(avg(f.wind_speed_kmh), 1) AS avg_wind_kmh
FROM workspace.gold.fact_earthquake f
WHERE f.weather_code IS NOT NULL
GROUP BY f.weather_code
ORDER BY event_count DESC;
```
- **Visualization:** Donut Chart
  - **Slices:** `weather_condition`
  - **Values:** `event_count`

### Dataset 6: Detailed Events Table (`events_drilldown`)
```sql
SELECT
    f.event_id,
    date_format(f.event_ts_utc, 'yyyy-MM-dd HH:mm:ss') AS event_time_utc,
    l.place,
    f.magnitude,
    m.magnitude_type,
    round(f.depth_km, 1) AS depth_km,
    f.significance,
    CASE WHEN f.tsunami = 1 THEN 'Yes' ELSE 'No' END AS tsunami_alert,
    round(f.temperature_c, 1) AS surface_temp_c,
    round(f.wind_speed_kmh, 1) AS wind_kmh,
    round(f.precipitation_mm, 2) AS precip_mm,
    f.url AS usgs_report_url
FROM workspace.gold.fact_earthquake f
LEFT JOIN workspace.gold.dim_location l USING (location_key)
LEFT JOIN workspace.gold.dim_magnitude_type m USING (magnitude_type_key)
ORDER BY f.event_ts_utc DESC;
```
- **Visualization:** Table with URL link on `usgs_report_url`.

---

## 4. How to Import into Databricks Lakeview

1. Navigate to **Dashboards** in the left sidebar of your Databricks workspace.
2. Click **Create Dashboard** (Lakeview).
3. Connect the dataset tab to the queries listed above using SQL Warehouse or Serverless compute.
4. Drag and drop visualization widgets onto the canvas following the layout diagram in Section 2.
5. Publish the dashboard and grant read access to your reviewers.

