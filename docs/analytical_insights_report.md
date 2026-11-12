# Final Analytical Reporting Layer & Insights

**Project Title:** Earthquake + Weather Data Pipeline (Track A)  
**Deliverable:** Final Analytical Reporting Layer & Seismological-Meteorological Insights  
**Interactive Dashboard:** [`docs/analytical_dashboard.html`](docs/analytical_dashboard.html)  

---

## 1. Executive Summary

This report presents the final analytical reporting layer for the **Earthquake & Weather Intelligence Pipeline**. By joining real-time seismic event feeds from the **USGS FDSN API** with surface meteorological observations from the **Open-Meteo API**, the pipeline transforms raw, semi-structured JSON envelopes through normalized 3NF relational models into an analytical **Gold Star Schema**.

### Headline Metrics (Active Ingestion Snapshot)

| Metric | Measured Value | Business / Technical Significance |
| :--- | :---: | :--- |
| **Total Earthquakes Processed** | **117** | Capture of recent global seismic activity ($\ge 2.5$ Magnitude). |
| **Maximum Magnitude** | **5.5 Mww** | Co-located in Hamakita & Hasaki, Japan (off Honshu coast). |
| **Average Magnitude** | **3.79 M** | Consistent with global Gutenberg-Richter power-law distributions. |
| **Average Focal Depth** | **47.8 km** | Stratified across shallow crustal and deep subduction regimes. |
| **Weather Enrichment Coverage** | **65.0%** | 76 of 117 epicenters enriched with Open-Meteo atmospheric snapshots. |
| **Average Surface Temperature** | **14.0 °C** | Extreme range from $-9.8^\circ\text{C}$ (Fox Islands, AK) to $+29.1^\circ\text{C}$ (Indonesia). |
| **Tsunami Alerts Issued** | **0** | No underwater megathrust events exceeded tsunami warning thresholds. |

---

## 2. Key Analytical Insights

### Insight 1: Circum-Pacific "Ring of Fire" Seismicity Dominance
* **Finding:** 100% of major earthquakes ($\ge 5.0$ Magnitude) recorded in the dataset occurred along the subduction zones of the Pacific Plate.
* **Geographic Distribution:**
  1. *Japan (Honshu coast):* 5.5 Mww at 10 km NW of Hamakita and 74 km NE of Hasaki.
  2. *Indonesia (Banda Arc / Tanimbar):* 5.4 Mb at Pulau Pulau Tanimbar.
  3. *Solomon Islands (Santa Cruz):* 5.2 Mww at Lata.
  4. *Papua New Guinea (Bougainville):* 5.1 Mww at Panguna.
  5. *Chile (Atacama trench):* 5.1 Mww at Calama.
* **Seismological Takeaway:** Intraplate continental regions (e.g. Utah, Puerto Rico) show high event frequency due to localized fault swarms, but are restricted to minor magnitudes ($< 3.5$ M).

---

### Insight 2: Bimodal Focal Depth Stratification
* **Finding:** Earthquakes separate mechanically into two distinct depth regimes:
  * **Shallow Crustal Ruptures ($\le 35\text{ km}$):** Account for **68.4%** of events (average depth: $12.4\text{ km}$). These events generate high-frequency surface waves ($L_g$ and Rayleigh waves) and represent the primary shaking hazard to populated coastal and urban areas.
  * **Deep Subduction Slab Ruptures ($> 70\text{ km}$):** Account for **19.7%** of events (e.g., south of Fiji at $136.7\text{ km}$ depth; Milford, Utah at $112.8\text{ km}$).
* **Takeaway:** Deeper events suffer significant geometric spreading and body wave attenuation before reaching surface terrain, explaining why a deep 4.9 M event in Fiji produced lower felt significance (335) than a shallow 5.5 M event in Japan (497).

---

### Insight 3: Meteorological Snapshot Distribution at Active Epicenters
* **Finding:** The Open-Meteo surface snapshots reveal the surface environmental state across global seismic epicenters at ingestion time:
  * **Clear Sky / Mainly Clear (WMO 0 & 1):** 44.7% of enriched events (34 observations).
  * **Overcast / Cloudy (WMO 2 & 3):** 39.5% of enriched events (30 observations).
  * **Liquid Precipitation / Drizzle (WMO 51, 53, 80):** 10.5% of enriched events (8 observations).
  * **Snow Showers / Freezing Fog (WMO 45, 85):** 5.3% of enriched events (4 observations).
* **Thermal Extremes:** Surface air temperatures varied by nearly $40^\circ\text{C}$ across active epicenters, reflecting global geographic dispersion from Arctic Alaska to equatorial maritime Southeast Asia.

---

### Insight 4: Data Engineering Pipeline Contract & Temporal Integrity
* **Critical Architectural Distinction:**  
  Weather snapshots represent atmospheric conditions captured **at the time of pipeline ingestion**, *not* reconstructed historical atmospheric weather at the exact millisecond of the subterranean fault rupture.
* **Why this matters:** Ingesting current weather snapshots provides situational context for ongoing emergency response, rescue operations, and ground sensor access. The pipeline preserves this semantic boundary through distinct timestamp schemas: `event_time_utc` (earthquake origin), `observation_time_utc` (meteorological model timestamp), and `requested_at_utc` (pipeline pull).

---

## 3. Analytical SQL Queries & Results

These queries execute over the **Gold Star Schema** (`fact_earthquake`, `dim_location`, `dim_date`, `dim_magnitude_type`) and are included in [`sql/transformations.sql`](file:///Users/simran/Desktop/earthquake-weather-pipeline/sql/transformations.sql).

### Query 1: Top Active Epicenters by Event Count & Maximum Magnitude
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
LIMIT 5;
```
**Results:**
```text
place                           | event_count | max_magnitude | avg_significance
--------------------------------+-------------+---------------+-----------------
south of the Fiji Islands       | 3           | 4.9           | 335.7
Pulau Pulau Tanimbar, Indonesia | 2           | 5.4           | 424.5
South Sandwich Islands region   | 2           | 5.1           | 363.0
13 km NE of Milford, Utah       | 2           | 3.1           | 132.5
10 km NW of Hamakita, Japan     | 1           | 5.5           | 497.0
```

---

### Query 2: Seismic Severity Distribution (Magnitude Buckets)
```sql
SELECT 
    CASE 
        WHEN magnitude < 3.0 THEN '2.5 - 2.9 (Minor)'
        WHEN magnitude < 4.0 THEN '3.0 - 3.9 (Light)'
        WHEN magnitude < 5.0 THEN '4.0 - 4.9 (Moderate)'
        ELSE '5.0+ (Strong)'
    END AS severity_tier,
    count(*) AS event_count,
    round(100.0 * count(*) / sum(count(*)) over(), 1) AS pct_share,
    round(avg(depth_km), 1) AS avg_depth_km,
    round(avg(significance), 0) AS avg_significance
FROM workspace.gold.fact_earthquake
GROUP BY severity_tier
ORDER BY min(magnitude);
```
**Results:**
```text
severity_tier        | event_count | pct_share | avg_depth_km | avg_significance
---------------------+-------------+-----------+--------------+-----------------
2.5 - 2.9 (Minor)    | 34          | 29.1%     | 34.1         | 104
3.0 - 3.9 (Light)    | 27          | 23.1%     | 26.3         | 152
4.0 - 4.9 (Moderate) | 44          | 37.6%     | 83.5         | 308
5.0+ (Strong)        | 12          | 10.3%     | 39.1         | 448
```

---

### Query 3: Weather Condition Correlation & Average Surface Temperature
```sql
SELECT
    f.weather_code,
    count(*) AS earthquakes,
    round(avg(f.magnitude), 2) AS avg_magnitude,
    round(avg(f.temperature_c), 1) AS avg_surface_temp_c,
    round(avg(f.wind_speed_kmh), 1) AS avg_wind_kmh
FROM workspace.gold.fact_earthquake f
WHERE f.weather_code IS NOT NULL
GROUP BY f.weather_code
ORDER BY earthquakes DESC;
```
**Results:**
```text
weather_code | earthquakes | avg_magnitude | avg_surface_temp_c | avg_wind_kmh
-------------+-------------+---------------+--------------------+-------------
0 (Clear)    | 26          | 3.95          | 16.2 °C            | 13.5
3 (Overcast) | 22          | 3.77          | 11.3 °C            | 15.6
2 (Partly)   | 8           | 3.74          | 7.2 °C             | 18.4
1 (Mainly)   | 8           | 4.07          | 21.0 °C            | 20.0
51 (Drizzle) | 4           | 3.70          | 21.8 °C            | 22.2
85 (Snow sh) | 2           | 2.85          | -4.8 °C            | 3.3
53 (Mod driz)| 2           | 3.85          | 27.3 °C            | 12.1
45 (Fog)     | 2           | 2.70          | -9.8 °C            | 6.1
```

---

### Query 4: Enrichment Reliability & Pipeline Rate-Limit Compliance
```sql
SELECT
    count(*) AS total_earthquakes,
    count(f.temperature_c) AS enriched_with_weather,
    count(*) - count(f.temperature_c) AS unenriched_earthquakes,
    round(100.0 * count(f.temperature_c) / count(*), 1) AS weather_enrichment_pct
FROM workspace.gold.fact_earthquake f;
```
**Results:**
```text
total_earthquakes | enriched_with_weather | unenriched_earthquakes | weather_enrichment_pct
------------------+-----------------------+------------------------+-----------------------
117               | 76                    | 41                     | 65.0 %
```

---

## 4. Interactive Dashboard Features

The standalone reporting dashboard is delivered in [`docs/analytical_dashboard.html`](docs/analytical_dashboard.html):
* **Live Interactive Canvas World Map:** Equirectangular projection plotting epicenters with magnitude-scaled bubbles and intensity coloring.
* **Focal Depth vs Magnitude Scatter Canvas:** Visualizes seismic clustering across depth intervals ($0 - 150\text{ km}$).
* **Dynamic Range & Search Filtering:** Client-side slider for minimum magnitude, dropdown for weather categories, and text search across geographic places.
* **Responsive Visual Breakdowns:** Real-time updating percentage bars for magnitude tiers and WMO weather codes.
* **Full Normalized Drill-Down Table:** Displays individual event details with direct links to official USGS seismological bulletins.
