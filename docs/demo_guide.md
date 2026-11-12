# 5-Minute Video Demo Recording Guide & Script

This guide provides a minute-by-minute script, on-screen actions, and key technical talking points for delivering an end-to-end demonstration of the **Track A: Earthquake + Weather Data Pipeline on Databricks**.

---

## ⏱️ Timeline Overview

| Minute | Segment | Focus Area | Key Visuals |
| :--- | :--- | :--- | :--- |
| **0:00 - 1:00** | Architecture & Contracts | Ingestion, Bronze/Silver/Gold flow, 3NF & Star Schemas | `docs/architecture.md`, `docs/er_diagram.md`, `docs/star_schema.md` |
| **1:00 - 2:00** | Live Ingestion & Validation | USGS GeoJSON + Open-Meteo weather snapshot ingestion | Terminal: `src/ingest.py`, `src/load_data.py`, `tests/test_pipeline.py` |
| **2:00 - 3:15** | Databricks Transformation | PySpark execution on Serverless Standard v6 | Databricks Notebook: Bronze, Silver, Gold cells |
| **3:15 - 4:15** | SQL Analytics & Schemas | Querying Gold star schema & 3NF tables in Unity Catalog | Databricks SQL Editor: `sql/transformations.sql` |
| **4:15 - 5:00** | Automation, Dashboard & Wrap-up | Databricks Workflow Job, Lakeview Dashboard, temporal nuance | `databricks_job.json`, `docs/dashboard_spec.md` |

---

## 🎙️ Step-by-Step Script & On-Screen Actions

### 0:00 – 1:00 | Architecture & Data Contracts

**Screen Action:** Open `README.md` and display the Mermaid diagrams from `docs/architecture.md` and `docs/er_diagram.md`.

**Speaker Script:**
> *"Welcome! In this demonstration, I will walk you through an end-to-end Track A data pipeline implemented on Databricks with Unity Catalog, PySpark, and Delta Lake.*
>
> *Our goal is to ingest public earthquake events from the USGS API, enrich them with real-time surface weather conditions from Open-Meteo, land raw JSON in a Unity Catalog Volume, and transform the data through Bronze, Silver, and Gold Delta layers.*
>
> *Architecturally, we satisfy two target modeling standards:*
> 1. *A **Normalized 3NF model** in Silver (`location`, `magnitude_type`, `earthquake`, and `weather_observation`), eliminating data redundancy.*
> 2. *An **Analytical Star Schema** in Gold (`fact_earthquake` joined to `dim_date`, `dim_location`, and `dim_magnitude_type`) optimized for high-performance SQL analytics."*

---

### 1:00 – 2:00 | Ingestion & Verification

**Screen Action:** Switch to terminal in project directory. Open `src/ingest.py` briefly, highlight the exponential backoff, jitter, and fair-use rate limiting. Then execute the ingestion and test suite.

**Commands to Run:**
```bash
# 1. Run live ingestion (fetches past 24h USGS events and Open-Meteo current weather)
python src/ingest.py --period day --min-magnitude 2.5 --limit 100 --weather-limit 25

# 2. Validate landing zone envelopes
python src/load_data.py landing_zone

# 3. Run the contract validation test suite
python -m unittest tests/test_pipeline.py
```

**Speaker Script:**
> *"Let's look at `src/ingest.py`. It requests USGS GeoJSON data and then matches each earthquake coordinate against Open-Meteo's weather endpoint. Notice our production resilience: it implements exponential backoff with jitter and respects HTTP 429 Retry-After headers.*
>
> *We save an immutable JSON envelope in `landing_zone/`. Running our local validator and unit test suite confirms that our live run contains real public earthquake data and valid weather snapshots conforming to all schema contracts."*

---

### 2:00 – 3:15 | Databricks PySpark Pipeline

**Screen Action:** Switch to the Databricks Workspace UI. Open `earthquake_pipeline_notebook.py` attached to Serverless Standard v6. Show the Volume `/Volumes/workspace/bronze/landing/`. Click **Run All**.

**Highlighting Cells:**
1. **Bronze Cell:** Show extraction of raw envelopes into `bronze.ingestion_runs`, `bronze.earthquake_events`, and `bronze.weather_snapshots`.
2. **Silver Cell:** Show deduplication logic.
   - Natural key: `event_id` (`feature.id`).
   - Window function: `row_number().over(Window.partitionBy("event_id").orderBy(col("updated_ms").desc(), col("_ingested_at").desc()))`.
   - Quality checks: `latitude.between(-90, 90)` and `longitude.between(-180, 180)`.
3. **Gold Cell:** Show deterministic dimension hashing with `xxhash64` and fact join.

**Speaker Script:**
> *"Now in Databricks, our self-contained notebook targets the `workspace` catalog. The JSON run file is placed in our Unity Catalog Volume.*
>
> *In Bronze, we preserve immutable raw payloads with ingestion metadata for full auditability and replay.*
>
> *In Silver, we parse coordinates, filter out malformed records, and handle USGS event updates deterministically. If USGS updates an event magnitude or location hours later, our window function retains the latest update. Furthermore, we split the entities into 3NF normalized tables.*
>
> *In Gold, we materialize a star schema where `dim_location` and `dim_magnitude_type` use deterministic surrogate keys, and `fact_earthquake` joins the latest weather snapshot."*

---

### 3:15 – 4:15 | Databricks SQL Analytics

**Screen Action:** Switch to Databricks SQL Editor. Run queries from `sql/transformations.sql`.

**Queries to Run:**
1. **Top Active Epicenters:**
   ```sql
   SELECT l.place, count(*) AS event_count, round(max(f.magnitude), 1) AS max_magnitude
   FROM workspace.gold.fact_earthquake f
   LEFT JOIN workspace.gold.dim_location l USING (location_key)
   GROUP BY l.place ORDER BY event_count DESC LIMIT 5;
   ```
2. **Weather Enrichment Coverage & Weather Breakdown:**
   ```sql
   SELECT count(*) AS total_quakes, count(temperature_c) AS enriched_with_weather,
          round(100.0 * count(temperature_c) / count(*), 1) AS coverage_pct
   FROM workspace.gold.fact_earthquake;
   ```

**Speaker Script:**
> *"Now let's query the Gold tables. Our first query groups by `dim_location`, surfacing the most active seismic zones.*
>
> *Notice our weather enrichment metric. We capped weather queries to 25 to respect Open-Meteo's fair use policy, so 25 out of our 35 earthquakes are enriched with temperature, wind speed, and precipitation.*
>
> ***Critical analytical nuance:*** *The weather fields represent a current/forecast snapshot at the time of pipeline ingestion, not historical atmospheric conditions at the exact moment the earthquake occurred underground. Our pipeline explicitly documents this temporal boundary to avoid misleading downstream analysts."*

---

### 4:15 – 5:00 | Workflow Automation, Dashboard & Wrap-up

**Screen Action:** Show `databricks_job.json` / Databricks Workflows UI, and display `docs/dashboard_spec.md`.

**Speaker Script:**
> *"For production operations, we've provided a Databricks Workflow Job definition (`databricks_job.json`) that can run on an automated cron schedule with automatic retries on Serverless compute.*
>
> *We also designed a complete Databricks AI/BI Lakeview dashboard specification in `docs/dashboard_spec.md` with geographic epicenter maps, magnitude-depth scatter plots, and drill-down tables.*
>
> *To summarize: we've delivered an end-to-end, tested, normalized, and star-modeled data pipeline from public APIs through Delta Lake Bronze, Silver, and Gold on Databricks. Thank you!"*

