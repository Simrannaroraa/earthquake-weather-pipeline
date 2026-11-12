# Earthquake + Weather Data Pipeline (Track A)

An end-to-end data engineering project that joins public USGS earthquake events with Open-Meteo current-weather snapshots, lands validated JSON envelopes, loads data into a normalized (3NF) relational schema, and orchestrates transformations through Bronze, Silver, and Gold Delta Lake tables with PySpark on Databricks.

---

## Repository Map

```text
src/                          Python ingestion and transformation scripts
  ingest.py                   USGS + Open-Meteo ingestion with backoff, retry & pre-save validation
  load_data.py                Loads raw JSON into normalized 3NF database with FK enforcement
  simulate_pipeline.py        Offline end-to-end simulation (Bronze/Silver/Gold & SQL analytics)
  transform_bronze.py         Bronze raw Delta tables ingestion (Databricks)
  transform_silver.py         Silver 3NF normalization, parsing & deduplication (Databricks)
  transform_gold.py           Gold analytical star schema generation (Databricks)
  run_pipeline.py             Databricks entrypoint / spark-submit runner
sql/                          Database DDL and analytical reporting
  create_tables.sql           Normalized 3NF relational schema DDL (ANSI SQL / SQLite / Postgres)
  databricks_delta_tables.sql Unity Catalog DDL for Bronze, Silver (3NF), and Gold Delta tables
  transformations.sql         Analytical SQL queries over Gold star schema
landing_zone/                 Landing zone for raw JSON ingestion envelopes
  sample_data.json            Verified actual API capture from USGS & Open-Meteo
  run_*.json                  Live ingestion runs from USGS and Open-Meteo APIs (git-ignored)
tests/                        Automated test suite
  test_pipeline.py            5 comprehensive unit & contract tests for Week 4 deliverables
docs/                         Architecture, ER diagrams, data dictionary, and strategy docs
  er_diagram.png              Rendered high-resolution 3NF Entity Relationship Diagram
  er_diagram.md               Mermaid diagram and cardinality definitions
  schema_documentation.md     Full 3NF data dictionary, constraints, and normalization proofs
  deduplication_strategy.md   Formal deduplication, versioning, and idempotency strategy
  architecture.md             End-to-end pipeline architecture across landing, 3NF, and Lakehouse
  dashboard_spec.md           Databricks AI/BI Lakeview dashboard specification
  demo_guide.md               Minute-by-minute 5-minute video demo recording script
scripts/                      Automation and helper utilities
  generate_erd_image.py       Script used to generate docs/er_diagram.png via matplotlib
  upload_to_volume.sh         Helper script to upload landing files to Unity Catalog Volume
earthquake_pipeline_notebook.py  Self-contained Databricks notebook with widget parameters
databricks_job.json           Databricks Workflow multi-task scheduled job definition
databricks.yml                Databricks Asset Bundle (DAB) deployment config
requirements.txt              Local Python dependencies
```

---

## 1. Week 4: Local Execution & Submission Instructions

Python 3.10+ is recommended. Set up a virtual environment, install requirements, fetch live API data, and execute the 3NF data loader:

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate

# 2. Install dependencies
python3 -m pip install -r requirements.txt

# 3. Fetch live API data (USGS earthquakes + Open-Meteo weather snapshots)
python3 src/ingest.py --period day --min-magnitude 2.5 --limit 100 --weather-limit 25

# 4. Load raw JSON into normalized 3NF database (creates earthquake_3nf.db)
python3 src/load_data.py landing_zone --db earthquake_3nf.db

# 5. Run the automated Week 4 test suite
python3 -m unittest tests/test_pipeline.py
```

### Key Week 4 Features
1. **API Ingestion (`src/ingest.py`):**
   - Retries network errors, HTTP 429, and server errors with bounded exponential backoff and random jitter; honors `Retry-After`.
   - Validates response contracts (*before* saving) using `validate_api_response()`.
   - Saves immutable JSON envelopes into `landing_zone/`.
2. **Normalized 3NF Relational Schema (`sql/create_tables.sql`):**
   - Four normalized entities: `location`, `magnitude_type`, `earthquake`, and `weather_observation`.
   - Eliminates redundant attributes, repeating groups, and transitive dependencies.
   - Enforces primary and foreign key constraints.
3. **Data Loading & Idempotent Deduplication (`src/load_data.py`):**
   - Executes `sql/create_tables.sql` to initialize schema.
   - Parses raw GeoJSON coordinates, unpacks atomic attributes, and populates parent entities before children.
   - Idempotent deduplication: USGS updates retain the record with the latest `properties.updated` timestamp; weather retains the latest request for `(event_id, observation_time_utc)`.
4. **Documentation:**
   - Visual ER diagram: `docs/er_diagram.png` and `docs/er_diagram.md`.
   - Complete data dictionary and 3NF proofs: `docs/schema_documentation.md`.
   - Formal deduplication explanation: `docs/deduplication_strategy.md`.

---

## 2. Cloud Platform Deployment (Databricks)

1. Use your Databricks workspace with Serverless Standard v6 (or runtime with PySpark & Delta Lake).
2. Ensure catalog `workspace` and schema `bronze` exist with a Volume at `/Volumes/workspace/bronze/landing/`.
3. Upload `landing_zone/sample_data.json` or live run files using `scripts/upload_to_volume.sh` or the Databricks UI.
4. Import `earthquake_pipeline_notebook.py` into your Databricks Workspace and click **Run All**.
5. Query `workspace.gold.fact_earthquake` using `sql/transformations.sql`.
6. To schedule as a job, deploy `databricks_job.json` via Databricks Workflows or DAB (`databricks.yml`).

---

## Week 4 Requirements Compliance Checklist

- [x] **API Ingestion:** `src/ingest.py` fetches live data with backoff, retries, error handling, rate limiting, and pre-save schema validation.
- [x] **Dependencies:** `requirements.txt` is complete, accurate, and free of unused packages.
- [x] **Sample Ingested Data:** `landing_zone/sample_data.json` is an actual, validated API capture (not fabricated).
- [x] **Normalized Database Design (3NF):** High-res ER diagram (`docs/er_diagram.png`, `docs/er_diagram.md`) and formal documentation (`docs/schema_documentation.md`).
- [x] **SQL Table Creation:** `sql/create_tables.sql` creates normalized 3NF tables with correct types, PKs, FKs, and constraints.
- [x] **Data Loading:** `src/load_data.py` loads raw JSON, maintains referential integrity, and handles nulls/errors.
- [x] **Deduplication Strategy:** Documented in `docs/deduplication_strategy.md` and implemented in `src/load_data.py`.
- [x] **Automated Testing:** All unit & integration tests pass (`tests/test_pipeline.py`).
