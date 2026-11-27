"""Week 5 local verification suite for Medallion architecture, Star Schema, and Dashboard.

Note: PySpark and Delta Lake are not installed in this local Python environment;
this suite validates PySpark/notebook syntax and Delta MERGE contracts statically,
and executes full end-to-end Bronze/Silver/Gold Star Schema + analytical query +
Streamlit dashboard verification using the local SQLite Medallion simulation over
real landing_zone/*.json data.
"""
from __future__ import annotations

import ast
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from dashboard import load_gold_dataframe
from src.simulate_pipeline import run_simulation


class Week5PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.landing_dir = Path("landing_zone")
        cls.conn: sqlite3.Connection = run_simulation(cls.landing_dir)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_pyspark_and_notebook_syntax_and_delta_merge(self):
        """Verify all PySpark scripts and Databricks notebook compile cleanly and use Delta MERGE."""
        pyspark_files = [
            Path("src/transform_bronze.py"),
            Path("src/transform_silver.py"),
            Path("src/transform_gold.py"),
            Path("src/run_pipeline.py"),
            Path("earthquake_pipeline_notebook.py"),
        ]
        for path in pyspark_files:
            self.assertTrue(path.exists(), f"Missing {path}")
            source = path.read_text(encoding="utf-8")
            ast.parse(source, filename=str(path))

        for merge_path in [
            Path("src/transform_bronze.py"),
            Path("src/transform_silver.py"),
            Path("src/transform_gold.py"),
            Path("earthquake_pipeline_notebook.py"),
        ]:
            text = merge_path.read_text(encoding="utf-8")
            self.assertIn("DeltaTable", text, f"{merge_path} must use DeltaTable for MERGE")
            self.assertIn(".merge(", text, f"{merge_path} must implement Delta MERGE")

        bronze_text = Path("src/transform_bronze.py").read_text(encoding="utf-8")
        self.assertIn("_metadata.file_path", bronze_text, "Use Unity Catalog _metadata.file_path")

    def test_databricks_configs_valid(self):
        """Verify databricks_job.json and databricks.yml structure."""
        job_cfg = json.loads(Path("databricks_job.json").read_text(encoding="utf-8"))
        self.assertIn("tasks", job_cfg)
        self.assertIn("environments", job_cfg)
        self.assertEqual(job_cfg["tasks"][0]["environment_key"], "serverless_standard_v6")

        yml_text = Path("databricks.yml").read_text(encoding="utf-8")
        self.assertIn("earthquake_pipeline_notebook.py", yml_text)
        self.assertIn("serverless_standard_v6", yml_text)

    def test_bronze_ingestion_counts(self):
        """Verify Bronze tables capture all landing runs, features, and weather snapshots."""
        cur = self.conn.cursor()
        run_files = list(self.landing_dir.glob("*.json"))
        cur.execute("SELECT count(*) FROM bronze_ingestion_runs")
        self.assertEqual(cur.fetchone()[0], len(run_files))

        cur.execute("SELECT count(*) FROM bronze_earthquake_events")
        self.assertGreater(cur.fetchone()[0], 0)

        cur.execute("SELECT count(*) FROM bronze_weather_snapshots")
        self.assertGreater(cur.fetchone()[0], 0)

    def test_silver_deduplication_and_data_quality(self):
        """Verify Silver deduplicates updated events and filters invalid coordinates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_landing = Path(tmpdir)
            # Run 1: automatic mag 4.0 + invalid coordinate record
            run1 = {
                "run_id": "20261101T000000Z",
                "ingested_at": "2026-11-01T00:00:00+00:00",
                "earthquakes": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "id": "eq_dup_1",
                            "geometry": {"coordinates": [-120.0, 35.0, 10.0]},
                            "properties": {"mag": 4.0, "place": "Test CA", "time": 1793491200000, "updated": 1793491300000, "status": "automatic", "magType": "ml"},
                        },
                        {
                            "id": "eq_bad_coord",
                            "geometry": {"coordinates": [999.0, 120.0, 5.0]},
                            "properties": {"mag": 3.0, "place": "Invalid", "time": 1793491200000, "updated": 1793491200000},
                        },
                    ],
                },
                "weather_snapshots": [],
            }
            # Run 2: reviewed mag 4.3 update for eq_dup_1
            run2 = {
                "run_id": "20261101T010000Z",
                "ingested_at": "2026-11-01T01:00:00+00:00",
                "earthquakes": {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "id": "eq_dup_1",
                            "geometry": {"coordinates": [-120.0, 35.0, 10.0]},
                            "properties": {"mag": 4.3, "place": "Test CA", "time": 1793491200000, "updated": 1793494800000, "status": "reviewed", "magType": "mw"},
                        }
                    ],
                },
                "weather_snapshots": [],
            }
            (tmp_landing / "run_1.json").write_text(json.dumps(run1), encoding="utf-8")
            (tmp_landing / "run_2.json").write_text(json.dumps(run2), encoding="utf-8")

            conn = run_simulation(tmp_landing)
            cur = conn.cursor()
            cur.execute("SELECT event_id, magnitude, status FROM silver_earthquakes")
            rows = cur.fetchall()
            conn.close()

            self.assertEqual(len(rows), 1, "Invalid coordinates must be dropped and duplicates merged")
            self.assertEqual(rows[0], ("eq_dup_1", 4.3, "reviewed"))

    def test_gold_star_schema_integrity_and_grain(self):
        """Verify Gold fact_earthquake grain and referential integrity to all dimensions."""
        cur = self.conn.cursor()

        cur.execute("SELECT count(*) FROM silver_earthquakes")
        silver_count = cur.fetchone()[0]
        cur.execute("SELECT count(*), count(DISTINCT event_id) FROM gold_fact_earthquake")
        fact_count, fact_distinct = cur.fetchone()

        self.assertEqual(fact_count, silver_count, "Fact grain must equal unique Silver events")
        self.assertEqual(fact_count, fact_distinct, "Fact event_id must be unique")

        # Referential integrity checks (no orphaned non-null FKs)
        cur.execute("SELECT count(*) FROM gold_fact_earthquake f LEFT JOIN gold_dim_date d ON f.date_key = d.date_key WHERE d.date_key IS NULL")
        self.assertEqual(cur.fetchone()[0], 0, "Orphaned date_key in fact_earthquake")

        cur.execute("SELECT count(*) FROM gold_fact_earthquake f LEFT JOIN gold_dim_location l ON f.location_key = l.location_key WHERE l.location_key IS NULL")
        self.assertEqual(cur.fetchone()[0], 0, "Orphaned location_key in fact_earthquake")

        cur.execute("SELECT count(*) FROM gold_fact_earthquake f LEFT JOIN gold_dim_magnitude_type m ON f.magnitude_type_key = m.magnitude_type_key WHERE m.magnitude_type_key IS NULL")
        self.assertEqual(cur.fetchone()[0], 0, "Orphaned magnitude_type_key in fact_earthquake")

    def test_analytical_queries(self):
        """Verify analytical queries over Gold tables return valid metrics."""
        cur = self.conn.cursor()
        cur.execute("""
            SELECT d.year, d.month, count(*), round(avg(f.magnitude), 2)
            FROM gold_fact_earthquake f
            LEFT JOIN gold_dim_date d ON f.date_key = d.date_key
            GROUP BY d.year, d.month
        """)
        monthly = cur.fetchall()
        self.assertGreater(len(monthly), 0)

        cur.execute("""
            SELECT count(*), count(temperature_c)
            FROM gold_fact_earthquake
        """)
        total, with_wx = cur.fetchone()
        self.assertGreater(total, 0)
        self.assertGreater(with_wx, 0)

    def test_streamlit_dashboard_renders(self):
        """Verify Streamlit dashboard loads Gold data and renders without errors."""
        df = load_gold_dataframe(self.landing_dir)
        self.assertFalse(df.empty)
        self.assertIn("magnitude", df.columns)
        self.assertIn("weather_condition", df.columns)

        from streamlit.testing.v1 import AppTest

        at = AppTest.from_file("dashboard.py", default_timeout=15)
        at.run()
        self.assertFalse(at.exception, f"Streamlit app raised exception: {at.exception}")
        self.assertGreaterEqual(len(at.metric), 5, "Dashboard should render 5 KPI metrics")


if __name__ == "__main__":
    unittest.main()

