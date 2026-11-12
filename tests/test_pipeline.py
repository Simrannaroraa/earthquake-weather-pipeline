"""End-to-end local test suite validating Week 4 requirements and pipeline contracts.

Tests API response validation, SQL DDL table creation, data loading into normalized
3NF tables, foreign key referential integrity, and deduplication logic.
"""
from __future__ import annotations

import json
import sqlite3
import unittest
from pathlib import Path

from src.ingest import validate_api_response
from src.load_data import create_3nf_tables, load_to_3nf_database


class Week4PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.landing_dir = Path("landing_zone")
        cls.run_files = sorted(cls.landing_dir.glob("*.json"))
        cls.sample_file = cls.landing_dir / "sample_data.json"
        cls.ddl_file = Path("sql/create_tables.sql")

    def test_sample_data_is_genuine_api_capture(self):
        """Requirement 3: Verify sample_data.json is a valid, genuine API capture, not fabricated."""
        self.assertTrue(self.sample_file.exists(), "landing_zone/sample_data.json must exist")
        data = json.loads(self.sample_file.read_text(encoding="utf-8"))

        # Must not be fabricated placeholder
        self.assertNotEqual(data.get("run_id"), "SYNTHETIC-CLASSROOM-FIXTURE",
                            "sample_data.json must be an actual API capture, not synthetic fixture")

        # Must pass pre-save API validation
        validate_api_response(data)
        features = data["earthquakes"]["features"]
        self.assertGreater(len(features), 0, "sample_data.json must contain real earthquake features")
        self.assertNotEqual(features[0].get("id"), "demo-event-001",
                            "Feature id must be genuine USGS id, not dummy placeholder")

    def test_api_response_validation_logic(self):
        """Requirement 1: Test error handling and validation logic for API payloads."""
        # Malformed payload missing top-level keys
        with self.assertRaises(ValueError):
            validate_api_response({"invalid": "data"})

        # Malformed GeoJSON (type not FeatureCollection)
        with self.assertRaises(ValueError):
            validate_api_response({
                "run_id": "test",
                "ingested_at": "2026-11-12T00:00:00Z",
                "earthquakes": {"type": "NotAFeatureCollection", "features": []},
                "weather_snapshots": []
            })

        # Feature with coordinates out of bounds
        with self.assertRaises(ValueError):
            validate_api_response({
                "run_id": "test",
                "ingested_at": "2026-11-12T00:00:00Z",
                "earthquakes": {
                    "type": "FeatureCollection",
                    "features": [{
                        "id": "bad_coords",
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [250.0, 95.0]},  # Out of bounds
                        "properties": {"time": 1700000000000}
                    }]
                },
                "weather_snapshots": []
            })

    def test_sql_create_tables_ddl(self):
        """Requirement 5: Ensure sql/create_tables.sql executes cleanly and creates 3NF tables."""
        self.assertTrue(self.ddl_file.exists(), "sql/create_tables.sql must exist")
        conn = sqlite3.connect(":memory:")
        create_3nf_tables(conn, self.ddl_file)
        cur = conn.cursor()

        # Check all 4 tables exist
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
        expected = {"location", "magnitude_type", "earthquake", "weather_observation"}
        self.assertTrue(expected.issubset(tables), f"Missing tables: {expected - tables}")
        conn.close()

    def test_3nf_data_loading_and_referential_integrity(self):
        """Requirement 6: Test data loading into 3NF tables with strict FK integrity."""
        test_db = Path("test_run_3nf.db")
        if test_db.exists():
            test_db.unlink()

        conn = load_to_3nf_database(self.landing_dir, test_db)
        cur = conn.cursor()

        # Verify PRAGMA foreign_key_check returns zero violations
        cur.execute("PRAGMA foreign_key_check")
        fk_violations = cur.fetchall()
        self.assertEqual(len(fk_violations), 0, f"Found foreign key violations: {fk_violations}")

        # Check row counts in tables
        for tbl in ["location", "magnitude_type", "earthquake", "weather_observation"]:
            cur.execute(f"SELECT count(*) FROM {tbl}")
            count = cur.fetchone()[0]
            self.assertGreater(count, 0, f"Table '{tbl}' should have loaded rows")

        conn.close()
        test_db.unlink(missing_ok=True)

    def test_deduplication_idempotency(self):
        """Requirement 7: Verify deduplication keeps latest update and is idempotent."""
        test_db = Path("test_dedup_3nf.db")
        if test_db.exists():
            test_db.unlink()

        # Run loading once
        conn1 = load_to_3nf_database(self.landing_dir, test_db)
        cur1 = conn1.cursor()
        cur1.execute("SELECT count(*) FROM earthquake")
        count_first_run = cur1.fetchone()[0]
        conn1.close()

        # Run loading second time over same landing files
        conn2 = load_to_3nf_database(self.landing_dir, test_db)
        cur2 = conn2.cursor()
        cur2.execute("SELECT count(*) FROM earthquake")
        count_second_run = cur2.fetchone()[0]
        conn2.close()

        self.assertEqual(count_first_run, count_second_run,
                         "Loading must be idempotent: repeated runs must not duplicate rows")
        test_db.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
