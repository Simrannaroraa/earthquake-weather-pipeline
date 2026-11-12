"""Local offline pipeline simulation and query execution.

Simulates the Databricks Delta Lake Bronze, Silver (3NF), and Gold (Star schema)
transformations locally using SQLite. Validates table creation, deduplication,
and executes all analytics queries from sql/transformations.sql.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def hash_key(*args: object) -> int:
    """Deterministic 64-bit integer hash simulating Spark xxhash64."""
    s = "|".join("" if a is None else str(a) for a in args)
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:15], 16)


def run_simulation(landing_dir: Path = Path("landing_zone")) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # 1. DDL: Create Tables matching Databricks Delta Schemas
    cur.executescript("""
    -- BRONZE
    CREATE TABLE bronze_ingestion_runs (
        run_id TEXT, _ingested_at TEXT, _source_file TEXT, raw_json TEXT
    );
    CREATE TABLE bronze_earthquake_events (
        run_id TEXT, _ingested_at TEXT, _source_file TEXT, event_id TEXT, raw_json TEXT
    );
    CREATE TABLE bronze_weather_snapshots (
        run_id TEXT, _ingested_at TEXT, event_id TEXT, requested_at TEXT, raw_json TEXT
    );

    -- SILVER (Normalized 3NF)
    CREATE TABLE silver_location (
        location_id INTEGER PRIMARY KEY,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        depth_km REAL,
        place TEXT
    );
    CREATE TABLE silver_magnitude_type (
        magnitude_type_id INTEGER PRIMARY KEY,
        code TEXT NOT NULL UNIQUE
    );
    CREATE TABLE silver_earthquake (
        event_id TEXT PRIMARY KEY,
        event_time_utc TEXT NOT NULL,
        updated_at_utc TEXT,
        magnitude REAL,
        location_id INTEGER,
        magnitude_type_id INTEGER,
        status TEXT,
        significance INTEGER,
        tsunami INTEGER,
        network TEXT,
        network_code TEXT,
        source_url TEXT,
        _ingested_at TEXT
    );
    CREATE TABLE silver_weather_observation (
        event_id TEXT,
        observation_time_utc TEXT,
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
    CREATE TABLE silver_earthquakes (
        event_id TEXT PRIMARY KEY,
        event_ts_utc TEXT,
        updated_at_utc TEXT,
        magnitude REAL,
        place TEXT,
        longitude REAL,
        latitude REAL,
        depth_km REAL,
        magnitude_type TEXT,
        status TEXT,
        tsunami INTEGER,
        significance INTEGER,
        network TEXT,
        network_code TEXT,
        url TEXT,
        _ingested_at TEXT
    );

    -- GOLD (Analytical Star Schema)
    CREATE TABLE gold_dim_date (
        date_key INTEGER PRIMARY KEY,
        date TEXT,
        year INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week INTEGER,
        day_name TEXT
    );
    CREATE TABLE gold_dim_location (
        location_key INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        place TEXT
    );
    CREATE TABLE gold_dim_magnitude_type (
        magnitude_type_key INTEGER PRIMARY KEY,
        magnitude_type TEXT
    );
    CREATE TABLE gold_fact_earthquake (
        event_id TEXT PRIMARY KEY,
        date_key INTEGER,
        location_key INTEGER,
        magnitude_type_key INTEGER,
        magnitude REAL,
        depth_km REAL,
        significance INTEGER,
        tsunami INTEGER,
        event_ts_utc TEXT,
        latitude REAL,
        longitude REAL,
        temperature_c REAL,
        relative_humidity_pct INTEGER,
        precipitation_mm REAL,
        weather_code INTEGER,
        wind_speed_kmh REAL,
        url TEXT
    );
    """)

    # 2. BRONZE INGESTION
    run_files = sorted(landing_dir.glob("*.json"))
    for file in run_files:
        content = file.read_text(encoding="utf-8")
        payload = json.loads(content)
        run_id = payload["run_id"]
        ingested_at = payload.get("ingested_at", datetime.now(timezone.utc).isoformat())

        cur.execute(
            "INSERT INTO bronze_ingestion_runs VALUES (?, ?, ?, ?)",
            (run_id, ingested_at, file.name, content),
        )

        for feat in payload.get("earthquakes", {}).get("features", []):
            cur.execute(
                "INSERT INTO bronze_earthquake_events VALUES (?, ?, ?, ?, ?)",
                (run_id, ingested_at, file.name, feat.get("id"), json.dumps(feat)),
            )

        for wx in payload.get("weather_snapshots", []):
            cur.execute(
                "INSERT INTO bronze_weather_snapshots VALUES (?, ?, ?, ?, ?)",
                (
                    run_id,
                    ingested_at,
                    wx.get("event_id"),
                    wx.get("requested_at"),
                    json.dumps(wx.get("response")),
                ),
            )

    # 3. SILVER TRANSFORMATIONS (Deduplication + 3NF)
    cur.execute("SELECT raw_json, _ingested_at FROM bronze_earthquake_events")
    raw_events = cur.fetchall()

    deduped_events = {}
    for raw_json, ing_at in raw_events:
        feat = json.loads(raw_json)
        eid = feat.get("id")
        props = feat.get("properties") or {}
        coords = feat.get("geometry", {}).get("coordinates") or []
        if not eid or not props.get("time") or len(coords) < 2:
            continue
        lon, lat = coords[0], coords[1]
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            continue

        updated_ms = props.get("updated") or 0
        if eid not in deduped_events or (updated_ms, ing_at) > (
            deduped_events[eid]["updated_ms"],
            deduped_events[eid]["_ingested_at"],
        ):
            depth = coords[2] if len(coords) > 2 else None
            t_ms = props["time"]
            event_ts = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat()
            updated_ts = (
                datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc).isoformat()
                if updated_ms
                else None
            )

            deduped_events[eid] = {
                "event_id": eid,
                "event_ts_utc": event_ts,
                "updated_at_utc": updated_ts,
                "updated_ms": updated_ms,
                "magnitude": props.get("mag"),
                "place": props.get("place"),
                "longitude": lon,
                "latitude": lat,
                "depth_km": depth,
                "magnitude_type": props.get("magType"),
                "status": props.get("status"),
                "tsunami": props.get("tsunami"),
                "significance": props.get("sig"),
                "network": props.get("net"),
                "network_code": props.get("code"),
                "url": props.get("url"),
                "_ingested_at": ing_at,
            }

    # Populate Silver Normalized entities
    locations: dict[tuple, int] = {}
    mag_types: dict[str, int] = {}

    for e in deduped_events.values():
        loc_tuple = (e["latitude"], e["longitude"], e["depth_km"], e["place"])
        if loc_tuple not in locations:
            loc_id = hash_key(*loc_tuple)
            locations[loc_tuple] = loc_id
            cur.execute(
                "INSERT OR IGNORE INTO silver_location VALUES (?, ?, ?, ?, ?)",
                (loc_id, e["latitude"], e["longitude"], e["depth_km"], e["place"]),
            )

        mt = e["magnitude_type"] or "unknown"
        if mt not in mag_types:
            mt_id = hash_key(mt)
            mag_types[mt] = mt_id
            cur.execute(
                "INSERT OR IGNORE INTO silver_magnitude_type VALUES (?, ?)",
                (mt_id, mt),
            )

        loc_id = locations[loc_tuple]
        mt_id = mag_types[mt]

        cur.execute(
            """INSERT INTO silver_earthquake VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                e["event_id"],
                e["event_ts_utc"],
                e["updated_at_utc"],
                e["magnitude"],
                loc_id,
                mt_id,
                e["status"],
                e["significance"],
                e["tsunami"],
                e["network"],
                e["network_code"],
                e["url"],
                e["_ingested_at"],
            ),
        )

        cur.execute(
            """INSERT INTO silver_earthquakes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                e["event_id"],
                e["event_ts_utc"],
                e["updated_at_utc"],
                e["magnitude"],
                e["place"],
                e["longitude"],
                e["latitude"],
                e["depth_km"],
                e["magnitude_type"],
                e["status"],
                e["tsunami"],
                e["significance"],
                e["network"],
                e["network_code"],
                e["url"],
                e["_ingested_at"],
            ),
        )

    # Weather Silver deduplication
    cur.execute(
        "SELECT event_id, requested_at, raw_json FROM bronze_weather_snapshots"
    )
    raw_weather = cur.fetchall()
    deduped_weather = {}
    for eid, req_at, raw_json in raw_weather:
        if not eid or not raw_json:
            continue
        resp = json.loads(raw_json)
        curr = resp.get("current") or {}
        obs_time = curr.get("time")
        if not obs_time:
            continue
        key = (eid, obs_time)
        if key not in deduped_weather or req_at > deduped_weather[key]["requested_at"]:
            deduped_weather[key] = {
                "event_id": eid,
                "observation_time_utc": obs_time,
                "requested_at": req_at,
                "grid_lat": resp.get("latitude"),
                "grid_lon": resp.get("longitude"),
                "temp_c": curr.get("temperature_2m"),
                "app_temp_c": curr.get("apparent_temperature"),
                "rh_pct": curr.get("relative_humidity_2m"),
                "precip_mm": curr.get("precipitation"),
                "weather_code": curr.get("weather_code"),
                "wind_kmh": curr.get("wind_speed_10m"),
            }

    for w in deduped_weather.values():
        cur.execute(
            """INSERT OR REPLACE INTO silver_weather_observation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                w["event_id"],
                w["observation_time_utc"],
                w["requested_at"],
                w["grid_lat"],
                w["grid_lon"],
                w["temp_c"],
                w["app_temp_c"],
                w["rh_pct"],
                w["precip_mm"],
                w["weather_code"],
                w["wind_kmh"],
            ),
        )

    # 4. GOLD TRANSFORMATIONS (Star Schema)
    # Dim Date
    dates_seen = set()
    for e in deduped_events.values():
        dt = datetime.fromisoformat(e["event_ts_utc"])
        d_str = dt.strftime("%Y-%m-%d")
        if d_str not in dates_seen:
            dates_seen.add(d_str)
            d_key = int(dt.strftime("%Y%m%d"))
            cur.execute(
                "INSERT INTO gold_dim_date VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    d_key,
                    d_str,
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.weekday(),
                    dt.strftime("%A"),
                ),
            )

    # Dim Location & Dim Magnitude Type
    for (lat, lon, depth, place), loc_id in locations.items():
        cur.execute(
            "INSERT OR IGNORE INTO gold_dim_location VALUES (?, ?, ?, ?)",
            (loc_id, lat, lon, place),
        )

    for mt, mt_id in mag_types.items():
        cur.execute(
            "INSERT OR IGNORE INTO gold_dim_magnitude_type VALUES (?, ?)", (mt_id, mt)
        )

    # Latest weather per event for fact
    latest_weather_per_event = {}
    for w in deduped_weather.values():
        eid = w["event_id"]
        if (
            eid not in latest_weather_per_event
            or w["requested_at"] > latest_weather_per_event[eid]["requested_at"]
        ):
            latest_weather_per_event[eid] = w

    # Gold Fact
    for e in deduped_events.values():
        dt = datetime.fromisoformat(e["event_ts_utc"])
        date_key = int(dt.strftime("%Y%m%d"))
        loc_key = locations[(e["latitude"], e["longitude"], e["depth_km"], e["place"])]
        mag_key = mag_types[e["magnitude_type"] or "unknown"]
        wx = latest_weather_per_event.get(e["event_id"])

        cur.execute(
            """INSERT INTO gold_fact_earthquake VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                e["event_id"],
                date_key,
                loc_key,
                mag_key,
                e["magnitude"],
                e["depth_km"],
                e["significance"],
                e["tsunami"],
                e["event_ts_utc"],
                e["latitude"],
                e["longitude"],
                wx["temp_c"] if wx else None,
                wx["rh_pct"] if wx else None,
                wx["precip_mm"] if wx else None,
                wx["weather_code"] if wx else None,
                wx["wind_kmh"] if wx else None,
                e["url"],
            ),
        )

    conn.commit()
    return conn


def print_table(title: str, headers: list[str], rows: list[tuple]) -> None:
    print(f"\n{'='*20} {title} {'='*20}")
    if not rows:
        print("(No rows)")
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for idx, val in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(str(val if val is not None else "")))

    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    sep_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    print(header_line)
    print(sep_line)
    for row in rows:
        line = " | ".join(str(val if val is not None else "").ljust(col_widths[i]) for i, val in enumerate(row))
        print(line)


def main() -> None:
    print("Running local end-to-end pipeline simulation...")
    conn = run_simulation()
    cur = conn.cursor()

    # Verify counts
    print("\nLayer Table Row Counts:")
    for tbl in [
        "bronze_ingestion_runs",
        "bronze_earthquake_events",
        "bronze_weather_snapshots",
        "silver_location",
        "silver_magnitude_type",
        "silver_earthquake",
        "silver_weather_observation",
        "silver_earthquakes",
        "gold_dim_date",
        "gold_dim_location",
        "gold_dim_magnitude_type",
        "gold_fact_earthquake",
    ]:
        cur.execute(f"SELECT count(*) FROM {tbl}")
        print(f"  {tbl}: {cur.fetchone()[0]}")

    # Query 1: Top Active Locations
    cur.execute("""
    SELECT
        l.place,
        count(*) AS event_count,
        round(max(f.magnitude), 1) AS max_magnitude,
        round(avg(f.significance), 1) AS avg_significance
    FROM gold_fact_earthquake f
    LEFT JOIN gold_dim_location l ON f.location_key = l.location_key
    GROUP BY l.place
    ORDER BY event_count DESC, max_magnitude DESC
    LIMIT 5;
    """)
    print_table("Query 1: Top Active Locations", ["place", "event_count", "max_magnitude", "avg_significance"], cur.fetchall())

    # Query 2: Weather Enrichment Coverage
    cur.execute("""
    SELECT
        count(*) AS total_earthquakes,
        count(f.temperature_c) AS enriched_with_weather,
        count(*) - count(f.temperature_c) AS without_weather,
        round(100.0 * count(f.temperature_c) / count(*), 1) AS weather_enrichment_pct
    FROM gold_fact_earthquake f;
    """)
    print_table("Query 2: Weather Enrichment Coverage", ["total_quakes", "enriched_weather", "without_weather", "coverage_pct"], cur.fetchall())

    # Query 3: Weather Condition Breakdown
    cur.execute("""
    SELECT
        f.weather_code,
        count(*) AS earthquakes,
        round(avg(f.magnitude), 2) AS avg_magnitude,
        round(avg(f.temperature_c), 1) AS avg_temp_c,
        round(avg(f.wind_speed_kmh), 1) AS avg_wind_kmh
    FROM gold_fact_earthquake f
    WHERE f.weather_code IS NOT NULL
    GROUP BY f.weather_code
    ORDER BY earthquakes DESC;
    """)
    print_table("Query 3: Weather Breakdown", ["weather_code", "earthquakes", "avg_magnitude", "avg_temp_c", "avg_wind_kmh"], cur.fetchall())

    # Query 4: Highest Magnitude Events
    cur.execute("""
    SELECT
        f.event_id,
        f.event_ts_utc,
        l.place,
        f.magnitude,
        f.depth_km,
        f.temperature_c,
        f.wind_speed_kmh
    FROM gold_fact_earthquake f
    LEFT JOIN gold_dim_location l ON f.location_key = l.location_key
    ORDER BY f.magnitude DESC
    LIMIT 5;
    """)
    print_table("Query 4: Highest Magnitude Events", ["event_id", "event_time_utc", "place", "mag", "depth_km", "temp_c", "wind_kmh"], cur.fetchall())

    conn.close()
    print("\nSimulation completed successfully!")


if __name__ == "__main__":
    main()

