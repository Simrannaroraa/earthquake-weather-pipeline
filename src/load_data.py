"""Week 4 Loader: Ingests raw JSON landing zone files into normalized (3NF) database tables.

Validates landing zone envelopes, executes the normalized 3NF table DDL,
populates entities (location, magnitude_type, earthquake, weather_observation),
and enforces deduplication on recurring or updated events.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def hash_key(*args: object) -> int:
    """Generate a deterministic 64-bit integer surrogate key."""
    s = "|".join("" if a is None else str(a) for a in args)
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:15], 16)


def create_3nf_tables(conn: sqlite3.Connection, ddl_path: Path | None = None) -> None:
    """Create normalized 3NF relational tables from sql/create_tables.sql."""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    ddl_file = ddl_path or Path("sql/create_tables.sql")
    if ddl_file.exists():
        sql_script = ddl_file.read_text(encoding="utf-8")
        cur.executescript(sql_script)
    else:
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS location (
            location_id INTEGER PRIMARY KEY,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            depth_km REAL,
            place TEXT
        );
        CREATE TABLE IF NOT EXISTS magnitude_type (
            magnitude_type_id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS earthquake (
            event_id TEXT PRIMARY KEY,
            event_time_utc TEXT NOT NULL,
            updated_at_utc TEXT,
            magnitude REAL,
            location_id INTEGER REFERENCES location(location_id),
            magnitude_type_id INTEGER REFERENCES magnitude_type(magnitude_type_id),
            status TEXT,
            significance INTEGER,
            tsunami INTEGER DEFAULT 0,
            network TEXT,
            network_code TEXT,
            source_url TEXT,
            _ingested_at TEXT
        );
        CREATE TABLE IF NOT EXISTS weather_observation (
            event_id TEXT REFERENCES earthquake(event_id),
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
        """)
    conn.commit()


def validate_envelopes(landing_dir: Path) -> list[Path]:
    """Validate JSON envelopes in landing zone."""
    run_files = sorted(landing_dir.glob("*.json"))
    if not run_files:
        raise FileNotFoundError(f"No JSON run files found in {landing_dir}")

    total_quakes = 0
    total_weather = 0
    for file in run_files:
        payload = json.loads(file.read_text(encoding="utf-8"))
        if not payload.get("run_id") or not payload.get("ingested_at"):
            raise ValueError(f"{file.name}: missing run_id or ingested_at")
        features = payload.get("earthquakes", {}).get("features", [])
        if not isinstance(features, list):
            raise ValueError(f"{file.name}: earthquakes.features must be an array")
        total_quakes += len(features)
        total_weather += len(payload.get("weather_snapshots", []))

    print(f"Validated {len(run_files)} run file(s) containing {total_quakes} earthquake(s) and {total_weather} weather snapshot(s).")
    return run_files


def load_to_3nf_database(landing_dir: Path, db_path: Path) -> sqlite3.Connection:
    """Parse raw JSON landing files and load into normalized 3NF tables with deduplication."""
    run_files = validate_envelopes(landing_dir)

    conn = sqlite3.connect(db_path)
    create_3nf_tables(conn)
    cur = conn.cursor()

    # In-memory staging for deterministic deduplication
    deduped_quakes: dict[str, dict] = {}
    deduped_weather: dict[tuple[str, str], dict] = {}

    for file in run_files:
        payload = json.loads(file.read_text(encoding="utf-8"))
        ingested_at = payload.get("ingested_at", datetime.now(timezone.utc).isoformat())

        # Process earthquakes
        for feat in payload.get("earthquakes", {}).get("features", []):
            eid = feat.get("id")
            props = feat.get("properties") or {}
            coords = feat.get("geometry", {}).get("coordinates") or []
            if not eid or not props.get("time") or len(coords) < 2:
                continue

            lon, lat = coords[0], coords[1]
            if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                continue

            updated_ms = props.get("updated") or 0

            # Deduplication: retain highest updated_ms, tie-break by ingested_at
            if eid not in deduped_quakes or (updated_ms, ingested_at) > (
                deduped_quakes[eid]["updated_ms"],
                deduped_quakes[eid]["_ingested_at"],
            ):
                t_ms = props["time"]
                event_ts = datetime.fromtimestamp(t_ms / 1000, tz=timezone.utc).isoformat()
                updated_ts = (
                    datetime.fromtimestamp(updated_ms / 1000, tz=timezone.utc).isoformat()
                    if updated_ms
                    else None
                )
                depth = coords[2] if len(coords) > 2 else None

                deduped_quakes[eid] = {
                    "event_id": eid,
                    "event_time_utc": event_ts,
                    "updated_at_utc": updated_ts,
                    "updated_ms": updated_ms,
                    "magnitude": props.get("mag"),
                    "place": props.get("place"),
                    "longitude": lon,
                    "latitude": lat,
                    "depth_km": depth,
                    "magnitude_type": props.get("magType") or "unknown",
                    "status": props.get("status"),
                    "significance": props.get("sig"),
                    "tsunami": props.get("tsunami"),
                    "network": props.get("net"),
                    "network_code": props.get("code"),
                    "source_url": props.get("url"),
                    "_ingested_at": ingested_at,
                }

        # Process weather snapshots
        for wx in payload.get("weather_snapshots", []):
            eid = wx.get("event_id")
            req_at = wx.get("requested_at")
            resp = wx.get("response") or {}
            curr = resp.get("current") or {}
            obs_time = curr.get("time")
            if not eid or not obs_time:
                continue

            wx_key = (eid, obs_time)
            # Deduplication: retain latest requested_at
            if wx_key not in deduped_weather or req_at > deduped_weather[wx_key]["requested_at_utc"]:
                deduped_weather[wx_key] = {
                    "event_id": eid,
                    "observation_time_utc": obs_time,
                    "requested_at_utc": req_at,
                    "grid_latitude": resp.get("latitude"),
                    "grid_longitude": resp.get("longitude"),
                    "temperature_c": curr.get("temperature_2m"),
                    "apparent_temperature_c": curr.get("apparent_temperature"),
                    "relative_humidity_pct": curr.get("relative_humidity_2m"),
                    "precipitation_mm": curr.get("precipitation"),
                    "weather_code": curr.get("weather_code"),
                    "wind_speed_kmh": curr.get("wind_speed_10m"),
                }

    # Populate 3NF Entities: LOCATION and MAGNITUDE_TYPE
    locations: dict[tuple, int] = {}
    mag_types: dict[str, int] = {}

    for q in deduped_quakes.values():
        loc_tuple = (q["latitude"], q["longitude"], q["depth_km"], q["place"])
        if loc_tuple not in locations:
            loc_id = hash_key(*loc_tuple)
            locations[loc_tuple] = loc_id
            cur.execute(
                "INSERT OR IGNORE INTO location VALUES (?, ?, ?, ?, ?)",
                (loc_id, q["latitude"], q["longitude"], q["depth_km"], q["place"]),
            )

        mt = q["magnitude_type"]
        if mt not in mag_types:
            mt_id = hash_key(mt)
            mag_types[mt] = mt_id
            cur.execute(
                "INSERT OR IGNORE INTO magnitude_type VALUES (?, ?)",
                (mt_id, mt),
            )

        cur.execute(
            """INSERT OR REPLACE INTO earthquake VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                q["event_id"],
                q["event_time_utc"],
                q["updated_at_utc"],
                q["magnitude"],
                locations[loc_tuple],
                mag_types[mt],
                q["status"],
                q["significance"],
                q["tsunami"],
                q["network"],
                q["network_code"],
                q["source_url"],
                q["_ingested_at"],
            ),
        )

    # Populate 3NF Entity: WEATHER_OBSERVATION
    for w in deduped_weather.values():
        if w["event_id"] in deduped_quakes:
            cur.execute(
                """INSERT OR REPLACE INTO weather_observation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    w["event_id"],
                    w["observation_time_utc"],
                    w["requested_at_utc"],
                    w["grid_latitude"],
                    w["grid_longitude"],
                    w["temperature_c"],
                    w["apparent_temperature_c"],
                    w["relative_humidity_pct"],
                    w["precipitation_mm"],
                    w["weather_code"],
                    w["wind_speed_kmh"],
                ),
            )

    conn.commit()

    print("\n--- Normalized 3NF Database Loading Summary ---")
    for tbl in ["location", "magnitude_type", "earthquake", "weather_observation"]:
        cur.execute(f"SELECT count(*) FROM {tbl}")
        print(f"  Table '{tbl}': {cur.fetchone()[0]} rows loaded")

    return conn


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", nargs="?", default="landing_zone", help="Landing zone folder")
    parser.add_argument("--db", default="earthquake_3nf.db", help="SQLite database output file")
    args = parser.parse_args()

    landing_path = Path(args.path)
    db_path = Path(args.db)

    print(f"Loading raw JSON from '{landing_path}' into 3NF database '{db_path}'...")
    conn = load_to_3nf_database(landing_path, db_path)

    # Sample Verification Query over Normalized Tables
    cur = conn.cursor()
    cur.execute("""
    SELECT e.event_id, e.event_time_utc, e.magnitude, m.code AS mag_type, l.place, w.temperature_c
    FROM earthquake e
    JOIN location l ON e.location_id = l.location_id
    JOIN magnitude_type m ON e.magnitude_type_id = m.magnitude_type_id
    LEFT JOIN weather_observation w ON e.event_id = w.event_id
    ORDER BY e.magnitude DESC
    LIMIT 5;
    """)
    print("\nSample Query from 3NF Tables (JOIN earthquake + location + magnitude_type + weather_observation):")
    print(f"{'Event ID':<12} | {'Time (UTC)':<22} | {'Mag':<4} | {'Type':<4} | {'Place':<36} | {'Temp (°C)':<8}")
    print("-" * 95)
    for row in cur.fetchall():
        eid, tm, mag, mtype, place, temp = row
        temp_str = f"{temp:.1f}" if temp is not None else "N/A"
        print(f"{eid:<12} | {tm[:19]:<22} | {mag:<4.1f} | {mtype:<4} | {place[:36]:<36} | {temp_str:<8}")

    conn.close()
    print(f"\n3NF Database successfully written to '{db_path}'.")


if __name__ == "__main__":
    main()
