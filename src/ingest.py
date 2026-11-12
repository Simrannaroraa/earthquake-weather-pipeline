"""Fetch recent USGS earthquakes and Open-Meteo weather snapshots.

Writes one immutable JSON envelope per run into landing_zone. Public APIs only;
no credentials are required. Open-Meteo's free endpoint has fair-use limits.
"""
from __future__ import annotations
import argparse, json, logging, os, random, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
import requests

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
METEO_URL = "https://api.open-meteo.com/v1/forecast"
LOG = logging.getLogger("earthquake_pipeline.ingest")


def request_json(session: requests.Session, url: str, params: dict[str, Any], *, attempts: int = 5) -> dict[str, Any]:
    """GET JSON with bounded exponential backoff, jitter and Retry-After support."""
    for attempt in range(attempts):
        try:
            response = session.get(url, params=params, timeout=(10, 45))
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt == attempts - 1:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else min(30, 2 ** attempt) + random.random()
                LOG.warning("Retryable HTTP %s from %s; wait %.1fs", response.status_code, response.url, delay)
                time.sleep(delay)
                continue
            response.raise_for_status()
            return response.json()
        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == attempts - 1:
                raise
            delay = min(30, 2 ** attempt) + random.random()
            LOG.warning("Request failed (%s); retry %d/%d in %.1fs", exc, attempt + 1, attempts, delay)
            time.sleep(delay)
    raise RuntimeError("Request attempts exhausted")


def fetch_weather(session: requests.Session, latitude: float, longitude: float) -> dict[str, Any]:
    return request_json(session, METEO_URL, {
        "latitude": latitude, "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,weather_code,wind_speed_10m",
        "timezone": "UTC",
    })


def validate_api_response(payload: dict[str, Any]) -> None:
    """Validate raw API payload structure and data contracts prior to landing."""
    if not isinstance(payload, dict):
        raise ValueError("Payload must be a dictionary")
    for req_key in ("run_id", "ingested_at", "earthquakes", "weather_snapshots"):
        if req_key not in payload:
            raise ValueError(f"Payload missing required top-level key: {req_key}")

    quakes = payload["earthquakes"]
    if not isinstance(quakes, dict) or quakes.get("type") != "FeatureCollection":
        raise ValueError("USGS response must be a GeoJSON FeatureCollection")

    features = quakes.get("features")
    if not isinstance(features, list):
        raise ValueError("FeatureCollection 'features' must be a list")

    for idx, feat in enumerate(features):
        if not isinstance(feat, dict):
            raise ValueError(f"Feature at index {idx} is not a dictionary")
        if not feat.get("id"):
            raise ValueError(f"Feature at index {idx} missing 'id'")
        coords = feat.get("geometry", {}).get("coordinates")
        if not isinstance(coords, list) or len(coords) < 2:
            raise ValueError(f"Feature {feat.get('id')} has invalid geometry coordinates")
        lon, lat = coords[0], coords[1]
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
            raise ValueError(f"Feature {feat.get('id')} coordinates out of bounds: ({lat}, {lon})")
        props = feat.get("properties")
        if not isinstance(props, dict) or props.get("time") is None:
            raise ValueError(f"Feature {feat.get('id')} missing properties.time")

    feature_ids = {f["id"] for f in features}
    for idx, snap in enumerate(payload.get("weather_snapshots", [])):
        if not isinstance(snap, dict):
            raise ValueError(f"Weather snapshot {idx} is not a dictionary")
        if "event_id" not in snap or snap["event_id"] not in feature_ids:
            raise ValueError(f"Weather snapshot {idx} has unreferenced event_id: {snap.get('event_id')}")
        resp = snap.get("response")
        if not isinstance(resp, dict) or "current" not in resp:
            raise ValueError(f"Weather snapshot for {snap.get('event_id')} missing 'current' payload")
        curr = resp["current"]
        if not isinstance(curr, dict) or "temperature_2m" not in curr or "time" not in curr:
            raise ValueError(f"Weather snapshot for {snap.get('event_id')} has invalid current weather schema")


def build_run(*, period: str = "day", minimum_magnitude: float = 2.5, limit: int = 100,
              weather_limit: int = 25, session: requests.Session | None = None) -> dict[str, Any]:
    session = session or requests.Session()
    query = {"format": "geojson", "orderby": "time", "limit": limit, "minmagnitude": minimum_magnitude}
    lookback = {"hour": timedelta(hours=1), "day": timedelta(days=1), "week": timedelta(days=7)}
    if period not in lookback: raise ValueError("period must be hour, day, or week")
    now = datetime.now(timezone.utc)
    query["starttime"] = (now - lookback[period]).strftime("%Y-%m-%dT%H:%M:%S")
    query["endtime"] = now.strftime("%Y-%m-%dT%H:%M:%S")
    if period == "hour": query["limit"] = min(limit, 1000)
    quakes = request_json(session, USGS_URL, query)
    weather: list[dict[str, Any]] = []
    features = quakes.get("features", [])
    # A location snapshot is requested once per earthquake, up to weather_limit.
    # Weather observation time is retained, since it may differ from quake time.
    for feature in features[:max(0, weather_limit)]:
        coords = feature.get("geometry", {}).get("coordinates") or []
        if len(coords) < 2 or coords[0] is None or coords[1] is None: continue
        try:
            record = fetch_weather(session, float(coords[1]), float(coords[0]))
            weather.append({"event_id": feature.get("id"), "requested_at": datetime.now(timezone.utc).isoformat(), "response": record})
        except requests.RequestException:
            LOG.exception("Weather request failed for event %s; preserving earthquake record", feature.get("id"))
    payload = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "usgs_fdsn_open_meteo",
        "parameters": {"period": period, "minimum_magnitude": minimum_magnitude, "limit": limit,
                       "weather_limit": weather_limit},
        "earthquakes": quakes,
        "weather_snapshots": weather,
    }
    # Validate the complete API payload structure before returning for save
    validate_api_response(payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--period", choices=["hour", "day", "week"], default="day")
    parser.add_argument("--min-magnitude", type=float, default=2.5)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--weather-limit", type=int, default=25, help="Cap weather calls to respect fair-use limits")
    parser.add_argument("--output-dir", type=Path, default=Path(os.getenv("LANDING_ZONE", "landing_zone")))
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    payload = build_run(period=args.period, minimum_magnitude=args.min_magnitude, limit=args.limit, weather_limit=args.weather_limit)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / f"run_{payload['run_id']}.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    LOG.info("Saved %d earthquakes and %d weather snapshots to %s", len(payload["earthquakes"].get("features", [])), len(payload["weather_snapshots"]), path)

if __name__ == "__main__": main()
