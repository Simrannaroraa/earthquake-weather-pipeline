# Gold Star Schema Documentation

![Gold Star Schema Diagram](star_schema.png)

```mermaid
erDiagram
  DIM_DATE ||--o{ FACT_EARTHQUAKE : occurs_on
  DIM_LOCATION ||--o{ FACT_EARTHQUAKE : occurs_at
  DIM_MAGNITUDE_TYPE ||--o{ FACT_EARTHQUAKE : measured_as

  DIM_DATE {
    int date_key PK
    date date
    int year
    int month
    int day
    int day_of_week
    string day_name
  }

  DIM_LOCATION {
    bigint location_key PK
    double latitude
    double longitude
    string place
  }

  DIM_MAGNITUDE_TYPE {
    bigint magnitude_type_key PK
    string magnitude_type
  }

  FACT_EARTHQUAKE {
    string event_id PK
    int date_key FK
    bigint location_key FK
    bigint magnitude_type_key FK
    double magnitude
    double depth_km
    int significance
    int tsunami
    timestamp event_ts_utc
    double latitude
    double longitude
    double temperature_c
    int relative_humidity_pct
    double precipitation_mm
    int weather_code
    double wind_speed_kmh
    string url
  }
```

## Star Schema Design Verification

- **Fact Table Grain:** Exactly **one row per unique USGS earthquake event (`event_id`)**.
- **Dimension Surrogate Keys:**
  - `date_key`: Deterministic `yyyyMMdd` integer derived from `event_ts_utc`.
  - `location_key`: Deterministic 64-bit `xxhash64(latitude, longitude, place)`.
  - `magnitude_type_key`: Deterministic 64-bit `xxhash64(magnitude_type)`.
- **Measures:** `magnitude`, `depth_km`, `significance`, `tsunami`, `temperature_c`, `relative_humidity_pct`, `precipitation_mm`, `wind_speed_kmh`.
- **Referential Integrity & Null Handling:** Nullable `place` and `magnitude_type` values are coalesced to `"Unknown"` / `"unknown"` prior to key generation and joins, ensuring zero orphaned foreign keys in `fact_earthquake`. Unmatched weather snapshots remain `NULL` without dropping earthquake events.
- **Duplicate Handling:** Upstream Silver deduplicates by latest `properties.updated` (tie-broken by `_ingested_at`). Gold attaches the single latest weather snapshot (`requested_at` descending) per `event_id` and uses Delta `MERGE` keyed on `event_id`.
