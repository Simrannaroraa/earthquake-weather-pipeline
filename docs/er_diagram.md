# Normalized 3NF ER diagram

```mermaid
erDiagram
  LOCATION ||--o{ EARTHQUAKE : locates
  MAGNITUDE_TYPE ||--o{ EARTHQUAKE : classifies
  EARTHQUAKE ||--o{ WEATHER_OBSERVATION : has_snapshot
  LOCATION {
    bigint location_id PK
    double latitude
    double longitude
    double depth_km
    string place
  }
  MAGNITUDE_TYPE {
    bigint magnitude_type_id PK
    string code
  }
  EARTHQUAKE {
    string event_id PK
    timestamp event_time_utc
    timestamp updated_at_utc
    double magnitude
    bigint location_id FK
    bigint magnitude_type_id FK
    string status
    int significance
    int tsunami
    string network
    string network_code
    string source_url
  }
  WEATHER_OBSERVATION {
    string event_id PK
    timestamp observation_time_utc PK
    timestamp requested_at_utc
    double temperature_c
    int relative_humidity_pct
    double precipitation_mm
    int weather_code
    double wind_speed_kmh
  }
```

Location and magnitude code attributes are stored once; each weather snapshot is an observation associated with an event. Delta key declarations are informational, so the job logic enforces uniqueness.
