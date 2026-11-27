"""Interactive Streamlit Analytical Dashboard connected to Gold Star Schema tables.

Run locally with:
    streamlit run dashboard.py
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from src.simulate_pipeline import run_simulation

WMO_WEATHER_LABELS = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Depositing rime fog",
    51: "Light drizzle",
    53: "Moderate drizzle",
    55: "Dense drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    80: "Slight rain showers",
    81: "Moderate rain showers",
    85: "Slight snow showers",
    95: "Thunderstorm",
}


def load_gold_dataframe(landing_dir: Path = Path("landing_zone")) -> pd.DataFrame:
    """Load Gold star schema tables joined into an analytical DataFrame.

    Uses Databricks SQL Warehouse if DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH,
    and DATABRICKS_TOKEN are set; otherwise builds Gold tables locally from real
    landing_zone/*.json files via the Bronze->Silver->Gold simulation pipeline.
    """
    host = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    query = """
    SELECT
        f.event_id,
        f.event_ts_utc,
        d.date AS event_date,
        d.year,
        d.month,
        d.day_name,
        l.place,
        f.latitude,
        f.longitude,
        f.magnitude,
        m.magnitude_type,
        f.depth_km,
        f.significance,
        f.tsunami,
        f.temperature_c,
        f.relative_humidity_pct,
        f.precipitation_mm,
        f.weather_code,
        f.wind_speed_kmh,
        f.url
    FROM {catalog_prefix}gold_fact_earthquake f
    LEFT JOIN {catalog_prefix}gold_dim_date d ON f.date_key = d.date_key
    LEFT JOIN {catalog_prefix}gold_dim_location l ON f.location_key = l.location_key
    LEFT JOIN {catalog_prefix}gold_dim_magnitude_type m ON f.magnitude_type_key = m.magnitude_type_key
    ORDER BY f.event_ts_utc DESC
    """

    if host and http_path and token:
        from databricks import sql as databricks_sql  # optional runtime dependency

        db_query = (
            query.replace("{catalog_prefix}gold_fact_earthquake", "workspace.gold.fact_earthquake")
            .replace("{catalog_prefix}gold_dim_date", "workspace.gold.dim_date")
            .replace("{catalog_prefix}gold_dim_location", "workspace.gold.dim_location")
            .replace("{catalog_prefix}gold_dim_magnitude_type", "workspace.gold.dim_magnitude_type")
        )
        with databricks_sql.connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            df = pd.read_sql(db_query, conn)
    else:
        conn: sqlite3.Connection = run_simulation(landing_dir=landing_dir)
        try:
            df = pd.read_sql_query(query.replace("{catalog_prefix}", ""), conn)
        finally:
            conn.close()

    df["event_ts_utc"] = pd.to_datetime(df["event_ts_utc"], utc=True, errors="coerce")
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce").dt.date
    df["weather_condition"] = df["weather_code"].map(
        lambda c: WMO_WEATHER_LABELS.get(int(c), f"WMO {int(c)}") if pd.notna(c) else "No Snapshot"
    )
    return df


def render_dashboard() -> None:
    st.set_page_config(
        page_title="Earthquake & Weather Alerts — Gold Intelligence",
        page_icon="🌍",
        layout="wide",
    )

    st.title("Earthquake & Weather Alerts — Gold Analytical Dashboard")
    st.caption(
        "Connected to Gold Star Schema (`fact_earthquake`, `dim_date`, `dim_location`, `dim_magnitude_type`). "
        "Note: Weather fields reflect current/forecast grid-cell snapshots at ingestion time."
    )

    df = load_gold_dataframe()
    if df.empty:
        st.warning("No earthquake records found in Gold tables.")
        return

    # Sidebar Interactive Filters
    st.sidebar.header("Interactive Filters")
    min_mag_val = float(df["magnitude"].min())
    max_mag_val = float(df["magnitude"].max())
    mag_range = st.sidebar.slider(
        "Magnitude Range",
        min_value=min_mag_val,
        max_value=max_mag_val,
        value=(min_mag_val, max_mag_val),
        step=0.1,
    )

    min_date = df["event_date"].min()
    max_date = df["event_date"].max()
    date_range = st.sidebar.date_input("Event Date Range (UTC)", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    place_search = st.sidebar.text_input("Search Place / Region", value="").strip().lower()
    weather_only = st.sidebar.checkbox("Only events with weather snapshot", value=False)
    tsunami_only = st.sidebar.checkbox("Only tsunami alert events", value=False)

    filtered = df[(df["magnitude"] >= mag_range[0]) & (df["magnitude"] <= mag_range[1])]
    if isinstance(date_range, tuple) and len(date_range) == 2:
        filtered = filtered[(filtered["event_date"] >= date_range[0]) & (filtered["event_date"] <= date_range[1])]
    if place_search:
        filtered = filtered[filtered["place"].fillna("").str.lower().str.contains(place_search)]
    if weather_only:
        filtered = filtered[filtered["temperature_c"].notna()]
    if tsunami_only:
        filtered = filtered[filtered["tsunami"] == 1]

    # Top KPI Metrics
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Earthquakes", f"{len(filtered):,}")
    c2.metric("Average Magnitude", f"{filtered['magnitude'].mean():.2f}" if not filtered.empty else "N/A")
    c3.metric("Maximum Magnitude", f"{filtered['magnitude'].max():.1f}" if not filtered.empty else "N/A")
    c4.metric("Tsunami Alerts", f"{int(filtered['tsunami'].fillna(0).sum())}" if not filtered.empty else "0")
    wx_cov = (filtered["temperature_c"].notna().mean() * 100) if not filtered.empty else 0.0
    c5.metric("Weather Coverage", f"{wx_cov:.1f}%")

    if filtered.empty:
        st.info("No records match the selected filters.")
        return

    # Row 1: Map & Trends Over Time
    col_map, col_trend = st.columns([3, 2])
    with col_map:
        st.subheader("Earthquake Distribution by Location")
        fig_map = px.scatter_geo(
            filtered,
            lat="latitude",
            lon="longitude",
            color="magnitude",
            size="magnitude",
            hover_name="place",
            hover_data={"depth_km": True, "temperature_c": True, "weather_condition": True, "tsunami": True},
            color_continuous_scale="YlOrRd",
            projection="natural earth",
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=380)
        st.plotly_chart(fig_map, use_container_width=True)

    with col_trend:
        st.subheader("Earthquake Trends Over Time")
        hourly = (
            filtered.assign(hour_utc=filtered["event_ts_utc"].dt.floor("h"))
            .groupby("hour_utc", as_index=False)
            .agg(event_count=("event_id", "count"), avg_magnitude=("magnitude", "mean"))
        )
        fig_trend = px.bar(
            hourly,
            x="hour_utc",
            y="event_count",
            color="avg_magnitude",
            labels={"hour_utc": "Time (UTC)", "event_count": "Earthquakes", "avg_magnitude": "Avg Mag"},
            color_continuous_scale="Blues",
        )
        fig_trend.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=380)
        st.plotly_chart(fig_trend, use_container_width=True)

    # Row 2: Magnitude vs Depth & Top Locations
    col_scatter, col_loc = st.columns(2)
    with col_scatter:
        st.subheader("Magnitude vs. Depth (km)")
        fig_scatter = px.scatter(
            filtered,
            x="magnitude",
            y="depth_km",
            color="magnitude_type",
            size="significance",
            hover_name="place",
            labels={"magnitude": "Magnitude", "depth_km": "Depth (km)", "magnitude_type": "Type"},
        )
        fig_scatter.update_yaxes(autorange="reversed")
        fig_scatter.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col_loc:
        st.subheader("Top Active Locations")
        top_places = (
            filtered.groupby("place", as_index=False)
            .agg(event_count=("event_id", "count"), max_magnitude=("magnitude", "max"))
            .sort_values(["event_count", "max_magnitude"], ascending=[False, False])
            .head(10)
        )
        fig_loc = px.bar(
            top_places,
            x="event_count",
            y="place",
            color="max_magnitude",
            orientation="h",
            labels={"event_count": "Events", "place": "Location", "max_magnitude": "Max Mag"},
            color_continuous_scale="OrRd",
        )
        fig_loc.update_yaxes(autorange="reversed")
        fig_loc.update_layout(margin=dict(l=0, r=0, t=10, b=0), height=350)
        st.plotly_chart(fig_loc, use_container_width=True)

    # Row 3: Weather Information at Earthquake Locations
    st.subheader("Weather Snapshot Insights at Earthquake Locations")
    wx_df = filtered[filtered["temperature_c"].notna()]
    col_w1, col_w2 = st.columns(2)
    with col_w1:
        if not wx_df.empty:
            wx_counts = wx_df.groupby("weather_condition", as_index=False).agg(
                earthquakes=("event_id", "count"), avg_temp_c=("temperature_c", "mean")
            )
            fig_wx = px.pie(wx_counts, names="weather_condition", values="earthquakes", hole=0.45, title="Weather Conditions Breakdown")
            fig_wx.update_layout(margin=dict(l=0, r=0, t=35, b=0), height=320)
            st.plotly_chart(fig_wx, use_container_width=True)
        else:
            st.info("No weather snapshots in current filter selection.")

    with col_w2:
        if not wx_df.empty:
            fig_temp = px.scatter(
                wx_df,
                x="temperature_c",
                y="wind_speed_kmh",
                color="magnitude",
                hover_name="place",
                title="Surface Temperature (°C) vs. Wind Speed (km/h)",
                labels={"temperature_c": "Temperature (°C)", "wind_speed_kmh": "Wind Speed (km/h)"},
                color_continuous_scale="Viridis",
            )
            fig_temp.update_layout(margin=dict(l=0, r=0, t=35, b=0), height=320)
            st.plotly_chart(fig_temp, use_container_width=True)

    # Row 4: Recent Earthquake Records
    st.subheader("Recent Earthquake Records")
    display_cols = [
        "event_id",
        "event_ts_utc",
        "place",
        "magnitude",
        "magnitude_type",
        "depth_km",
        "tsunami",
        "temperature_c",
        "wind_speed_kmh",
        "weather_condition",
        "url",
    ]
    st.dataframe(
        filtered[display_cols].head(50),
        use_container_width=True,
        hide_index=True,
        column_config={"url": st.column_config.LinkColumn("USGS Report")},
    )


if __name__ == "__main__":
    render_dashboard()

