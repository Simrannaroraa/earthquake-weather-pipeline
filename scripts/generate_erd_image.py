"""Generate a high-resolution PNG image of the Normalized 3NF ER diagram."""
import os
import matplotlib
os.environ["MPLCONFIGDIR"] = "/tmp/matplotlib"
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

def draw_erd():
    fig, ax = plt.subplots(figsize=(14, 9), dpi=300)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis("off")

    # Styling colors
    header_color = "#1E3A8A"      # Deep Navy
    header_text = "#FFFFFF"
    bg_color = "#F8FAFC"          # Slate light
    border_color = "#334155"
    pk_color = "#D97706"          # Amber
    fk_color = "#2563EB"          # Blue
    attr_color = "#0F172A"

    def draw_entity(x, y, w, h, title, attributes):
        # Card background
        box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=bg_color, edgecolor=border_color, linewidth=1.5)
        ax.add_patch(box)

        # Header box
        hdr_h = 0.75
        hdr = FancyBboxPatch((x, y + h - hdr_h), w, hdr_h, boxstyle="round,pad=0.08,rounding_size=0.15",
                             facecolor=header_color, edgecolor=border_color, linewidth=1.5)
        ax.add_patch(hdr)
        ax.text(x + w / 2, y + h - hdr_h / 2, title, ha="center", va="center",
                color=header_text, fontsize=12, fontweight="bold", fontfamily="sans-serif")

        # Attributes
        line_y = y + h - hdr_h - 0.35
        for key_type, col_name, col_type in attributes:
            badge_color = pk_color if "PK" in key_type else (fk_color if "FK" in key_type else "#64748B")
            ax.text(x + 0.25, line_y, key_type, ha="left", va="center",
                    color=badge_color, fontsize=9, fontweight="bold", fontfamily="monospace")
            ax.text(x + 1.1, line_y, col_name, ha="left", va="center",
                    color=attr_color, fontsize=9.5, fontweight="semibold", fontfamily="sans-serif")
            ax.text(x + w - 0.25, line_y, col_type, ha="right", va="center",
                    color="#64748B", fontsize=9, fontfamily="monospace")
            line_y -= 0.36

    # 1. LOCATION (Top Left)
    draw_entity(0.8, 5.0, 4.0, 3.2, "LOCATION", [
        ("PK", "location_id", "INTEGER"),
        ("",   "latitude", "REAL NOT NULL"),
        ("",   "longitude", "REAL NOT NULL"),
        ("",   "depth_km", "REAL"),
        ("",   "place", "TEXT"),
    ])

    # 2. MAGNITUDE_TYPE (Bottom Left)
    draw_entity(0.8, 1.2, 4.0, 2.5, "MAGNITUDE_TYPE", [
        ("PK", "magnitude_type_id", "INTEGER"),
        ("UQ", "code", "TEXT NOT NULL"),
    ])

    # 3. EARTHQUAKE (Center)
    draw_entity(5.6, 2.0, 4.4, 5.8, "EARTHQUAKE", [
        ("PK", "event_id", "TEXT"),
        ("",   "event_time_utc", "TEXT NOT NULL"),
        ("",   "updated_at_utc", "TEXT"),
        ("",   "magnitude", "REAL"),
        ("FK", "location_id", "INTEGER REFERENCES location"),
        ("FK", "magnitude_type_id", "INTEGER REFERENCES magnitude_type"),
        ("",   "status", "TEXT"),
        ("",   "significance", "INTEGER"),
        ("",   "tsunami", "INTEGER"),
        ("",   "network", "TEXT"),
        ("",   "network_code", "TEXT"),
        ("",   "source_url", "TEXT"),
        ("",   "_ingested_at", "TEXT"),
    ])

    # 4. WEATHER_OBSERVATION (Right)
    draw_entity(10.6, 2.5, 4.2, 5.0, "WEATHER_OBSERVATION", [
        ("PK,FK", "event_id", "TEXT REFERENCES earthquake"),
        ("PK",    "observation_time_utc", "TEXT"),
        ("",      "requested_at_utc", "TEXT"),
        ("",      "grid_latitude", "REAL"),
        ("",      "grid_longitude", "REAL"),
        ("",      "temperature_c", "REAL"),
        ("",      "apparent_temp_c", "REAL"),
        ("",      "relative_humidity_pct", "INTEGER"),
        ("",      "precipitation_mm", "REAL"),
        ("",      "weather_code", "INTEGER"),
        ("",      "wind_speed_kmh", "REAL"),
    ])

    # Connectors
    # Location to Earthquake
    ax.annotate("", xy=(5.6, 6.0), xytext=(4.8, 6.0),
                arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(5.2, 6.15, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    # Magnitude Type to Earthquake
    ax.annotate("", xy=(5.6, 3.2), xytext=(4.8, 2.5),
                arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(5.2, 3.0, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    # Earthquake to Weather Observation
    ax.annotate("", xy=(10.6, 5.0), xytext=(10.0, 5.0),
                arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(10.3, 5.15, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    # Title
    ax.text(7.0, 8.5, "Normalized (3NF) Relational Database Schema",
            ha="center", va="center", fontsize=18, fontweight="bold", color="#0F172A")
    ax.text(7.0, 8.1, "Track A: USGS Earthquake Events & Open-Meteo Weather Observations",
            ha="center", va="center", fontsize=11, color="#475569")

    plt.tight_layout()
    plt.savefig("docs/er_diagram.png", bbox_inches="tight", dpi=300)
    plt.close()
    print("Saved docs/er_diagram.png successfully.")

if __name__ == "__main__":
    draw_erd()

