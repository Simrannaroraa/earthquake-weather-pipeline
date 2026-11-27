"""Generate a high-resolution PNG image of the Gold Analytical Star Schema."""
import os

os.environ["MPLCONFIGDIR"] = "/tmp/matplotlib"
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch


def draw_star_schema():
    fig, ax = plt.subplots(figsize=(14, 9), dpi=300)
    ax.set_xlim(0, 14)
    ax.set_ylim(0, 9)
    ax.axis("off")

    fact_hdr = "#7C2D12"
    dim_hdr = "#1E3A8A"
    bg_color = "#F8FAFC"
    border_color = "#334155"
    pk_color = "#D97706"
    fk_color = "#2563EB"
    attr_color = "#0F172A"

    def draw_table(x, y, w, h, title, hdr_color, attributes):
        box = FancyBboxPatch(
            (x, y),
            w,
            h,
            boxstyle="round,pad=0.08,rounding_size=0.15",
            facecolor=bg_color,
            edgecolor=border_color,
            linewidth=1.5,
        )
        ax.add_patch(box)

        hdr_h = 0.65
        hdr = FancyBboxPatch(
            (x, y + h - hdr_h),
            w,
            hdr_h,
            boxstyle="round,pad=0.08,rounding_size=0.15",
            facecolor=hdr_color,
            edgecolor=border_color,
            linewidth=1.5,
        )
        ax.add_patch(hdr)
        ax.text(
            x + w / 2,
            y + h - hdr_h / 2,
            title,
            ha="center",
            va="center",
            color="#FFFFFF",
            fontsize=11.5,
            fontweight="bold",
            fontfamily="sans-serif",
        )

        line_y = y + h - hdr_h - 0.32
        for key_type, col_name, col_type in attributes:
            badge_color = pk_color if "PK" in key_type else (fk_color if "FK" in key_type else "#64748B")
            ax.text(x + 0.2, line_y, key_type, ha="left", va="center", color=badge_color, fontsize=8.5, fontweight="bold", fontfamily="monospace")
            ax.text(x + 0.85, line_y, col_name, ha="left", va="center", color=attr_color, fontsize=9, fontweight="semibold", fontfamily="sans-serif")
            ax.text(x + w - 0.2, line_y, col_type, ha="right", va="center", color="#64748B", fontsize=8.5, fontfamily="monospace")
            line_y -= 0.32

    # DIM_DATE (Top Left)
    draw_table(
        0.5,
        4.9,
        3.8,
        3.1,
        "DIM_DATE",
        dim_hdr,
        [
            ("PK", "date_key", "INT"),
            ("", "date", "DATE"),
            ("", "year", "INT"),
            ("", "month", "INT"),
            ("", "day", "INT"),
            ("", "day_of_week", "INT"),
            ("", "day_name", "STRING"),
        ],
    )

    # DIM_LOCATION (Bottom Left)
    draw_table(
        0.5,
        1.0,
        3.8,
        2.3,
        "DIM_LOCATION",
        dim_hdr,
        [
            ("PK", "location_key", "BIGINT"),
            ("", "latitude", "DOUBLE"),
            ("", "longitude", "DOUBLE"),
            ("", "place", "STRING"),
        ],
    )

    # DIM_MAGNITUDE_TYPE (Right)
    draw_table(
        9.8,
        3.4,
        3.7,
        1.7,
        "DIM_MAGNITUDE_TYPE",
        dim_hdr,
        [
            ("PK", "magnitude_type_key", "BIGINT"),
            ("", "magnitude_type", "STRING"),
        ],
    )

    # FACT_EARTHQUAKE (Center)
    draw_table(
        5.0,
        1.0,
        4.1,
        6.4,
        "FACT_EARTHQUAKE",
        fact_hdr,
        [
            ("PK", "event_id", "STRING"),
            ("FK", "date_key", "INT"),
            ("FK", "location_key", "BIGINT"),
            ("FK", "magnitude_type_key", "BIGINT"),
            ("", "magnitude", "DOUBLE"),
            ("", "depth_km", "DOUBLE"),
            ("", "significance", "INT"),
            ("", "tsunami", "INT"),
            ("", "event_ts_utc", "TIMESTAMP"),
            ("", "latitude", "DOUBLE"),
            ("", "longitude", "DOUBLE"),
            ("", "temperature_c", "DOUBLE"),
            ("", "relative_humidity_pct", "INT"),
            ("", "precipitation_mm", "DOUBLE"),
            ("", "weather_code", "INT"),
            ("", "wind_speed_kmh", "DOUBLE"),
            ("", "url", "STRING"),
        ],
    )

    # Connectors
    ax.annotate("", xy=(5.0, 6.0), xytext=(4.3, 6.2), arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(4.65, 6.3, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    ax.annotate("", xy=(5.0, 2.5), xytext=(4.3, 2.2), arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(4.65, 2.55, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    ax.annotate("", xy=(9.1, 4.2), xytext=(9.8, 4.2), arrowprops=dict(arrowstyle="-|>", color="#1E293B", lw=2, mutation_scale=15))
    ax.text(9.45, 4.4, "1 : N", ha="center", fontsize=9, fontweight="bold", color="#1E293B")

    ax.text(7.0, 8.55, "Gold Analytical Star Schema", ha="center", va="center", fontsize=18, fontweight="bold", color="#0F172A")
    ax.text(7.0, 8.15, "Grain: 1 row per unique USGS earthquake event (with latest Open-Meteo weather snapshot)", ha="center", va="center", fontsize=10.5, color="#475569")

    plt.tight_layout()
    plt.savefig("docs/star_schema.png", bbox_inches="tight", dpi=300)
    plt.close()
    print("Saved docs/star_schema.png successfully.")


if __name__ == "__main__":
    draw_star_schema()

