"""Build an analytical Star Schema in Gold Delta tables from Silver Delta."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def _upsert_delta(spark: SparkSession, df: DataFrame, table_name: str, condition: str) -> None:
    """Upsert DataFrame into a Gold Delta table using MERGE, creating it on first run."""
    if spark.catalog.tableExists(table_name):
        (
            DeltaTable.forName(spark, table_name)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def run(spark: SparkSession, catalog: str) -> int:
    """Build dim_date, dim_location, dim_magnitude_type, and fact_earthquake in Gold."""
    q = spark.table(f"{catalog}.silver.earthquakes")

    date_dim = (
        q.select(F.to_date("event_ts_utc").alias("date"))
        .distinct()
        .filter("date is not null")
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            "date",
            F.year("date").alias("year"),
            F.month("date").alias("month"),
            F.dayofmonth("date").alias("day"),
            F.dayofweek("date").alias("day_of_week"),
            F.date_format("date", "EEEE").alias("day_name"),
        )
    )
    _upsert_delta(spark, date_dim, f"{catalog}.gold.dim_date", "target.date_key = source.date_key")

    loc = (
        q.select("latitude", "longitude", F.coalesce(F.col("place"), F.lit("Unknown")).alias("place"))
        .distinct()
        .withColumn("location_key", F.xxhash64("latitude", "longitude", "place"))
        .select("location_key", "latitude", "longitude", "place")
    )
    _upsert_delta(spark, loc, f"{catalog}.gold.dim_location", "target.location_key = source.location_key")

    mag = (
        q.select(F.coalesce(F.col("magnitude_type"), F.lit("unknown")).alias("magnitude_type"))
        .distinct()
        .withColumn("magnitude_type_key", F.xxhash64("magnitude_type"))
        .select("magnitude_type_key", "magnitude_type")
    )
    _upsert_delta(
        spark,
        mag,
        f"{catalog}.gold.dim_magnitude_type",
        "target.magnitude_type_key = source.magnitude_type_key",
    )

    w = spark.table(f"{catalog}.silver.weather_snapshots")
    wx = (
        w.withColumn("rn", F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("requested_at").desc_nulls_last())))
        .filter("rn = 1")
        .drop("rn")
    )

    q_norm = q.withColumn("place_clean", F.coalesce(F.col("place"), F.lit("Unknown"))).withColumn(
        "mag_type_clean", F.coalesce(F.col("magnitude_type"), F.lit("unknown"))
    )

    fact = (
        q_norm.join(
            loc,
            (q_norm.latitude == loc.latitude) & (q_norm.longitude == loc.longitude) & (q_norm.place_clean == loc.place),
            "left",
        )
        .join(mag, q_norm.mag_type_clean == mag.magnitude_type, "left")
        .join(date_dim, F.to_date(q_norm.event_ts_utc) == date_dim.date, "left")
        .join(wx, "event_id", "left")
        .select(
            q_norm.event_id,
            date_dim.date_key,
            loc.location_key,
            mag.magnitude_type_key,
            q_norm.magnitude,
            q_norm.depth_km,
            q_norm.significance,
            q_norm.tsunami,
            q_norm.event_ts_utc,
            q_norm.latitude,
            q_norm.longitude,
            wx.temperature_c,
            wx.relative_humidity_pct,
            wx.precipitation_mm,
            wx.weather_code,
            wx.wind_speed_kmh,
            q_norm.url,
        )
    )
    _upsert_delta(spark, fact, f"{catalog}.gold.fact_earthquake", "target.event_id = source.event_id")
    return fact.count()
