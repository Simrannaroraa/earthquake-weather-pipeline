"""Build a compact analytical star schema from Silver Delta tables."""
from pyspark.sql import SparkSession, functions as F


def run(spark: SparkSession, catalog: str):
    q = spark.table(f"{catalog}.silver.earthquakes")
    # Dimensions are rebuilt deterministically; production pipelines can use MERGE for SCD handling.
    date_dim = q.select(F.to_date("event_ts_utc").alias("date")).distinct().filter("date is not null").select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date", F.year("date").alias("year"), F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"), F.dayofweek("date").alias("day_of_week"), F.date_format("date", "EEEE").alias("day_name"))
    date_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_date")
    loc = q.select("latitude", "longitude", "place").distinct().withColumn("location_key", F.xxhash64("latitude", "longitude", "place"))
    loc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_location")
    mag = q.select("magnitude_type").distinct().withColumn("magnitude_type_key", F.xxhash64("magnitude_type"))
    mag.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.dim_magnitude_type")
    w = spark.table(f"{catalog}.silver.weather_snapshots")
    wx = w
    # Keep one latest weather snapshot for the event fact; silver retains the full snapshot history.
    from pyspark.sql import Window
    wx = wx.withColumn("rn", F.row_number().over(Window.partitionBy("event_id").orderBy(F.col("requested_at").desc_nulls_last()))).filter("rn=1").drop("rn")
    fact = (q.join(loc, ["latitude", "longitude", "place"], "left")
        .join(mag, "magnitude_type", "left")
        .join(date_dim, F.to_date(q.event_ts_utc) == date_dim.date, "left")
        .join(wx, "event_id", "left")
        .select("event_id", "date_key", "location_key", "magnitude_type_key", "magnitude", "depth_km", "significance", "tsunami",
                "event_ts_utc", "latitude", "longitude", "temperature_c", "relative_humidity_pct", "precipitation_mm", "weather_code", "wind_speed_kmh", "url"))
    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.gold.fact_earthquake")
    return fact.count()
