# Databricks notebook source
# MAGIC %md
# MAGIC # Earthquake + Weather Pipeline
# MAGIC Self-contained Track A pipeline for the uploaded Unity Catalog Volume.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "workspace")
    dbutils.widgets.text("landing_path", "/Volumes/workspace/bronze/landing/")
    catalog = dbutils.widgets.get("catalog")
    landing_path = dbutils.widgets.get("landing_path")
except Exception:
    catalog = "workspace"
    landing_path = "/Volumes/workspace/bronze/landing/"

# COMMAND ----------

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

"""Append immutable landing envelopes and extracted source objects to Bronze Delta."""
from pyspark.sql import SparkSession, functions as F


def build_bronze(spark: SparkSession, landing_path: str, catalog: str):
    raw = (spark.read.option("multiLine", "true").json(landing_path)
           .withColumn("_ingested_at", F.coalesce(F.to_timestamp("ingested_at"), F.current_timestamp()))
           .withColumn("_source_file", F.input_file_name()))
    raw.select("run_id", "_ingested_at", "_source_file", F.to_json(F.struct(*[F.col(c) for c in raw.columns if not c.startswith("_")])).alias("raw_json")) \
       .write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze.ingestion_runs")
    quakes = (raw.select("run_id", "_ingested_at", "_source_file", F.explode_outer("earthquakes.features").alias("feature"))
              .select("run_id", "_ingested_at", "_source_file", F.col("feature.id").alias("event_id"), F.to_json("feature").alias("raw_json")))
    quakes.write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze.earthquake_events")
    wx = (raw.select("run_id", "_ingested_at", F.explode_outer("weather_snapshots").alias("w"))
          .select("run_id", "_ingested_at", F.col("w.event_id").alias("event_id"), F.to_timestamp("w.requested_at").alias("requested_at"), F.to_json("w.response").alias("raw_json")))
    wx.write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze.weather_snapshots")
    return raw.count()

# COMMAND ----------

"""Normalize, validate, and deduplicate Bronze records into Silver Delta."""
from pyspark.sql import SparkSession, functions as F, Window


def build_silver(spark: SparkSession, catalog: str):
    bq = spark.table(f"{catalog}.bronze.earthquake_events")
    parsed = bq.withColumn("j", F.from_json("raw_json", "struct<id:string,geometry:struct<coordinates:array<double>>,properties:struct<mag:double,place:string,time:bigint,updated:bigint,url:string,detail:string,status:string,tsunami:int,sig:int,net:string,code:string,types:string,title:string,alert:string,felt:int,cdi:double,mmi:double,rms:double,dmin:double,magType:string,type:string,depth:double>>"))
    quake = parsed.select("event_id", F.col("j.properties.time").alias("event_time_ms"), F.col("j.properties.updated").alias("updated_ms"),
        F.col("j.properties.mag").alias("magnitude"), F.col("j.properties.place").alias("place"),
        F.element_at("j.geometry.coordinates", 1).alias("longitude"), F.element_at("j.geometry.coordinates", 2).alias("latitude"),
        F.element_at("j.geometry.coordinates", 3).alias("depth_km"), F.col("j.properties.url").alias("url"),
        F.col("j.properties.status").alias("status"), F.col("j.properties.tsunami").alias("tsunami"),
        F.col("j.properties.sig").alias("significance"), F.col("j.properties.net").alias("network"),
        F.col("j.properties.code").alias("network_code"), F.col("j.properties.magType").alias("magnitude_type"),
        F.to_timestamp(F.from_unixtime(F.col("j.properties.time") / 1000)).alias("event_ts_utc"),
        F.to_timestamp(F.from_unixtime(F.col("j.properties.updated") / 1000)).alias("updated_at_utc"), "_ingested_at")
    quake = quake.filter(F.col("event_id").isNotNull() & F.col("event_time_ms").isNotNull() & F.col("latitude").between(-90, 90) & F.col("longitude").between(-180, 180))
    w = Window.partitionBy("event_id").orderBy(F.col("updated_ms").desc_nulls_last(), F.col("_ingested_at").desc())
    quake = quake.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn", "event_time_ms", "updated_ms")
    # Materialize normalized 3NF entities for the SQL deliverable and downstream reuse.
    locations = (quake.select("latitude", "longitude", "depth_km", "place").distinct()
                 .withColumn("location_id", F.xxhash64("latitude", "longitude", "depth_km", "place")))
    locations.select("location_id", "latitude", "longitude", "depth_km", "place").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.location")
    magnitude_types = (quake.select(F.col("magnitude_type").alias("code")).distinct()
                       .withColumn("magnitude_type_id", F.xxhash64("code")))
    magnitude_types.select("magnitude_type_id", "code").write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.magnitude_type")
    normalized = (quake.join(locations, ["latitude", "longitude", "depth_km", "place"], "left")
                  .join(magnitude_types, quake.magnitude_type == magnitude_types.code, "left")
                  .select(quake.event_id, quake.event_ts_utc.alias("event_time_utc"), quake.updated_at_utc, quake.magnitude, "location_id", "magnitude_type_id",
                          quake.status, quake.significance, quake.tsunami, quake.network, quake.network_code, quake.url.alias("source_url"), quake._ingested_at))
    normalized.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.earthquake")
    # Retain an event-wide Silver view/table to keep the Star transformation transparent.
    quake.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.earthquakes")

    bw = spark.table(f"{catalog}.bronze.weather_snapshots")
    wj = bw.withColumn("j", F.from_json("raw_json", "struct<latitude:double,longitude:double,elevation:double,timezone:string,current:struct<time:string,interval:int,temperature_2m:double,relative_humidity_2m:int,apparent_temperature:double,precipitation:double,weather_code:int,wind_speed_10m:double>,current_units:map<string,string>>"))
    weather = wj.select("event_id", F.to_timestamp("requested_at").alias("requested_at"), F.to_timestamp("j.current.time").alias("observation_ts_utc"),
        F.col("j.latitude").alias("grid_latitude"), F.col("j.longitude").alias("grid_longitude"), F.col("j.elevation").alias("elevation_m"),
        F.col("j.timezone").alias("timezone"), F.col("j.current.temperature_2m").alias("temperature_c"),
        F.col("j.current.relative_humidity_2m").alias("relative_humidity_pct"), F.col("j.current.apparent_temperature").alias("apparent_temperature_c"),
        F.col("j.current.precipitation").alias("precipitation_mm"), F.col("j.current.weather_code").alias("weather_code"), F.col("j.current.wind_speed_10m").alias("wind_speed_kmh"))
    weather = weather.filter(F.col("event_id").isNotNull()).withColumn("rn", F.row_number().over(Window.partitionBy("event_id", "observation_ts_utc").orderBy(F.col("requested_at").desc_nulls_last())))
    curated_weather = weather.filter("rn = 1").drop("rn")
    curated_weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.weather_snapshots")
    curated_weather.select("event_id", F.col("observation_ts_utc").alias("observation_time_utc"), F.col("requested_at").alias("requested_at_utc"),
        "grid_latitude", "grid_longitude", "temperature_c", "apparent_temperature_c", "relative_humidity_pct", "precipitation_mm", "weather_code", "wind_speed_kmh") \
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.weather_observation")
    return quake.count()

# COMMAND ----------

"""Build a compact analytical star schema from Silver Delta tables."""
from pyspark.sql import SparkSession, functions as F


def build_gold(spark: SparkSession, catalog: str):
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

# COMMAND ----------

landing_count = build_bronze(spark, landing_path, catalog)
print(f"Bronze run envelopes read: {landing_count}")

# COMMAND ----------

silver_events = build_silver(spark, catalog)
print(f"Unique validated Silver earthquakes: {silver_events}")

# COMMAND ----------

gold_rows = build_gold(spark, catalog)
print(f"Gold fact rows: {gold_rows}")

# COMMAND ----------

display(spark.table(f"{catalog}.gold.fact_earthquake").orderBy("event_ts_utc", ascending=False).limit(20))
