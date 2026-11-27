# Databricks notebook source
# MAGIC %md
# MAGIC # Earthquake + Weather Data Intelligence Pipeline (Track A)
# MAGIC Self-contained Medallion Architecture (Bronze -> Silver 3NF -> Gold Star Schema) using PySpark & Delta Lake.

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

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.landing")

# COMMAND ----------

"""Bronze Layer: Idempotently land raw JSON envelopes and extracted source objects to Delta."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def _merge_insert_only(spark: SparkSession, df: DataFrame, table_name: str, condition: str) -> None:
    if spark.catalog.tableExists(table_name):
        (
            DeltaTable.forName(spark, table_name)
            .alias("target")
            .merge(df.alias("source"), condition)
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)


def _upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    condition: str,
    update_condition: str | None = None,
) -> None:
    if spark.catalog.tableExists(table_name):
        merger = DeltaTable.forName(spark, table_name).alias("target").merge(df.alias("source"), condition)
        if update_condition:
            merger = merger.whenMatchedUpdateAll(condition=update_condition)
        else:
            merger = merger.whenMatchedUpdateAll()
        merger.whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)


def build_bronze(spark: SparkSession, landing_path: str, catalog: str) -> int:
    raw = (
        spark.read.option("multiLine", "true")
        .json(landing_path)
        .withColumn("_ingested_at", F.coalesce(F.to_timestamp("ingested_at"), F.current_timestamp()))
        .withColumn("_source_file", F.expr("_metadata.file_path"))
    )

    runs_df = raw.select(
        "run_id",
        "_ingested_at",
        "_source_file",
        F.to_json(F.struct(*[F.col(c) for c in raw.columns if not c.startswith("_")])).alias("raw_json"),
    ).dropDuplicates(["run_id"])
    _merge_insert_only(spark, runs_df, f"{catalog}.bronze.ingestion_runs", "target.run_id = source.run_id")

    quakes_df = (
        raw.select("run_id", "_ingested_at", "_source_file", F.explode_outer("earthquakes.features").alias("feature"))
        .select(
            "run_id",
            "_ingested_at",
            "_source_file",
            F.col("feature.id").alias("event_id"),
            F.to_json("feature").alias("raw_json"),
        )
        .filter(F.col("event_id").isNotNull())
        .dropDuplicates(["run_id", "event_id"])
    )
    _merge_insert_only(
        spark,
        quakes_df,
        f"{catalog}.bronze.earthquake_events",
        "target.run_id = source.run_id AND target.event_id = source.event_id",
    )

    wx_df = (
        raw.select("run_id", "_ingested_at", F.explode_outer("weather_snapshots").alias("w"))
        .select(
            "run_id",
            "_ingested_at",
            F.col("w.event_id").alias("event_id"),
            F.to_timestamp("w.requested_at").alias("requested_at"),
            F.to_json("w.response").alias("raw_json"),
        )
        .filter(F.col("event_id").isNotNull())
        .dropDuplicates(["run_id", "event_id", "requested_at"])
    )
    _merge_insert_only(
        spark,
        wx_df,
        f"{catalog}.bronze.weather_snapshots",
        "target.run_id = source.run_id AND target.event_id = source.event_id AND target.requested_at <=> source.requested_at",
    )
    return runs_df.count()

# COMMAND ----------

"""Silver Layer: Normalize (3NF), validate, and deduplicate Bronze records via Delta MERGE."""


def build_silver(spark: SparkSession, catalog: str) -> int:
    bq = spark.table(f"{catalog}.bronze.earthquake_events")
    quake_schema = (
        "struct<id:string,geometry:struct<coordinates:array<double>>,"
        "properties:struct<mag:double,place:string,time:bigint,updated:bigint,url:string,"
        "detail:string,status:string,tsunami:int,sig:int,net:string,code:string,types:string,"
        "title:string,alert:string,felt:int,cdi:double,mmi:double,rms:double,dmin:double,"
        "magType:string,type:string,depth:double>>"
    )
    parsed = bq.withColumn("j", F.from_json("raw_json", quake_schema))
    quake = parsed.select(
        "event_id",
        F.col("j.properties.time").alias("event_time_ms"),
        F.col("j.properties.updated").alias("updated_ms"),
        F.col("j.properties.mag").alias("magnitude"),
        F.coalesce(F.col("j.properties.place"), F.lit("Unknown")).alias("place"),
        F.element_at("j.geometry.coordinates", 1).alias("longitude"),
        F.element_at("j.geometry.coordinates", 2).alias("latitude"),
        F.element_at("j.geometry.coordinates", 3).alias("depth_km"),
        F.col("j.properties.url").alias("url"),
        F.col("j.properties.status").alias("status"),
        F.coalesce(F.col("j.properties.tsunami"), F.lit(0)).alias("tsunami"),
        F.col("j.properties.sig").alias("significance"),
        F.col("j.properties.net").alias("network"),
        F.col("j.properties.code").alias("network_code"),
        F.coalesce(F.col("j.properties.magType"), F.lit("unknown")).alias("magnitude_type"),
        F.to_timestamp(F.from_unixtime(F.col("j.properties.time") / 1000)).alias("event_ts_utc"),
        F.to_timestamp(F.from_unixtime(F.col("j.properties.updated") / 1000)).alias("updated_at_utc"),
        "_ingested_at",
    )
    quake = quake.filter(
        F.col("event_id").isNotNull()
        & F.col("event_time_ms").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
    )
    w_quake = Window.partitionBy("event_id").orderBy(F.col("updated_ms").desc_nulls_last(), F.col("_ingested_at").desc())
    quake = quake.withColumn("rn", F.row_number().over(w_quake)).filter("rn = 1").drop("rn", "event_time_ms", "updated_ms")

    locations = (
        quake.select("latitude", "longitude", "depth_km", "place")
        .distinct()
        .withColumn("location_id", F.xxhash64("latitude", "longitude", "depth_km", "place"))
        .select("location_id", "latitude", "longitude", "depth_km", "place")
    )
    _upsert_delta(spark, locations, f"{catalog}.silver.location", "target.location_id = source.location_id")

    magnitude_types = (
        quake.select(F.col("magnitude_type").alias("code"))
        .distinct()
        .withColumn("magnitude_type_id", F.xxhash64("code"))
        .select("magnitude_type_id", "code")
    )
    _upsert_delta(spark, magnitude_types, f"{catalog}.silver.magnitude_type", "target.magnitude_type_id = source.magnitude_type_id")

    normalized = (
        quake.join(
            locations,
            (quake.latitude == locations.latitude)
            & (quake.longitude == locations.longitude)
            & (quake.depth_km.eqNullSafe(locations.depth_km))
            & (quake.place == locations.place),
            "left",
        )
        .join(magnitude_types, quake.magnitude_type == magnitude_types.code, "left")
        .select(
            quake.event_id,
            quake.event_ts_utc.alias("event_time_utc"),
            quake.updated_at_utc,
            quake.magnitude,
            "location_id",
            "magnitude_type_id",
            quake.status,
            quake.significance,
            quake.tsunami,
            quake.network,
            quake.network_code,
            quake.url.alias("source_url"),
            quake._ingested_at,
        )
    )
    _upsert_delta(
        spark,
        normalized,
        f"{catalog}.silver.earthquake",
        "target.event_id = source.event_id",
        update_condition="source.updated_at_utc >= target.updated_at_utc OR target.updated_at_utc IS NULL",
    )
    _upsert_delta(
        spark,
        quake,
        f"{catalog}.silver.earthquakes",
        "target.event_id = source.event_id",
        update_condition="source.updated_at_utc >= target.updated_at_utc OR target.updated_at_utc IS NULL",
    )

    bw = spark.table(f"{catalog}.bronze.weather_snapshots")
    wx_schema = (
        "struct<latitude:double,longitude:double,elevation:double,timezone:string,"
        "current:struct<time:string,interval:int,temperature_2m:double,relative_humidity_2m:int,"
        "apparent_temperature:double,precipitation:double,weather_code:int,wind_speed_10m:double>,"
        "current_units:map<string,string>>"
    )
    wj = bw.withColumn("j", F.from_json("raw_json", wx_schema))
    weather = wj.select(
        "event_id",
        F.to_timestamp("requested_at").alias("requested_at"),
        F.to_timestamp("j.current.time").alias("observation_ts_utc"),
        F.col("j.latitude").alias("grid_latitude"),
        F.col("j.longitude").alias("grid_longitude"),
        F.col("j.elevation").alias("elevation_m"),
        F.col("j.timezone").alias("timezone"),
        F.col("j.current.temperature_2m").alias("temperature_c"),
        F.col("j.current.relative_humidity_2m").alias("relative_humidity_pct"),
        F.col("j.current.apparent_temperature").alias("apparent_temperature_c"),
        F.col("j.current.precipitation").alias("precipitation_mm"),
        F.col("j.current.weather_code").alias("weather_code"),
        F.col("j.current.wind_speed_10m").alias("wind_speed_kmh"),
    )
    w_wx = Window.partitionBy("event_id", "observation_ts_utc").orderBy(F.col("requested_at").desc_nulls_last())
    curated_weather = (
        weather.filter(F.col("event_id").isNotNull() & F.col("observation_ts_utc").isNotNull())
        .withColumn("rn", F.row_number().over(w_wx))
        .filter("rn = 1")
        .drop("rn")
    )
    _upsert_delta(
        spark,
        curated_weather,
        f"{catalog}.silver.weather_snapshots",
        "target.event_id = source.event_id AND target.observation_ts_utc = source.observation_ts_utc",
        update_condition="source.requested_at >= target.requested_at OR target.requested_at IS NULL",
    )

    weather_obs = curated_weather.select(
        "event_id",
        F.col("observation_ts_utc").alias("observation_time_utc"),
        F.col("requested_at").alias("requested_at_utc"),
        "grid_latitude",
        "grid_longitude",
        "temperature_c",
        "apparent_temperature_c",
        "relative_humidity_pct",
        "precipitation_mm",
        "weather_code",
        "wind_speed_kmh",
    )
    _upsert_delta(
        spark,
        weather_obs,
        f"{catalog}.silver.weather_observation",
        "target.event_id = source.event_id AND target.observation_time_utc = source.observation_time_utc",
        update_condition="source.requested_at_utc >= target.requested_at_utc OR target.requested_at_utc IS NULL",
    )
    return quake.count()

# COMMAND ----------

"""Gold Layer: Build an analytical Star Schema from Silver Delta tables via Delta MERGE."""


def build_gold(spark: SparkSession, catalog: str) -> int:
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
    _upsert_delta(spark, mag, f"{catalog}.gold.dim_magnitude_type", "target.magnitude_type_key = source.magnitude_type_key")

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

# COMMAND ----------

landing_count = build_bronze(spark, landing_path, catalog)
print(f"Bronze run envelopes processed: {landing_count}")

# COMMAND ----------

silver_events = build_silver(spark, catalog)
print(f"Unique validated Silver earthquakes: {silver_events}")

# COMMAND ----------

gold_rows = build_gold(spark, catalog)
print(f"Gold fact rows: {gold_rows}")

# COMMAND ----------

display(spark.table(f"{catalog}.gold.fact_earthquake").orderBy("event_ts_utc", ascending=False).limit(20))
