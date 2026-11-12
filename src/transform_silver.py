"""Normalize, validate, and deduplicate Bronze records into Silver Delta."""
from pyspark.sql import SparkSession, functions as F, Window


def run(spark: SparkSession, catalog: str):
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
