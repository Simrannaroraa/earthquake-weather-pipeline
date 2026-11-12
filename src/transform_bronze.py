"""Append immutable landing envelopes and extracted source objects to Bronze Delta."""
from pyspark.sql import SparkSession, functions as F


def run(spark: SparkSession, landing_path: str, catalog: str):
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
