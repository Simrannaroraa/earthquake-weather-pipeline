"""Idempotently land raw JSON envelopes and extracted source objects to Bronze Delta."""
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, functions as F


def _merge_insert_only(spark: SparkSession, df: DataFrame, table_name: str, condition: str) -> None:
    """Append only unseen records into Bronze Delta using MERGE."""
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


def run(spark: SparkSession, landing_path: str, catalog: str) -> int:
    """Read raw landing JSON envelopes and merge unseen runs into Bronze Delta tables."""
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
