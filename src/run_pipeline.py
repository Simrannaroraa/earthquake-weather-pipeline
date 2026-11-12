"""Databricks entry point. Run with spark-submit or as a Databricks notebook job."""
import argparse
from pyspark.sql import SparkSession
from transform_bronze import run as bronze
from transform_silver import run as silver
from transform_gold import run as gold


def main():
    p = argparse.ArgumentParser(); p.add_argument("--landing", required=True, help="DBFS/Volumes path containing run_*.json")
    p.add_argument("--catalog", default="workspace"); args = p.parse_args()
    spark = SparkSession.builder.appName("earthquake-weather-pipeline").getOrCreate()
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{schema}")
    print("Bronze landing envelopes:", bronze(spark, args.landing, args.catalog))
    print("Silver unique events:", silver(spark, args.catalog))
    print("Gold fact rows:", gold(spark, args.catalog))
    spark.stop()

if __name__ == "__main__": main()
