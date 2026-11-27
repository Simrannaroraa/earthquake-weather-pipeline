"""Databricks entry point. Run with spark-submit or as a Databricks Python task."""
import argparse
from pyspark.sql import SparkSession
from transform_bronze import run as bronze
from transform_silver import run as silver
from transform_gold import run as gold


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--landing", required=True, help="Unity Catalog Volume path containing run_*.json")
    parser.add_argument("--catalog", default="workspace")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("earthquake-weather-pipeline").getOrCreate()
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{schema}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {args.catalog}.bronze.landing")

    print("Bronze landing envelopes:", bronze(spark, args.landing, args.catalog))
    print("Silver unique events:", silver(spark, args.catalog))
    print("Gold fact rows:", gold(spark, args.catalog))
    spark.stop()


if __name__ == "__main__":
    main()
