import argparse
from pyspark.sql import SparkSession


def setup_uc(spark: SparkSession, catalog: str) -> None:

    # Schemas = medallion layers
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

    # Volumes — ingestion artifacts live in the bronze layer
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze._internal")
    # Silver control artifacts (checkpoints)
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.silver._internal")

    # Gold table with explicit schema (business product)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.trip_daily_metrics (
            trip_date DATE,
            total_revenue DOUBLE,
            trip_count BIGINT
        ) USING DELTA
    """)

    print(f"UC setup completed for catalog {catalog}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    setup_uc(spark, args.catalog)


if __name__ == "__main__":
    main()
