import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_gold(spark: SparkSession, catalog: str) -> None:
    """Aggregate silver into daily revenue and trip volume via MERGE upsert."""
    source_table = f"{catalog}.silver.trips"
    gold_table = f"{catalog}.gold.trip_daily_metrics"

    daily = (
        spark.table(source_table)
        .groupBy(F.to_date("pickup_datetime").alias("trip_date"))
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("*").alias("trip_count"),
        )
    )

    daily.createOrReplaceTempView("daily_agg")

    spark.sql(f"""
        MERGE INTO {gold_table} AS target
        USING daily_agg AS source
        ON target.trip_date = source.trip_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"Gold merge completed for {gold_table}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    build_gold(spark, args.catalog)


if __name__ == "__main__":
    main()
