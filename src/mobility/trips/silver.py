import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def rename_columns(df: DataFrame) -> DataFrame:
    """Standardize source column names to snake_case."""
    return (
        df
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumnRenamed("Airport_fee", "airport_fee")
    )


def apply_quarantine_reason(df: DataFrame) -> DataFrame:
    """Add a quarantine_reason column: null when valid, first failing rule otherwise.

    Assumes columns are already renamed (pickup_datetime, dropoff_datetime, etc.)
    and that file_name is present for date-range validation.
    """
    return df.withColumn(
        "quarantine_reason",
        F.when(
            F.date_format(
                "pickup_datetime", "yyyy-MM") != F.regexp_extract("file_name", r"(\d{4}-\d{2})", 1),
            "date_out_of_file_range",
        )
        .when(
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 3600 > 24,
            "implausible_duration",
        )
        .when(F.col("dropoff_datetime") < F.col("pickup_datetime"), "inverted_timestamps")
        .when(F.col("trip_distance") <= 0, "non_positive_distance")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when((F.col("passenger_count").isNull()) | (F.col("passenger_count") == 0), "invalid_passenger_count")
        .otherwise(None),
    )


def transform_silver(spark: SparkSession, catalog: str) -> None:
    """Stream from bronze, apply transformations, route valid/invalid rows."""
    source_table = f"{catalog}.bronze.trips"
    silver_table = f"{catalog}.silver.trips"
    quarantine_table = f"{catalog}.silver.trips_quarantine"
    silver_checkpoint = f"/Volumes/{catalog}/silver/_internal/_checkpoints/trips"
    quarantine_checkpoint = f"/Volumes/{catalog}/silver/_internal/_checkpoints/trips_quarantine"

    stream = spark.readStream.table(source_table)
    renamed = rename_columns(stream)
    with_reason = apply_quarantine_reason(renamed)

    silver = with_reason.filter(
        F.col("quarantine_reason").isNull()).drop("quarantine_reason")
    quarantine = with_reason.filter(F.col("quarantine_reason").isNotNull())

    silver_query = (
        silver.writeStream
        .option("checkpointLocation", silver_checkpoint)
        .trigger(availableNow=True)
        .toTable(silver_table)
    )
    quarantine_query = (
        quarantine.writeStream
        .option("checkpointLocation", quarantine_checkpoint)
        .trigger(availableNow=True)
        .toTable(quarantine_table)
    )

    silver_query.awaitTermination()
    quarantine_query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    transform_silver(spark, args.catalog)


if __name__ == "__main__":
    main()
