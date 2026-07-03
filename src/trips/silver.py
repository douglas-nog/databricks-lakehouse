from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def transform_silver(spark: SparkSession, source_table: str,
                     silver_checkpoint: str, silver_table: str,
                     quarantine_checkpoint: str, quarantine_table: str) -> None:
    stream = spark.readStream.table(source_table)

    renamed = (
        stream
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("RatecodeID", "ratecode_id")
        .withColumnRenamed("PULocationID", "pu_location_id")
        .withColumnRenamed("DOLocationID", "do_location_id")
        .withColumnRenamed("Airport_fee", "airport_fee")
    )

    with_reason = renamed.withColumn(
        "quarantine_reason",
        F.when(
            F.date_format(
                "pickup_datetime", "yyyy-MM") != F.regexp_extract("file_name", r"(\d{4}-\d{2})", 1),
            "date_out_of_file_range"
        )
        .when(
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 3600 > 24,
            "implausible_duration"
        )
        .when(F.col("dropoff_datetime") < F.col("pickup_datetime"), "inverted_timestamps")
        .when(F.col("trip_distance") <= 0, "non_positive_distance")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when((F.col("passenger_count").isNull()) | (F.col("passenger_count") == 0), "invalid_passenger_count")
        .otherwise(None)
    )

    silver = with_reason.filter(
        F.col("quarantine_reason").isNull()).drop("quarantine_reason")

    quarantine_silver = with_reason.filter(
        F.col("quarantine_reason").isNotNull())

    silver_query = (
        silver.writeStream
        .option("checkpointLocation", silver_checkpoint)
        .trigger(availableNow=True)
        .toTable(silver_table)
    )

    quarantine_query = (
        quarantine_silver.writeStream
        .option("checkpointLocation", quarantine_checkpoint)
        .trigger(availableNow=True)
        .toTable(quarantine_table)
    )

    silver_query.awaitTermination()
    quarantine_query.awaitTermination()


if __name__ == "__main__":
    spark = DatabricksSession.builder.profile("dbc-azure-lab").getOrCreate()
    transform_silver(
        spark=spark,
        source_table="databrickslab.trips.bronze",
        silver_checkpoint="/Volumes/databrickslab/trips/_internal/_checkpoints/silver",
        quarantine_checkpoint="/Volumes/databrickslab/trips/_internal/_checkpoints/quarantine_silver",
        silver_table="databrickslab.trips.silver",
        quarantine_table="databrickslab.trips.quarantine_silver"
    )
