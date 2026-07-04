import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ingest_bronze(spark: SparkSession, catalog: str) -> None:
    source_path = f"/Volumes/{catalog}/bronze/raw"
    checkpoint_path = f"/Volumes/{catalog}/bronze/_internal/_checkpoints/trips"
    schema_path = f"/Volumes/{catalog}/bronze/_internal/_schemas/trips"
    target_table = f"{catalog}.bronze.trips"

    query = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(source_path)
        .select(
            "*",
            F.col("_metadata.file_name").alias("file_name"),
            F.current_timestamp().alias("ingestion_time"),
        )
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    ingest_bronze(spark, args.catalog)


if __name__ == "__main__":
    main()
