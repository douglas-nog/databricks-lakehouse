from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def ingest_bronze(spark: SparkSession, source_path: str, checkpoint_path: str,
                  schema_path: str, target_table: str) -> None:
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


if __name__ == "__main__":
    spark = DatabricksSession.builder.profile("dbc-azure-lab").getOrCreate()
    ingest_bronze(
        spark=spark,
        source_path="/Volumes/databrickslab/trips/raw",
        checkpoint_path="/Volumes/databrickslab/trips/_internal/_checkpoints/bronze",
        schema_path="/Volumes/databrickslab/trips/_internal/_schemas/bronze",
        target_table="databrickslab.trips.bronze",
    )
