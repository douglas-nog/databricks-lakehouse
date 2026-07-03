from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_gold(spark: SparkSession, source_table: str, gold_table: str) -> None:
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


if __name__ == "__main__":
    spark = DatabricksSession.builder.profile("dbc-azure-lab").getOrCreate()
    build_gold(
        spark=spark,
        source_table="databrickslab.trips.silver",
        gold_table="databrickslab.trips.gold"
    )
