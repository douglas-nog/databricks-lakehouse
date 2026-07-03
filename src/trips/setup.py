from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("dbc-azure-lab").getOrCreate()

CATALOG = "databrickslab"
SCHEMA = "trips"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}._internal")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.gold (
        trip_date DATE,
        total_revenue DOUBLE,
        trip_count BIGINT
    ) USING DELTA
""")

print("UC Setup completed")
