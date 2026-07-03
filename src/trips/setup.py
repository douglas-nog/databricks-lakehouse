from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.profile("dbc-azure-lab").getOrCreate()

CATALOG = "databrickslab"
SCHEMA = "trips"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

print("UC Setup completed")
