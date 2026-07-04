import pytest
from datetime import datetime
from pyspark.sql import SparkSession

from mobility.silver import apply_quarantine_reason


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("silver-tests")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def _row(pickup, dropoff, distance, fare, passengers, file_name="yellow_tripdata_2024-01.parquet"):
    return (pickup, dropoff, distance, fare, passengers, file_name)


COLUMNS = ["pickup_datetime", "dropoff_datetime", "trip_distance",
           "fare_amount", "passenger_count", "file_name"]


def _reason(spark, row):
    df = spark.createDataFrame([row], COLUMNS)
    result = apply_quarantine_reason(df).collect()[0]
    return result["quarantine_reason"]


def test_valid_row_has_no_reason(spark):
    row = _row(datetime(2024, 1, 15, 10, 0), datetime(
        2024, 1, 15, 10, 30), 5.0, 20.0, 2)
    assert _reason(spark, row) is None


def test_negative_fare_is_quarantined(spark):
    row = _row(datetime(2024, 1, 15, 10, 0), datetime(
        2024, 1, 15, 10, 30), 5.0, -1.0, 2)
    assert _reason(spark, row) == "negative_fare"


def test_non_positive_distance_is_quarantined(spark):
    row = _row(datetime(2024, 1, 15, 10, 0), datetime(
        2024, 1, 15, 10, 30), 0.0, 20.0, 2)
    assert _reason(spark, row) == "non_positive_distance"


def test_inverted_timestamps_is_quarantined(spark):
    row = _row(datetime(2024, 1, 15, 10, 30),
               datetime(2024, 1, 15, 10, 0), 5.0, 20.0, 2)
    assert _reason(spark, row) == "inverted_timestamps"


def test_date_out_of_file_range_is_quarantined(spark):
    # pickup in 2002 but file is 2024-01
    row = _row(datetime(2002, 12, 31, 10, 0), datetime(
        2002, 12, 31, 10, 30), 5.0, 20.0, 2)
    assert _reason(spark, row) == "date_out_of_file_range"


def test_rule_priority_date_over_fare(spark):
    # both date-out-of-range AND negative fare; date wins (first in chain)
    row = _row(datetime(2002, 1, 1, 10, 0), datetime(
        2002, 1, 1, 10, 30), 5.0, -1.0, 2)
    assert _reason(spark, row) == "date_out_of_file_range"
