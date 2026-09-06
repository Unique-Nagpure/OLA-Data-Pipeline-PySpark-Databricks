"""
Unit tests for src/validation_rules.py and src/transformations.py.
Run locally with: pytest tests/
"""

import sys
import os
import pytest
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from validation_rules import classify_trips, split_valid_invalid, deduplicate_trips
from transformations import enrich_trips, daily_revenue_by_city


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("ola-tests").getOrCreate()


def _sample_trip(**overrides):
    base = dict(
        trip_id="T1", customer_id="C1", driver_id="D1", vehicle_id="V1",
        pickup_location_id="L1", drop_location_id="L2",
        pickup_ts="2024-01-01T10:00:00", drop_ts="2024-01-01T10:30:00",
        distance_km=5.0, fare_amount=100.0, payment_type="UPI",
        status="COMPLETED", customer_rating=5,
    )
    base.update(overrides)
    return base


def test_classify_trips_flags_negative_fare(spark):
    df = spark.createDataFrame([_sample_trip(fare_amount=-50.0)])
    result = classify_trips(df)
    assert result.first()["rejection_reason"] == "negative_fare"


def test_classify_trips_flags_missing_customer(spark):
    df = spark.createDataFrame([_sample_trip(trip_id="T1"), _sample_trip(trip_id="T2", customer_id=None)])
    result = classify_trips(df)
    row = result.filter("trip_id = 'T2'").first()
    assert row["rejection_reason"] == "missing_customer_id"


def test_classify_trips_passes_clean_row(spark):
    df = spark.createDataFrame([_sample_trip()])
    result = classify_trips(df)
    assert result.first()["rejection_reason"] is None


def test_split_valid_invalid(spark):
    df = spark.createDataFrame([
        _sample_trip(trip_id="T1"),
        _sample_trip(trip_id="T2", distance_km=-1.0),
    ])
    valid_df, quarantine_df = split_valid_invalid(df)
    assert valid_df.count() == 1
    assert quarantine_df.count() == 1
    assert valid_df.first()["trip_id"] == "T1"


def test_deduplicate_trips_removes_exact_duplicates(spark):
    df = spark.createDataFrame([_sample_trip(trip_id="T1"), _sample_trip(trip_id="T1")])
    result = deduplicate_trips(df)
    assert result.count() == 1


def test_daily_revenue_by_city_excludes_cancelled(spark):
    from pyspark.sql import functions as F

    enriched = spark.createDataFrame(
        [
            ("2024-01-01", "Indore", "T1", 100.0, 5.0, "COMPLETED"),
            ("2024-01-01", "Indore", "T2", 50.0, 2.0, "CANCELLED"),
        ],
        ["trip_date", "trip_city", "trip_id", "fare_amount", "distance_km", "status"],
    ).withColumn("trip_date", F.to_date("trip_date"))

    result = daily_revenue_by_city(enriched)
    row = result.first()
    assert row["total_revenue"] == 100.0
    assert row["completed_trips"] == 1
