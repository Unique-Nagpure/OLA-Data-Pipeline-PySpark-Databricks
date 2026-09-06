"""
validation_rules.py
---------------------
Row-level business validation for the trips fact table. Unlike
quality_checks.py (which only measures and reports), these functions
classify and split records: bad rows are quarantined with a reason
rather than silently dropped, so they remain auditable.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def classify_trips(df: DataFrame) -> DataFrame:
    """
    Add a `rejection_reason` column (NULL if the row is valid) based on
    OLA trip business rules:
      - customer_id / driver_id must be present
      - fare_amount and distance_km must be non-negative
      - status must be one of the known values
      - for completed trips, drop_ts must be after pickup_ts
      - customer_rating (if present) must be between 1 and 5
    """
    valid_statuses = ["COMPLETED", "CANCELLED"]

    return df.withColumn(
        "rejection_reason",
        F.when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("driver_id").isNull(), "missing_driver_id")
        .when(F.col("fare_amount") < 0, "negative_fare")
        .when(F.col("distance_km") < 0, "negative_distance")
        .when(~F.col("status").isin(valid_statuses), "invalid_status")
        .when(
            (F.col("status") == "COMPLETED") & (F.col("drop_ts") <= F.col("pickup_ts")),
            "drop_before_pickup",
        )
        .when(
            F.col("customer_rating").isNotNull()
            & ((F.col("customer_rating") < 1) | (F.col("customer_rating") > 5)),
            "rating_out_of_range",
        )
        .otherwise(None),
    )


def split_valid_invalid(df: DataFrame):
    """Split a classified DataFrame into (valid_df, quarantine_df)."""
    classified = classify_trips(df)
    valid_df = classified.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    quarantine_df = classified.filter(F.col("rejection_reason").isNotNull())
    return valid_df, quarantine_df


def deduplicate_trips(df: DataFrame) -> DataFrame:
    """Drop exact duplicate trip_id rows, keeping the first occurrence."""
    return df.dropDuplicates(["trip_id"])
