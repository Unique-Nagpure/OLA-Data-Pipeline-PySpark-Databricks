"""
transformations.py
--------------------
Reusable PySpark transformation functions for the Data Transformation
notebook: enriching the trips fact table with dimension attributes and
derived columns, then building gold-level aggregates.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def enrich_trips(trips_df: DataFrame, customers_df: DataFrame, drivers_df: DataFrame,
                  vehicles_df: DataFrame, locations_df: DataFrame) -> DataFrame:
    """
    Join the cleaned trips fact table with all dimension tables and add
    derived columns: trip duration, fare per km, time-of-day bucket,
    weekend flag, and day name.
    """
    pickup_loc = locations_df.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("area_name").alias("pickup_area"),
        F.col("city").alias("trip_city"),
    )
    drop_loc = locations_df.select(
        F.col("location_id").alias("drop_location_id"),
        F.col("area_name").alias("drop_area"),
    )

    enriched = (
        trips_df.join(customers_df.select("customer_id", "name", "city").withColumnRenamed("name", "customer_name"),
                       on="customer_id", how="left")
        .join(drivers_df.select("driver_id", "name", "vehicle_id", "rating").withColumnRenamed("name", "driver_name")
              .withColumnRenamed("rating", "driver_rating"),
              on="driver_id", how="left")
        .join(vehicles_df.select("vehicle_id", "vehicle_type", "make", "model"),
              on="vehicle_id", how="left")
        .join(pickup_loc, on="pickup_location_id", how="left")
        .join(drop_loc, on="drop_location_id", how="left")
    )

    enriched = (
        enriched.withColumn("pickup_ts", F.to_timestamp("pickup_ts"))
        .withColumn("drop_ts", F.to_timestamp("drop_ts"))
        .withColumn(
            "trip_duration_minutes",
            F.when(F.col("status") == "COMPLETED",
                   (F.col("drop_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60)
            .otherwise(None),
        )
        .withColumn(
            "fare_per_km",
            F.when((F.col("status") == "COMPLETED") & (F.col("distance_km") > 0),
                   F.round(F.col("fare_amount") / F.col("distance_km"), 2))
            .otherwise(None),
        )
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn(
            "time_of_day",
            F.when(F.col("pickup_hour").between(5, 11), "Morning")
            .when(F.col("pickup_hour").between(12, 16), "Afternoon")
            .when(F.col("pickup_hour").between(17, 20), "Evening")
            .otherwise("Night"),
        )
        .withColumn("day_name", F.date_format("pickup_ts", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("pickup_ts").isin([1, 7]))
        .withColumn("trip_date", F.to_date("pickup_ts"))
    )

    return enriched


def daily_revenue_by_city(enriched_df: DataFrame) -> DataFrame:
    """Gold aggregate: daily completed-trip revenue and ride count per city."""
    return (
        enriched_df.filter(F.col("status") == "COMPLETED")
        .groupBy("trip_date", "trip_city")
        .agg(
            F.sum("fare_amount").alias("total_revenue"),
            F.count("trip_id").alias("completed_trips"),
            F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
        )
        .orderBy("trip_date", "trip_city")
    )


def vehicle_type_performance(enriched_df: DataFrame) -> DataFrame:
    """Gold aggregate: revenue and average fare-per-km by vehicle type."""
    return (
        enriched_df.filter(F.col("status") == "COMPLETED")
        .groupBy("vehicle_type")
        .agg(
            F.count("trip_id").alias("total_trips"),
            F.sum("fare_amount").alias("total_revenue"),
            F.round(F.avg("fare_per_km"), 2).alias("avg_fare_per_km"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def driver_leaderboard(enriched_df: DataFrame) -> DataFrame:
    """Gold aggregate: top drivers by completed trips and revenue generated."""
    return (
        enriched_df.filter(F.col("status") == "COMPLETED")
        .groupBy("driver_id", "driver_name")
        .agg(
            F.count("trip_id").alias("completed_trips"),
            F.sum("fare_amount").alias("total_revenue"),
            F.round(F.avg("customer_rating"), 2).alias("avg_customer_rating"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def cancellation_rate_by_city(enriched_df: DataFrame) -> DataFrame:
    """Gold aggregate: trip cancellation rate per city."""
    return (
        enriched_df.groupBy("trip_city")
        .agg(
            F.count("trip_id").alias("total_trips"),
            F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_trips"),
        )
        .withColumn("cancellation_rate_pct", F.round(F.col("cancelled_trips") / F.col("total_trips") * 100, 2))
        .orderBy(F.col("cancellation_rate_pct").desc())
    )
