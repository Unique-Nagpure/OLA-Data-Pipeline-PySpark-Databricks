"""
ingestion_utils.py
-------------------
Format-agnostic ingestion helpers so the ingestion notebook can read
CSV, JSON, and Parquet sources through one consistent code path
instead of one-off logic per format.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_source(spark: SparkSession, path: str, fmt: str, schema=None, options: dict = None) -> DataFrame:
    """
    Read a raw source file of a given format (csv, json, parquet) into a DataFrame.
    `schema` (StructType) is optional but recommended for csv/json to avoid
    inference drift between ingestion runs.
    """
    options = options or {}
    reader = spark.read.format(fmt)

    for key, value in options.items():
        reader = reader.option(key, value)

    if schema is not None:
        reader = reader.schema(schema)
    elif fmt in ("csv", "json"):
        reader = reader.option("inferSchema", "true")

    if fmt == "csv":
        reader = reader.option("header", "true")

    return reader.load(path)


def add_ingestion_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Attach standard lineage/audit columns applied to every bronze table."""
    return (
        df.withColumn("_source_system", F.lit(source_name))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_bronze_table(df: DataFrame, catalog: str, table_name: str, mode: str = "overwrite") -> None:
    """Persist an ingested DataFrame as a bronze Delta table."""
    (
        df.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.bronze.{table_name}")
    )
