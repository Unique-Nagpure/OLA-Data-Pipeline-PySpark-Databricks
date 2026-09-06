"""
quality_checks.py
-------------------
Pure PySpark functions that compute data-quality metrics for a table
without modifying it. The Data Quality Check notebook calls these and
logs the results into a dq_results Delta table; nothing here drops or
fixes rows — that happens downstream in the Data Validation notebook.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def null_check(df: DataFrame, required_cols: list) -> DataFrame:
    """
    Count nulls per required column. Returns one row per column with
    the null count and null percentage.
    """
    total = df.count()
    rows = []
    for col in required_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        rows.append((col, null_count, total, round(null_count / total * 100, 2) if total else 0.0))
    return df.sql_ctx.sparkSession.createDataFrame(
        rows, ["column_name", "null_count", "total_rows", "null_pct"]
    )


def duplicate_check(df: DataFrame, key_cols: list) -> int:
    """Return the number of duplicate rows based on the given key column(s)."""
    total = df.count()
    distinct = df.dropDuplicates(key_cols).count()
    return total - distinct


def referential_integrity_check(spark: SparkSession, fact_df: DataFrame, fact_key: str,
                                 dim_df: DataFrame, dim_key: str) -> int:
    """
    Count fact rows whose foreign key does not exist in the dimension table
    (orphan records) — implemented as a SQL left-anti join.
    """
    fact_df.createOrReplaceTempView("_fact_tmp")
    dim_df.createOrReplaceTempView("_dim_tmp")
    orphans = spark.sql(f"""
        SELECT f.* FROM _fact_tmp f
        LEFT ANTI JOIN _dim_tmp d
        ON f.{fact_key} = d.{dim_key}
        WHERE f.{fact_key} IS NOT NULL
    """)
    return orphans.count()


def build_dq_report(spark: SparkSession, table_name: str, checks: list) -> DataFrame:
    """
    Assemble a list of (check_name, status, failed_count, details) tuples
    into a standard DQ report DataFrame with a run timestamp, ready to
    append to the dq_results Delta table.
    """
    rows = [(table_name, name, status, failed_count, details) for name, status, failed_count, details in checks]
    df = spark.createDataFrame(rows, ["table_name", "check_name", "status", "failed_count", "details"])
    return df.withColumn("run_ts", F.current_timestamp())
