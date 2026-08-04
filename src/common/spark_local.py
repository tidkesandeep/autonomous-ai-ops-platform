"""Local Spark session helpers for tests (no Unity Catalog required)."""

from __future__ import annotations

from pyspark.sql import SparkSession


def local_spark(app_name: str = "ai-ops-tests") -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
