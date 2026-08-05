"""Quarantine bad rows and leave a clean subset in the live table."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.common.constants import DEMO_SILVER, OPS_GOLD

TABLE_FORMAT = "delta"


def _quarantine_table(source_table: str) -> str:
    # demo.silver.customers → demo.silver.customers_quarantine
    return f"{source_table}_quarantine"


def _overwrite(spark: Any, table: str, df: Any) -> None:
    # Materialize to avoid overwrite-while-read on local Spark.
    material = spark.createDataFrame(df.collect(), schema=df.schema)
    writer = material.write.format(TABLE_FORMAT).mode("overwrite")
    if TABLE_FORMAT == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(table)


def quarantine_null_keys(
    spark: Any,
    *,
    source_table: str,
    column: str,
    key: str,
) -> dict[str, Any]:
    """Move rows with null ``column`` into a quarantine table; keep non-null rows."""
    from pyspark.sql import functions as F

    df = spark.table(source_table)
    if column not in df.columns:
        return {"ok": False, "error": f"column {column} not in {source_table}"}
    bad = df.filter(F.col(column).isNull())
    good = df.filter(F.col(column).isNotNull())
    q_table = _quarantine_table(source_table)
    bad_count = bad.count()
    good_count = good.count()
    _overwrite(spark, q_table, bad.withColumn("quarantined_at", F.lit(datetime.now(UTC).replace(tzinfo=None))))
    _overwrite(spark, source_table, good)
    return {
        "ok": True,
        "strategy": "drop_null_keys",
        "source_table": source_table,
        "quarantine_table": q_table,
        "quarantined_rows": bad_count,
        "kept_rows": good_count,
        "column": column,
        "key": key,
    }


def quarantine_duplicates(
    spark: Any,
    *,
    source_table: str,
    key: str,
) -> dict[str, Any]:
    """Quarantine duplicate natural keys; keep one row per key (latest if timestamp exists)."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = spark.table(source_table)
    if key not in df.columns:
        return {"ok": False, "error": f"key {key} not in {source_table}"}
    order_cols = []
    for candidate in ("updated_at", "created_at", "order_ts", "event_ts"):
        if candidate in df.columns:
            order_cols.append(F.col(candidate).desc_nulls_last())
    if not order_cols:
        order_cols = [F.monotonically_increasing_id().desc()]
    w = Window.partitionBy(key).orderBy(*order_cols)
    ranked = df.withColumn("_rn", F.row_number().over(w))
    good = ranked.filter(F.col("_rn") == 1).drop("_rn")
    bad = ranked.filter(F.col("_rn") > 1).drop("_rn")
    q_table = _quarantine_table(source_table)
    bad_count = bad.count()
    good_count = good.count()
    _overwrite(spark, q_table, bad.withColumn("quarantined_at", F.lit(datetime.now(UTC).replace(tzinfo=None))))
    _overwrite(spark, source_table, good)
    return {
        "ok": True,
        "strategy": "keep_latest_per_key",
        "source_table": source_table,
        "quarantine_table": q_table,
        "quarantined_rows": bad_count,
        "kept_rows": good_count,
        "key": key,
    }


def run_quarantine(spark: Any, parameters: dict[str, Any], *, pipeline_key: str) -> dict[str, Any]:
    strategy = parameters.get("strategy") or "drop_null_keys"
    table = parameters.get("table") or _default_table(pipeline_key, strategy)
    if strategy == "drop_null_keys":
        return quarantine_null_keys(
            spark,
            source_table=table,
            column=str(parameters.get("column") or "email"),
            key=str(parameters.get("key") or "customer_id"),
        )
    if strategy == "keep_latest_per_key":
        return quarantine_duplicates(
            spark,
            source_table=table,
            key=str(parameters.get("key") or "order_id"),
        )
    return {"ok": False, "error": f"unknown quarantine strategy: {strategy}"}


def _default_table(pipeline_key: str, strategy: str) -> str:
    if pipeline_key == "customers_scd2" or strategy == "drop_null_keys":
        return f"{DEMO_SILVER}.customers"
    if pipeline_key == "orders_ingest" or strategy == "keep_latest_per_key":
        return f"{DEMO_SILVER}.orders"
    return f"{DEMO_SILVER}.customers"


def record_remediation_run(spark: Any, incident_id: str, result: dict[str, Any]) -> None:
    """Append a small ops audit row for analytics (best-effort)."""
    import json

    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    table = f"{OPS_GOLD}.remediation_runs"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          incident_id STRING,
          remediation_type STRING,
          result_json STRING,
          ran_at TIMESTAMP
        ) USING {TABLE_FORMAT}
        """
    )
    schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("remediation_type", StringType(), False),
            StructField("result_json", StringType(), False),
            StructField("ran_at", TimestampType(), False),
        ]
    )
    row = {
        "incident_id": incident_id,
        "remediation_type": "quarantine_reprocess",
        "result_json": json.dumps(result, default=str),
        "ran_at": datetime.now(UTC).replace(tzinfo=None),
    }
    spark.createDataFrame([row], schema=schema).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)
