"""Failure injection harness — mutates demo data and records ground truth."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.common.constants import DEMO_BRONZE, DEMO_SILVER, FAILURE_TYPES, OPS_BRONZE

TABLE_FORMAT = "delta"


@dataclass(frozen=True)
class InjectionResult:
    injection_id: str
    failure_type: str
    pipeline_key: str
    job_run_id: str
    target_table: str
    detail: dict[str, Any]


def _ensure_injected_failures_table(spark: Any, table: str | None = None) -> None:
    table = table or f"{OPS_BRONZE}.injected_failures"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_BRONZE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          injection_id STRING,
          failure_type STRING,
          pipeline_key STRING,
          job_run_id STRING,
          target_table STRING,
          detail_json STRING,
          injected_at TIMESTAMP
        )
        USING {TABLE_FORMAT}
        """
    )


def _record_ground_truth(spark: Any, result: InjectionResult) -> None:
    import json

    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    table = f"{OPS_BRONZE}.injected_failures"
    _ensure_injected_failures_table(spark, table)
    schema = StructType(
        [
            StructField("injection_id", StringType(), False),
            StructField("failure_type", StringType(), False),
            StructField("pipeline_key", StringType(), False),
            StructField("job_run_id", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("detail_json", StringType(), False),
            StructField("injected_at", TimestampType(), False),
        ]
    )
    row = {
        "injection_id": result.injection_id,
        "failure_type": result.failure_type,
        "pipeline_key": result.pipeline_key,
        "job_run_id": result.job_run_id,
        "target_table": result.target_table,
        "detail_json": json.dumps(result.detail, default=str),
        "injected_at": datetime.now(UTC).replace(tzinfo=None),
    }
    spark.createDataFrame([row], schema=schema).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)


def _materialize(df: Any) -> Any:
    """Break lineage without PERSIST (unsupported on serverless)."""
    return df.sparkSession.createDataFrame(df.collect(), schema=df.schema)


def _overwrite_table(spark: Any, table: str, df: Any) -> None:
    """Overwrite a table without reading it concurrently (local Spark + Delta)."""
    tmp = f"{table}__chaos_tmp"
    spark.sql(f"DROP TABLE IF EXISTS {tmp}")
    writer = df.write.format(TABLE_FORMAT).mode("overwrite")
    if TABLE_FORMAT == "delta":
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(tmp)
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.table(tmp).write.format(TABLE_FORMAT).mode("overwrite").saveAsTable(table)
    spark.sql(f"DROP TABLE IF EXISTS {tmp}")


def inject_null_spike(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Null out email on a large share of silver customers."""
    table = f"{DEMO_SILVER}.customers"
    run_id = job_run_id or f"chaos-null-{uuid.uuid4().hex[:10]}"
    df = spark.table(table)
    from pyspark.sql import functions as F

    mutated = _materialize(
        df.withColumn(
            "email",
            F.when((F.hash(F.col("customer_id")) % 2) == 0, F.lit(None)).otherwise(F.col("email")),
        )
    )
    _overwrite_table(spark, table, mutated)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="null_spike",
        pipeline_key="customers_scd2",
        job_run_id=run_id,
        target_table=table,
        detail={"column": "email", "approx_null_fraction": 0.5},
    )
    _record_ground_truth(spark, result)
    return result


def inject_volume_anomaly(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Drop most bronze orders to simulate a volume collapse."""
    table = f"{DEMO_BRONZE}.raw_orders"
    run_id = job_run_id or f"chaos-volume-{uuid.uuid4().hex[:10]}"
    df = spark.table(table)
    kept = _materialize(df.limit(max(1, df.count() // 20)))
    _overwrite_table(spark, table, kept)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="volume_anomaly",
        pipeline_key="orders_ingest",
        job_run_id=run_id,
        target_table=table,
        detail={"kept_fraction": 0.05},
    )
    _record_ground_truth(spark, result)
    return result


def inject_duplicate_explosion(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Duplicate silver orders to explode row counts."""
    table = f"{DEMO_SILVER}.orders"
    run_id = job_run_id or f"chaos-dup-{uuid.uuid4().hex[:10]}"
    df = spark.table(table)
    exploded = _materialize(df.unionByName(df))
    _overwrite_table(spark, table, exploded)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="duplicate_explosion",
        pipeline_key="orders_ingest",
        job_run_id=run_id,
        target_table=table,
        detail={"multiplier": 2},
    )
    _record_ground_truth(spark, result)
    return result


def inject_schema_drift(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Rename a bronze products column to simulate upstream schema drift."""
    table = f"{DEMO_BRONZE}.raw_products"
    run_id = job_run_id or f"chaos-schema-{uuid.uuid4().hex[:10]}"
    df = spark.table(table)
    if "category" in df.columns:
        mutated = df.withColumnRenamed("category", "product_category_v2")
    else:
        from pyspark.sql import functions as F

        mutated = df.withColumn("unexpected_col", F.lit("drift"))
    mutated = _materialize(mutated)
    _overwrite_table(spark, table, mutated)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="schema_drift",
        pipeline_key="products_catalog",
        job_run_id=run_id,
        target_table=table,
        detail={"change": "rename_or_add_column"},
    )
    _record_ground_truth(spark, result)
    return result


def inject_late_data(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Shift event timestamps far into the past relative to processing time."""
    table = f"{DEMO_SILVER}.events"
    run_id = job_run_id or f"chaos-late-{uuid.uuid4().hex[:10]}"
    from pyspark.sql import functions as F

    df = spark.table(table)
    ts_col = "event_ts" if "event_ts" in df.columns else ("ts" if "ts" in df.columns else None)
    if ts_col is None:
        mutated = df.withColumn("event_lag_hours", F.lit(72))
    else:
        mutated = df.withColumn(ts_col, F.col(ts_col) - F.expr("INTERVAL 72 HOURS"))
    mutated = _materialize(mutated)
    _overwrite_table(spark, table, mutated)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="late_data",
        pipeline_key="events_clickstream",
        job_run_id=run_id,
        target_table=table,
        detail={"lag_hours": 72, "ts_col": ts_col},
    )
    _record_ground_truth(spark, result)
    return result


def inject_job_crash_marker(spark: Any, *, job_run_id: str | None = None) -> InjectionResult:
    """Record a synthetic crash ground-truth row (actual crash uses fixture job)."""
    run_id = job_run_id or f"chaos-crash-{uuid.uuid4().hex[:10]}"
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type="job_crash",
        pipeline_key="ops_force_fail",
        job_run_id=run_id,
        target_table=f"{OPS_BRONZE}.raw_task_logs",
        detail={"note": "Use ops-force-fail-fixture job for a real FAILED task; this records expected class"},
    )
    _record_ground_truth(spark, result)
    return result


INJECTORS = {
    "null_spike": inject_null_spike,
    "volume_anomaly": inject_volume_anomaly,
    "duplicate_explosion": inject_duplicate_explosion,
    "schema_drift": inject_schema_drift,
    "late_data": inject_late_data,
    "job_crash": inject_job_crash_marker,
}


def inject(spark: Any, failure_type: str, *, job_run_id: str | None = None) -> InjectionResult:
    if failure_type not in INJECTORS:
        raise ValueError(f"Unknown failure_type {failure_type}; expected one of {FAILURE_TYPES}")
    return INJECTORS[failure_type](spark, job_run_id=job_run_id)


def inject_all(spark: Any) -> list[InjectionResult]:
    return [inject(spark, ft) for ft in FAILURE_TYPES]
