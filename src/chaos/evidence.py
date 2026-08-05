"""Seed ops telemetry / DQ / crash-log evidence so detection can see chaos injections."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from src.chaos.injector import TABLE_FORMAT, InjectionResult
from src.common.constants import OPS_BRONZE, OPS_GOLD
from src.ops.dq import run_dq_checks


def _ensure_telemetry_table(spark: Any, table: str = f"{OPS_BRONZE}.task_telemetry") -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_BRONZE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          run_id STRING,
          pipeline_key STRING,
          task_name STRING,
          started_at TIMESTAMP,
          ended_at TIMESTAMP,
          duration_ms BIGINT,
          status STRING,
          rows_in BIGINT,
          rows_out BIGINT,
          schema_snapshot_json STRING,
          error_class STRING,
          error_message STRING,
          stacktrace STRING,
          metadata_json STRING
        )
        USING {TABLE_FORMAT}
        """
    )


def _write_telemetry_rows(spark: Any, rows: list[dict[str, Any]]) -> None:
    from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

    _ensure_telemetry_table(spark)
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("pipeline_key", StringType(), False),
            StructField("task_name", StringType(), False),
            StructField("started_at", TimestampType(), False),
            StructField("ended_at", TimestampType(), False),
            StructField("duration_ms", LongType(), False),
            StructField("status", StringType(), False),
            StructField("rows_in", LongType(), True),
            StructField("rows_out", LongType(), True),
            StructField("schema_snapshot_json", StringType(), True),
            StructField("error_class", StringType(), True),
            StructField("error_message", StringType(), True),
            StructField("stacktrace", StringType(), True),
            StructField("metadata_json", StringType(), True),
        ]
    )
    spark.createDataFrame(rows, schema=schema).write.mode("append").format(TABLE_FORMAT).saveAsTable(
        f"{OPS_BRONZE}.task_telemetry"
    )


def _write_crash_log(spark: Any, result: InjectionResult) -> None:
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_BRONZE}")
    table = f"{OPS_BRONZE}.raw_task_logs"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          run_id STRING,
          task_run_id STRING,
          job_id STRING,
          pipeline_key STRING,
          task_key STRING,
          lifecycle_state STRING,
          result_state STRING,
          error_signature STRING,
          raw_output STRING,
          collected_at TIMESTAMP
        )
        USING {TABLE_FORMAT}
        """
    )
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("task_run_id", StringType(), False),
            StructField("job_id", StringType(), False),
            StructField("pipeline_key", StringType(), False),
            StructField("task_key", StringType(), False),
            StructField("lifecycle_state", StringType(), True),
            StructField("result_state", StringType(), True),
            StructField("error_signature", StringType(), True),
            StructField("raw_output", StringType(), True),
            StructField("collected_at", TimestampType(), False),
        ]
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    row = {
        "run_id": result.job_run_id,
        "task_run_id": f"{result.job_run_id}-task",
        "job_id": "chaos-fixture",
        "pipeline_key": result.pipeline_key,
        "task_key": "force_fail",
        "lifecycle_state": "TERMINATED",
        "result_state": "FAILED",
        "error_signature": "OutOfMemoryError",
        "raw_output": "OutOfMemoryError: Java heap space\nTraceback (chaos injection)",
        "collected_at": now,
    }
    spark.createDataFrame([row], schema=schema).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)


def seed_ops_evidence(
    spark: Any,
    result: InjectionResult,
    *,
    checks_path: str | None = None,
) -> dict[str, Any]:
    """Write the ops evidence that detection rules expect for this injection."""
    now = datetime.now(UTC).replace(tzinfo=None)
    earlier = now - timedelta(hours=2)
    info: dict[str, Any] = {"failure_type": result.failure_type, "job_run_id": result.job_run_id}

    if result.failure_type in {"null_spike", "duplicate_explosion", "late_data"}:
        if not checks_path:
            raise ValueError("checks_path required for DQ-backed failure classes")
        dq = run_dq_checks(
            spark,
            checks_path=checks_path,
            run_id=result.job_run_id,
            pipeline_key=result.pipeline_key,
            table_name=f"{OPS_GOLD}.fact_dq_check",
        )
        info["dq"] = dq

    elif result.failure_type == "volume_anomaly":
        rows_out = int(spark.table(result.target_table).count())
        hist = [
            {
                "run_id": f"{result.job_run_id}-hist-{i}",
                "pipeline_key": result.pipeline_key,
                "task_name": "load",
                "started_at": earlier - timedelta(minutes=i),
                "ended_at": earlier - timedelta(minutes=i),
                "duration_ms": 1000,
                "status": "SUCCESS",
                "rows_in": 5000,
                "rows_out": 5000,
                "schema_snapshot_json": None,
                "error_class": None,
                "error_message": None,
                "stacktrace": None,
                "metadata_json": "{}",
            }
            for i in range(3)
        ]
        current = {
            "run_id": result.job_run_id,
            "pipeline_key": result.pipeline_key,
            "task_name": "load",
            "started_at": now,
            "ended_at": now,
            "duration_ms": 800,
            "status": "SUCCESS",
            "rows_in": rows_out,
            "rows_out": rows_out,
            "schema_snapshot_json": None,
            "error_class": None,
            "error_message": None,
            "stacktrace": None,
            "metadata_json": "{}",
        }
        _write_telemetry_rows(spark, [*hist, current])
        info["telemetry_rows_out"] = rows_out

    elif result.failure_type == "schema_drift":
        prev_schema = {
            "product_id": "string",
            "sku": "string",
            "name": "string",
            "category": "string",
            "price_usd": "double",
            "is_active": "boolean",
            "updated_at": "timestamp",
        }
        curr_schema = {f.name: f.dataType.simpleString() for f in spark.table(result.target_table).schema.fields}
        hist = {
            "run_id": f"{result.job_run_id}-hist",
            "pipeline_key": result.pipeline_key,
            "task_name": "load",
            "started_at": earlier,
            "ended_at": earlier,
            "duration_ms": 1000,
            "status": "SUCCESS",
            "rows_in": 200,
            "rows_out": 200,
            "schema_snapshot_json": json.dumps(prev_schema),
            "error_class": None,
            "error_message": None,
            "stacktrace": None,
            "metadata_json": "{}",
        }
        current = {
            "run_id": result.job_run_id,
            "pipeline_key": result.pipeline_key,
            "task_name": "load",
            "started_at": now,
            "ended_at": now,
            "duration_ms": 1100,
            "status": "SUCCESS",
            "rows_in": 200,
            "rows_out": 200,
            "schema_snapshot_json": json.dumps(curr_schema),
            "error_class": None,
            "error_message": None,
            "stacktrace": None,
            "metadata_json": "{}",
        }
        _write_telemetry_rows(spark, [hist, current])
        info["schema_current_cols"] = sorted(curr_schema)

    elif result.failure_type == "job_crash":
        _write_crash_log(spark, result)
        info["raw_task_log"] = True

    else:
        raise ValueError(f"No evidence seeder for {result.failure_type}")

    # Touch ops gold schema so scoring notebooks can assume it exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    return info
