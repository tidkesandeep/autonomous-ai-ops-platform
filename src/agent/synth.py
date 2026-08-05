"""Synthetic incident + ops evidence for fast agent evaluation (no medallion rebuild)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from src.chaos.evidence import _write_crash_log, _write_telemetry_rows
from src.chaos.injector import InjectionResult
from src.common.constants import OPS_GOLD
from src.ops.dq import TABLE_FORMAT


def open_synthetic_incident(
    conn: Any,
    *,
    job_run_id: str,
    pipeline_key: str,
    failure_type: str,
    severity: str = "medium",
) -> str:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO incidents (job_run_id, pipeline_key, primary_failure_type, severity, status)
            VALUES (%s, %s, %s, %s, 'OPEN')
            ON CONFLICT (job_run_id) DO UPDATE
              SET primary_failure_type = EXCLUDED.primary_failure_type,
                  pipeline_key = EXCLUDED.pipeline_key,
                  status = 'OPEN'
            RETURNING incident_id
            """,
            (job_run_id, pipeline_key, failure_type, severity),
        )
        incident_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO incident_status_events (incident_id, from_status, to_status, changed_by)
            VALUES (%s, NULL, 'OPEN', 'synth_eval')
            """,
            (incident_id,),
        )
        cur.execute(
            """
            INSERT INTO incident_signals (incident_id, failure_type, detected_by, evidence_json)
            VALUES (%s, %s, 'workflow', CAST(%s AS jsonb))
            ON CONFLICT (incident_id, failure_type, detected_by) DO NOTHING
            """,
            (incident_id, failure_type, json.dumps({"source": "synth_eval"})),
        )
        conn.commit()
        return str(incident_id)
    finally:
        cur.close()


def seed_synthetic_evidence(spark: Any, failure_type: str, job_run_id: str, pipeline_key: str) -> None:
    """Write minimal ops rows so read tools return non-empty evidence."""
    now = datetime.now(UTC).replace(tzinfo=None)
    earlier = now - timedelta(hours=2)
    result = InjectionResult(
        injection_id=uuid.uuid4().hex,
        failure_type=failure_type,
        pipeline_key=pipeline_key,
        job_run_id=job_run_id,
        target_table="demo.silver.orders",
        detail={"synth": True},
    )

    if failure_type == "job_crash":
        _write_crash_log(spark, result)
        return

    if failure_type == "schema_drift":
        _write_telemetry_rows(
            spark,
            [
                {
                    "run_id": f"{job_run_id}-hist",
                    "pipeline_key": pipeline_key,
                    "task_name": "load",
                    "started_at": earlier,
                    "ended_at": earlier,
                    "duration_ms": 1000,
                    "status": "SUCCESS",
                    "rows_in": 100,
                    "rows_out": 100,
                    "schema_snapshot_json": json.dumps({"a": "string"}),
                    "error_class": None,
                    "error_message": None,
                    "stacktrace": None,
                    "metadata_json": "{}",
                },
                {
                    "run_id": job_run_id,
                    "pipeline_key": pipeline_key,
                    "task_name": "load",
                    "started_at": now,
                    "ended_at": now,
                    "duration_ms": 1100,
                    "status": "SUCCESS",
                    "rows_in": 100,
                    "rows_out": 100,
                    "schema_snapshot_json": json.dumps({"a": "string", "b": "int"}),
                    "error_class": None,
                    "error_message": None,
                    "stacktrace": None,
                    "metadata_json": "{}",
                },
            ],
        )
        return

    if failure_type == "volume_anomaly":
        _write_telemetry_rows(
            spark,
            [
                {
                    "run_id": f"{job_run_id}-hist",
                    "pipeline_key": pipeline_key,
                    "task_name": "load",
                    "started_at": earlier,
                    "ended_at": earlier,
                    "duration_ms": 1000,
                    "status": "SUCCESS",
                    "rows_in": 5000,
                    "rows_out": 5000,
                    "schema_snapshot_json": None,
                    "error_class": None,
                    "error_message": None,
                    "stacktrace": None,
                    "metadata_json": "{}",
                },
                {
                    "run_id": job_run_id,
                    "pipeline_key": pipeline_key,
                    "task_name": "load",
                    "started_at": now,
                    "ended_at": now,
                    "duration_ms": 800,
                    "status": "SUCCESS",
                    "rows_in": 100,
                    "rows_out": 100,
                    "schema_snapshot_json": None,
                    "error_class": None,
                    "error_message": None,
                    "stacktrace": None,
                    "metadata_json": "{}",
                },
            ],
        )
        return

    # DQ-backed classes: write a failed fact_dq_check row
    check_name = {
        "null_spike": "customers_email_null_rate",
        "duplicate_explosion": "orders_duplicate_rate",
        "late_data": "events_lag_under_48h",
    }.get(failure_type, f"{failure_type}_check")
    metric = {
        "null_spike": "null_rate",
        "duplicate_explosion": "duplicate_rate",
        "late_data": "pct_late",
    }.get(failure_type, "metric")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    table = f"{OPS_GOLD}.fact_dq_check"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          run_id STRING,
          pipeline_key STRING,
          check_name STRING,
          table_name STRING,
          metric_name STRING,
          observed_value DOUBLE,
          threshold_value DOUBLE,
          comparator STRING,
          passed BOOLEAN,
          checked_at TIMESTAMP
        )
        USING {TABLE_FORMAT}
        """
    )
    row = {
        "run_id": job_run_id,
        "pipeline_key": pipeline_key,
        "check_name": check_name,
        "table_name": "demo.silver.customers",
        "metric_name": metric,
        "observed_value": 0.9,
        "threshold_value": 0.2,
        "comparator": "<=",
        "passed": False,
        "checked_at": now,
    }
    spark.createDataFrame([row]).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)
