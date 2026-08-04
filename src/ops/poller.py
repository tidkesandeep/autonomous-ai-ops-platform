"""Databricks Jobs API poller for crash/OOM telemetry ingestion."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from src.common.constants import OPS_BRONZE

ERROR_RE = re.compile(r"(Exception|Error|OOM|OutOfMemoryError|Traceback)", re.IGNORECASE)


@dataclass
class FailedTaskLog:
    run_id: str
    task_run_id: str
    job_id: str
    pipeline_key: str
    task_key: str
    lifecycle_state: str
    result_state: str | None
    error_signature: str
    raw_output: str
    collected_at: datetime


class DatabricksJobsApiClient:
    """Small REST client for `runs/list` + `runs/get-output`."""

    def __init__(self, host: str, token: str, timeout_s: int = 30) -> None:
        self.host = host.rstrip("/")
        self.timeout_s = timeout_s
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    @classmethod
    def from_env(cls) -> DatabricksJobsApiClient:
        host = os.environ["DATABRICKS_HOST"]
        token = os.environ["DATABRICKS_TOKEN"]
        return cls(host=host, token=token)

    @classmethod
    def from_databricks_notebook(cls, spark: Any, dbutils: Any) -> DatabricksJobsApiClient:
        """Build a client from notebook workspace context (no PAT in source)."""
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        token = ctx.apiToken().get()
        host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
        return cls(host=host, token=token)

    def runs_list(self, completed_only: bool = True, start_time_from_ms: int | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {
            "completed_only": str(completed_only).lower(),
            "expand_tasks": "true",
        }
        if start_time_from_ms is not None:
            params["start_time_from"] = start_time_from_ms
        resp = self.session.get(
            f"{self.host}/api/2.1/jobs/runs/list",
            params=params,
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()

    def run_output(self, run_id: int | str) -> dict[str, Any]:
        resp = self.session.get(
            f"{self.host}/api/2.1/jobs/runs/get-output",
            params={"run_id": run_id},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()


def _signature(text: str) -> str:
    match = ERROR_RE.search(text)
    if not match:
        return "unknown_failure"
    return match.group(0)


def failed_task_logs_from_runs_payload(payload: dict[str, Any], now: datetime | None = None) -> list[FailedTaskLog]:
    """Extract failed tasks from `runs/list` payload; output fetch happens later."""
    now = now or datetime.now(UTC)
    rows: list[FailedTaskLog] = []
    for run in payload.get("runs", []):
        run_id = str(run.get("run_id"))
        job_id = str(run.get("job_id"))
        pipeline_key = run.get("run_name") or f"job_{job_id}"
        for task in run.get("tasks", []):
            state = task.get("state", {}) or {}
            lifecycle = state.get("life_cycle_state", "")
            result = state.get("result_state")
            if result not in {"FAILED", "TIMEDOUT"} and lifecycle not in {"INTERNAL_ERROR"}:
                continue
            task_run_id = str(task.get("run_id", run_id))
            task_key = task.get("task_key", "unknown_task")
            message = state.get("state_message", "") or ""
            rows.append(
                FailedTaskLog(
                    run_id=run_id,
                    task_run_id=task_run_id,
                    job_id=job_id,
                    pipeline_key=pipeline_key,
                    task_key=task_key,
                    lifecycle_state=lifecycle,
                    result_state=result,
                    error_signature=_signature(message),
                    raw_output=message,
                    collected_at=now,
                )
            )
    return rows


def ingest_recent_failed_task_runs(
    spark: Any,
    client: DatabricksJobsApiClient,
    lookback_hours: int = 2,
    table_name: str = f"{OPS_BRONZE}.raw_task_logs",
) -> int:
    """Poll failed tasks and append raw logs into ops bronze."""
    since_ms = int((datetime.now(UTC) - timedelta(hours=lookback_hours)).timestamp() * 1000)
    runs = client.runs_list(completed_only=True, start_time_from_ms=since_ms)
    rows = failed_task_logs_from_runs_payload(runs)

    enriched: list[dict[str, Any]] = []
    for row in rows:
        merged = row.raw_output
        try:
            output = client.run_output(row.task_run_id)
            notebook_output = (((output.get("notebook_output") or {}).get("result")) or "").strip()
            error = (output.get("error") or "").strip()
            merged = "\n".join(p for p in [row.raw_output, error, notebook_output] if p)
        except requests.HTTPError as exc:
            merged = "\n".join(
                p for p in [row.raw_output, f"get-output failed: {exc.response.status_code} {exc.response.text[:500]}"] if p
            )
        enriched.append(
            {
                "run_id": row.run_id,
                "task_run_id": row.task_run_id,
                "job_id": row.job_id,
                "pipeline_key": row.pipeline_key,
                "task_key": row.task_key,
                "lifecycle_state": row.lifecycle_state,
                "result_state": row.result_state,
                "error_signature": _signature(merged),
                "raw_output": merged,
                "collected_at": row.collected_at.replace(tzinfo=None),
            }
        )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_BRONZE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
        USING DELTA
        """
    )

    if not enriched:
        return 0

    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

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
    spark.createDataFrame(enriched, schema=schema).write.mode("append").format("delta").saveAsTable(table_name)
    return len(enriched)
