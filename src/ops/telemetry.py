"""Task telemetry capture for ops bronze tables."""

from __future__ import annotations

import json
import time
import traceback
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from typing import Any, Protocol

from src.common.constants import OPS_BRONZE


@dataclass
class TelemetryRecord:
    """One task-run telemetry event (success or failure)."""

    run_id: str
    pipeline_key: str
    task_name: str
    started_at: datetime
    ended_at: datetime
    duration_ms: int
    status: str
    rows_in: int | None = None
    rows_out: int | None = None
    schema_snapshot_json: str | None = None
    error_class: str | None = None
    error_message: str | None = None
    stacktrace: str | None = None
    metadata_json: str = field(default_factory=lambda: "{}")

    @classmethod
    def base(cls, run_id: str, pipeline_key: str, task_name: str) -> TelemetryRecord:
        now = datetime.now(UTC)
        return cls(
            run_id=run_id,
            pipeline_key=pipeline_key,
            task_name=task_name,
            started_at=now,
            ended_at=now,
            duration_ms=0,
            status="RUNNING",
        )


class TelemetrySink(Protocol):
    """Persistence backend for telemetry records."""

    def write(self, record: TelemetryRecord) -> None: ...


class SparkTelemetrySink:
    """Delta table sink for task telemetry."""

    def __init__(self, spark: Any, table_name: str = f"{OPS_BRONZE}.task_telemetry") -> None:
        self.spark = spark
        self.table_name = table_name

    def _ensure_table(self) -> None:
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_BRONZE}")
        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
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
            USING DELTA
            """
        )

    def write(self, record: TelemetryRecord) -> None:
        from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

        self._ensure_table()
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
        row = {
            "run_id": record.run_id,
            "pipeline_key": record.pipeline_key,
            "task_name": record.task_name,
            "started_at": record.started_at.replace(tzinfo=None),
            "ended_at": record.ended_at.replace(tzinfo=None),
            "duration_ms": int(record.duration_ms),
            "status": record.status,
            "rows_in": None if record.rows_in is None else int(record.rows_in),
            "rows_out": None if record.rows_out is None else int(record.rows_out),
            "schema_snapshot_json": record.schema_snapshot_json,
            "error_class": record.error_class,
            "error_message": record.error_message,
            "stacktrace": record.stacktrace,
            "metadata_json": record.metadata_json,
        }
        (
            self.spark.createDataFrame([row], schema=schema)
            .write.mode("append")
            .format("delta")
            .saveAsTable(self.table_name)
        )


def _coerce_metadata(value: Any) -> str:
    if value is None:
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def with_telemetry(
    sink: TelemetrySink,
    pipeline_key: str,
    task_name: str,
    metadata_factory: Callable[..., dict[str, Any]] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to persist one telemetry row for each task invocation."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            record = TelemetryRecord.base(
                run_id=f"{pipeline_key}-{task_name}-{uuid.uuid4().hex[:12]}",
                pipeline_key=pipeline_key,
                task_name=task_name,
            )
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                record.status = "SUCCESS"
                if isinstance(result, dict):
                    record.rows_in = result.get("rows_in")
                    record.rows_out = result.get("rows_out")
                    if "schema_snapshot" in result:
                        record.schema_snapshot_json = _coerce_metadata(result.get("schema_snapshot"))
                return result
            except Exception as exc:
                record.status = "FAILED"
                record.error_class = exc.__class__.__name__
                record.error_message = str(exc)
                record.stacktrace = traceback.format_exc()
                raise
            finally:
                record.duration_ms = int((time.perf_counter() - start) * 1000)
                record.ended_at = datetime.now(UTC)
                if metadata_factory is not None:
                    record.metadata_json = _coerce_metadata(metadata_factory(*args, **kwargs))
                sink.write(record)

        return wrapper

    return decorator
