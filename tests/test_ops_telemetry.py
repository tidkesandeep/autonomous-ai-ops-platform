from __future__ import annotations

from dataclasses import asdict

import pytest

from src.ops.telemetry import TelemetryRecord, with_telemetry


class MemorySink:
    def __init__(self) -> None:
        self.rows: list[TelemetryRecord] = []

    def write(self, record: TelemetryRecord) -> None:
        self.rows.append(record)


def test_telemetry_decorator_success():
    sink = MemorySink()

    @with_telemetry(sink=sink, pipeline_key="orders_ingest", task_name="silver_step")
    def sample() -> dict[str, int]:
        return {"rows_in": 100, "rows_out": 95}

    result = sample()
    assert result["rows_out"] == 95
    assert len(sink.rows) == 1
    row = sink.rows[0]
    assert row.status == "SUCCESS"
    assert row.rows_in == 100
    assert row.rows_out == 95
    assert row.duration_ms >= 0


def test_telemetry_decorator_failure():
    sink = MemorySink()

    @with_telemetry(sink=sink, pipeline_key="orders_ingest", task_name="gold_step")
    def boom() -> None:
        raise RuntimeError("task crashed")

    with pytest.raises(RuntimeError):
        boom()

    assert len(sink.rows) == 1
    row = sink.rows[0]
    assert row.status == "FAILED"
    assert row.error_class == "RuntimeError"
    assert "task crashed" in (row.error_message or "")
    assert "RuntimeError" in (row.stacktrace or "")


def test_telemetry_record_serializable():
    row = TelemetryRecord.base(run_id="r1", pipeline_key="orders_ingest", task_name="x")
    serialized = asdict(row)
    assert serialized["run_id"] == "r1"
