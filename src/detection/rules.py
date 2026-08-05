"""Rule evaluators that turn ops telemetry into DetectedSignal rows."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from src.common.constants import FAILURE_TYPES
from src.detection.baselines import decide_duration, decide_null_rate, decide_volume
from src.detection.signals import DetectedSignal

# Map DQ check name prefixes / metric names → failure class
DQ_FAILURE_MAP = {
    "null": "null_spike",
    "duplicate": "duplicate_explosion",
    "dup": "duplicate_explosion",
    "volume": "volume_anomaly",
    "row_count": "volume_anomaly",
    "late": "late_data",
    "lag": "late_data",
    "schema": "schema_drift",
}


def _map_dq_to_failure(check_name: str, metric_name: str) -> str:
    blob = f"{check_name} {metric_name}".lower()
    for needle, failure in DQ_FAILURE_MAP.items():
        if needle in blob:
            return failure
    return "null_spike"


def detect_job_crashes(task_logs: list[dict[str, Any]], detected_by: str = "poller") -> list[DetectedSignal]:
    """Crash/OOM/FAILED tasks from Jobs API poller output."""
    signals: list[DetectedSignal] = []
    for row in task_logs:
        result = (row.get("result_state") or "").upper()
        lifecycle = (row.get("lifecycle_state") or "").upper()
        signature = (row.get("error_signature") or "").lower()
        raw = (row.get("raw_output") or "").lower()
        is_fail = result in {"FAILED", "TIMEDOUT"} or lifecycle == "INTERNAL_ERROR"
        is_oom = "oom" in signature or "outofmemory" in raw or "out of memory" in raw
        if not is_fail and not is_oom:
            continue
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=str(row.get("pipeline_key") or "unknown"),
                failure_type="job_crash",
                detected_by=detected_by,
                severity="high" if is_oom or result == "TIMEDOUT" else "medium",
                evidence={
                    "task_key": row.get("task_key"),
                    "result_state": result,
                    "lifecycle_state": lifecycle,
                    "error_signature": row.get("error_signature"),
                    "raw_output_excerpt": (row.get("raw_output") or "")[:1000],
                },
            )
        )
    return signals


def detect_status_failures(telemetry: list[dict[str, Any]], detected_by: str = "workflow") -> list[DetectedSignal]:
    """FAILED status rows from in-task telemetry (non-crash path)."""
    signals: list[DetectedSignal] = []
    for row in telemetry:
        if (row.get("status") or "").upper() != "FAILED":
            continue
        err = (row.get("error_class") or "") + " " + (row.get("error_message") or "")
        failure = "job_crash" if "memory" in err.lower() or "oom" in err.lower() else "job_crash"
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=str(row.get("pipeline_key") or "unknown"),
                failure_type=failure,
                detected_by=detected_by,
                severity="high",
                evidence={
                    "task_name": row.get("task_name"),
                    "error_class": row.get("error_class"),
                    "error_message": row.get("error_message"),
                    "stacktrace_excerpt": (row.get("stacktrace") or "")[:1000],
                },
            )
        )
    return signals


def detect_dq_failures(dq_rows: list[dict[str, Any]], detected_by: str = "workflow") -> list[DetectedSignal]:
    """Failed DQ checks mapped onto the six failure classes."""
    signals: list[DetectedSignal] = []
    for row in dq_rows:
        if row.get("passed") in (True, "true", "True", 1):
            continue
        failure = _map_dq_to_failure(str(row.get("check_name", "")), str(row.get("metric_name", "")))
        if failure not in FAILURE_TYPES:
            failure = "null_spike"
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=str(row.get("pipeline_key") or "unknown"),
                failure_type=failure,
                detected_by=detected_by,
                severity="medium",
                evidence={
                    "check_name": row.get("check_name"),
                    "table_name": row.get("table_name"),
                    "metric_name": row.get("metric_name"),
                    "observed_value": row.get("observed_value"),
                    "threshold_value": row.get("threshold_value"),
                    "comparator": row.get("comparator"),
                },
            )
        )
    return signals


def detect_volume_anomalies(
    telemetry: list[dict[str, Any]],
    history_by_pipeline: dict[str, list[float]],
    detected_by: str = "workflow",
) -> list[DetectedSignal]:
    signals: list[DetectedSignal] = []
    for row in telemetry:
        rows_out = row.get("rows_out")
        if rows_out is None:
            continue
        pipeline = str(row.get("pipeline_key") or "unknown")
        decision = decide_volume(float(rows_out), history_by_pipeline.get(pipeline, []))
        if not decision.is_anomaly:
            continue
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=pipeline,
                failure_type="volume_anomaly",
                detected_by=detected_by,
                severity="medium",
                evidence={
                    "task_name": row.get("task_name"),
                    "rows_out": rows_out,
                    "mode": decision.mode,
                    "baseline": decision.baseline,
                    "score": decision.score,
                    "reason": decision.reason,
                },
            )
        )
    return signals


def detect_duration_anomalies(
    telemetry: list[dict[str, Any]],
    history_by_pipeline: dict[str, list[float]],
    detected_by: str = "workflow",
) -> list[DetectedSignal]:
    """Duration spikes are recorded as volume_anomaly-adjacent evidence under job_crash only if extreme —

    Plan maps duration cold-start to anomaly baselines used for triage; we emit volume_anomaly only
    for row counts. Duration alone does not open a dedicated failure class — skip unless used by agent.
    Kept as helper for future gold metrics; currently returns empty list by design.
    """
    _ = (telemetry, history_by_pipeline, detected_by, decide_duration)
    return []


def detect_null_spikes_from_metadata(
    telemetry: list[dict[str, Any]],
    history_by_pipeline: dict[str, list[float]],
    detected_by: str = "workflow",
) -> list[DetectedSignal]:
    """Null-rate anomalies when tasks stash null_rate in metadata_json."""
    signals: list[DetectedSignal] = []
    for row in telemetry:
        meta_raw = row.get("metadata_json") or "{}"
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else dict(meta_raw)
        except json.JSONDecodeError:
            continue
        if "null_rate" not in meta:
            continue
        observed = float(meta["null_rate"])
        pipeline = str(row.get("pipeline_key") or "unknown")
        decision = decide_null_rate(observed, history_by_pipeline.get(pipeline, []))
        if not decision.is_anomaly:
            continue
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=pipeline,
                failure_type="null_spike",
                detected_by=detected_by,
                severity="medium",
                evidence={
                    "task_name": row.get("task_name"),
                    "null_rate": observed,
                    "mode": decision.mode,
                    "baseline": decision.baseline,
                    "score": decision.score,
                    "reason": decision.reason,
                },
            )
        )
    return signals


def detect_schema_drift(
    current_snapshots: list[dict[str, Any]],
    previous_by_pipeline: dict[str, dict[str, Any]],
    detected_by: str = "workflow",
) -> list[DetectedSignal]:
    """Compare schema_snapshot_json column sets vs the previous run for the same pipeline."""
    signals: list[DetectedSignal] = []
    for row in current_snapshots:
        snap_raw = row.get("schema_snapshot_json")
        if not snap_raw:
            continue
        try:
            current = json.loads(snap_raw) if isinstance(snap_raw, str) else dict(snap_raw)
        except json.JSONDecodeError:
            continue
        pipeline = str(row.get("pipeline_key") or "unknown")
        previous = previous_by_pipeline.get(pipeline)
        if not previous:
            continue
        cur_cols = set(current.keys()) if isinstance(current, dict) else set()
        prev_cols = set(previous.keys()) if isinstance(previous, dict) else set()
        added = sorted(cur_cols - prev_cols)
        removed = sorted(prev_cols - cur_cols)
        type_changes = []
        for col in sorted(cur_cols & prev_cols):
            if current.get(col) != previous.get(col):
                type_changes.append({"column": col, "from": previous.get(col), "to": current.get(col)})
        if not added and not removed and not type_changes:
            continue
        signals.append(
            DetectedSignal(
                job_run_id=str(row["run_id"]),
                pipeline_key=pipeline,
                failure_type="schema_drift",
                detected_by=detected_by,
                severity="high",
                evidence={
                    "added_columns": added,
                    "removed_columns": removed,
                    "type_changes": type_changes,
                },
            )
        )
    return signals


def group_history_numeric(
    rows: list[dict[str, Any]],
    value_key: str,
    pipeline_key: str = "pipeline_key",
) -> dict[str, list[float]]:
    """Build ascending history lists per pipeline from older telemetry rows."""
    buckets: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        val = row.get(value_key)
        if val is None:
            continue
        buckets[str(row.get(pipeline_key) or "unknown")].append(float(val))
    return dict(buckets)


def evaluate_all(
    *,
    telemetry_current: list[dict[str, Any]],
    telemetry_history: list[dict[str, Any]],
    dq_rows: list[dict[str, Any]],
    task_logs: list[dict[str, Any]],
    previous_schemas: dict[str, dict[str, Any]],
) -> list[DetectedSignal]:
    """Run the full rule suite and return all signals (may include multiple per run)."""
    volume_hist = group_history_numeric(telemetry_history, "rows_out")
    null_hist: dict[str, list[float]] = defaultdict(list)
    for row in telemetry_history:
        meta_raw = row.get("metadata_json") or "{}"
        try:
            meta = json.loads(meta_raw) if isinstance(meta_raw, str) else dict(meta_raw)
        except json.JSONDecodeError:
            continue
        if "null_rate" in meta:
            null_hist[str(row.get("pipeline_key") or "unknown")].append(float(meta["null_rate"]))

    signals: list[DetectedSignal] = []
    signals.extend(detect_job_crashes(task_logs, detected_by="poller"))
    signals.extend(detect_status_failures(telemetry_current, detected_by="workflow"))
    signals.extend(detect_dq_failures(dq_rows, detected_by="workflow"))
    signals.extend(detect_volume_anomalies(telemetry_current, volume_hist, detected_by="workflow"))
    signals.extend(detect_null_spikes_from_metadata(telemetry_current, dict(null_hist), detected_by="workflow"))
    signals.extend(detect_schema_drift(telemetry_current, previous_schemas, detected_by="workflow"))
    return signals
