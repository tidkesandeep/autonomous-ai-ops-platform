"""Detection engine: read ops Delta tables, evaluate rules, write Lakebase incidents."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from src.common.constants import OPS_BRONZE, OPS_GOLD
from src.detection.incidents import IncidentStore, record_signals
from src.detection.rules import evaluate_all
from src.detection.signals import DetectedSignal

logger = logging.getLogger(__name__)


def _rows(spark: Any, sql: str) -> list[dict[str, Any]]:
    return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]


def load_detection_inputs(
    spark: Any,
    *,
    lookback_hours: int = 24,
    telemetry_table: str | None = None,
    dq_table: str | None = None,
    logs_table: str | None = None,
) -> dict[str, Any]:
    """Pull recent ops rows used by the rule suite."""
    telemetry_table = telemetry_table or f"{OPS_BRONZE}.task_telemetry"
    dq_table = dq_table or f"{OPS_GOLD}.fact_dq_check"
    logs_table = logs_table or f"{OPS_BRONZE}.raw_task_logs"
    telem = _rows(
        spark,
        f"""
        SELECT * FROM {telemetry_table}
        WHERE ended_at >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS
        ORDER BY ended_at ASC
        """
        if _table_exists(spark, telemetry_table)
        else "SELECT 1 WHERE 1=0",
    )
    # Split: last row per (pipeline, task) as current; earlier as history
    current: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in reversed(telem):
        key = (str(row.get("pipeline_key")), str(row.get("task_name")))
        if key not in seen:
            current.append(row)
            seen.add(key)
        else:
            history.append(row)
    history.reverse()

    previous_schemas: dict[str, dict[str, Any]] = {}
    for row in history:
        pipeline = str(row.get("pipeline_key") or "unknown")
        if pipeline in previous_schemas:
            continue
        snap = row.get("schema_snapshot_json")
        if not snap:
            continue
        try:
            previous_schemas[pipeline] = json.loads(snap) if isinstance(snap, str) else dict(snap)
        except json.JSONDecodeError:
            continue

    dq = _rows(
        spark,
        f"""
        SELECT * FROM {dq_table}
        WHERE checked_at >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS
        """
        if _table_exists(spark, dq_table)
        else "SELECT 1 WHERE 1=0",
    )
    logs = _rows(
        spark,
        f"""
        SELECT * FROM {logs_table}
        WHERE collected_at >= current_timestamp() - INTERVAL {int(lookback_hours)} HOURS
        """
        if _table_exists(spark, logs_table)
        else "SELECT 1 WHERE 1=0",
    )
    return {
        "telemetry_current": current,
        "telemetry_history": history,
        "dq_rows": dq,
        "task_logs": logs,
        "previous_schemas": previous_schemas,
        "loaded_at": datetime.now(UTC).isoformat(),
    }


def _table_exists(spark: Any, table: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {table}").collect()
        return True
    except Exception:
        return False


def run_detection(
    spark: Any,
    store: IncidentStore,
    *,
    lookback_hours: int = 24,
    signals: list[DetectedSignal] | None = None,
) -> dict[str, Any]:
    """Evaluate rules against ops tables (or provided signals) and persist incidents."""
    if signals is None:
        inputs = load_detection_inputs(spark, lookback_hours=lookback_hours)
        signals = evaluate_all(
            telemetry_current=inputs["telemetry_current"],
            telemetry_history=inputs["telemetry_history"],
            dq_rows=inputs["dq_rows"],
            task_logs=inputs["task_logs"],
            previous_schemas=inputs["previous_schemas"],
        )
    else:
        inputs = {"provided_signals": len(signals)}

    results = record_signals(store, signals)
    created = sum(1 for r in results if r.created)
    summary = {
        "signals_evaluated": len(signals),
        "signals_written": len(results),
        "incidents_created": created,
        "incident_ids": sorted({r.incident_id for r in results}),
        "lookback_hours": lookback_hours,
        "inputs": {k: (len(v) if isinstance(v, list) else v) for k, v in inputs.items()},
        "ran_at": datetime.now(UTC).isoformat(),
    }
    logger.info("detection summary: %s", summary)
    return summary


def local_cutoff(hours: int) -> datetime:
    return datetime.now(UTC) - timedelta(hours=hours)
