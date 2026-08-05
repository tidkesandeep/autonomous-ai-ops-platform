"""Pinned failure-class → remediation mapping (see implementation plan §5)."""

from __future__ import annotations

from typing import Any

REMEDIATION_FOR: dict[str, tuple[str, dict[str, Any]]] = {
    "job_crash": (
        "retry_adjusted_config",
        {
            "spark.sql.shuffle.partitions": "200",
            "maxRecordsPerFile": "50000",
            "max_retries": 2,
            "timeout_seconds": 3600,
        },
    ),
    "late_data": (
        "retry_adjusted_config",
        {"lookback_hours": 168, "mode": "replay_window"},
    ),
    "null_spike": (
        "quarantine_reprocess",
        {"strategy": "drop_null_keys", "column": "email", "key": "customer_id"},
    ),
    "duplicate_explosion": (
        "quarantine_reprocess",
        {"strategy": "keep_latest_per_key", "key": "order_id"},
    ),
    "schema_drift": (
        "schema_evolution_ddl",
        {"action": "generate_and_apply_additive"},
    ),
    "volume_anomaly": (
        "diagnosis_only",
        {"reason": "no safe automatic remediation for volume spikes"},
    ),
}


def remediation_for_failure(failure_type: str | None) -> tuple[str, dict[str, Any]]:
    if not failure_type:
        return "diagnosis_only", {"reason": "unknown failure type"}
    return REMEDIATION_FOR.get(failure_type, ("diagnosis_only", {"reason": f"unmapped:{failure_type}"}))
