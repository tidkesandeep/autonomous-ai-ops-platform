"""Phase 3 prove loop: reset → inject each class → seed evidence → detect → score."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.chaos.evidence import seed_ops_evidence
from src.chaos.injector import inject
from src.chaos.reset_demo import reset_demo, reset_demo_tables
from src.common.constants import FAILURE_TYPES, OPS_BRONZE, OPS_GOLD
from src.detection.engine import run_detection
from src.detection.incidents import IncidentStore
from src.detection.scoring import Scorecard, score_detections


def clear_ops_evidence(spark: Any) -> list[str]:
    """Drop telemetry/DQ/logs but keep injected_failures ground truth."""
    dropped: list[str] = []
    for table in (
        f"{OPS_BRONZE}.task_telemetry",
        f"{OPS_BRONZE}.raw_task_logs",
        f"{OPS_GOLD}.fact_dq_check",
    ):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            dropped.append(table)
        except Exception:  # noqa: BLE001
            pass
    return dropped


def prove_one(
    spark: Any,
    store: IncidentStore,
    failure_type: str,
    *,
    checks_path: str,
    job_run_id: str | None = None,
) -> dict[str, Any]:
    """Reset demo data, inject one class, seed evidence, run detection."""
    reset_demo_tables(spark, seed=42)
    clear_ops_evidence(spark)
    run_id = job_run_id or f"prove-{failure_type}-{datetime.now(UTC).strftime('%H%M%S')}"
    injection = inject(spark, failure_type, job_run_id=run_id)
    evidence = seed_ops_evidence(spark, injection, checks_path=checks_path)
    detection = run_detection(spark, store, lookback_hours=48)
    return {
        "failure_type": failure_type,
        "injection": {
            "job_run_id": injection.job_run_id,
            "pipeline_key": injection.pipeline_key,
            "target_table": injection.target_table,
            "detail": injection.detail,
        },
        "evidence": evidence,
        "detection": detection,
    }


def prove_all_failure_classes(
    spark: Any,
    store: IncidentStore,
    *,
    checks_path: str,
    conn: Any | None = None,
    do_initial_reset: bool = True,
) -> dict[str, Any]:
    """Week 3 exit-criteria prove: all six classes → matching OPEN incidents."""
    if do_initial_reset:
        reset_demo(spark, conn, seed=42, reset_lakebase_state=conn is not None)

    per_class: list[dict[str, Any]] = []
    for failure_type in FAILURE_TYPES:
        per_class.append(
            prove_one(
                spark,
                store,
                failure_type,
                checks_path=checks_path,
                job_run_id=f"prove-{failure_type}",
            )
        )

    expected_run_ids = {f"prove-{ft}" for ft in FAILURE_TYPES}
    expected = [{"job_run_id": f"prove-{ft}", "failure_type": ft} for ft in FAILURE_TYPES]
    detected = _load_detected_primaries(store, expected_run_ids)
    primary_card = score_detections(
        expected,
        [{"job_run_id": d["job_run_id"], "failure_type": d["primary_failure_type"]} for d in detected],
    )
    signal_rows = _load_detected_signals(store, expected_run_ids)
    signal_card = score_detections(expected, signal_rows)

    matches = []
    for exp in expected:
        row = next((d for d in detected if d["job_run_id"] == exp["job_run_id"]), None)
        matches.append(
            {
                "job_run_id": exp["job_run_id"],
                "expected": exp["failure_type"],
                "primary_failure_type": None if row is None else row["primary_failure_type"],
                "status": None if row is None else row.get("status"),
                "ok": bool(
                    row
                    and row["primary_failure_type"] == exp["failure_type"]
                    and row.get("status") == "OPEN"
                ),
            }
        )

    return {
        "ran_at": datetime.now(UTC).isoformat(),
        "per_class": per_class,
        "matches": matches,
        "primary_scorecard": primary_card.as_dict(),
        "signal_scorecard": signal_card.as_dict(),
        "exit_criteria_met": all(m["ok"] for m in matches)
        and primary_card.precision == 1.0
        and primary_card.recall == 1.0,
    }


def _in_clause(ids: set[str]) -> tuple[str, list[str]]:
    ordered = sorted(ids)
    return ",".join(["%s"] * len(ordered)), ordered


def _load_detected_primaries(store: IncidentStore, expected_run_ids: set[str]) -> list[dict[str, Any]]:
    incidents = getattr(store, "incidents", None)
    if isinstance(incidents, dict):
        return [
            {
                "job_run_id": job_run_id,
                "primary_failure_type": inc.get("primary_failure_type"),
                "status": inc.get("status"),
            }
            for job_run_id, inc in incidents.items()
            if job_run_id in expected_run_ids
        ]

    placeholders, values = _in_clause(expected_run_ids)
    cur = store.conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT job_run_id, primary_failure_type, status
            FROM incidents
            WHERE job_run_id IN ({placeholders})
            """,
            values,
        )
        return [
            {"job_run_id": r[0], "primary_failure_type": r[1], "status": r[2]}
            for r in cur.fetchall()
        ]
    finally:
        cur.close()


def _load_detected_signals(store: IncidentStore, expected_run_ids: set[str]) -> list[dict[str, Any]]:
    incidents = getattr(store, "incidents", None)
    signals = getattr(store, "signals", None)
    if isinstance(incidents, dict) and isinstance(signals, list):
        id_to_run = {v["incident_id"]: k for k, v in incidents.items()}
        return [
            {"job_run_id": id_to_run[s["incident_id"]], "failure_type": s["failure_type"]}
            for s in signals
            if id_to_run.get(s["incident_id"]) in expected_run_ids
        ]

    placeholders, values = _in_clause(expected_run_ids)
    cur = store.conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT i.job_run_id, s.failure_type
            FROM incident_signals s
            JOIN incidents i ON i.incident_id = s.incident_id
            WHERE i.job_run_id IN ({placeholders})
            """,
            values,
        )
        return [{"job_run_id": r[0], "failure_type": r[1]} for r in cur.fetchall()]
    finally:
        cur.close()


def scorecard_to_json(card: Scorecard | dict[str, Any]) -> str:
    payload = card.as_dict() if isinstance(card, Scorecard) else card
    return json.dumps(payload, indent=2)
