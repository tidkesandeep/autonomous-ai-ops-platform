"""Bulk-seed Lakebase incidents for console volume (~100) with unique run ids."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from src.agent.runner import run_agent
from src.agent.synth import open_synthetic_incident, seed_synthetic_evidence
from src.agent.tools_write import (
    load_incident,
    log_agent_action,
    propose_remediation,
    save_rca_report,
    update_incident_status,
)
from src.common.constants import FAILURE_TYPES
from src.remediation.approvals import _insert_approval, approve_incident, reject_incident
from src.remediation.mapping import REMEDIATION_FOR, remediation_for_failure

PIPELINE_FOR = {
    "null_spike": "customers_scd2",
    "duplicate_explosion": "orders_ingest",
    "late_data": "events_clickstream",
    "volume_anomaly": "orders_ingest",
    "schema_drift": "products_catalog",
    "job_crash": "ops_force_fail",
}

ROOT_CAUSE = {
    "null_spike": "Null rate exceeded threshold on a monitored column (DQ null check failed).",
    "duplicate_explosion": "Natural-key duplicate rate exceeded threshold after a join/merge.",
    "late_data": "Event lag exceeded the late-data watermark (pct_late / lag_minutes).",
    "volume_anomaly": "Row volume moved outside the cold-start/z-score envelope vs prior runs.",
    "schema_drift": "Schema drift detected vs prior snapshot (additive columns).",
    "job_crash": "Task crashed/failed at runtime (see task logs).",
}


def _count_incidents(conn: Any) -> int:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM incidents")
        return int(cur.fetchone()[0])
    finally:
        cur.close()


def _status_breakdown(conn: Any) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT status, COUNT(*)::int FROM incidents GROUP BY 1")
        return {str(r[0]): int(r[1]) for r in cur.fetchall()}
    finally:
        cur.close()


def _list_awaiting(conn: Any) -> list[dict[str, str]]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT incident_id::text, primary_failure_type
            FROM incidents
            WHERE status = 'AWAITING_APPROVAL'
            ORDER BY detected_at DESC
            """
        )
        return [
            {"incident_id": r[0], "primary_failure_type": r[1] or ""}
            for r in cur.fetchall()
        ]
    finally:
        cur.close()


def _fast_investigate(conn: Any, incident_id: str, *, reports_dir: str) -> dict[str, Any]:
    """Console-ready investigation without LLM or heavy Spark reads."""
    incident = load_incident(conn, incident_id=incident_id)
    if not incident:
        return {"ok": False, "error": "incident not found", "incident_id": incident_id}

    iid = incident["incident_id"]
    failure = incident.get("primary_failure_type") or "unknown"
    pipeline = incident["pipeline_key"]
    run_id = incident["job_run_id"]
    rem_type, rem_params = REMEDIATION_FOR.get(failure, ("diagnosis_only", {}))

    update_incident_status(conn, iid, "INVESTIGATING", changed_by="bulk_seed")
    log_agent_action(
        conn, iid, "update_incident_status", {"to": "INVESTIGATING"}, "status→INVESTIGATING"
    )
    log_agent_action(
        conn,
        iid,
        "query_run_history",
        {"pipeline_key": pipeline},
        f"synthetic history for {failure}",
    )
    log_agent_action(
        conn,
        iid,
        "get_dq_failures",
        {"run_id": run_id},
        f"synthetic dq signal for {failure}",
    )

    report = {
        "incident_id": iid,
        "job_run_id": run_id,
        "pipeline_key": pipeline,
        "summary": f"Bulk-seed investigation of {failure} on pipeline `{pipeline}`.",
        "root_cause": ROOT_CAUSE.get(failure, f"Primary signal `{failure}`."),
        "root_cause_type": failure,
        "blast_radius": f"Pipeline `{pipeline}` and downstream silver/gold consumers.",
        "evidence": [
            f"primary_failure_type={failure}",
            "source=bulk_seed_fast",
            f"job_run_id={run_id}",
        ],
        "suspected_commit_sha": None,
        "remediation_proposal": f"{rem_type}: {rem_params}",
        "cited_runbook": None,
        "tool_trace_len": 3,
    }
    saved = save_rca_report(conn, iid, report, reports_dir=reports_dir)
    log_agent_action(conn, iid, "save_rca_report", {"path": saved.get("rca_report_path")}, "RCA saved")
    proposal = propose_remediation(conn, iid, rem_type, rem_params, notes=report["root_cause"])
    log_agent_action(conn, iid, "propose_remediation", {"type": rem_type}, "proposal recorded")
    return {
        "ok": True,
        "incident_id": iid,
        "status": "AWAITING_APPROVAL",
        "proposal": proposal,
        "saved": saved,
        "mode": "fast",
    }


def bulk_seed_console(
    spark: Any,
    conn: Any,
    *,
    reports_dir: str,
    target_total: int = 100,
    resolve_n: int = 18,
    reject_n: int = 8,
    mode: str = "fast",
    write_evidence: bool = False,
) -> dict[str, Any]:
    """Create unique synthetic incidents until ``target_total``, then shape statuses.

    Modes:
    - ``fast`` (default): Lakebase RCA + proposal only — suitable for ~100 console rows.
    - ``agent``: full ``run_agent`` per new incident (slow / LLM cost).
    - ``open``: leave incidents OPEN (no investigation).

    Status shaping targets absolute counts (idempotent on re-run):
    - ~``resolve_n`` RESOLVED
    - ~``reject_n`` INVESTIGATING
    - remainder stay AWAITING_APPROVAL
    """
    mode = (mode or "fast").lower().strip()
    before = _count_incidents(conn)
    need = max(0, int(target_total) - before)
    batch = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    created: list[dict[str, Any]] = []
    severities = ("low", "medium", "high", "critical")

    failure_cycle = list(FAILURE_TYPES)
    i = 0
    while len(created) < need:
        failure_type = failure_cycle[i % len(failure_cycle)]
        severity = severities[i % len(severities)]
        i += 1
        pipeline = PIPELINE_FOR.get(failure_type, "orders_ingest")
        run_id = f"bulk-{batch}-{failure_type}-{len(created) + 1:03d}-{uuid.uuid4().hex[:8]}"
        if write_evidence and spark is not None:
            seed_synthetic_evidence(spark, failure_type, run_id, pipeline)
        incident_id = open_synthetic_incident(
            conn,
            job_run_id=run_id,
            pipeline_key=pipeline,
            failure_type=failure_type,
            severity=severity,
        )
        agent_ok = None
        status = "OPEN"
        if mode == "agent":
            result = run_agent(
                spark,
                conn,
                incident_id=incident_id,
                reports_dir=reports_dir,
                use_langgraph=True,
            )
            agent_ok = bool(result.get("ok"))
            status = str(result.get("status") or "AWAITING_APPROVAL")
        elif mode == "fast":
            result = _fast_investigate(conn, incident_id, reports_dir=reports_dir)
            agent_ok = bool(result.get("ok"))
            status = str(result.get("status") or "AWAITING_APPROVAL")
        created.append(
            {
                "incident_id": incident_id,
                "job_run_id": run_id,
                "failure_type": failure_type,
                "agent_ok": agent_ok,
                "status": status,
            }
        )

    breakdown = _status_breakdown(conn)
    need_resolve = max(0, int(resolve_n) - breakdown.get("RESOLVED", 0))
    need_reject = max(0, int(reject_n) - breakdown.get("INVESTIGATING", 0))

    awaiting = _list_awaiting(conn)
    awaiting_sorted = sorted(
        awaiting,
        key=lambda r: (0 if r["primary_failure_type"] == "volume_anomaly" else 1, r["incident_id"]),
    )
    resolve_targets = awaiting_sorted[:need_resolve]
    remaining = awaiting_sorted[need_resolve:]
    reject_targets = remaining[:need_reject]

    resolved: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []

    for row in resolve_targets:
        iid = row["incident_id"]
        ft = row["primary_failure_type"]
        if ft == "volume_anomaly":
            out = approve_incident(
                conn,
                iid,
                decided_by="bulk_seed",
                notes="bulk seed: diagnosis_only resolve",
                trigger_job=False,
            )
            resolved.append({"incident_id": iid, "via": "approve_diagnosis_only", "result": out})
        else:
            rem_type, _ = remediation_for_failure(ft)
            approval_id = _insert_approval(
                conn,
                incident_id=iid,
                decision="approved",
                decided_by="bulk_seed",
                remediation_type=rem_type,
                notes="bulk seed: marked RESOLVED for console volume (no job dispatch)",
            )
            update_incident_status(conn, iid, "RESOLVED", changed_by="bulk_seed")
            resolved.append(
                {
                    "incident_id": iid,
                    "via": "status_update",
                    "approval_id": approval_id,
                    "remediation_type": rem_type,
                }
            )

    for row in reject_targets:
        out = reject_incident(
            conn,
            row["incident_id"],
            decided_by="bulk_seed",
            notes="bulk seed: reject → INVESTIGATING for filter diversity",
        )
        rejected.append({"incident_id": row["incident_id"], "result": out})

    after = _count_incidents(conn)
    return {
        "ok": True,
        "batch": batch,
        "mode": mode,
        "before_total": before,
        "created": len(created),
        "after_total": after,
        "target_total": target_total,
        "status_breakdown": _status_breakdown(conn),
        "resolved_n": len(resolved),
        "rejected_n": len(rejected),
        "sample_created": created[:6],
        "created_ids": [c["incident_id"] for c in created],
    }
