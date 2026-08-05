"""Incident console service helpers (approve/reject + listings).

Self-contained for Databricks Apps deploy (source root = ``app/``).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests
from db import fetchall, postgres_connection

REMEDIATION_FOR = {
    "job_crash": ("retry_adjusted_config", {"spark.sql.shuffle.partitions": "200", "max_retries": 2}),
    "late_data": ("retry_adjusted_config", {"lookback_hours": 168, "mode": "replay_window"}),
    "null_spike": ("quarantine_reprocess", {"strategy": "drop_null_keys", "column": "email"}),
    "duplicate_explosion": ("quarantine_reprocess", {"strategy": "keep_latest_per_key", "key": "order_id"}),
    "schema_drift": ("schema_evolution_ddl", {"action": "generate_and_apply_additive"}),
    "volume_anomaly": ("diagnosis_only", {"reason": "no safe automatic remediation"}),
}

REMEDIATION_LABELS = {
    "quarantine_reprocess": "Quarantine bad rows and rewrite the clean table",
    "retry_adjusted_config": "Retry the pipeline with safer Spark / window settings",
    "schema_evolution_ddl": "Apply additive schema evolution DDL",
    "diagnosis_only": "Diagnose only — no automatic data changes",
}

FAILURE_CLASS_LABELS = {
    "null_spike": "Null spike",
    "volume_anomaly": "Volume anomaly",
    "duplicate_explosion": "Duplicate explosion",
    "schema_drift": "Schema drift",
    "late_data": "Late data",
    "job_crash": "Job crash",
}


def list_incidents(
    limit: int = 50,
    status: str | None = None,
    failure_type: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if status and status != "ALL":
        clauses.append("status = %s")
        params.append(status)
    if failure_type and failure_type != "ALL":
        clauses.append("primary_failure_type = %s")
        params.append(failure_type)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    return fetchall(
        f"""
        SELECT incident_id::text, job_run_id, pipeline_key, primary_failure_type,
               severity, status, rca_report_path, linked_commit_sha, detected_at
        FROM incidents
        {where}
        ORDER BY detected_at DESC
        LIMIT %s
        """,
        tuple(params),
    )


def count_incidents_by_status() -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT status, COUNT(*)::int AS n
        FROM incidents
        GROUP BY status
        ORDER BY n DESC
        """
    )


def count_incidents_by_failure_type() -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT COALESCE(primary_failure_type, 'unknown') AS failure_type, COUNT(*)::int AS n
        FROM incidents
        GROUP BY 1
        ORDER BY n DESC
        """
    )


def remediation_summary(remediation_type: str | None, parameters: dict[str, Any] | None = None) -> str:
    """Human-readable one-liner for the decision panel."""
    if not remediation_type:
        return "No remediation proposed yet."
    label = REMEDIATION_LABELS.get(remediation_type, remediation_type)
    params = parameters or {}
    extras: list[str] = []
    for key in ("strategy", "column", "key", "action", "mode", "reason"):
        if params.get(key) not in (None, ""):
            extras.append(f"{key}={params[key]}")
    if extras:
        return f"{label} ({', '.join(extras)})"
    return label


def get_incident(incident_id: str) -> dict[str, Any] | None:
    rows = fetchall(
        """
        SELECT incident_id::text, job_run_id, pipeline_key, primary_failure_type,
               severity, status, rca_report_path, linked_commit_sha, detected_at
        FROM incidents WHERE incident_id = %s::uuid
        """,
        (incident_id,),
    )
    return rows[0] if rows else None


def list_signals(incident_id: str) -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT failure_type, detected_by, detected_at, evidence_json
        FROM incident_signals WHERE incident_id = %s::uuid
        ORDER BY detected_at
        """,
        (incident_id,),
    )


def list_status_events(incident_id: str) -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT from_status, to_status, changed_at, changed_by
        FROM incident_status_events
        WHERE incident_id = %s::uuid
        ORDER BY changed_at
        """,
        (incident_id,),
    )


def list_agent_actions(incident_id: str, limit: int = 50) -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT tool_name, outputs_summary, created_at
        FROM agent_actions
        WHERE incident_id = %s::uuid
        ORDER BY created_at
        LIMIT %s
        """,
        (incident_id, limit),
    )


def list_approvals(incident_id: str) -> list[dict[str, Any]]:
    return fetchall(
        """
        SELECT decision, decided_by, decided_at, remediation_type, notes
        FROM approvals WHERE incident_id = %s::uuid
        ORDER BY decided_at DESC
        """,
        (incident_id,),
    )


def read_rca_excerpt(path: str | None, max_chars: int = 4000) -> str:
    if not path:
        return ""
    try:
        p = Path(path)
        if p.exists():
            return p.read_text(encoding="utf-8")[:max_chars]
    except Exception:  # noqa: BLE001
        pass
    return f"(RCA at `{path}` — open in workspace files if not mounted in the app)"


def latest_proposal(incident_id: str) -> dict[str, Any] | None:
    rows = fetchall(
        """
        SELECT detail_json, created_at
        FROM audit_log
        WHERE entity_type = 'incident'
          AND entity_id = %s
          AND action = 'propose_remediation'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (incident_id,),
    )
    if not rows:
        return None
    detail = rows[0]["detail_json"]
    if isinstance(detail, str):
        detail = json.loads(detail)
    return {"detail": detail, "created_at": rows[0]["created_at"]}


def _update_status(conn: Any, incident_id: str, to_status: str, changed_by: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM incidents WHERE incident_id = %s::uuid", (incident_id,))
        row = cur.fetchone()
        if not row:
            return
        from_status = row[0]
        if from_status == to_status:
            return
        cur.execute(
            "UPDATE incidents SET status = %s WHERE incident_id = %s::uuid",
            (to_status, incident_id),
        )
        cur.execute(
            """
            INSERT INTO incident_status_events (incident_id, from_status, to_status, changed_by)
            VALUES (%s::uuid, %s, %s, %s)
            """,
            (incident_id, from_status, to_status, changed_by),
        )
        conn.commit()
    finally:
        cur.close()


def _insert_approval(
    conn: Any,
    *,
    incident_id: str,
    decision: str,
    decided_by: str,
    remediation_type: str | None,
    notes: str,
) -> str:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO approvals (incident_id, decision, decided_by, remediation_type, notes)
            VALUES (%s::uuid, %s, %s, %s, %s)
            RETURNING approval_id
            """,
            (incident_id, decision, decided_by, remediation_type, notes),
        )
        approval_id = str(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO audit_log (actor, action, entity_type, entity_id, detail_json)
            VALUES (%s, %s, 'incident', %s, CAST(%s AS jsonb))
            """,
            (
                decided_by,
                f"approval_{decision}",
                incident_id,
                json.dumps({"approval_id": approval_id, "remediation_type": remediation_type, "notes": notes}),
            ),
        )
        conn.commit()
        return approval_id
    finally:
        cur.close()


def trigger_remediation_job(
    *,
    incident_id: str,
    remediation_type: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    jid = os.environ.get("REMEDIATION_JOB_ID")
    if not jid:
        return {"ok": False, "error": "REMEDIATION_JOB_ID unset", "skipped": True}
    if not host or not token:
        try:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            host = (w.config.host or "").rstrip("/")
            token = w.config.token
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"no databricks auth: {exc}", "skipped": True}
    if not host or not token:
        return {"ok": False, "error": "DATABRICKS_HOST/TOKEN missing", "skipped": True}

    resp = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "job_id": int(jid),
            "notebook_params": {
                "incident_id": incident_id,
                "remediation_type": remediation_type,
                "parameters_json": json.dumps(parameters, default=str),
            },
        },
        timeout=60,
    )
    if resp.status_code >= 400:
        return {"ok": False, "status_code": resp.status_code, "body": resp.text[:1000]}
    return {"ok": True, "run_id": resp.json().get("run_id"), "job_id": int(jid)}


def do_approve(incident_id: str, decided_by: str, notes: str = "") -> dict[str, Any]:
    incident = get_incident(incident_id)
    if not incident:
        return {"ok": False, "error": "incident not found"}
    if incident["status"] not in ("AWAITING_APPROVAL", "INVESTIGATING", "OPEN"):
        return {"ok": False, "error": f"cannot approve from status {incident['status']}"}

    proposal = latest_proposal(incident_id)
    rem_type = None
    rem_params: dict[str, Any] = {}
    if proposal and isinstance(proposal.get("detail"), dict):
        rem_type = proposal["detail"].get("remediation_type")
        rem_params = proposal["detail"].get("parameters") or {}
    if not rem_type:
        rem_type, rem_params = REMEDIATION_FOR.get(
            incident.get("primary_failure_type") or "",
            ("diagnosis_only", {}),
        )

    with postgres_connection() as conn:
        approval_id = _insert_approval(
            conn,
            incident_id=incident_id,
            decision="approved",
            decided_by=decided_by,
            remediation_type=rem_type,
            notes=notes,
        )
        if rem_type == "diagnosis_only":
            _update_status(conn, incident_id, "RESOLVED", decided_by)
            return {
                "ok": True,
                "approval_id": approval_id,
                "remediation_type": rem_type,
                "status": "RESOLVED",
                "dispatch": None,
                "note": "diagnosis_only — resolved without remediation job",
            }

    dispatch = trigger_remediation_job(
        incident_id=incident_id,
        remediation_type=rem_type or "diagnosis_only",
        parameters=rem_params,
    )
    return {
        "ok": True,
        "approval_id": approval_id,
        "remediation_type": rem_type,
        "parameters": rem_params,
        "dispatch": dispatch,
    }


def do_reject(incident_id: str, decided_by: str, notes: str = "") -> dict[str, Any]:
    proposal = latest_proposal(incident_id)
    rem_type = None
    if proposal and isinstance(proposal.get("detail"), dict):
        rem_type = proposal["detail"].get("remediation_type")
    with postgres_connection() as conn:
        approval_id = _insert_approval(
            conn,
            incident_id=incident_id,
            decision="rejected",
            decided_by=decided_by,
            remediation_type=rem_type,
            notes=notes,
        )
        _update_status(conn, incident_id, "INVESTIGATING", decided_by)
    return {"ok": True, "approval_id": approval_id, "status": "INVESTIGATING"}


def proposal_for(incident_id: str) -> dict[str, Any] | None:
    return latest_proposal(incident_id)


def current_user_email(default: str = "operator") -> str:
    return os.environ.get("APP_USER_EMAIL") or default
