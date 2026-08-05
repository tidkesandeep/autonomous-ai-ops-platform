"""Approve / reject remediation proposals in Lakebase and optionally dispatch Jobs."""

from __future__ import annotations

import json
import os
from typing import Any

import requests

from src.agent.tools_write import load_incident, update_incident_status
from src.remediation.mapping import remediation_for_failure


def latest_proposal(conn: Any, incident_id: str) -> dict[str, Any] | None:
    cur = conn.cursor()
    try:
        cur.execute(
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
        row = cur.fetchone()
        if not row:
            return None
        detail = row[0]
        if isinstance(detail, str):
            detail = json.loads(detail)
        return {"detail": detail, "created_at": row[1]}
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
            VALUES (%s, %s, %s, %s, %s)
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
                json.dumps(
                    {
                        "approval_id": approval_id,
                        "remediation_type": remediation_type,
                        "notes": notes,
                    },
                    default=str,
                ),
            ),
        )
        conn.commit()
        return approval_id
    finally:
        cur.close()


def approve_incident(
    conn: Any,
    incident_id: str,
    *,
    decided_by: str = "operator",
    notes: str = "",
    trigger_job: bool = True,
    remediation_job_id: str | int | None = None,
) -> dict[str, Any]:
    """Record approval and optionally fire ``ops-remediate`` via Jobs API."""
    incident = load_incident(conn, incident_id=incident_id)
    if not incident:
        return {"ok": False, "error": "incident not found"}
    if incident["status"] not in ("AWAITING_APPROVAL", "INVESTIGATING", "OPEN"):
        return {"ok": False, "error": f"cannot approve from status {incident['status']}"}

    proposal = latest_proposal(conn, incident_id)
    rem_type = None
    rem_params: dict[str, Any] = {}
    if proposal and isinstance(proposal.get("detail"), dict):
        rem_type = proposal["detail"].get("remediation_type")
        rem_params = proposal["detail"].get("parameters") or {}
    if not rem_type:
        rem_type, rem_params = remediation_for_failure(incident.get("primary_failure_type"))

    approval_id = _insert_approval(
        conn,
        incident_id=incident_id,
        decision="approved",
        decided_by=decided_by,
        remediation_type=rem_type,
        notes=notes,
    )

    dispatch: dict[str, Any] | None = None
    if rem_type == "diagnosis_only":
        update_incident_status(conn, incident_id, "RESOLVED", changed_by=decided_by)
        return {
            "ok": True,
            "approval_id": approval_id,
            "remediation_type": rem_type,
            "status": "RESOLVED",
            "dispatch": None,
            "note": "diagnosis_only — resolved without remediation job",
        }

    if trigger_job:
        dispatch = trigger_remediation_job(
            incident_id=incident_id,
            remediation_type=rem_type,
            parameters=rem_params,
            job_id=remediation_job_id,
        )

    return {
        "ok": True,
        "approval_id": approval_id,
        "remediation_type": rem_type,
        "parameters": rem_params,
        "status": incident["status"],
        "dispatch": dispatch,
    }


def reject_incident(
    conn: Any,
    incident_id: str,
    *,
    decided_by: str = "operator",
    notes: str = "",
) -> dict[str, Any]:
    incident = load_incident(conn, incident_id=incident_id)
    if not incident:
        return {"ok": False, "error": "incident not found"}
    proposal = latest_proposal(conn, incident_id)
    rem_type = None
    if proposal and isinstance(proposal.get("detail"), dict):
        rem_type = proposal["detail"].get("remediation_type")
    approval_id = _insert_approval(
        conn,
        incident_id=incident_id,
        decision="rejected",
        decided_by=decided_by,
        remediation_type=rem_type,
        notes=notes,
    )
    update_incident_status(conn, incident_id, "INVESTIGATING", changed_by=decided_by)
    return {"ok": True, "approval_id": approval_id, "status": "INVESTIGATING"}


def trigger_remediation_job(
    *,
    incident_id: str,
    remediation_type: str,
    parameters: dict[str, Any],
    job_id: str | int | None = None,
) -> dict[str, Any]:
    """Call Jobs run-now for the remediation notebook job."""
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    jid = job_id or os.environ.get("REMEDIATION_JOB_ID")
    if not host or not token:
        return {"ok": False, "error": "DATABRICKS_HOST/TOKEN missing", "skipped": True}
    if not jid:
        return {"ok": False, "error": "REMEDIATION_JOB_ID unset", "skipped": True}

    url = f"{host}/api/2.1/jobs/run-now"
    payload = {
        "job_id": int(jid),
        "notebook_params": {
            "incident_id": incident_id,
            "remediation_type": remediation_type,
            "parameters_json": json.dumps(parameters, default=str),
        },
    }
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=60,
    )
    if resp.status_code >= 400:
        return {"ok": False, "status_code": resp.status_code, "body": resp.text[:1000]}
    data = resp.json()
    return {"ok": True, "run_id": data.get("run_id"), "job_id": int(jid)}
