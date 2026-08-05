"""Write tools — Lakebase incident updates, agent_actions, RCA, remediation proposals."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID


def _cur(conn: Any):
    return conn.cursor()


def update_incident_status(
    conn: Any,
    incident_id: str,
    to_status: str,
    *,
    changed_by: str = "agent",
) -> dict[str, Any]:
    cur = _cur(conn)
    try:
        cur.execute("SELECT status FROM incidents WHERE incident_id = %s", (incident_id,))
        row = cur.fetchone()
        if not row:
            return {"ok": False, "error": "incident not found", "incident_id": incident_id}
        from_status = row[0]
        if from_status == to_status:
            return {"ok": True, "noop": True, "status": to_status}
        cur.execute(
            "UPDATE incidents SET status = %s WHERE incident_id = %s",
            (to_status, incident_id),
        )
        cur.execute(
            """
            INSERT INTO incident_status_events (incident_id, from_status, to_status, changed_by)
            VALUES (%s, %s, %s, %s)
            """,
            (incident_id, from_status, to_status, changed_by),
        )
        conn.commit()
        return {"ok": True, "from_status": from_status, "to_status": to_status}
    finally:
        cur.close()


def log_agent_action(
    conn: Any,
    incident_id: str,
    tool_name: str,
    inputs: dict[str, Any],
    outputs_summary: str,
) -> dict[str, Any]:
    cur = _cur(conn)
    try:
        cur.execute(
            """
            INSERT INTO agent_actions (incident_id, tool_name, inputs_json, outputs_summary)
            VALUES (%s, %s, CAST(%s AS jsonb), %s)
            RETURNING action_id
            """,
            (incident_id, tool_name, json.dumps(inputs, default=str), outputs_summary[:4000]),
        )
        action_id = cur.fetchone()[0]
        conn.commit()
        return {"ok": True, "action_id": str(action_id)}
    finally:
        cur.close()


def save_rca_report(
    conn: Any,
    incident_id: str,
    report: dict[str, Any],
    *,
    reports_dir: str,
) -> dict[str, Any]:
    """Write Markdown+JSON RCA to disk and link path on the incident."""
    root = Path(reports_dir)
    root.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    base = root / f"rca_{incident_id}_{stamp}"
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_markdown(report), encoding="utf-8")

    cur = _cur(conn)
    try:
        cur.execute(
            """
            UPDATE incidents
            SET rca_report_path = %s,
                linked_commit_sha = COALESCE(%s, linked_commit_sha)
            WHERE incident_id = %s
            """,
            (str(md_path), report.get("suspected_commit_sha"), incident_id),
        )
        conn.commit()
    finally:
        cur.close()
    return {"ok": True, "rca_report_path": str(md_path), "json_path": str(json_path)}


def propose_remediation(
    conn: Any,
    incident_id: str,
    remediation_type: str,
    parameters: dict[str, Any],
    *,
    notes: str = "",
) -> dict[str, Any]:
    """Record remediation proposal in audit_log; flip status to AWAITING_APPROVAL."""
    allowed = {"retry_adjusted_config", "quarantine_reprocess", "schema_evolution_ddl", "diagnosis_only"}
    if remediation_type not in allowed:
        return {"ok": False, "error": f"unsupported remediation_type: {remediation_type}"}
    update_incident_status(conn, incident_id, "AWAITING_APPROVAL", changed_by="agent")
    cur = _cur(conn)
    try:
        detail = {
            "remediation_type": remediation_type,
            "parameters": parameters,
            "notes": notes,
            "proposed_at": datetime.now(UTC).isoformat(),
        }
        cur.execute(
            """
            INSERT INTO audit_log (actor, action, entity_type, entity_id, detail_json)
            VALUES ('agent', 'propose_remediation', 'incident', %s, CAST(%s AS jsonb))
            """,
            (incident_id, json.dumps(detail, default=str)),
        )
        conn.commit()
        return {"ok": True, "proposal": detail}
    finally:
        cur.close()


def load_incident(conn: Any, incident_id: str | None = None, job_run_id: str | None = None) -> dict[str, Any] | None:
    cur = _cur(conn)
    try:
        if incident_id:
            cur.execute(
                """
                SELECT incident_id, job_run_id, pipeline_key, primary_failure_type, severity, status,
                       rca_report_path, linked_commit_sha, detected_at
                FROM incidents WHERE incident_id = %s
                """,
                (incident_id,),
            )
        elif job_run_id:
            cur.execute(
                """
                SELECT incident_id, job_run_id, pipeline_key, primary_failure_type, severity, status,
                       rca_report_path, linked_commit_sha, detected_at
                FROM incidents WHERE job_run_id = %s
                """,
                (job_run_id,),
            )
        else:
            return None
        row = cur.fetchone()
        if not row:
            return None
        keys = [
            "incident_id",
            "job_run_id",
            "pipeline_key",
            "primary_failure_type",
            "severity",
            "status",
            "rca_report_path",
            "linked_commit_sha",
            "detected_at",
        ]
        data = dict(zip(keys, row, strict=True))
        data["incident_id"] = str(data["incident_id"] if not isinstance(data["incident_id"], UUID) else data["incident_id"])
        cur.execute(
            """
            SELECT failure_type, detected_by, evidence_json
            FROM incident_signals WHERE incident_id = %s
            """,
            (data["incident_id"],),
        )
        signals = []
        for ft, by, evidence in cur.fetchall():
            if isinstance(evidence, str):
                try:
                    evidence = json.loads(evidence)
                except json.JSONDecodeError:
                    pass
            signals.append({"failure_type": ft, "detected_by": by, "evidence": evidence})
        data["signals"] = signals
        return data
    finally:
        cur.close()


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        f"# RCA — {report.get('incident_id', '')}",
        "",
        f"**Summary:** {report.get('summary', '')}",
        "",
        f"**Root cause:** {report.get('root_cause', '')}",
        "",
        f"**Failure type:** `{report.get('root_cause_type', '')}`",
        "",
        f"**Blast radius:** {report.get('blast_radius', '')}",
        "",
        "## Evidence",
    ]
    for item in report.get("evidence", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            f"**Suspected commit:** {report.get('suspected_commit_sha') or 'n/a'}",
            "",
            f"**Remediation proposal:** {report.get('remediation_proposal', '')}",
            "",
            f"**Cited runbook:** {report.get('cited_runbook', '')}",
            "",
        ]
    )
    return "\n".join(lines)
