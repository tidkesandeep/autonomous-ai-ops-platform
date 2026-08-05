"""Execute an approved remediation and mark the incident RESOLVED."""

from __future__ import annotations

import json
from typing import Any

from src.agent.tools_write import load_incident, update_incident_status
from src.remediation.mapping import remediation_for_failure
from src.remediation.quarantine import record_remediation_run, run_quarantine
from src.remediation.retry_config import apply_retry_adjusted_config
from src.remediation.schema_ddl import run_schema_evolution


def execute_remediation(
    spark: Any,
    conn: Any,
    *,
    incident_id: str,
    remediation_type: str | None = None,
    parameters: dict[str, Any] | None = None,
    resolve: bool = True,
) -> dict[str, Any]:
    incident = load_incident(conn, incident_id=incident_id)
    if not incident:
        return {"ok": False, "error": "incident not found"}

    rem_type = remediation_type
    rem_params = dict(parameters or {})
    if not rem_type:
        rem_type, default_params = remediation_for_failure(incident.get("primary_failure_type"))
        for k, v in default_params.items():
            rem_params.setdefault(k, v)

    pipeline = incident["pipeline_key"]
    result: dict[str, Any]

    if rem_type == "quarantine_reprocess":
        result = run_quarantine(spark, rem_params, pipeline_key=pipeline)
        if result.get("ok"):
            record_remediation_run(spark, incident_id, result)
    elif rem_type == "retry_adjusted_config":
        result = apply_retry_adjusted_config(
            spark,
            incident_id=incident_id,
            pipeline_key=pipeline,
            parameters=rem_params,
        )
    elif rem_type == "schema_evolution_ddl":
        result = run_schema_evolution(
            spark,
            incident_id=incident_id,
            pipeline_key=pipeline,
            parameters=rem_params,
        )
    elif rem_type == "diagnosis_only":
        result = {"ok": True, "remediation_type": "diagnosis_only", "note": "no data changes"}
    else:
        return {"ok": False, "error": f"unsupported remediation_type: {rem_type}"}

    if not result.get("ok"):
        return {"ok": False, "incident_id": incident_id, "remediation_type": rem_type, "result": result}

    if resolve:
        update_incident_status(conn, incident_id, "RESOLVED", changed_by="remediation_job")
        _audit(conn, incident_id, rem_type, result)

    return {
        "ok": True,
        "incident_id": incident_id,
        "remediation_type": rem_type,
        "parameters": rem_params,
        "result": result,
        "status": "RESOLVED" if resolve else incident["status"],
    }


def _audit(conn: Any, incident_id: str, rem_type: str, result: dict[str, Any]) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO audit_log (actor, action, entity_type, entity_id, detail_json)
            VALUES ('remediation_job', 'remediation_executed', 'incident', %s, CAST(%s AS jsonb))
            """,
            (
                incident_id,
                json.dumps({"remediation_type": rem_type, "result": result}, default=str),
            ),
        )
        conn.commit()
    finally:
        cur.close()
