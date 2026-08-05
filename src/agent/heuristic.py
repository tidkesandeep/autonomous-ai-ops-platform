"""Deterministic investigator — tool loop without requiring an LLM API key.

Uses incident signals + ops evidence + runbook RAG to produce a structured RCA.
When LiteLLM keys exist, ``src.agent.graph`` can refine the narrative; this path
alone is enough to score well against injected chaos ground truth.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from src.agent.tools_read import (
    correlate_github_commits,
    diff_schema,
    get_dq_failures,
    query_run_history,
    read_task_logs,
    sample_bad_records,
    search_runbooks_tool,
    time_travel_compare,
)
from src.agent.tools_write import (
    load_incident,
    log_agent_action,
    propose_remediation,
    save_rca_report,
    update_incident_status,
)
from src.detection.slack import notify_raw

REMEDIATION_FOR = {
    "job_crash": ("retry_adjusted_config", {"timeout_seconds": 3600, "retry": 1}),
    "schema_drift": ("schema_evolution_ddl", {"action": "generate_ddl"}),
    "duplicate_explosion": ("quarantine_reprocess", {"strategy": "keep_latest_per_key"}),
    "null_spike": ("quarantine_reprocess", {"strategy": "drop_null_keys"}),
    "volume_anomaly": ("diagnosis_only", {"reason": "no safe automatic remediation"}),
    "late_data": ("retry_adjusted_config", {"mode": "replay_window"}),
}


def _summarize(obj: Any, limit: int = 800) -> str:
    text = obj if isinstance(obj, str) else json.dumps(obj, default=str)
    return text[:limit]


def investigate_incident(
    spark: Any,
    conn: Any,
    *,
    incident_id: str | None = None,
    job_run_id: str | None = None,
    reports_dir: str,
    runbooks_embedded: bool = True,
    notify_slack: bool = True,
) -> dict[str, Any]:
    """Run the full investigation write path for one incident."""
    incident = load_incident(conn, incident_id=incident_id, job_run_id=job_run_id)
    if not incident:
        return {"ok": False, "error": "incident not found", "incident_id": incident_id, "job_run_id": job_run_id}

    iid = incident["incident_id"]
    pipeline = incident["pipeline_key"]
    failure = incident.get("primary_failure_type") or "unknown"
    run_id = incident["job_run_id"]

    update_incident_status(conn, iid, "INVESTIGATING", changed_by="agent")
    log_agent_action(conn, iid, "update_incident_status", {"to": "INVESTIGATING"}, "status→INVESTIGATING")

    tool_trace: list[dict[str, Any]] = []

    def call(name: str, fn: Callable[..., Any], **kwargs: Any) -> Any:
        result = fn(**kwargs)
        log_agent_action(conn, iid, name, kwargs, _summarize(result))
        tool_trace.append({"tool": name, "inputs": kwargs, "outputs": result})
        return result

    history = call("query_run_history", query_run_history, spark=spark, pipeline_key=pipeline, limit=8)
    dq = call("get_dq_failures", get_dq_failures, spark=spark, run_id=run_id)
    logs = call("read_task_logs", read_task_logs, spark=spark, run_id=run_id, limit=10)
    schema = call("diff_schema", diff_schema, spark=spark, pipeline_key=pipeline)
    commits = call("correlate_github_commits", correlate_github_commits, pipeline_key=pipeline, limit=5)

    table_guess = None
    for sig in incident.get("signals") or []:
        ev = sig.get("evidence") or {}
        if isinstance(ev, dict) and ev.get("table_name"):
            table_guess = ev["table_name"]
            break
    samples = []
    travel = {}
    if table_guess:
        samples = call("sample_bad_records", sample_bad_records, spark=spark, table_name=table_guess, limit=5)
        travel = call("time_travel_compare", time_travel_compare, spark=spark, table_name=table_guess)

    runbook_hits = []
    if runbooks_embedded:
        runbook_hits = call(
            "search_runbooks",
            search_runbooks_tool,
            spark=spark,
            query=f"{failure} {pipeline} incident investigation",
            failure_type=failure if failure != "unknown" else None,
            top_k=3,
        )

    cited = runbook_hits[0]["runbook_path"] if runbook_hits and "runbook_path" in runbook_hits[0] else None
    suspect = None
    for c in commits:
        if isinstance(c, dict) and c.get("sha") and "note" not in c and "error" not in c:
            suspect = c["sha"]
            break

    rem_type, rem_params = REMEDIATION_FOR.get(failure, ("diagnosis_only", {}))
    evidence_lines = [
        f"primary_failure_type={failure}",
        f"signals={[s.get('failure_type') for s in incident.get('signals') or []]}",
        f"dq_failures={len(dq) if isinstance(dq, list) else dq}",
        f"task_logs={len(logs) if isinstance(logs, list) else logs}",
        f"schema_diff={schema}",
    ]
    if samples:
        evidence_lines.append(f"sampled_bad_records={_summarize(samples, 300)}")
    if travel:
        evidence_lines.append(f"time_travel={travel}")

    report = {
        "incident_id": iid,
        "job_run_id": run_id,
        "pipeline_key": pipeline,
        "summary": f"Investigation of {failure} on pipeline `{pipeline}` (run `{run_id}`).",
        "root_cause": _root_cause_text(failure, dq, logs, schema),
        "root_cause_type": failure,
        "blast_radius": f"Pipeline `{pipeline}` and downstream consumers of its silver/gold tables.",
        "evidence": evidence_lines,
        "suspected_commit_sha": suspect,
        "remediation_proposal": f"{rem_type}: {rem_params}",
        "cited_runbook": cited,
        "tool_trace_len": len(tool_trace),
        "history_sample": history[:3] if isinstance(history, list) else history,
    }

    saved = save_rca_report(conn, iid, report, reports_dir=reports_dir)
    log_agent_action(conn, iid, "save_rca_report", {"path": saved.get("rca_report_path")}, "RCA saved")
    proposal = propose_remediation(conn, iid, rem_type, rem_params, notes=report["root_cause"])
    log_agent_action(conn, iid, "propose_remediation", {"type": rem_type}, _summarize(proposal))

    if notify_slack:
        notify_raw(
            f":mag: RCA ready for `{iid}`\n"
            f"• cause: `{failure}`\n"
            f"• pipeline: `{pipeline}`\n"
            f"• report: `{saved.get('rca_report_path')}`"
        )

    return {
        "ok": True,
        "incident_id": iid,
        "report": report,
        "saved": saved,
        "proposal": proposal,
        "tools_used": [t["tool"] for t in tool_trace],
    }


def _root_cause_text(failure: str, dq: Any, logs: Any, schema: Any) -> str:
    if failure == "null_spike":
        return "Null rate exceeded threshold on a monitored column (DQ null check failed)."
    if failure == "duplicate_explosion":
        return "Natural-key duplicate rate exceeded threshold after a join/merge."
    if failure == "late_data":
        return "Event lag exceeded the late-data watermark (pct_late / lag_minutes)."
    if failure == "volume_anomaly":
        return "Row volume moved outside the cold-start/z-score envelope vs prior runs."
    if failure == "schema_drift":
        added = (schema or {}).get("added") if isinstance(schema, dict) else None
        removed = (schema or {}).get("removed") if isinstance(schema, dict) else None
        return f"Schema drift detected vs prior snapshot (added={added}, removed={removed})."
    if failure == "job_crash":
        sig = None
        if isinstance(logs, list) and logs:
            sig = logs[0].get("error_signature") or logs[0].get("raw_output_excerpt")
        return f"Task crashed/failed at runtime ({sig or 'see raw_task_logs'})."
    return f"Primary signal `{failure}` with supporting ops evidence."
