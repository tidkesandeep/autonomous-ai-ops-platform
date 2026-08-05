"""Phase 5 prove: inject → detect → agent → approve → remediate → RESOLVED."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from src.agent.runner import run_agent
from src.agent.synth import open_synthetic_incident, seed_synthetic_evidence
from src.chaos.injector import inject
from src.remediation.approvals import approve_incident
from src.remediation.execute import execute_remediation
from src.remediation.mapping import remediation_for_failure
from src.remediation.sync_analytics import sync_lakebase_mirrors


def prove_full_loop(
    spark: Any,
    conn: Any,
    *,
    reports_dir: str,
    failure_type: str = "null_spike",
    use_chaos: bool = False,
    sync_mirrors: bool = True,
) -> dict[str, Any]:
    """Prove human-in-the-loop remediation end-to-end.

    Default path uses synthetic ops evidence (reliable on Free Edition).
    Set ``use_chaos=True`` to also mutate demo tables before remediating.
    """
    run_id = f"phase5-prove-{failure_type}-{uuid4().hex[:8]}"
    rem_type, rem_params = remediation_for_failure(failure_type)

    pipeline = {
        "null_spike": "customers_scd2",
        "duplicate_explosion": "orders_ingest",
        "late_data": "events_clickstream",
        "volume_anomaly": "orders_ingest",
        "schema_drift": "products_catalog",
        "job_crash": "ops_force_fail",
    }.get(failure_type, "customers_scd2")

    chaos_result = None
    if use_chaos and failure_type in (
        "null_spike",
        "duplicate_explosion",
        "schema_drift",
        "late_data",
        "volume_anomaly",
        "job_crash",
    ):
        chaos_result = inject(spark, failure_type, job_run_id=run_id)
        pipeline = chaos_result.pipeline_key

    seed_synthetic_evidence(spark, failure_type, run_id, pipeline)
    incident_id = open_synthetic_incident(
        conn,
        job_run_id=run_id,
        pipeline_key=pipeline,
        failure_type=failure_type,
    )

    agent = run_agent(
        spark,
        conn,
        incident_id=incident_id,
        reports_dir=reports_dir,
        use_langgraph=True,
    )

    # Approve without Jobs dispatch — execute remediation in-process for the prove job.
    approval = approve_incident(
        conn,
        incident_id,
        decided_by="phase5_prove",
        notes="automated prove loop",
        trigger_job=False,
    )

    remediation = execute_remediation(
        spark,
        conn,
        incident_id=incident_id,
        remediation_type=rem_type,
        parameters=rem_params,
        resolve=True,
    )

    cur = conn.cursor()
    try:
        cur.execute("SELECT status, rca_report_path FROM incidents WHERE incident_id = %s", (incident_id,))
        status, rca_path = cur.fetchone()
        cur.execute(
            "SELECT COUNT(*) FROM approvals WHERE incident_id = %s AND decision = 'approved'",
            (incident_id,),
        )
        n_approvals = cur.fetchone()[0]
    finally:
        cur.close()

    mirrors = sync_lakebase_mirrors(spark) if sync_mirrors else {"ok": None, "skipped": True}

    ok = bool(
        agent.get("ok")
        and approval.get("ok")
        and remediation.get("ok")
        and status == "RESOLVED"
        and rca_path
        and n_approvals >= 1
    )
    return {
        "ok": ok,
        "ran_at": datetime.now(UTC).isoformat(),
        "failure_type": failure_type,
        "job_run_id": run_id,
        "incident_id": incident_id,
        "remediation_type": rem_type,
        "status": status,
        "rca_report_path": rca_path,
        "approvals": n_approvals,
        "agent_ok": agent.get("ok"),
        "approval": approval,
        "remediation": remediation,
        "chaos": chaos_result.__dict__ if chaos_result else None,
        "mirrors": mirrors,
        "exit_criteria_met": ok,
    }
