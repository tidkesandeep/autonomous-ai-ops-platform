"""Phase 4 evaluation loop: 6 failure classes × N repeats, auto-graded."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from src.agent.evaluate import auto_grade_report, summarize_grades, write_grades_csv
from src.agent.runner import run_agent
from src.agent.synth import open_synthetic_incident, seed_synthetic_evidence
from src.common.constants import FAILURE_TYPES, PIPELINE_KEYS

PIPELINE_FOR = {
    "null_spike": "customers_scd2",
    "duplicate_explosion": "orders_ingest",
    "late_data": "events_clickstream",
    "volume_anomaly": "orders_ingest",
    "schema_drift": "products_catalog",
    "job_crash": "ops_force_fail",
}


def run_agent_eval(
    spark: Any,
    conn: Any,
    *,
    reports_dir: str,
    grades_csv: str,
    repeats: int = 3,
) -> dict[str, Any]:
    grades = []
    details = []
    for failure_type in FAILURE_TYPES:
        pipeline = PIPELINE_FOR.get(failure_type, PIPELINE_KEYS[0])
        for i in range(1, repeats + 1):
            run_id = f"agent-eval-{failure_type}-{i}"
            seed_synthetic_evidence(spark, failure_type, run_id, pipeline)
            incident_id = open_synthetic_incident(
                conn,
                job_run_id=run_id,
                pipeline_key=pipeline,
                failure_type=failure_type,
            )
            result = run_agent(
                spark,
                conn,
                incident_id=incident_id,
                reports_dir=reports_dir,
                use_langgraph=True,
            )
            report = result.get("report") or {}
            grade = auto_grade_report(report, failure_type)
            grades.append(grade)
            details.append(
                {
                    "run_id": run_id,
                    "incident_id": incident_id,
                    "failure_type": failure_type,
                    "ok": result.get("ok"),
                    "score": grade.score,
                    "rca_path": (result.get("saved") or {}).get("rca_report_path"),
                }
            )

    write_grades_csv(grades_csv, grades)
    summary = summarize_grades(grades)
    return {
        "ran_at": datetime.now(UTC).isoformat(),
        "repeats": repeats,
        "summary": summary,
        "details": details,
        "grades_csv": grades_csv,
        "exit_criteria_met": summary.get("exit_criteria_met", False),
    }
