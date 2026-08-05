"""Agent entrypoint used by notebooks/jobs."""

from __future__ import annotations

from typing import Any

from src.agent.graph import llm_available, run_langgraph_agent
from src.agent.heuristic import investigate_incident


def run_agent(
    spark: Any,
    conn: Any,
    *,
    incident_id: str | None = None,
    job_run_id: str | None = None,
    reports_dir: str,
    use_langgraph: bool | None = None,
) -> dict[str, Any]:
    """Run investigation. Defaults to LangGraph wrapper when available/requested."""
    if use_langgraph is None:
        use_langgraph = True  # wrapper always works; LLM polish is optional inside
    if use_langgraph:
        result = run_langgraph_agent(
            spark,
            conn,
            incident_id=incident_id,
            job_run_id=job_run_id,
            reports_dir=reports_dir,
        )
    else:
        result = investigate_incident(
            spark,
            conn,
            incident_id=incident_id,
            job_run_id=job_run_id,
            reports_dir=reports_dir,
        )
    result["llm_available"] = llm_available()
    return result
