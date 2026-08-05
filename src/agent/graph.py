"""Optional LangGraph agent when LiteLLM credentials are available."""

from __future__ import annotations

import json
import os
from typing import Any, TypedDict

from src.agent.heuristic import investigate_incident


class AgentState(TypedDict, total=False):
    incident_id: str
    job_run_id: str
    messages: list[str]
    result: dict[str, Any]


def llm_available() -> bool:
    return bool(os.environ.get("GROQ_API_KEY") or os.environ.get("GEMINI_API_KEY") or os.environ.get("OPENAI_API_KEY"))


def build_graph():
    """Minimal LangGraph: triage node delegates to the deterministic investigator.

    Full free-form tool-calling can be expanded once API keys are configured;
    this keeps the graph importable and demoable without secrets.
    """
    from langgraph.graph import END, StateGraph

    def triage(state: AgentState) -> AgentState:
        state = dict(state)
        state.setdefault("messages", []).append("triage:start")
        return state  # type: ignore[return-value]

    def investigate(state: AgentState, spark: Any = None, conn: Any = None, reports_dir: str = "") -> AgentState:
        # Bound at runtime via closure in run_langgraph_agent
        raise RuntimeError("investigate node must be closed over spark/conn")

    g = StateGraph(AgentState)
    g.add_node("triage", triage)
    g.add_node("investigate", lambda s: s)
    g.add_edge("triage", "investigate")
    g.add_edge("investigate", END)
    g.set_entry_point("triage")
    return g.compile()


def run_langgraph_agent(
    spark: Any,
    conn: Any,
    *,
    incident_id: str | None = None,
    job_run_id: str | None = None,
    reports_dir: str,
) -> dict[str, Any]:
    """Execute graph; currently wraps heuristic and optionally asks LLM to polish summary."""
    from langgraph.graph import END, StateGraph

    def triage(state: AgentState) -> AgentState:
        out = dict(state)
        out.setdefault("messages", []).append("triage")
        return out  # type: ignore[return-value]

    def investigate(state: AgentState) -> AgentState:
        result = investigate_incident(
            spark,
            conn,
            incident_id=state.get("incident_id") or incident_id,
            job_run_id=state.get("job_run_id") or job_run_id,
            reports_dir=reports_dir,
        )
        if llm_available() and result.get("ok"):
            try:
                result["report"]["summary"] = _polish_summary(result["report"])
            except Exception as exc:  # noqa: BLE001
                result["llm_polish_error"] = str(exc)
        out = dict(state)
        out["result"] = result
        out.setdefault("messages", []).append("investigate:done")
        return out  # type: ignore[return-value]

    g = StateGraph(AgentState)
    g.add_node("triage", triage)
    g.add_node("investigate", investigate)
    g.add_edge("triage", "investigate")
    g.add_edge("investigate", END)
    g.set_entry_point("triage")
    app = g.compile()
    final = app.invoke({"incident_id": incident_id or "", "job_run_id": job_run_id or ""})
    return final.get("result") or {"ok": False, "error": "graph produced no result", "state": final}


def _polish_summary(report: dict[str, Any]) -> str:
    from litellm import completion

    model = os.environ.get("LITELLM_MODEL") or (
        "groq/llama-3.3-70b-versatile" if os.environ.get("GROQ_API_KEY") else "gemini/gemini-2.0-flash"
    )
    prompt = (
        "Rewrite this incident RCA summary in 2 crisp sentences for Slack. "
        "Keep the failure type accurate.\n\n"
        + json.dumps({k: report.get(k) for k in ("root_cause_type", "root_cause", "pipeline_key", "summary")}, indent=2)
    )
    resp = completion(model=model, messages=[{"role": "user", "content": prompt}])
    content = resp["choices"][0]["message"]["content"]
    return str(content).strip() or report.get("summary", "")
