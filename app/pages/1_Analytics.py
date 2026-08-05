"""Analytics page — reads synced ops.gold Delta mirrors (SQL warehouse), Lakebase fallback."""

from __future__ import annotations

import streamlit as st

import sys
from pathlib import Path

# Streamlit multipage runs this file with cwd=app/; ensure sibling imports work.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import fetchall, sql_warehouse_query

st.set_page_config(page_title="AI Ops Analytics", layout="wide")
st.title("Ops Analytics")
st.caption("Prefers `ops.gold.*_delta` mirrors via SQL warehouse; falls back to Lakebase.")


def _warehouse_or_pg(delta_sql: str, pg_sql: str, cols: list[str]) -> tuple[list[dict], str]:
    try:
        rows = sql_warehouse_query(delta_sql)
        return [dict(zip(cols, r)) for r in rows], "delta"
    except Exception:  # noqa: BLE001
        return fetchall(pg_sql), "lakebase"


incidents, src1 = _warehouse_or_pg(
    """
    SELECT status, COUNT(*) AS n
    FROM ops.gold.incidents_delta
    GROUP BY status
    ORDER BY n DESC
    """,
    """
    SELECT status, COUNT(*)::int AS n
    FROM incidents
    GROUP BY status
    ORDER BY n DESC
    """,
    ["status", "n"],
)

st.subheader("Incidents by status")
st.caption(f"source={src1}")
st.dataframe(incidents, use_container_width=True)

mttr, src2 = _warehouse_or_pg(
    """
    SELECT
      ROUND(AVG(unix_timestamp(resolved.changed_at) - unix_timestamp(opened.changed_at)) / 60.0, 2) AS mttr_minutes,
      COUNT(*) AS resolved_n
    FROM (
      SELECT incident_id, MIN(changed_at) AS changed_at
      FROM ops.gold.incident_status_events_delta
      WHERE to_status = 'OPEN' OR from_status IS NULL
      GROUP BY incident_id
    ) opened
    JOIN (
      SELECT incident_id, MIN(changed_at) AS changed_at
      FROM ops.gold.incident_status_events_delta
      WHERE to_status = 'RESOLVED'
      GROUP BY incident_id
    ) resolved
      ON opened.incident_id = resolved.incident_id
    """,
    """
    SELECT
      ROUND(EXTRACT(EPOCH FROM AVG(resolved.changed_at - opened.changed_at)) / 60.0, 2) AS mttr_minutes,
      COUNT(*)::int AS resolved_n
    FROM (
      SELECT incident_id, MIN(changed_at) AS changed_at
      FROM incident_status_events
      WHERE to_status = 'OPEN' OR from_status IS NULL
      GROUP BY incident_id
    ) opened
    JOIN (
      SELECT incident_id, MIN(changed_at) AS changed_at
      FROM incident_status_events
      WHERE to_status = 'RESOLVED'
      GROUP BY incident_id
    ) resolved ON opened.incident_id = resolved.incident_id
    """,
    ["mttr_minutes", "resolved_n"],
)
st.subheader("MTTR (open → resolved)")
st.caption(f"source={src2}")
st.dataframe(mttr, use_container_width=True)

approvals, src3 = _warehouse_or_pg(
    """
    SELECT decision, COUNT(*) AS n
    FROM ops.gold.approvals_delta
    GROUP BY decision
    """,
    """
    SELECT decision, COUNT(*)::int AS n FROM approvals GROUP BY decision
    """,
    ["decision", "n"],
)
st.subheader("Approval rates")
st.caption(f"source={src3}")
st.dataframe(approvals, use_container_width=True)

tools, src4 = _warehouse_or_pg(
    """
    SELECT tool_name, COUNT(*) AS n
    FROM ops.gold.agent_actions_delta
    GROUP BY tool_name
    ORDER BY n DESC
    LIMIT 20
    """,
    """
    SELECT tool_name, COUNT(*)::int AS n
    FROM agent_actions
    GROUP BY tool_name
    ORDER BY n DESC
    LIMIT 20
    """,
    ["tool_name", "n"],
)
st.subheader("Most-used agent tools")
st.caption(f"source={src4}")
st.dataframe(tools, use_container_width=True)

failures, src5 = _warehouse_or_pg(
    """
    SELECT primary_failure_type, COUNT(*) AS n
    FROM ops.gold.incidents_delta
    GROUP BY primary_failure_type
    ORDER BY n DESC
    """,
    """
    SELECT primary_failure_type, COUNT(*)::int AS n
    FROM incidents
    GROUP BY primary_failure_type
    ORDER BY n DESC
    """,
    ["primary_failure_type", "n"],
)
st.subheader("Incidents by failure type")
st.caption(f"source={src5}")
st.dataframe(failures, use_container_width=True)
