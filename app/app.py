"""Autonomous AI Ops — Incident Console (Databricks App)."""

from __future__ import annotations

import os

import streamlit as st

from services import (
    current_user_email,
    do_approve,
    do_reject,
    get_incident,
    list_agent_actions,
    list_approvals,
    list_incidents,
    list_signals,
    list_status_events,
    proposal_for,
    read_rca_excerpt,
)

st.set_page_config(page_title="AI Ops Console", page_icon="🛠️", layout="wide")
st.title("Autonomous AI Ops — Incident Console")
st.caption("Approve or reject agent remediation proposals. Writes go to Lakebase.")

status_filter = st.sidebar.selectbox(
    "Status filter",
    ["AWAITING_APPROVAL", "ALL", "OPEN", "INVESTIGATING", "RESOLVED"],
    index=0,
)
limit = st.sidebar.slider("Limit", 10, 200, 50)
operator = st.sidebar.text_input("Acting as", value=current_user_email("sandeeptidke.work@gmail.com"))

try:
    incidents = list_incidents(limit=limit, status=status_filter)
except Exception as exc:  # noqa: BLE001
    st.error(f"Lakebase connection failed: {exc}")
    st.stop()

if not incidents:
    st.info("No incidents for this filter.")
    st.stop()

labels = [
    f"{i['status']} · {i['primary_failure_type'] or '?'} · {i['pipeline_key']} · {i['job_run_id']}"
    for i in incidents
]
choice = st.selectbox("Incident", options=range(len(labels)), format_func=lambda idx: labels[idx])
selected = incidents[choice]
iid = selected["incident_id"]

detail = get_incident(iid) or selected
col1, col2, col3 = st.columns(3)
col1.metric("Status", detail["status"])
col2.metric("Failure", detail.get("primary_failure_type") or "—")
col3.metric("Pipeline", detail.get("pipeline_key") or "—")

st.write(
    {
        "incident_id": iid,
        "job_run_id": detail.get("job_run_id"),
        "detected_at": str(detail.get("detected_at")),
        "linked_commit_sha": detail.get("linked_commit_sha"),
        "rca_report_path": detail.get("rca_report_path"),
    }
)

prop = proposal_for(iid)
if prop:
    st.subheader("Remediation proposal")
    st.json(prop.get("detail") or prop)
else:
    st.warning("No propose_remediation audit row yet.")

tabs = st.tabs(["RCA", "Signals", "Timeline", "Agent actions", "Approvals"])
with tabs[0]:
    st.code(read_rca_excerpt(detail.get("rca_report_path")), language="markdown")
with tabs[1]:
    st.dataframe(list_signals(iid), use_container_width=True)
with tabs[2]:
    st.dataframe(list_status_events(iid), use_container_width=True)
with tabs[3]:
    st.dataframe(list_agent_actions(iid), use_container_width=True)
with tabs[4]:
    st.dataframe(list_approvals(iid), use_container_width=True)

st.subheader("Decision")
notes = st.text_area("Notes", value="")
c_a, c_r = st.columns(2)
with c_a:
    if st.button("Approve & remediate", type="primary", use_container_width=True):
        if detail["status"] == "RESOLVED":
            st.warning("Already resolved.")
        else:
            result = do_approve(iid, decided_by=operator, notes=notes)
            st.json(result)
            if result.get("ok"):
                st.success("Approval recorded.")
                if result.get("dispatch") and result["dispatch"].get("ok"):
                    st.info(f"Remediation job run_id={result['dispatch'].get('run_id')}")
                elif result.get("remediation_type") == "diagnosis_only":
                    st.info("diagnosis_only — incident resolved without a job.")
                elif result.get("dispatch") and result["dispatch"].get("skipped"):
                    st.warning(
                        "Approval saved but job dispatch skipped "
                        f"({result['dispatch'].get('error')}). "
                        "Set REMEDIATION_JOB_ID or run ops-remediate manually."
                    )
with c_r:
    if st.button("Reject", use_container_width=True):
        result = do_reject(iid, decided_by=operator, notes=notes)
        st.json(result)
        if result.get("ok"):
            st.success("Rejected — status set to INVESTIGATING.")

st.sidebar.markdown("---")
st.sidebar.caption(f"REMEDIATION_JOB_ID={os.environ.get('REMEDIATION_JOB_ID') or 'unset'}")
