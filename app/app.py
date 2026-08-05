"""Autonomous AI Ops — Incident Console (Databricks App)."""

from __future__ import annotations

import os

import streamlit as st
from services import (
    FAILURE_CLASS_LABELS,
    REMEDIATION_FOR,
    count_incidents_by_failure_type,
    count_incidents_by_status,
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
    remediation_summary,
)

st.set_page_config(page_title="AI Ops Console", page_icon="🛠️", layout="wide")
st.title("Autonomous AI Ops — Incident Console")
st.caption("Approve or reject agent remediation proposals. Writes go to Lakebase.")

# --- Sidebar filters ---
status_filter = st.sidebar.selectbox(
    "Status filter",
    ["AWAITING_APPROVAL", "ALL", "OPEN", "INVESTIGATING", "RESOLVED"],
    index=0,
)
failure_options = ["ALL", *sorted(FAILURE_CLASS_LABELS.keys())]
failure_filter = st.sidebar.selectbox(
    "Failure class",
    failure_options,
    index=0,
    format_func=lambda v: "All classes" if v == "ALL" else FAILURE_CLASS_LABELS.get(v, v),
)
limit = st.sidebar.slider("Limit", 10, 200, 50)
operator = st.sidebar.text_input("Acting as", value=current_user_email("sandeeptidke.work@gmail.com"))
if st.sidebar.button("Refresh list", use_container_width=True):
    st.rerun()

try:
    status_counts = {r["status"]: r["n"] for r in count_incidents_by_status()}
    class_counts = {r["failure_type"]: r["n"] for r in count_incidents_by_failure_type()}
    incidents = list_incidents(limit=limit, status=status_filter, failure_type=failure_filter)
except Exception as exc:  # noqa: BLE001
    st.error(f"Lakebase connection failed: {exc}")
    st.stop()

st.sidebar.markdown("---")
st.sidebar.caption(
    " · ".join(f"{k}:{status_counts.get(k, 0)}" for k in ("AWAITING_APPROVAL", "OPEN", "RESOLVED"))
    or "No incidents yet"
)
st.sidebar.caption(
    "Classes: "
    + (
        ", ".join(f"{FAILURE_CLASS_LABELS.get(k, k)}:{v}" for k, v in class_counts.items())
        if class_counts
        else "none"
    )
)
st.sidebar.caption(f"REMEDIATION_JOB_ID={os.environ.get('REMEDIATION_JOB_ID') or 'unset'}")

if not incidents:
    st.info("No incidents for this filter combination.")
    st.stop()

# --- Incident table + selection ---
table_rows = [
    {
        "status": i.get("status"),
        "failure_type": i.get("primary_failure_type") or "—",
        "pipeline": i.get("pipeline_key") or "—",
        "job_run_id": i.get("job_run_id") or "—",
        "detected_at": str(i.get("detected_at") or ""),
        "incident_id": i.get("incident_id"),
    }
    for i in incidents
]
st.subheader("Incidents")
st.dataframe(table_rows, use_container_width=True, hide_index=True)

# Keep selection sticky across refreshes when the incident is still in the filtered list.
ids = [i["incident_id"] for i in incidents]
default_iid = st.session_state.get("selected_incident_id")
if default_iid not in ids:
    default_iid = ids[0]
default_index = ids.index(default_iid)

labels = [
    f"{i['status']} · {i['primary_failure_type'] or '?'} · {i['pipeline_key']} · {i['incident_id'][:8]}"
    for i in incidents
]
choice = st.selectbox(
    "Selected incident",
    options=range(len(labels)),
    index=default_index,
    format_func=lambda idx: labels[idx],
)
selected = incidents[choice]
iid = selected["incident_id"]
st.session_state["selected_incident_id"] = iid

detail = get_incident(iid) or selected
col1, col2, col3, col4 = st.columns(4)
col1.metric("Status", detail["status"])
col2.metric("Failure", FAILURE_CLASS_LABELS.get(detail.get("primary_failure_type") or "", detail.get("primary_failure_type") or "—"))
col3.metric("Pipeline", detail.get("pipeline_key") or "—")
col4.metric("Severity", detail.get("severity") or "—")

st.markdown(
    f"**Incident** `{iid}`  \n"
    f"**Job run** `{detail.get('job_run_id') or '—'}`  \n"
    f"**Detected** `{detail.get('detected_at') or '—'}`  \n"
    f"**Linked commit** `{detail.get('linked_commit_sha') or '—'}`  \n"
    f"**RCA path** `{detail.get('rca_report_path') or '—'}`"
)

# --- Proposal (human-readable first) ---
prop = proposal_for(iid)
st.subheader("Remediation proposal")
if prop and isinstance(prop.get("detail"), dict):
    detail_prop = prop["detail"]
    rem_type = detail_prop.get("remediation_type")
    rem_params = detail_prop.get("parameters") or {}
    st.success(remediation_summary(rem_type, rem_params if isinstance(rem_params, dict) else {}))
    c1, c2 = st.columns(2)
    c1.write(f"**Type:** `{rem_type}`")
    c2.write(f"**Failure class:** `{detail.get('primary_failure_type') or '—'}`")
    with st.expander("Raw proposal JSON"):
        st.json(detail_prop)
elif prop:
    st.warning("Proposal found but detail is not a structured object.")
    with st.expander("Raw proposal"):
        st.json(prop)
else:
    # Fall back to mapping so operators still see the intended action.
    mapped_type, mapped_params = REMEDIATION_FOR.get(
        detail.get("primary_failure_type") or "",
        ("diagnosis_only", {"reason": "no proposal yet"}),
    )
    st.warning("No `propose_remediation` audit row yet — showing mapped default action.")
    st.info(remediation_summary(mapped_type, mapped_params))

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

# --- Decision ---
st.subheader("Decision")
status = detail.get("status")
can_decide = status in ("AWAITING_APPROVAL", "INVESTIGATING", "OPEN")
if status == "RESOLVED":
    st.info("This incident is already **RESOLVED**. Approve / Reject are disabled.")
elif status == "AWAITING_APPROVAL":
    st.caption("Ready for operator decision. Approve dispatches remediation (or resolves diagnosis_only).")
else:
    st.caption(f"Status is **{status}**. You can still approve/reject if needed.")

notes = st.text_area("Notes", value="", disabled=not can_decide)
c_a, c_r = st.columns(2)
with c_a:
    if st.button(
        "Approve & remediate",
        type="primary",
        use_container_width=True,
        disabled=not can_decide,
    ):
        result = do_approve(iid, decided_by=operator, notes=notes)
        if result.get("ok"):
            st.success("Approval recorded.")
            if result.get("dispatch") and result["dispatch"].get("ok"):
                st.info(f"Remediation job started · run_id=`{result['dispatch'].get('run_id')}`")
            elif result.get("remediation_type") == "diagnosis_only":
                st.info("diagnosis_only — incident resolved without a remediation job.")
            elif result.get("dispatch") and result["dispatch"].get("skipped"):
                st.warning(
                    "Approval saved but job dispatch skipped "
                    f"({result['dispatch'].get('error')}). "
                    "Set REMEDIATION_JOB_ID or run ops-remediate manually."
                )
            with st.expander("Approval response"):
                st.json(result)
            st.rerun()
        else:
            st.error(result.get("error") or "Approve failed")
            st.json(result)
with c_r:
    if st.button("Reject", use_container_width=True, disabled=not can_decide):
        result = do_reject(iid, decided_by=operator, notes=notes)
        if result.get("ok"):
            st.success("Rejected — status set to INVESTIGATING.")
            with st.expander("Reject response"):
                st.json(result)
            st.rerun()
        else:
            st.error(result.get("error") or "Reject failed")
            st.json(result)
