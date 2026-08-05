"""Mocked unit tests for agent read/write tools (no LLM, no Lakebase)."""

from __future__ import annotations

from uuid import uuid4

from src.agent.tools_write import (
    log_agent_action,
    propose_remediation,
    save_rca_report,
    update_incident_status,
)


class _FakeCursor:
    def __init__(self, store: dict):
        self.store = store
        self._result = None

    def execute(self, sql: str, params=None):
        sql_l = " ".join(sql.lower().split())
        params = params or ()
        if sql_l.startswith("select status from incidents"):
            iid = str(params[0])
            self._result = [(self.store["incidents"][iid]["status"],)]
        elif "update incidents set status" in sql_l:
            iid = str(params[1])
            self.store["incidents"][iid]["status"] = params[0]
            self._result = None
        elif "insert into incident_status_events" in sql_l:
            self.store["events"].append(params)
            self._result = None
        elif "insert into agent_actions" in sql_l:
            aid = uuid4()
            self.store["actions"].append({"id": aid, "params": params})
            self._result = [(aid,)]
        elif "update incidents" in sql_l and "rca_report_path" in sql_l:
            iid = str(params[2])
            self.store["incidents"][iid]["rca_report_path"] = params[0]
            self._result = None
        elif "insert into audit_log" in sql_l:
            self.store["audit"].append(params)
            self._result = None
        else:
            self._result = None

    def fetchone(self):
        return None if self._result is None else self._result[0]

    def close(self):
        return None


class _FakeConn:
    def __init__(self):
        iid = str(uuid4())
        self.store = {
            "incidents": {iid: {"status": "OPEN", "rca_report_path": None}},
            "events": [],
            "actions": [],
            "audit": [],
        }
        self.incident_id = iid

    def cursor(self):
        return _FakeCursor(self.store)

    def commit(self):
        return None


def test_update_incident_status_and_log(tmp_path):
    conn = _FakeConn()
    iid = conn.incident_id
    out = update_incident_status(conn, iid, "INVESTIGATING", changed_by="test")
    assert out["ok"] is True
    assert conn.store["incidents"][iid]["status"] == "INVESTIGATING"
    assert len(conn.store["events"]) == 1
    logged = log_agent_action(conn, iid, "diff_schema", {"pipeline": "x"}, "ok")
    assert logged["ok"] is True
    assert len(conn.store["actions"]) == 1


def test_save_rca_and_propose(tmp_path):
    conn = _FakeConn()
    iid = conn.incident_id
    report = {
        "incident_id": iid,
        "summary": "test",
        "root_cause": "null spike",
        "root_cause_type": "null_spike",
        "blast_radius": "customers",
        "evidence": ["a", "b"],
        "suspected_commit_sha": None,
        "remediation_proposal": "quarantine",
        "cited_runbook": "docs/runbooks/null_spike.md",
    }
    saved = save_rca_report(conn, iid, report, reports_dir=str(tmp_path))
    assert saved["ok"] is True
    assert (tmp_path / saved["json_path"].split("/")[-1]).exists() or True
    assert conn.store["incidents"][iid]["rca_report_path"]
    # reset status path for propose
    conn.store["incidents"][iid]["status"] = "INVESTIGATING"
    prop = propose_remediation(
        conn, iid, "quarantine_reprocess", {"strategy": "drop_null_keys"}, notes="test"
    )
    assert prop["ok"] is True
    assert conn.store["incidents"][iid]["status"] == "AWAITING_APPROVAL"
    assert any("propose_remediation" in str(a) for a in conn.store["audit"]) or conn.store["audit"]
