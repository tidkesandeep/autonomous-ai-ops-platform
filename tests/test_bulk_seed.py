"""Unit tests for console bulk seed (mocked Postgres)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from src.agent.bulk_seed import _fast_investigate, bulk_seed_console


class _FakeCursor:
    def __init__(self, store: dict[str, Any]):
        self.store = store
        self._result: Any = None
        self._fetchall: list[Any] = []

    def execute(self, sql: str, params: tuple | None = None) -> None:
        q = " ".join(sql.split()).lower()
        params = params or ()
        if "select count(*) from incidents" in q:
            self._result = (len(self.store["incidents"]),)
        elif "select status, count(*)" in q:
            counts: dict[str, int] = {}
            for row in self.store["incidents"].values():
                counts[row["status"]] = counts.get(row["status"], 0) + 1
            self._fetchall = [(k, v) for k, v in counts.items()]
            self._result = None
        elif "where status = 'awaiting_approval'" in q:
            rows = [
                (iid, row["primary_failure_type"])
                for iid, row in self.store["incidents"].items()
                if row["status"] == "AWAITING_APPROVAL"
            ]
            self._fetchall = rows
            self._result = None
        elif "insert into incidents" in q:
            iid = f"inc-{len(self.store['incidents']) + 1}"
            self.store["incidents"][iid] = {
                "incident_id": iid,
                "job_run_id": params[0],
                "pipeline_key": params[1],
                "primary_failure_type": params[2],
                "severity": params[3],
                "status": "OPEN",
                "rca_report_path": None,
                "linked_commit_sha": None,
                "detected_at": "2026-01-01",
                "signals": [],
            }
            self._result = (iid,)
        elif "select status from incidents" in q:
            iid = str(params[0])
            self._result = (self.store["incidents"][iid]["status"],)
        elif "update incidents set status" in q:
            iid = str(params[1])
            self.store["incidents"][iid]["status"] = params[0]
            self._result = None
        elif "update incidents" in q and "rca_report_path" in q:
            iid = str(params[2])
            self.store["incidents"][iid]["rca_report_path"] = params[0]
            self._result = None
        elif "insert into approvals" in q:
            aid = f"appr-{len(self.store['approvals']) + 1}"
            self.store["approvals"].append({"approval_id": aid, "params": params})
            self._result = (aid,)
        elif "insert into agent_actions" in q:
            self.store["actions"].append(params)
            self._result = (f"act-{len(self.store['actions'])}",)
        elif "insert into audit_log" in q:
            self.store["audit"].append(params)
            self._result = None
        elif "insert into incident_status_events" in q:
            self.store["events"].append(params)
            self._result = None
        elif "insert into incident_signals" in q:
            self.store["signals"].append(params)
            self._result = None
        elif "from incidents where incident_id" in q:
            iid = str(params[0])
            row = self.store["incidents"].get(iid)
            if not row:
                self._result = None
            else:
                self._result = (
                    row["incident_id"],
                    row["job_run_id"],
                    row["pipeline_key"],
                    row["primary_failure_type"],
                    row["severity"],
                    row["status"],
                    row["rca_report_path"],
                    row["linked_commit_sha"],
                    row["detected_at"],
                )
        elif "from incident_signals" in q:
            self._fetchall = []
            self._result = None
        elif "from audit_log" in q and "propose_remediation" in q:
            iid = str(params[0])
            props = [a for a in self.store["audit"] if len(a) >= 3 and str(a[2]) == iid]
            if not props:
                self._result = None
            else:
                # detail_json is last arg in insert; for select return detail + created_at
                detail = props[-1][-1] if props[-1] else "{}"
                self._result = (detail, "2026-01-01")
        else:
            self._result = None
            self._fetchall = []

    def fetchone(self):
        return self._result

    def fetchall(self):
        return self._fetchall

    def close(self) -> None:
        return None


class _FakeConn:
    def __init__(self):
        self.store: dict[str, Any] = {
            "incidents": {},
            "actions": [],
            "audit": [],
            "events": [],
            "signals": [],
            "approvals": [],
        }

    def cursor(self):
        return _FakeCursor(self.store)

    def commit(self) -> None:
        return None


def test_fast_investigate_moves_to_awaiting(tmp_path):
    conn = _FakeConn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO incidents (job_run_id, pipeline_key, primary_failure_type, severity, status)
        VALUES (%s, %s, %s, %s, 'OPEN')
        RETURNING incident_id
        """,
        ("run-1", "orders_ingest", "null_spike", "medium"),
    )
    iid = cur.fetchone()[0]
    out = _fast_investigate(conn, iid, reports_dir=str(tmp_path))
    assert out["ok"] is True
    assert conn.store["incidents"][iid]["status"] == "AWAITING_APPROVAL"
    assert conn.store["incidents"][iid]["rca_report_path"]
    assert any("propose_remediation" in str(a) for a in conn.store["audit"]) or conn.store["audit"]


def test_bulk_seed_reaches_target_and_status_mix(tmp_path):
    conn = _FakeConn()
    spark = MagicMock()
    result = bulk_seed_console(
        spark,
        conn,
        reports_dir=str(tmp_path),
        target_total=12,
        resolve_n=3,
        reject_n=2,
        mode="fast",
        write_evidence=False,
    )
    assert result["ok"] is True
    assert result["after_total"] == 12
    assert result["created"] == 12
    breakdown = result["status_breakdown"]
    assert breakdown.get("RESOLVED", 0) == 3
    assert breakdown.get("INVESTIGATING", 0) == 2
    assert breakdown.get("AWAITING_APPROVAL", 0) == 7
