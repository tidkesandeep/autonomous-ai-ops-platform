"""Unit tests for Databricks App service helpers (no live Lakebase required)."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))


@pytest.fixture()
def services(monkeypatch):
    # Ensure a clean import of app/services.py against mocked db.
    for mod in ("services", "db"):
        sys.modules.pop(mod, None)
    import db as app_db  # noqa: PLC0415

    monkeypatch.setattr(app_db, "fetchall", lambda *a, **k: [])
    monkeypatch.setattr(app_db, "postgres_connection", lambda: _DummyCtx())
    return importlib.import_module("services")


class _DummyConn:
    def cursor(self):
        return _DummyCur()

    def commit(self):
        return None


class _DummyCur:
    def execute(self, *a, **k):
        return None

    def fetchone(self):
        return ("OPEN",)

    def close(self):
        return None


class _DummyCtx:
    def __enter__(self):
        return _DummyConn()

    def __exit__(self, *exc):
        return False


def test_list_agent_actions_passes_limit(services, monkeypatch):
    captured: dict = {}

    def fake_fetchall(sql, params=None):
        captured["params"] = params
        return []

    monkeypatch.setattr(services, "fetchall", fake_fetchall)
    services.list_agent_actions("inc-1", limit=7)
    assert captured["params"] == ("inc-1", 7)


def test_list_incidents_filters_status(services, monkeypatch):
    captured: dict = {}

    def fake_fetchall(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return [{"incident_id": "x", "status": "AWAITING_APPROVAL"}]

    monkeypatch.setattr(services, "fetchall", fake_fetchall)
    rows = services.list_incidents(limit=10, status="AWAITING_APPROVAL")
    assert rows[0]["status"] == "AWAITING_APPROVAL"
    assert captured["params"] == ("AWAITING_APPROVAL", 10)
    assert "status = %s" in captured["sql"]


def test_list_incidents_filters_failure_type(services, monkeypatch):
    captured: dict = {}

    def fake_fetchall(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(services, "fetchall", fake_fetchall)
    services.list_incidents(limit=20, status="ALL", failure_type="null_spike")
    assert captured["params"] == ("null_spike", 20)
    assert "primary_failure_type = %s" in captured["sql"]
    assert "status = %s" not in captured["sql"]


def test_list_incidents_filters_status_and_class(services, monkeypatch):
    captured: dict = {}

    def fake_fetchall(sql, params=None):
        captured["params"] = params
        return []

    monkeypatch.setattr(services, "fetchall", fake_fetchall)
    services.list_incidents(limit=5, status="OPEN", failure_type="job_crash")
    assert captured["params"] == ("OPEN", "job_crash", 5)


def test_remediation_summary_humanizes():
    from services import remediation_summary

    text = remediation_summary("quarantine_reprocess", {"strategy": "drop_null_keys", "column": "email"})
    assert "Quarantine" in text
    assert "drop_null_keys" in text
    assert remediation_summary(None) == "No remediation proposed yet."


def test_list_incidents_all_skips_where(services, monkeypatch):
    captured: dict = {}

    def fake_fetchall(sql, params=None):
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(services, "fetchall", fake_fetchall)
    services.list_incidents(limit=5, status="ALL")
    assert "WHERE" not in captured["sql"]
    assert captured["params"] == (5,)


def test_trigger_remediation_job_posts_notebook_params(services, monkeypatch):
    monkeypatch.setenv("REMEDIATION_JOB_ID", "298394127011671")
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-test")

    posted: dict = {}

    class Resp:
        status_code = 200

        def json(self):
            return {"run_id": 123}

        text = "ok"

    def fake_post(url, headers=None, json=None, timeout=None):  # noqa: A002
        posted["url"] = url
        posted["json"] = json
        posted["headers"] = headers
        return Resp()

    monkeypatch.setattr(services.requests, "post", fake_post)
    out = services.trigger_remediation_job(
        incident_id="inc-1",
        remediation_type="quarantine_reprocess",
        parameters={"strategy": "drop_null_keys"},
    )
    assert out["ok"] is True
    assert out["run_id"] == 123
    assert posted["json"]["job_id"] == 298394127011671
    assert posted["json"]["notebook_params"]["incident_id"] == "inc-1"
    assert posted["json"]["notebook_params"]["remediation_type"] == "quarantine_reprocess"
    params = json.loads(posted["json"]["notebook_params"]["parameters_json"])
    assert params["strategy"] == "drop_null_keys"


def test_do_approve_diagnosis_only_resolves_without_job(services, monkeypatch):
    monkeypatch.setattr(
        services,
        "get_incident",
        lambda iid: {
            "incident_id": iid,
            "status": "AWAITING_APPROVAL",
            "primary_failure_type": "volume_anomaly",
        },
    )
    monkeypatch.setattr(services, "latest_proposal", lambda iid: None)
    monkeypatch.setattr(services, "_insert_approval", lambda *a, **k: 99)
    updated: list[str] = []

    def fake_update(conn, incident_id, to_status, changed_by):
        updated.append(to_status)

    monkeypatch.setattr(services, "_update_status", fake_update)
    dispatched: list = []
    monkeypatch.setattr(
        services,
        "trigger_remediation_job",
        lambda **k: dispatched.append(k) or {"ok": True},
    )

    result = services.do_approve("inc-vol", decided_by="tester")
    assert result["ok"] is True
    assert result["remediation_type"] == "diagnosis_only"
    assert result["status"] == "RESOLVED"
    assert updated == ["RESOLVED"]
    assert dispatched == []


def test_do_approve_dispatches_remediation_job(services, monkeypatch):
    monkeypatch.setattr(
        services,
        "get_incident",
        lambda iid: {
            "incident_id": iid,
            "status": "AWAITING_APPROVAL",
            "primary_failure_type": "null_spike",
        },
    )
    monkeypatch.setattr(
        services,
        "latest_proposal",
        lambda iid: {
            "detail": {
                "remediation_type": "quarantine_reprocess",
                "parameters": {"strategy": "drop_null_keys"},
            }
        },
    )
    monkeypatch.setattr(services, "_insert_approval", lambda *a, **k: 1)
    monkeypatch.setattr(services, "_update_status", lambda *a, **k: None)
    monkeypatch.setattr(
        services,
        "trigger_remediation_job",
        lambda **k: {"ok": True, "run_id": 42, **k},
    )
    result = services.do_approve("inc-null", decided_by="tester")
    assert result["ok"] is True
    assert result["dispatch"]["run_id"] == 42
    assert result["dispatch"]["incident_id"] == "inc-null"
