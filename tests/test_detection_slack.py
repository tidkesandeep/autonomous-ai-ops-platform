"""Tests for Slack notify no-op and scoring helpers."""

from __future__ import annotations

import os

from src.detection.slack import notify_incident_opened


def test_slack_noop_without_webhook(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    ok = notify_incident_opened(
        incident_id="inc-1",
        job_run_id="run-1",
        pipeline_key="orders_ingest",
        primary_failure_type="null_spike",
        severity="medium",
    )
    assert ok is False


def test_slack_posts_when_url_set(monkeypatch):
    calls: list[object] = []

    class FakeResp:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(req, timeout=15):  # noqa: ARG001
        calls.append(req)
        return FakeResp()

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setattr("src.detection.slack.request.urlopen", fake_urlopen)
    ok = notify_incident_opened(
        incident_id="inc-2",
        job_run_id="run-2",
        pipeline_key="orders_ingest",
        primary_failure_type="job_crash",
        severity="high",
    )
    assert ok is True
    assert len(calls) == 1
    assert os.environ["SLACK_WEBHOOK_URL"].startswith("https://")
