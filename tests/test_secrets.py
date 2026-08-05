"""Unit tests for secret hydration (no real secrets required)."""

from __future__ import annotations

import src.common.secrets as secrets


def test_hydrate_skips_when_env_set(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setattr(secrets, "_read_secret", lambda scope, key: None)
    status = secrets.hydrate_env_from_secret_scope(overwrite=False)
    assert "SLACK_WEBHOOK_URL" in status["skipped_already_set"]
    assert "slack-webhook-url" not in status["missing_secret_keys"]
    assert "github-token" in status["missing_secret_keys"]


def test_hydrate_loads_mapped_secrets(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)

    def fake_read(scope, key):
        return {
            "slack-webhook-url": "https://hooks.slack.com/services/T/B/X",
            "github-token": "ghp_test",
            "gemini-api-key": "gem_test",
        }.get(key)

    monkeypatch.setattr(secrets, "_read_secret", fake_read)
    status = secrets.hydrate_env_from_secret_scope(overwrite=True)
    assert "SLACK_WEBHOOK_URL" in status["loaded"]
    assert "GITHUB_TOKEN" in status["loaded"]
    assert "GEMINI_API_KEY" in status["loaded"]
