"""Slack Incoming Webhook notifications for incident lifecycle events."""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib import error, request

logger = logging.getLogger(__name__)


def notify_incident_opened(
    *,
    incident_id: str,
    job_run_id: str,
    pipeline_key: str,
    primary_failure_type: str | None,
    severity: str,
    webhook_url: str | None = None,
) -> bool:
    """Post a Slack message when a new incident is opened.

    Returns True if a webhook was called successfully.
    When ``SLACK_WEBHOOK_URL`` is unset, logs and returns False (no-op).
    """
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    text = (
        f":rotating_light: Incident opened `{incident_id}`\n"
        f"• pipeline: `{pipeline_key}`\n"
        f"• run: `{job_run_id}`\n"
        f"• primary: `{primary_failure_type or 'unknown'}`\n"
        f"• severity: `{severity}`"
    )
    if not url:
        logger.info("SLACK_WEBHOOK_URL unset; skipping notify: %s", text.replace("\n", " | "))
        return False

    payload = json.dumps({"text": text}).encode("utf-8")
    req = request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=15) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except error.URLError as exc:
        logger.warning("Slack webhook failed: %s", exc)
        return False


def notify_raw(text: str, webhook_url: str | None = None, **_: Any) -> bool:
    """Generic Slack text post (used by tests / agent later)."""
    url = webhook_url or os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        logger.info("SLACK_WEBHOOK_URL unset; skipping: %s", text)
        return False
    payload = json.dumps({"text": text}).encode("utf-8")
    req = request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=15) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except error.URLError as exc:
        logger.warning("Slack webhook failed: %s", exc)
        return False
