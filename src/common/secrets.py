"""Load operator secrets from Databricks secret scope ``aiops`` into ``os.environ``.

Jobs/notebooks call ``hydrate_env_from_secret_scope()`` early so Slack, GitHub,
and LLM clients see standard env vars without hard-coding values.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_SCOPE = "aiops"

# secret key → environment variable
SECRET_ENV_MAP: dict[str, str] = {
    "slack-webhook-url": "SLACK_WEBHOOK_URL",
    "github-token": "GITHUB_TOKEN",
    "github-repo": "GITHUB_REPO",
    "gemini-api-key": "GEMINI_API_KEY",
    "groq-api-key": "GROQ_API_KEY",
    "embedding-model": "EMBEDDING_MODEL",
}


def _read_secret(scope: str, key: str) -> str | None:
    """Prefer dbutils in notebooks; fall back to Secrets API via SDK/CLI env."""
    # 1) Databricks notebook dbutils
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            dbutils = DBUtils(spark)
            return dbutils.secrets.get(scope=scope, key=key)
    except Exception:  # noqa: BLE001
        pass

    # 2) WorkspaceClient (Apps / local with auth)
    try:
        from databricks.sdk import WorkspaceClient

        raw = WorkspaceClient().secrets.get_secret(scope=scope, key=key).value
        if raw is None:
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8")
        # SDK may return base64
        import base64

        try:
            return base64.b64decode(raw).decode("utf-8")
        except Exception:  # noqa: BLE001
            return str(raw)
    except Exception:  # noqa: BLE001
        pass

    # 3) REST via host/token already in env
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN")
    if host and token:
        try:
            import requests

            resp = requests.get(
                f"{host}/api/2.0/secrets/get",
                headers={"Authorization": f"Bearer {token}"},
                params={"scope": scope, "key": key},
                timeout=30,
            )
            if resp.status_code == 200:
                import base64

                return base64.b64decode(resp.json()["value"]).decode("utf-8")
        except Exception:  # noqa: BLE001
            pass
    return None


def hydrate_env_from_secret_scope(
    scope: str = DEFAULT_SCOPE,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Copy known secrets into ``os.environ`` when missing (or always if overwrite).

    Returns a status dict (never includes secret values).
    """
    loaded: list[str] = []
    missing: list[str] = []
    skipped: list[str] = []
    for secret_key, env_key in SECRET_ENV_MAP.items():
        if os.environ.get(env_key) and not overwrite:
            skipped.append(env_key)
            continue
        value = _read_secret(scope, secret_key)
        if value:
            os.environ[env_key] = value
            loaded.append(env_key)
        else:
            missing.append(secret_key)
    # Sensible default for repo if only token was set
    os.environ.setdefault("GITHUB_REPO", "tidkesandeep/autonomous-ai-ops-platform")
    status = {
        "scope": scope,
        "loaded": loaded,
        "missing_secret_keys": missing,
        "skipped_already_set": skipped,
    }
    logger.info("secrets hydrate: %s", status)
    return status
