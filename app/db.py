"""Lakebase connectivity for the Databricks App (secret URL or JWT fallback)."""

from __future__ import annotations

import base64
import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pg8000.dbapi
import requests


def _workspace_token_host() -> tuple[str | None, str | None]:
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if host and token:
        return host.rstrip("/"), token
    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        cfg = w.config
        return (cfg.host or "").rstrip("/") or None, cfg.token
    except Exception:  # noqa: BLE001
        return None, None


def _url_from_secret() -> str | None:
    """Return a postgres:// URL from secret/env, or None to use JWT fallback.

    Ignores non-postgres values (e.g. a workspace https URL mistakenly stored as
    ``database/lakebase-url``).
    """
    scope = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
    key = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")
    candidates: list[str] = []
    try:
        from databricks.sdk import WorkspaceClient

        secret = WorkspaceClient().secrets.get_secret(scope=scope, key=key)
        raw = secret.value
        if isinstance(raw, bytes):
            candidates.append(raw.decode("utf-8"))
        else:
            try:
                candidates.append(base64.b64decode(raw).decode("utf-8"))
            except Exception:  # noqa: BLE001
                candidates.append(str(raw))
    except Exception:  # noqa: BLE001
        pass
    for env_key in ("LAKEBASE_URL", "DATABASE_URL"):
        if os.environ.get(env_key):
            candidates.append(os.environ[env_key])
    for url in candidates:
        if url.startswith(("postgres://", "postgresql://")):
            return url
    return None


def _jwt_password(host: str, token: str, instance: str) -> str:
    resp = requests.post(
        f"{host}/api/2.0/database/credentials",
        headers={"Authorization": f"Bearer {token}"},
        json={"instance_names": [instance], "request_id": str(uuid.uuid4())},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def connect_kwargs() -> dict[str, Any]:
    url = _url_from_secret()
    if url and url.startswith("postgres"):
        # postgresql://user:pass@host:5432/db?sslmode=require
        from urllib.parse import unquote, urlparse

        u = urlparse(url)
        return {
            "host": u.hostname,
            "port": u.port or 5432,
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "database": (u.path or "/databricks_postgres").lstrip("/") or "databricks_postgres",
            "ssl_context": True,
        }

    host, token = _workspace_token_host()
    instance = os.environ.get("LAKEBASE_INSTANCE", "aiops-lakebase")
    password = os.environ.get("LAKEBASE_PASSWORD") or os.environ.get("DATABASE_PASSWORD")
    if not password:
        if not host or not token:
            raise RuntimeError("No Lakebase URL secret and no Databricks auth for JWT")
        password = _jwt_password(host, token, instance)
    return {
        "host": os.environ.get(
            "LAKEBASE_HOST",
            "ep-snowy-violet-d8t4xovo.database.us-east-2.cloud.databricks.com",
        ),
        "port": int(os.environ.get("LAKEBASE_PORT", "5432")),
        "user": os.environ.get("LAKEBASE_USER")
        or os.environ.get("DATABRICKS_USER")
        or "sandeeptidke.work@gmail.com",
        "database": os.environ.get("LAKEBASE_DB", "databricks_postgres"),
        "password": password,
        "ssl_context": True,
    }


@contextmanager
def postgres_connection() -> Iterator[Any]:
    conn = pg8000.dbapi.connect(**connect_kwargs())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def fetchall(sql: str, params: tuple | list | None = None) -> list[dict[str, Any]]:
    with postgres_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
        finally:
            cur.close()


def sql_warehouse_query(sql: str, warehouse_id: str | None = None) -> list[list[Any]]:
    """Run a SQL warehouse statement (for ops.gold Delta analytics)."""
    host, token = _workspace_token_host()
    if not host or not token:
        raise RuntimeError("Databricks auth required for SQL warehouse")
    wh = warehouse_id or os.environ.get("SQL_WAREHOUSE_ID", "4a3ce36aae2d0b64")
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}"},
        json={"warehouse_id": wh, "statement": sql, "wait_timeout": "50s"},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(json.dumps(body.get("status"), default=str))
    return (body.get("result") or {}).get("data_array") or []
