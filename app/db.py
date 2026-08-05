"""Lakebase connectivity for the Databricks App (secret URL, PG* env, or JWT)."""

from __future__ import annotations

import base64
import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import unquote, urlparse

import pg8000.dbapi
import requests


def _workspace_token_host() -> tuple[str | None, str | None]:
    """Resolve Databricks host + bearer token for REST/JWT credential minting."""
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/") or None
    token = os.environ.get("DATABRICKS_TOKEN") or None
    if host and token:
        return host, token

    # Databricks Apps inject SP OAuth client credentials.
    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    if host and client_id and client_secret:
        try:
            token = _oauth_m2m_token(host, client_id, client_secret)
            if token:
                return host, token
        except Exception:  # noqa: BLE001
            pass

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        cfg = w.config
        h = (cfg.host or "").rstrip("/") or host
        t = cfg.token or token
        if h and t:
            return h, t
        # SDK may use OAuth; ask it for an authorized header/token if available.
        if h and hasattr(cfg, "authenticate"):
            headers = cfg.authenticate()
            auth = (headers or {}).get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                return h, auth.split(" ", 1)[1].strip()
    except Exception:  # noqa: BLE001
        pass

    # Notebook / job contexts sometimes expose dbutils.
    try:
        import builtins

        if hasattr(builtins, "dbutils"):
            ctx = builtins.dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            token = ctx.apiToken().get()
            api_url = None
            try:
                v = ctx.apiUrl()
                api_url = v.get() if hasattr(v, "get") else v
            except Exception:  # noqa: BLE001
                api_url = host
            if api_url and token:
                return str(api_url).rstrip("/"), str(token)
    except Exception:  # noqa: BLE001
        pass

    return host, token


def _oauth_m2m_token(host: str, client_id: str, client_secret: str) -> str:
    """Client-credentials token for Databricks Apps service principal."""
    # Prefer OIDC token endpoint; fall back to legacy login.
    oidc = f"{host.rstrip('/')}/oidc/v1/token"
    resp = requests.post(
        oidc,
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis",
        },
        auth=(client_id, client_secret),
        timeout=30,
    )
    if resp.status_code >= 400:
        legacy = f"{host.rstrip('/')}/oidc/oauth2/v2.0/token"
        resp = requests.post(
            legacy,
            data={
                "grant_type": "client_credentials",
                "scope": "all-apis",
            },
            auth=(client_id, client_secret),
            timeout=30,
        )
    resp.raise_for_status()
    return resp.json()["access_token"]


def _decode_secret_value(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    text = str(raw)
    try:
        return base64.b64decode(text).decode("utf-8")
    except Exception:  # noqa: BLE001
        return text


def _url_from_secret_or_env() -> str | None:
    """Return a postgres:// URL from env (Apps valueFrom) or secret scope."""
    candidates: list[str] = []
    for env_key in ("LAKEBASE_URL", "DATABASE_URL", "lakebase-secret"):
        if os.environ.get(env_key):
            candidates.append(os.environ[env_key])

    # Apps with a secret resource named lakebase-secret may inject it under that key.
    scope = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
    key = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")
    try:
        from databricks.sdk import WorkspaceClient

        secret = WorkspaceClient().secrets.get_secret(scope=scope, key=key)
        candidates.append(_decode_secret_value(secret.value))
    except Exception:  # noqa: BLE001
        pass

    for url in candidates:
        url = (url or "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            return url
    return None


def _kwargs_from_pg_env() -> dict[str, Any] | None:
    """Use Databricks Apps database-resource PG* env vars when present."""
    host = os.environ.get("PGHOST")
    user = os.environ.get("PGUSER")
    database = os.environ.get("PGDATABASE")
    if not (host and user and database):
        return None
    password = os.environ.get("PGPASSWORD") or os.environ.get("LAKEBASE_PASSWORD")
    if not password:
        whost, token = _workspace_token_host()
        instance = os.environ.get("LAKEBASE_INSTANCE", "aiops-lakebase")
        if not whost or not token:
            return None
        password = _jwt_password(whost, token, instance)
    return {
        "host": host,
        "port": int(os.environ.get("PGPORT", "5432")),
        "user": user,
        "password": password,
        "database": database,
        "ssl_context": True,
    }


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
    # 1) Explicit postgres URL (Apps valueFrom / fixed secret / local env)
    url = _url_from_secret_or_env()
    if url:
        u = urlparse(url)
        return {
            "host": u.hostname,
            "port": u.port or 5432,
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "database": (u.path or "/databricks_postgres").lstrip("/") or "databricks_postgres",
            "ssl_context": True,
        }

    # 2) Apps Lakebase database resource (PGHOST/PGUSER/…)
    pg = _kwargs_from_pg_env()
    if pg:
        return pg

    # 3) JWT fallback using workspace host + PAT/SP token
    host, token = _workspace_token_host()
    instance = os.environ.get("LAKEBASE_INSTANCE", "aiops-lakebase")
    password = os.environ.get("LAKEBASE_PASSWORD") or os.environ.get("DATABASE_PASSWORD")
    if not password:
        if not host or not token:
            raise RuntimeError(
                "No Lakebase URL secret and no Databricks auth for JWT. "
                "Ensure app.yaml injects LAKEBASE_URL via valueFrom: lakebase-secret "
                "(postgres://… URL), or add a Lakebase database resource / DATABRICKS_TOKEN."
            )
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
