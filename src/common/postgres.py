"""Postgres / Lakebase connection helpers.

Uses ``pg8000`` (pure Python) so Databricks serverless notebooks do not need
native ``psycopg`` wheels that frequently crash the kernel on install.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pg8000.dbapi
import requests


def lakebase_password_from_cli(instance_name: str = "aiops-lakebase") -> str:
    """Generate a short-lived Lakebase JWT via Databricks CLI."""
    proc = subprocess.run(
        [
            "databricks",
            "database",
            "generate-database-credential",
            "--json",
            f'{{"instance_names":["{instance_name}"]}}',
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(proc.stdout)["token"]


def lakebase_password_from_api(
    host: str,
    token: str,
    instance_name: str = "aiops-lakebase",
    timeout_s: int = 30,
) -> str:
    """Generate a Lakebase JWT via Workspace REST API (works inside Jobs notebooks)."""
    url = f"{host.rstrip('/')}/api/2.0/database/credentials"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"instance_names": [instance_name], "request_id": str(uuid.uuid4())},
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return resp.json()["token"]


def resolve_lakebase_password(instance_name: str = "aiops-lakebase") -> str:
    """Prefer explicit env password, then REST API, then CLI."""
    if password := os.environ.get("LAKEBASE_PASSWORD") or os.environ.get("DATABASE_PASSWORD"):
        return password

    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if host and token:
        try:
            return lakebase_password_from_api(host, token, instance_name=instance_name)
        except Exception:
            pass

    return lakebase_password_from_cli(instance_name)


def connect_kwargs_from_env() -> dict[str, Any]:
    """Build pg8000 connect kwargs from env / Databricks defaults."""
    host = os.environ.get(
        "LAKEBASE_HOST",
        "ep-snowy-violet-d8t4xovo.database.us-east-2.cloud.databricks.com",
    )
    user = os.environ.get("LAKEBASE_USER") or os.environ.get("DATABRICKS_USER") or "sandeeptidke.work@gmail.com"
    dbname = os.environ.get("LAKEBASE_DB", "databricks_postgres")
    instance = os.environ.get("LAKEBASE_INSTANCE", "aiops-lakebase")
    password = resolve_lakebase_password(instance)
    return {
        "host": host,
        "port": int(os.environ.get("LAKEBASE_PORT", "5432")),
        "user": user,
        "database": dbname,
        "password": password,
        "ssl_context": True,
    }


@contextmanager
def postgres_connection(**overrides: Any) -> Iterator[Any]:
    kwargs = connect_kwargs_from_env()
    kwargs.update(overrides)
    # DATABASE_URL is not used with pg8000 kwargs; prefer discrete vars.
    conn = pg8000.dbapi.connect(**kwargs)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
