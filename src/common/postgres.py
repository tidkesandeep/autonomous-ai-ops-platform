"""Postgres / Lakebase connection helpers."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg


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
    import json

    return json.loads(proc.stdout)["token"]


def connect_kwargs_from_env() -> dict[str, Any]:
    """Build psycopg connect kwargs from env / Databricks defaults."""
    if url := os.environ.get("DATABASE_URL"):
        return {"conninfo": url}

    host = os.environ.get(
        "LAKEBASE_HOST",
        "ep-snowy-violet-d8t4xovo.database.us-east-2.cloud.databricks.com",
    )
    user = os.environ.get("LAKEBASE_USER") or os.environ.get("DATABRICKS_USER") or "sandeeptidke.work@gmail.com"
    dbname = os.environ.get("LAKEBASE_DB", "databricks_postgres")
    password = os.environ.get("LAKEBASE_PASSWORD") or os.environ.get("DATABASE_PASSWORD")
    if not password:
        instance = os.environ.get("LAKEBASE_INSTANCE", "aiops-lakebase")
        password = lakebase_password_from_cli(instance)
    return {
        "host": host,
        "port": int(os.environ.get("LAKEBASE_PORT", "5432")),
        "user": user,
        "dbname": dbname,
        "password": password,
        "sslmode": os.environ.get("LAKEBASE_SSLMODE", "require"),
    }


@contextmanager
def postgres_connection(**overrides: Any) -> Iterator[psycopg.Connection]:
    kwargs = connect_kwargs_from_env()
    kwargs.update(overrides)
    conn = psycopg.connect(**kwargs)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
