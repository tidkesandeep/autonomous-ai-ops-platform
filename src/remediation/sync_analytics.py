"""Refresh Lakebase → ops Delta analytics mirrors via CTAS (Path A)."""

from __future__ import annotations

from typing import Any

from src.common.constants import OPS_GOLD

# UC online catalog exposing Lakebase public tables (read-only).
LAKEBASE_UC = "lakebase_app.public"

MIRRORS = (
    ("incidents", "incidents_delta"),
    ("incident_signals", "incident_signals_delta"),
    ("incident_status_events", "incident_status_events_delta"),
    ("approvals", "approvals_delta"),
    ("agent_actions", "agent_actions_delta"),
    ("audit_log", "audit_log_delta"),
)


def sync_lakebase_mirrors(spark: Any, *, source_schema: str = LAKEBASE_UC) -> dict[str, Any]:
    """CREATE OR REPLACE each ops.gold.*_delta mirror from lakebase_app.public."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    results: dict[str, Any] = {}
    for src, dest in MIRRORS:
        dest_fqn = f"{OPS_GOLD}.{dest}"
        src_fqn = f"{source_schema}.{src}"
        try:
            spark.sql(f"CREATE OR REPLACE TABLE {dest_fqn} AS SELECT * FROM {src_fqn}")
            n = spark.table(dest_fqn).count()
            results[dest] = {"ok": True, "rows": n, "source": src_fqn}
        except Exception as exc:  # noqa: BLE001
            results[dest] = {"ok": False, "error": str(exc), "source": src_fqn}
    return {"ok": all(v.get("ok") for v in results.values()), "mirrors": results}
