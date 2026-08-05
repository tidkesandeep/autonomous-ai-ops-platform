"""Reset demo + ops telemetry + Lakebase app state to a clean baseline."""

from __future__ import annotations

import logging
from typing import Any

from src.common.constants import DEMO_BRONZE, DEMO_GOLD, DEMO_SILVER, OPS_BRONZE, OPS_GOLD
from src.demo.pipelines import TABLE_FORMAT, run_medallion

logger = logging.getLogger(__name__)

OPS_TABLES = (
    f"{OPS_BRONZE}.task_telemetry",
    f"{OPS_BRONZE}.raw_task_logs",
    f"{OPS_BRONZE}.injected_failures",
    f"{OPS_GOLD}.fact_dq_check",
    f"{OPS_GOLD}.incidents_delta",
    f"{OPS_GOLD}.incident_signals_delta",
    f"{OPS_GOLD}.incident_status_events_delta",
)

LAKEBASE_TRUNCATE_SQL = """
TRUNCATE TABLE incident_signals;
TRUNCATE TABLE incident_status_events;
TRUNCATE TABLE approvals;
TRUNCATE TABLE agent_actions;
TRUNCATE TABLE audit_log;
TRUNCATE TABLE incidents;
"""


def _drop_if_exists(spark: Any, table: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def reset_ops_delta(spark: Any) -> list[str]:
    dropped: list[str] = []
    for table in OPS_TABLES:
        try:
            _drop_if_exists(spark, table)
            dropped.append(table)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not drop %s: %s", table, exc)
    return dropped


def reset_demo_tables(spark: Any, *, seed: int = 42) -> dict[str, Any]:
    """Regenerate demo bronze→silver→gold from the seeded generator."""
    return run_medallion(spark, seed=seed)


def reset_lakebase(conn: Any) -> None:
    """Truncate all app-state tables in one statement (FK-safe)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            TRUNCATE TABLE
              incident_signals,
              incident_status_events,
              approvals,
              agent_actions,
              audit_log,
              incidents
            RESTART IDENTITY CASCADE
            """
        )
    finally:
        cur.close()
    conn.commit()


def reset_demo(
    spark: Any,
    conn: Any | None = None,
    *,
    seed: int = 42,
    reset_lakebase_state: bool = True,
) -> dict[str, Any]:
    """One-shot demo reset: demo data + ops Delta + optional Lakebase truncate."""
    demo_counts = reset_demo_tables(spark, seed=seed)
    dropped = reset_ops_delta(spark)
    lakebase_reset = False
    if reset_lakebase_state and conn is not None:
        reset_lakebase(conn)
        lakebase_reset = True
    return {
        "demo_counts": demo_counts,
        "ops_tables_dropped": dropped,
        "lakebase_reset": lakebase_reset,
        "table_format": TABLE_FORMAT,
        "schemas": [DEMO_BRONZE, DEMO_SILVER, DEMO_GOLD],
    }
