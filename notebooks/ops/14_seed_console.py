# Databricks notebook source
# MAGIC %md
# MAGIC # Seed console incidents (all 6 failure classes)
# MAGIC
# MAGIC Opens one Lakebase incident per failure class and runs the agent so they land
# MAGIC in **AWAITING_APPROVAL** for `aiops-console`.
# MAGIC
# MAGIC Unlike `ops-phase3-prove`, this does **not** truncate Lakebase at the end.
# MAGIC
# MAGIC Optional: set `approve_diagnosis_only=true` to auto-approve `volume_anomaly`
# MAGIC (diagnosis_only → RESOLVED) so the console has both AWAITING and RESOLVED.

# COMMAND ----------

# MAGIC %pip install pg8000 numpy litellm langgraph langchain-core PyGithub requests faker --quiet
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import sys
from pathlib import Path

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
os.environ["DATABRICKS_TOKEN"] = ctx.apiToken().get()
os.environ["DATABRICKS_HOST"] = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
os.environ.setdefault("LAKEBASE_INSTANCE", "aiops-lakebase")
os.environ.setdefault("LAKEBASE_USER", "sandeeptidke.work@gmail.com")

from src.common.secrets import hydrate_env_from_secret_scope

print("secrets", hydrate_env_from_secret_scope())

from src.agent.runner import run_agent
from src.chaos.reset_demo import reset_demo
from src.common.constants import FAILURE_TYPES
from src.common.postgres import postgres_connection
from src.detection.incidents import PostgresIncidentStore
from src.detection.prove import prove_all_failure_classes
from src.remediation.approvals import approve_incident
from src.remediation.sync_analytics import sync_lakebase_mirrors

# COMMAND ----------

dbutils.widgets.dropdown("clear_existing", "false", ["true", "false"])
dbutils.widgets.dropdown("run_agent", "true", ["true", "false"])
dbutils.widgets.dropdown("approve_diagnosis_only", "true", ["true", "false"])
dbutils.widgets.text("checks_path", f"{ROOT}/config/dq_checks.yml")

clear_existing = dbutils.widgets.get("clear_existing").lower() in ("1", "true", "yes")
do_agent = dbutils.widgets.get("run_agent").lower() in ("1", "true", "yes")
approve_diag = dbutils.widgets.get("approve_diagnosis_only").lower() in ("1", "true", "yes")
checks_path = dbutils.widgets.get("checks_path")

reports_dir = f"{ROOT}/docs/metrics/rca"
Path(reports_dir).mkdir(parents=True, exist_ok=True)

# COMMAND ----------

with postgres_connection() as conn:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_incident_signal_dedup
            ON incident_signals (incident_id, failure_type, detected_by)
            """
        )
    finally:
        cur.close()
    conn.commit()

    if clear_existing:
        print("reset", reset_demo(spark, conn, seed=42, reset_lakebase_state=True))

    store = PostgresIncidentStore(conn, notify=True, changed_by="console_seed")
    prove = prove_all_failure_classes(
        spark,
        store,
        checks_path=checks_path,
        conn=conn,
        do_initial_reset=True,
    )
    assert prove.get("exit_criteria_met"), prove

    # Collect OPEN incidents created for the prove-* job_run_ids
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT incident_id::text, job_run_id, primary_failure_type, status
            FROM incidents
            WHERE job_run_id LIKE 'prove-%'
            ORDER BY primary_failure_type
            """
        )
        opened = [
            {
                "incident_id": r[0],
                "job_run_id": r[1],
                "primary_failure_type": r[2],
                "status": r[3],
            }
            for r in cur.fetchall()
        ]
    finally:
        cur.close()

    agent_results = []
    if do_agent:
        for row in opened:
            result = run_agent(
                spark,
                conn,
                incident_id=row["incident_id"],
                reports_dir=reports_dir,
            )
            agent_results.append(
                {
                    "incident_id": row["incident_id"],
                    "failure_type": row["primary_failure_type"],
                    "ok": bool(result.get("ok")),
                    "status": result.get("status"),
                }
            )

    approved = []
    if approve_diag:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT incident_id::text, primary_failure_type
                FROM incidents
                WHERE status = 'AWAITING_APPROVAL'
                  AND primary_failure_type = 'volume_anomaly'
                ORDER BY detected_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        finally:
            cur.close()
        if row:
            approved.append(
                approve_incident(
                    conn,
                    row[0],
                    decided_by="console_seed",
                    notes="seed: diagnosis_only → RESOLVED for filter diversity",
                    trigger_job=False,
                )
            )

summary = {
    "failure_types": list(FAILURE_TYPES),
    "prove_exit_criteria_met": prove.get("exit_criteria_met"),
    "opened": opened,
    "agent_results": agent_results,
    "approved": approved,
}
print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

mirrors = sync_lakebase_mirrors(spark)
print(json.dumps(mirrors, indent=2, default=str))
assert mirrors.get("ok"), mirrors
assert len(opened) >= len(FAILURE_TYPES), summary
