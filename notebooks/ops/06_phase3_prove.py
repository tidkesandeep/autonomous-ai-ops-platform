"""
# Phase 3 Prove — Week 3 exit criteria

For each of the 6 failure classes:
  reset demo → inject → seed ops evidence → detect → open Lakebase incident

Then score primary_failure_type match and mirror Lakebase → Delta.
Finally verify reset_demo cleans the environment.
"""

# Databricks notebook source
# MAGIC %pip install faker pg8000 requests PyYAML --quiet
# MAGIC %restart_python

# COMMAND ----------

import json
import os
import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
os.environ["DATABRICKS_TOKEN"] = ctx.apiToken().get()
os.environ["DATABRICKS_HOST"] = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
os.environ.setdefault("LAKEBASE_INSTANCE", "aiops-lakebase")
os.environ.setdefault("LAKEBASE_USER", "sandeeptidke.work@gmail.com")

from src.chaos.reset_demo import reset_demo
from src.common.postgres import postgres_connection
from src.detection.incidents import PostgresIncidentStore
from src.detection.prove import prove_all_failure_classes

# COMMAND ----------

dbutils.widgets.text("checks_path", f"{ROOT}/config/dq_checks.yml")
checks_path = dbutils.widgets.get("checks_path")

with postgres_connection() as conn:
    # Ensure signal dedup index exists
    cur = conn.cursor()
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_incident_signal_dedup
        ON incident_signals (incident_id, failure_type, detected_by)
        """
    )
    cur.close()
    conn.commit()

    store = PostgresIncidentStore(conn, notify=True, changed_by="phase3_prove")
    report = prove_all_failure_classes(
        spark,
        store,
        checks_path=checks_path,
        conn=conn,
        do_initial_reset=True,
    )

print(json.dumps({k: report[k] for k in ("ran_at", "matches", "primary_scorecard", "signal_scorecard", "exit_criteria_met")}, indent=2))
assert report["exit_criteria_met"], f"Phase 3 exit criteria not met: {report['matches']}"

# COMMAND ----------

# Mirror to Delta analytics tables (before reset — exit criteria requires visibility here)
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.gold")
for src, dst in [
    ("lakebase_app.public.incidents", "ops.gold.incidents_delta"),
    ("lakebase_app.public.incident_signals", "ops.gold.incident_signals_delta"),
    ("lakebase_app.public.incident_status_events", "ops.gold.incident_status_events_delta"),
]:
    spark.sql(f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}")
    print(dst, spark.table(dst).count())

prove_delta = spark.sql(
    """
    SELECT job_run_id, pipeline_key, primary_failure_type, status
    FROM ops.gold.incidents_delta
    WHERE job_run_id LIKE 'prove-%'
    ORDER BY job_run_id
    """
).collect()
print("prove_delta_rows", [r.asDict() for r in prove_delta])
assert len(prove_delta) == 6, prove_delta
for r in prove_delta:
    expected = r["job_run_id"].replace("prove-", "", 1)
    assert r["primary_failure_type"] == expected, r
    assert r["status"] == "OPEN", r
print("DELTA_VISIBILITY_OK")

# Persist scorecard into the workspace project tree for git pickup
score_path = f"{ROOT}/docs/metrics/phase3_scorecard.json"
with open(score_path, "w", encoding="utf-8") as fh:
    json.dump(report, fh, indent=2, default=str)
print("wrote", score_path)

# COMMAND ----------

# Verify reset_demo returns a clean environment
with postgres_connection() as conn:
    reset_summary = reset_demo(spark, conn, seed=42, reset_lakebase_state=True)

cur_check = {
    "demo_orders": spark.table("demo.gold.fact_orders").count(),
    "ops_telemetry_exists": spark.catalog.tableExists("ops.bronze.task_telemetry"),
}
with postgres_connection() as conn:
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM incidents")
    cur_check["lakebase_incidents"] = c.fetchone()[0]
    c.close()

print(json.dumps({"reset_keys": list(reset_summary.keys()), "after_reset": cur_check}, indent=2))
assert cur_check["lakebase_incidents"] == 0, "reset_demo did not truncate incidents"
assert cur_check["demo_orders"] > 0, "reset_demo did not regenerate demo gold"
print("RESET_DEMO_OK")
print("PHASE3_EXIT_CRITERIA_MET")
