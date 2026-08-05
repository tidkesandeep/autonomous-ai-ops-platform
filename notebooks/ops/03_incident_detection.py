# Databricks notebook source
# MAGIC %md
# MAGIC # Incident Detection Engine
# MAGIC
# MAGIC Phase 3: evaluate ops telemetry / DQ / crash logs → open Lakebase incidents
# MAGIC (one per job_run_id) + append incident_signals + Slack notify (if configured).

# COMMAND ----------

# MAGIC %pip install pg8000 requests --quiet
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

from src.common.postgres import postgres_connection
from src.detection.engine import run_detection
from src.detection.incidents import PostgresIncidentStore

# COMMAND ----------

dbutils.widgets.text("lookback_hours", "24")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

with postgres_connection() as conn:
    store = PostgresIncidentStore(conn, notify=True, changed_by="detection_job")
    summary = run_detection(spark, store, lookback_hours=lookback_hours)

print(json.dumps(summary, indent=2, default=str))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS ops.gold")
for src, dst in [
    ("lakebase_app.public.incidents", "ops.gold.incidents_delta"),
    ("lakebase_app.public.incident_signals", "ops.gold.incident_signals_delta"),
    ("lakebase_app.public.incident_status_events", "ops.gold.incident_status_events_delta"),
]:
    try:
        spark.sql(f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}")
        print(f"synced {src} -> {dst}:", spark.table(dst).count())
    except Exception as exc:
        print(f"sync skipped for {src}: {exc}")

# COMMAND ----------

display(spark.sql("SELECT * FROM ops.gold.incidents_delta ORDER BY detected_at DESC LIMIT 50"))
