"""
# Incident Detection Engine

Phase 3: evaluate ops telemetry / DQ / crash logs → open Lakebase incidents
(one per job_run_id) + append incident_signals + Slack notify (if configured).
"""

# Databricks notebook source
# MAGIC %pip install psycopg[binary] PyYAML requests --quiet
# MAGIC
# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

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

# Mirror Lakebase app-state into ops Delta for analytics (Path A CTAS bridge)
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
