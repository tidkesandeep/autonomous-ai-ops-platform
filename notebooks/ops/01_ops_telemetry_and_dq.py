"""
# Ops Telemetry + DQ

Phase 2 observability:
- task telemetry decorator
- YAML-driven DQ checks
"""

# Databricks notebook source
# MAGIC %pip install pyyaml requests --quiet
# MAGIC
# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import uuid

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.ops.dq import run_dq_checks
from src.ops.telemetry import SparkTelemetrySink, with_telemetry

# COMMAND ----------

dbutils.widgets.text("pipeline_key", "orders_ingest")
dbutils.widgets.text("run_id", "")

pipeline_key = dbutils.widgets.get("pipeline_key")
run_id = dbutils.widgets.get("run_id") or f"ops-telemetry-{uuid.uuid4().hex[:10]}"

sink = SparkTelemetrySink(spark)


@with_telemetry(sink=sink, pipeline_key=pipeline_key, task_name="telemetry_probe")
def telemetry_probe():
    n = spark.sql("SELECT COUNT(*) AS n FROM demo.gold.fact_orders").collect()[0]["n"]
    return {"rows_out": int(n), "rows_in": int(n)}


print("run_id:", run_id)
print("telemetry probe rows:", telemetry_probe())

# COMMAND ----------

dq_summary = run_dq_checks(
    spark,
    checks_path=f"{ROOT}/config/dq_checks.yml",
    run_id=run_id,
    pipeline_key=pipeline_key,
)
print(dq_summary)

# COMMAND ----------

display(spark.sql("SELECT * FROM ops.bronze.task_telemetry ORDER BY ended_at DESC LIMIT 20"))
display(spark.sql("SELECT * FROM ops.gold.fact_dq_check ORDER BY checked_at DESC LIMIT 20"))
