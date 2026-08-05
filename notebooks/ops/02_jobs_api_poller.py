# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs API Crash Poller
# MAGIC
# MAGIC Phase 2 poller: ingest failed/internal-error task outputs into ops.bronze.raw_task_logs.

# COMMAND ----------

# MAGIC %pip install requests --quiet
# MAGIC
# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.ops.poller import DatabricksJobsApiClient, ingest_recent_failed_task_runs

# COMMAND ----------

dbutils.widgets.text("lookback_hours", "24")
lookback_hours = int(dbutils.widgets.get("lookback_hours"))

# Ensure target exists even when no failures are found in this window.
spark.sql("CREATE SCHEMA IF NOT EXISTS ops.bronze")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS ops.bronze.raw_task_logs (
      run_id STRING,
      task_run_id STRING,
      job_id STRING,
      pipeline_key STRING,
      task_key STRING,
      lifecycle_state STRING,
      result_state STRING,
      error_signature STRING,
      raw_output STRING,
      collected_at TIMESTAMP
    )
    USING DELTA
    """
)

client = DatabricksJobsApiClient.from_databricks_notebook(spark, dbutils)
ingested = ingest_recent_failed_task_runs(spark, client, lookback_hours=lookback_hours)
print(f"raw task logs ingested: {ingested}")

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT run_id, task_key, result_state, error_signature, collected_at
        FROM ops.bronze.raw_task_logs
        ORDER BY collected_at DESC
        LIMIT 50
        """
    )
)
