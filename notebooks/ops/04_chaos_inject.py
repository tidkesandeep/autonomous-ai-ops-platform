"""
# Chaos Failure Injection

Phase 3: inject one of the six failure classes into demo tables and record
ground truth in ops.bronze.injected_failures.
"""

# Databricks notebook source
# MAGIC %pip install faker --quiet
# MAGIC
# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.chaos.injector import inject

# COMMAND ----------

dbutils.widgets.dropdown(
    "failure_type",
    "null_spike",
    ["null_spike", "volume_anomaly", "duplicate_explosion", "schema_drift", "late_data", "job_crash"],
)
dbutils.widgets.text("job_run_id", "")

failure_type = dbutils.widgets.get("failure_type")
job_run_id = dbutils.widgets.get("job_run_id") or None

result = inject(spark, failure_type, job_run_id=job_run_id)
print(json.dumps(result.__dict__, indent=2, default=str))

# COMMAND ----------

display(
    spark.sql(
        """
        SELECT * FROM ops.bronze.injected_failures
        ORDER BY injected_at DESC
        LIMIT 20
        """
    )
)
