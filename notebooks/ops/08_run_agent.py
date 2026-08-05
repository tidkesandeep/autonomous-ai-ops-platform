# Databricks notebook source
# MAGIC %md
# MAGIC # Run Investigation Agent
# MAGIC
# MAGIC Phase 4: investigate one Lakebase incident (by incident_id or job_run_id).

# COMMAND ----------

# MAGIC %pip install pg8000 numpy litellm langgraph langchain-core PyGithub requests --quiet
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

from src.agent.runner import run_agent
from src.common.postgres import postgres_connection

# COMMAND ----------

dbutils.widgets.text("incident_id", "")
dbutils.widgets.text("job_run_id", "")
dbutils.widgets.text("reports_dir", f"{ROOT}/docs/metrics/rca")

incident_id = dbutils.widgets.get("incident_id") or None
job_run_id = dbutils.widgets.get("job_run_id") or None
reports_dir = dbutils.widgets.get("reports_dir")

with postgres_connection() as conn:
    result = run_agent(
        spark,
        conn,
        incident_id=incident_id,
        job_run_id=job_run_id,
        reports_dir=reports_dir,
    )

print(json.dumps(result, indent=2, default=str)[:5000])
