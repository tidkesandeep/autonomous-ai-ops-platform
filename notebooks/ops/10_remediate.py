# Databricks notebook source
# MAGIC %md
# MAGIC # Remediation job (Phase 5)
# MAGIC
# MAGIC Runs an approved remediation for one incident, then marks it RESOLVED.

# COMMAND ----------

# MAGIC %pip install pg8000 numpy requests --quiet
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
from src.remediation.execute import execute_remediation

# COMMAND ----------

dbutils.widgets.text("incident_id", "")
dbutils.widgets.text("remediation_type", "")
dbutils.widgets.text("parameters_json", "{}")

incident_id = dbutils.widgets.get("incident_id").strip()
remediation_type = dbutils.widgets.get("remediation_type").strip() or None
try:
    parameters = json.loads(dbutils.widgets.get("parameters_json") or "{}")
except json.JSONDecodeError:
    parameters = {}

assert incident_id, "incident_id widget required"

with postgres_connection() as conn:
    result = execute_remediation(
        spark,
        conn,
        incident_id=incident_id,
        remediation_type=remediation_type,
        parameters=parameters,
        resolve=True,
    )

print(json.dumps(result, indent=2, default=str))
assert result.get("ok"), result
