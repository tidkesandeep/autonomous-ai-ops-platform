# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Reset
# MAGIC
# MAGIC Phase 3: regenerate demo medallion tables, drop ops telemetry, truncate Lakebase.

# COMMAND ----------

# MAGIC %pip install faker pg8000 requests --quiet
# MAGIC %restart_python
# MAGIC
# COMMAND ----------

dbutils.library.restartPython()

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

# COMMAND ----------

dbutils.widgets.text("seed", "42")
dbutils.widgets.dropdown("reset_lakebase", "true", ["true", "false"])

seed = int(dbutils.widgets.get("seed"))
do_lakebase = dbutils.widgets.get("reset_lakebase") == "true"

if do_lakebase:
    with postgres_connection() as conn:
        summary = reset_demo(spark, conn, seed=seed, reset_lakebase_state=True)
else:
    summary = reset_demo(spark, None, seed=seed, reset_lakebase_state=False)

print(json.dumps(summary, indent=2, default=str))
