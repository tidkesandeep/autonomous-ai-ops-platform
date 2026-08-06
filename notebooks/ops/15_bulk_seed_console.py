# Databricks notebook source
# MAGIC %md
# MAGIC # Bulk seed console (~100 incidents)
# MAGIC
# MAGIC Creates unique synthetic incidents until ``target_total``, then shapes statuses:
# MAGIC - ``resolve_n`` → RESOLVED (absolute target)
# MAGIC - ``reject_n`` → INVESTIGATING (absolute target)
# MAGIC - remainder stay AWAITING_APPROVAL
# MAGIC
# MAGIC Default ``mode=fast`` writes RCA + proposals in Lakebase without LLM calls.
# MAGIC Unlike ``ops-phase3-prove``, this does **not** wipe Lakebase.

# COMMAND ----------

# MAGIC %pip install pg8000 numpy litellm langgraph langchain-core PyGithub requests --quiet
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

from src.agent.bulk_seed import bulk_seed_console
from src.common.postgres import postgres_connection
from src.remediation.sync_analytics import sync_lakebase_mirrors

# COMMAND ----------

dbutils.widgets.text("target_total", "100")
dbutils.widgets.text("resolve_n", "18")
dbutils.widgets.text("reject_n", "8")
dbutils.widgets.dropdown("mode", "fast", ["fast", "agent", "open"])
dbutils.widgets.dropdown("write_evidence", "false", ["true", "false"])

target_total = int(dbutils.widgets.get("target_total"))
resolve_n = int(dbutils.widgets.get("resolve_n"))
reject_n = int(dbutils.widgets.get("reject_n"))
mode = dbutils.widgets.get("mode")
write_evidence = dbutils.widgets.get("write_evidence").lower() in ("1", "true", "yes")

reports_dir = f"{ROOT}/docs/metrics/rca"
Path(reports_dir).mkdir(parents=True, exist_ok=True)

with postgres_connection() as conn:
    result = bulk_seed_console(
        spark,
        conn,
        reports_dir=reports_dir,
        target_total=target_total,
        resolve_n=resolve_n,
        reject_n=reject_n,
        mode=mode,
        write_evidence=write_evidence,
    )

print(json.dumps(result, indent=2, default=str))
assert result.get("ok"), result
assert result.get("after_total", 0) >= min(
    target_total, result.get("before_total", 0) + result.get("created", 0)
)

# COMMAND ----------

mirrors = sync_lakebase_mirrors(spark)
print(json.dumps(mirrors, indent=2, default=str))
assert mirrors.get("ok"), mirrors
