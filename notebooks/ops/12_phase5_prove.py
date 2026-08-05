# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 prove — full human-in-the-loop remediation
# MAGIC
# MAGIC inject/seed → agent RCA → approve → remediate → RESOLVED (+ Delta mirrors)

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

from src.agent.rag import rebuild_runbook_embeddings
from src.common.postgres import postgres_connection
from src.remediation.prove import prove_full_loop

# COMMAND ----------

dbutils.widgets.text("failure_type", "null_spike")
dbutils.widgets.text("use_chaos", "true")
failure_type = dbutils.widgets.get("failure_type")
use_chaos = dbutils.widgets.get("use_chaos").lower() in ("1", "true", "yes")

rebuild_runbook_embeddings(spark, f"{ROOT}/docs/runbooks")
reports_dir = f"{ROOT}/docs/metrics/rca"
Path(reports_dir).mkdir(parents=True, exist_ok=True)
Path(f"{ROOT}/docs/metrics").mkdir(parents=True, exist_ok=True)

with postgres_connection() as conn:
    report = prove_full_loop(
        spark,
        conn,
        reports_dir=reports_dir,
        failure_type=failure_type,
        use_chaos=use_chaos,
        sync_mirrors=True,
    )

print(json.dumps(report, indent=2, default=str))
assert report.get("exit_criteria_met"), report

score_path = Path(f"{ROOT}/docs/metrics/phase5_scorecard.json")
score_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
print("PHASE5_EXIT_CRITERIA_MET", score_path)
