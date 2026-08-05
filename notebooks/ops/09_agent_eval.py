# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Evaluation (Phase 4 exit criteria)
# MAGIC
# MAGIC Runs 6 failure classes × 3 repeats with synthetic ops evidence, auto-grades RCAs,
# MAGIC and writes docs/metrics/agent_grades.csv + phase4_scorecard.json.

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

from src.agent.eval_loop import run_agent_eval
from src.agent.rag import rebuild_runbook_embeddings
from src.common.postgres import postgres_connection

# COMMAND ----------

dbutils.widgets.text("repeats", "3")
repeats = int(dbutils.widgets.get("repeats"))

# Ensure embeddings exist
emb = rebuild_runbook_embeddings(spark, f"{ROOT}/docs/runbooks")
print("embeddings", emb)

reports_dir = f"{ROOT}/docs/metrics/rca"
grades_csv = f"{ROOT}/docs/metrics/agent_grades.csv"
Path(reports_dir).mkdir(parents=True, exist_ok=True)
Path(f"{ROOT}/docs/metrics").mkdir(parents=True, exist_ok=True)

with postgres_connection() as conn:
    report = run_agent_eval(
        spark,
        conn,
        reports_dir=reports_dir,
        grades_csv=grades_csv,
        repeats=repeats,
    )

print(json.dumps({k: report[k] for k in ("ran_at", "summary", "exit_criteria_met")}, indent=2))
assert report["exit_criteria_met"], report["summary"]

score_path = Path(f"{ROOT}/docs/metrics/phase4_scorecard.json")
score_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
print("PHASE4_EXIT_CRITERIA_MET", score_path)
