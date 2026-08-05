# Databricks notebook source
# MAGIC %md
# MAGIC # Embed Runbooks
# MAGIC
# MAGIC Phase 4: chunk docs/runbooks/*.md and rebuild ops.gold.runbook_embeddings.
# MAGIC After `aiops/gemini-api-key` is set, backend should be `api:gemini/...` not `hash`.

# COMMAND ----------

# MAGIC %pip install numpy litellm --quiet
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

from src.common.secrets import hydrate_env_from_secret_scope

print("secrets", hydrate_env_from_secret_scope())

from src.agent.rag import rebuild_runbook_embeddings

# COMMAND ----------

summary = rebuild_runbook_embeddings(spark, f"{ROOT}/docs/runbooks")
print(json.dumps(summary, indent=2))
display(spark.sql("SELECT chunk_id, title, failure_type, embedding_backend FROM ops.gold.runbook_embeddings LIMIT 20"))
