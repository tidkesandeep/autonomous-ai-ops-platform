"""
# Embed Runbooks

Phase 4: chunk docs/runbooks/*.md and rebuild ops.gold.runbook_embeddings.
"""

# Databricks notebook source
# MAGIC %pip install numpy litellm --quiet
# MAGIC %restart_python

# COMMAND ----------

import json
import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.agent.rag import rebuild_runbook_embeddings

# COMMAND ----------

summary = rebuild_runbook_embeddings(spark, f"{ROOT}/docs/runbooks")
print(json.dumps(summary, indent=2))
display(spark.sql("SELECT chunk_id, title, failure_type, embedding_backend FROM ops.gold.runbook_embeddings"))
