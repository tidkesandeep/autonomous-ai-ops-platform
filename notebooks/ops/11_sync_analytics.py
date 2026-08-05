"""
# Sync Lakebase → ops Delta analytics mirrors (Phase 5)
"""

# Databricks notebook source

# COMMAND ----------

import json
import sys

ROOT = "/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform"
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.remediation.sync_analytics import sync_lakebase_mirrors

# COMMAND ----------

result = sync_lakebase_mirrors(spark)
print(json.dumps(result, indent=2, default=str))
assert result.get("ok"), result
