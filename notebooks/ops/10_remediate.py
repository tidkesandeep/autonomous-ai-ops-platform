# Databricks notebook source
# MAGIC %md
# MAGIC # Remediation job (Phase 5)
# MAGIC
# MAGIC Runs an approved remediation for one incident, then marks it RESOLVED.
# MAGIC
# MAGIC **Required job parameter:** `incident_id` (UUID). Running the job from the UI
# MAGIC without notebook params will fail fast — use Approve in `aiops-console` or:
# MAGIC
# MAGIC ```bash
# MAGIC databricks jobs run-now --json '{
# MAGIC   "job_id": 298394127011671,
# MAGIC   "notebook_params": {
# MAGIC     "incident_id": "<UUID>",
# MAGIC     "remediation_type": "quarantine_reprocess",
# MAGIC     "parameters_json": "{\"strategy\":\"drop_null_keys\",\"column\":\"email\",\"key\":\"customer_id\"}"
# MAGIC   }
# MAGIC }'
# MAGIC ```

# COMMAND ----------

# Avoid %restart_python on serverless when deps are already present — restart mid-job
# races the session teardown and produces misleading Log4j kernel/ThreadPool errors.
import importlib.util
import subprocess
import sys

_missing = [
    pkg
    for pkg, mod in (("pg8000", "pg8000"), ("numpy", "numpy"), ("requests", "requests"))
    if importlib.util.find_spec(mod) is None
]
if _missing:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", *_missing])

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

from src.common.secrets import hydrate_env_from_secret_scope
print("secrets", hydrate_env_from_secret_scope())

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

if not incident_id:
    raise ValueError(
        "incident_id notebook parameter is required (got empty string). "
        "Do not Run Now without params. Approve in aiops-console, or: "
        'databricks jobs run-now --json \'{"job_id":298394127011671,'
        '"notebook_params":{"incident_id":"<UUID>","remediation_type":"",'
        '"parameters_json":"{}"}}\''
    )

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
