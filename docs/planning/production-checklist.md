# Production-Level Checklist

Capstone §7 — status as of Phase 6 (2026-08-05).

| # | Criterion | Status | Evidence |
|---|---|---|---|
| 1 | Everything as code in Git; DABs deploys jobs | ✅ | `databricks.yml` + `resources/*.yml`; `databricks bundle deploy -t dev` recreates 13 jobs |
| 2 | CI: lint + tests on every PR | ✅ | `.github/workflows/ci.yml` — ruff, pytest, sqlfluff |
| 3 | Secrets in secret stores, zero keys in code | ✅ | `.env.example` only; Apps use `database/lakebase-url`; PAT in `~/.databrickscfg` |
| 4 | Idempotent pipelines | ✅ | Demo CTAS/overwrite; incident upsert on `job_run_id`; signal dedup unique index |
| 5 | Retries + alerting on workflows | ✅ | Job `max_retries` on critical tasks; Slack notifier (no-ops until webhook); failure emails optional |
| 6 | Auditable incidents / agent / approvals | ✅ | Lakebase + `ops.gold.*_delta` mirrors via `ops-sync-analytics` |
| 7 | Human approval before data-changing remediation | ✅ | App Approve/Reject; remediation job only after approval |
| 8 | `reset_demo` one command | ✅ | `ops-reset-demo` / `src/chaos/reset_demo.py` |
| 9 | Runbooks + README operable by a stranger | ✅ | `docs/runbooks/*`, README setup + demo script |
| 10 | Measured results with rubric | ✅ | phase3/4/5 scorecards under `docs/metrics/` |
| 11 | Six bootcamp requirements met | ✅ | See README coverage map |

## Bootcamp requirement coverage

| Requirement | Where |
|---|---|
| Spark pipeline | `src/demo/pipelines.py`, `demo-medallion-pipeline` |
| GitHub + Slack + LLM APIs | `correlate_github_commits`, `src/detection/slack.py`, LiteLLM in `src/agent/` |
| Unstructured data | task logs, reviews, runbook Markdown → embeddings |
| Databricks App frontend | `aiops-console` Streamlit app |
| Read/write AI agent | `src/agent/tools_read.py` + `tools_write.py` |
| Lakebase → Delta analytics | `src/remediation/sync_analytics.py`, Analytics page |
