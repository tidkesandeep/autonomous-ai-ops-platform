# Autonomous AI Ops Platform

AI agent that autonomously investigates Databricks pipeline failures — detect,
gather evidence, write an RCA, propose a fix, human approves.

> Read [`PROJECT_BRIEF.md`](PROJECT_BRIEF.md) first. Week-by-week plan:
> [`docs/planning/capstone-implementation-plan.md`](docs/planning/capstone-implementation-plan.md).

## What this is

| Half | Role |
|---|---|
| **Monitored system** (`demo` catalog) | Simulated e-commerce medallion pipelines we deliberately break |
| **Monitoring system** (`ops` + Lakebase) | Telemetry, detection, LangGraph agent, Streamlit approval app |

Six failure classes: schema drift, null spike, volume anomaly, duplicate explosion,
late-arriving data, job crash/OOM.

## Storage layers

| Layer | Holds |
|---|---|
| `demo` (Delta) | Bronze / silver / gold e-commerce data |
| `ops` (Delta) | Pipeline telemetry, DQ results, logs, runbook embeddings, synced analytics |
| Lakebase (Postgres) | `incidents`, `incident_signals`, `incident_status_events`, approvals, agent actions, audit log |

## Repo layout

```
PROJECT_BRIEF.md
databricks.yml              # Databricks Asset Bundles
resources/                  # Job defs + Lakebase SQL schema
notebooks/demo/             # Bootstrap + medallion notebooks
src/
  common/                   # Shared constants
  demo/                     # Generator + bronze/silver/gold pipelines
  ops/ detection/ chaos/ agent/   # Week 2–4 (scaffolded)
app/                        # Streamlit Databricks App (Week 5)
docs/planning/              # Implementation plan + pre-flight checklist
docs/runbooks/              # RAG corpus (Week 4)
tests/
```

## Status (Week 1 in progress)

- [x] Repo scaffold, CI, DABs skeleton
- [x] Deterministic e-commerce generator + medallion pipelines (local-tested)
- [ ] Free Edition pre-flight (§8b path selection) — see `docs/planning/pre-flight-checklist.md`
- [ ] Deploy `demo-medallion-pipeline` to a Databricks workspace

**Deployment path:** TBD after pre-flight (A = Lakebase + Databricks App).

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pyspark
pytest -q
ruff check src tests
```

## Databricks deploy (after workspace auth)

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run demo_medallion_pipeline -t dev
```

Pipeline code is Git-canonical — sync via Databricks Git folders; do not edit
notebooks only in the workspace (commit correlation depends on real Git history).

## Hard constraints

- Databricks **Free Edition** only (fallbacks documented in the plan §8b)
- No destructive auto-remediation — human approval required
- No model fine-tuning — prompted agent + RAG only
