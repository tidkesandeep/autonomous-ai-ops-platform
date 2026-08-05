# Pre-flight Checklist — Free Edition De-risks

Completed against workspace `https://dbc-da72c144-83db.cloud.databricks.com`
(user: `sandeeptidke.work@gmail.com`) on 2026-08-04.

## Hard dependencies

| # | Check | Pass criteria | Result |
|---|---|---|---|
| 1 | Databricks Apps | Hello-world Streamlit deploys as a Databricks App | ✅ `aiops-hello` RUNNING — https://aiops-hello-7474653382320337.aws.databricksapps.com |
| 2 | Lakebase + sync | Create DB + one table; bridge into Delta | ✅ Instance `aiops-lakebase` AVAILABLE. Postgres table `preflight_ping` readable as `lakebase_app.public.preflight_ping` and mirrored to `ops.bronze.preflight_ping_delta` via SQL CTAS. Native "synced tables" product is **Delta → Postgres** (opposite direction); analytics path is **Lakebase UC online catalog → scheduled MERGE/CTAS into `ops` Delta**. |
| 3a | Embedding API | Databricks job returns vectors via Gemini `text-embedding-004` | ⬜ Pending — needs `GEMINI_API_KEY` / `GROQ_API_KEY` in secrets |
| 3b | Inter-job auth | Job with PAT can call `jobs/run-now` and `runs/get-output` | ✅ Created `aiops-preflight-job`, `run-now` → SUCCESS |
| 3c | `run_if = ALL_DONE` | Final detection task still runs when upstream fails | ⬜ Pending — verify when wiring DABs detection task |
| 4 | Slack + GitHub | Webhook + read-only PAT | ⬜ Pending — create when needed |

## Workspace bootstrap (done)

- Catalogs: `demo`, `ops` (created via SQL warehouse; CLI create needs Default Storage path)
- Schemas: `demo.{bronze,silver,gold}`, `ops.{bronze,silver,gold}`
- SQL warehouse: `Serverless Starter Warehouse` (`4a3ce36aae2d0b64`)
- Lakebase: `aiops-lakebase` (CU_1, PG 16) + UC online catalog `lakebase_app`
- Hello app source: `/Workspace/Users/sandeeptidke.work@gmail.com/aiops-hello-app`

## Deployment path (pick one)

- [x] **A** — Lakebase + Databricks App + analytics sync via `lakebase_app` → `ops` Delta (CTAS/MERGE)
- [ ] **B** — Lakebase + Streamlit Community Cloud
- [ ] **C** — Neon + Databricks App + JDBC MERGE
- [ ] **D** — Neon + Community Cloud (**last resort**)

**Chosen path:** **A** (with documented analytics sync mechanism above)

## Demo medallion deploy (2026-08-04)

- Job: `demo-medallion-pipeline` (`605068665132316`) — run SUCCESS
- Workspace code: `/Workspace/Users/sandeeptidke.work@gmail.com/autonomous-ai-ops-platform`
- Row counts: bronze customers 1000 / products 200 / orders 5000 / events 20k / reviews 2000; gold `fact_orders` 5000, `dim_customer` 1000, `dim_product` 191

## Auth notes (for operators)

| Secret | Where |
|---|---|
| Host | `https://dbc-da72c144-83db.cloud.databricks.com` |
| Token | PAT in local `~/.databrickscfg` or `DATABRICKS_TOKEN` — **never commit; rotate if pasted in chat** |
| Lakebase | Host `ep-snowy-violet-d8t4xovo.database.us-east-2.cloud.databricks.com`, db `databricks_postgres`, user = workspace email, password = `databricks database generate-database-credential` JWT |

## Security action required

A PAT was shared in chat during setup. **Revoke it** in Settings → Developer → Access tokens, generate a new one, and keep the new value only in local env / password manager.

## Phase 2 live jobs (2026-08-04)

- `ops-telemetry-dq` (`140965093926378`) — SUCCESS; writes `ops.bronze.task_telemetry` + `ops.gold.fact_dq_check`
- `ops-jobs-api-poller` (`1022675266232756`) — SUCCESS; writes `ops.bronze.raw_task_logs` (expand_tasks enabled)
- Fixture job `ops-force-fail-fixture` used to validate crash-path ingestion

## Phase 3 live verification (2026-08-05)

- Lakebase app-state schema applied (`incidents`, `incident_signals`, `incident_status_events`, …)
- Jobs: `ops-chaos-inject` (`272642187820883`), `ops-incident-detection` (`654518031702213`)
- Proven loop: chaos `null_spike` (`phase3-chaos-null-4`) → DQ `customers_email_null_rate` failed (0.514) → one OPEN incident with `primary_failure_type=null_spike`
- Analytics bridge: `ops.gold.incidents_delta` (and signals/status mirrors) refreshed via CTAS from `lakebase_app`
- Slack webhook still pending (notifier no-ops when `SLACK_WEBHOOK_URL` unset)

## Phase 3 exit criteria (2026-08-05) — MET

- Prove job `ops-phase3-prove` (`298045802108029`), run `952987383190044`: all **6** injected classes opened exactly one OPEN incident with matching `primary_failure_type`
- Visible in `ops.gold.incidents_delta` before reset (precision=1.0, recall=1.0) — see `docs/metrics/phase3_scorecard.json`
- `ops-reset-demo` (`51194964878044`) run `557672395643522` SUCCESS: Lakebase incidents truncated to 0; demo gold regenerated
- Signal dedup unique index `uq_incident_signal_dedup` applied
- Slack webhook still pending (notify writes `audit_log`; webhook no-ops until `SLACK_WEBHOOK_URL` is set)

## Phase 4 live verification (2026-08-05)

- Runbooks (9) chunked + embedded → `ops.gold.runbook_embeddings` (36 chunks) via `ops-embed-runbooks` (`341843632355099`)
- Agent jobs: `ops-run-agent` (`946973285799613`), `ops-agent-eval` (`468032202039268`)
- Eval run `284412746768412` SUCCESS: **18**/18 synthetic incidents (6 classes × 3) → `AWAITING_APPROVAL` with `rca_report_path` set; **162** eval `agent_actions`
- Heuristic tool loop grades without LLM keys; optional LiteLLM polish when `GROQ_API_KEY` / `GEMINI_API_KEY` set
- Slack RCA notify no-ops until `SLACK_WEBHOOK_URL` is set (same as Phase 3)

## Phase 4 exit criteria (2026-08-05) — MET

- Mean RCA rubric **2.0**/2 across **18** runs; **0** zeros — see `docs/metrics/phase4_scorecard.json`
- All 18 reports linked on Lakebase `incidents.rca_report_path` (workspace `docs/metrics/rca/`)
- Embedding API / GitHub PAT / Slack webhook still pending for full LLM polish + commit correlation (hash embeddings + heuristic path proven)

