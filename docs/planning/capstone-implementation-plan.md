# Autonomous AI Ops Platform
## Production-Level Capstone Implementation Plan — DataExpert.io Free Bootcamp

**Repository:** `autonomous-ai-ops-platform`

---

## 1. Goals & Objectives

### Primary Goal
Build an end-to-end, production-grade system that automatically detects Databricks pipeline failures, investigates root causes using an AI agent, and proposes remediation for human approval — reducing mean-time-to-resolution (MTTR) from hours to minutes.

### Objectives (measurable)
1. **Detection**: Automatically detect ≥ 6 distinct failure classes (schema drift, null spike, volume anomaly, duplicate explosion, late-arriving data, job crash/OOM) with ≥ 90% precision on injected failures.
2. **Investigation**: AI agent produces a root-cause report (what failed, why, blast radius, evidence) within **5 minutes** on the happy path and **8 minutes** on the crash path — see §4b for triggering and why the two budgets differ.
3. **Remediation**: For ≥ 3 failure classes, generate an actionable, reviewable fix (retry with adjusted job config, quarantine bad records, schema evolution proposal). **All execution is human-gated** — the agent proposes, never applies.
4. **Observability**: Every job run, quality check, and incident is queryable via SQL — pipeline telemetry in Delta; app state in Lakebase, synced into Delta for analytics.
5. **Production hygiene**: Version control, CI (tests + linting), automated deployment, docs, and a recorded demo — the "would I hand this to a team?" bar.

### Official Bootcamp Requirements — Coverage Map
| Requirement | How this project covers it |
|---|---|
| Data pipeline in Spark | Medallion pipelines (bronze→silver→gold) for the e-commerce domain + ops telemetry pipelines, all PySpark/Spark SQL |
| ≥ 1 third-party API | **GitHub API** — agent correlates incidents with recent commits/PRs to the pipeline repo; plus **Slack webhook** notifications and the Groq/Gemini LLM API |
| Unstructured data processing | (a) Raw **task logs / error stack traces** ingested as text into bronze, parsed with Spark; (b) **customer review text** classified via LLM in a Spark job; (c) runbook Markdown → embedding pipeline |
| Databricks App with a frontend | Streamlit incident console deployed as a **Databricks App** (runs inside the workspace, next to the data) |
| An AI Agent that does stuff | LangGraph investigation agent with **read AND write tools**: queries Delta, diffs schemas, correlates GitHub commits, writes RCA reports, updates incident state in Lakebase, proposes remediations. The App submits the remediation job on human approval — the agent never applies a fix itself |
| CDF from Lakebase → Delta analytics | App state (`incidents`, `incident_status_events`, `approvals`, `agent_actions`, `audit_log`) lives in **Lakebase** (OLTP); **Lakebase synced tables push it into Delta**, powering the app analytics page (incident trends, MTTR, agent accuracy). See §5 for the terminology note on "CDF" |

### Non-Goals (say these explicitly in your README — evaluators love scope discipline)
- Not connecting to a real employer workspace (**5 simulated pipelines** — enough to demonstrate all 6 failure classes; more adds runtime, not credit).
- Not building auto-remediation for destructive actions (human-in-the-loop approval required).
- Not fine-tuning models (prompted agents + RAG only).

---

## 2. Free Edition vs Pay-As-You-Go — Recommendation

**Go with Databricks Free Edition. Do not pay.**

| Factor | Free Edition | Pay-as-you-go |
|---|---|---|
| Serverless compute, Unity Catalog, Delta, Jobs/Workflows, SQL warehouse | ✅ Included | ✅ |
| Cost | $0 | Easy to burn $50–150 in 6 weeks with agent loops re-running pipelines |
| Evaluation | Bootcamp judges the design & engineering, not cluster size | No credit given for paid infra |
| Risk | Compute limits → design smaller data (that's a feature: forces good engineering) | Bill anxiety while learning |

**Only** consider pay-as-you-go ($20–40 budget) if a Week 1 pre-flight check hits a hard wall — e.g., Lakebase or Databricks Apps is unavailable in Free Edition. Even then, every blocker in this plan has a documented free fallback (see Section 8). Everything else runs on free tiers.

**LLM cost**: use **Groq free tier** (Llama 3.3 70B, fast) or **Google Gemini free tier** as primary. Wrap calls in **LiteLLM** so you can swap providers with one config line. **Ollama is for local development and testing only** — it cannot run on Databricks serverless, so it is not a production fallback. If both API providers rate-limit you mid-demo, the answer is retry/backoff and cached tool results, not a local model. $0.

---

## 3. Exact Technology Stack (every choice, with the "why")

| Layer | Choice | Why |
|---|---|---|
| Language | **Python 3.11+** (PySpark) | Bootcamp standard, agent ecosystem, Databricks native |
| SQL dialect | **Spark SQL / Databricks SQL (ANSI mode)** | What Free Edition runs; use it for all transformations where SQL is clearer |
| Notebooks | **Databricks notebooks** for pipelines + exploration; **VS Code/Cursor locally** for the agent package (proper `.py` modules, not notebook spaghetti) | Notebooks for Spark, real Python package for AI logic = production signal |
| Storage format | **Delta Lake** on Unity Catalog (`catalog.schema.table` 3-level namespace) | ACID, time travel (you'll use time travel for incident forensics — great demo moment) |
| Architecture | **Medallion: bronze → silver → gold** | Bootcamp-aligned, evaluator-recognized |
| Orchestration | **Databricks Workflows (Lakeflow Jobs)** with task dependencies + retries | Free, native, produces the run metadata your platform monitors |
| Data quality | **SQL-based expectation checks** written to `ops.fact_dq_check` | Custom = you understand it; results become agent evidence |
| Anomaly detection | **Rules + statistics** (z-score / IQR on row counts, null rates, run duration) in PySpark; no ML needed | Explainable, defensible, free |
| AI agent | **LangGraph** (stateful multi-step agent) + **LiteLLM** (Groq primary, Gemini fallback) | Tool-calling loops, provider-agnostic |
| RAG store | **Delta table `ops.runbook_embeddings`** + in-memory cosine similarity (numpy) | ~60–100 runbook chunks — a vector DB is overkill and local ChromaDB dies with ephemeral compute. A Delta table survives restarts, is queryable, and search is <10ms in numpy |
| Embedding model | **One API-based model for both corpus and query** — Gemini `text-embedding-004` free tier via LiteLLM | Corpus and query vectors MUST come from the same model or cosine similarity is meaningless. Using an API for both means `sentence-transformers` never runs on Databricks serverless — no model download, no wheel size, no egress problem. Corpus embedding is ~100 calls once at build time; query embedding is one call per search |
| Embedding fallback | `sentence-transformers` `all-MiniLM-L6-v2` with weights cached in a Unity Catalog Volume, used for **both** corpus and query | Only if the API path fails Week 1 verification. Swapping models means re-embedding the corpus — never mix |
| Telemetry — in-task | Python decorator wrapping each task: row counts, durations, DQ results, schema snapshots → `ops.bronze_*` | Captures everything while the task is alive |
| Telemetry — crashes | **Databricks Jobs API 2.1 poller** (`runs/list` → `runs/get-output`) every 2 min | A decorator cannot catch OOM or driver death — the process is gone. This is the only reliable path to crash/OOM detection and raw stack traces |
| Knowledge base | Runbooks (Markdown) in `docs/runbooks/`, chunked and embedded into `ops.runbook_embeddings` on deploy | The agent cites runbooks in its reports; rebuild is a single idempotent job |
| App state (OLTP) | **Lakebase** (managed Postgres) for `incidents`, `incident_signals`, `incident_status_events`, `approvals`, `agent_actions`, `audit_log` | Row-level updates (status flips, approvals) belong in OLTP, not Delta; explicitly required by the bootcamp. `UNIQUE(job_run_id)` = one incident per run; signals carry every detected failure type. Verify availability/limits in Free Edition Week 1 |
| App analytics sync | **Lakebase synced tables → Delta** (the bootcamp calls this "CDF"; see §5) | The required analytics bridge; analytics page reads the Delta side. Fallback: scheduled JDBC read + MERGE |
| Commit correlation | **GitHub API** (PyGithub / REST, free) | Agent tool: "did a recent PR to the pipeline repo cause this schema drift?" **Requires that all pipeline code lives in Git and reaches the workspace via Databricks Git folders** — never edited in-workspace. Otherwise commit correlation is theater. State this constraint in the README; the demo must use real commits |
| Frontend | **Streamlit deployed as a Databricks App** (free; verify Apps enabled Week 1) | Meets the "Databricks App with frontend" requirement; runs next to the data, so no PAT/export workaround needed |
| Notifications | **Slack Incoming Webhook** (free) | Third-party API requirement + production-realistic alerting on incident open / RCA posted |
| Unstructured data | Task logs & stack traces (text) + customer review text via LLM classification + runbook Markdown embeddings | Meets the unstructured-data requirement three ways |
| Version control | **GitHub** (public repo) + Databricks Git folders sync | Portfolio visibility |
| CI | **GitHub Actions**: `pytest`, `ruff`, `sqlfluff` on PRs | Production hygiene |
| Deployment | **Databricks Asset Bundles (DABs)** — `databricks.yml` defines jobs as code | This one choice screams "production," most capstones skip it |
| Secrets | Databricks secrets / GitHub Actions secrets; **never** hardcoded keys | Table stakes |
| Docs | `README.md` + architecture diagram (Mermaid) + `docs/runbooks/` | Runbooks double as RAG corpus |

---

## 4. Architecture (data flow)

```
[5 simulated pipelines: raw → bronze → silver → gold]
  incl. unstructured: customer reviews (LLM-classified in a Spark job)
        │ (some runs intentionally injected with failures)
        ▼
[Telemetry capture — two mechanisms]
 (a) in-task decorator: row counts, durations, DQ results,
     schema snapshots  → ops.bronze.*
 (b) Jobs API poller (every 2 min): runs/list → runs/get-output
     catches crashes/OOM the decorator can't (process died)
     → ops.bronze.raw_task_logs (raw error text + stack traces)
        ▼
[Ops dimensional model] ops.gold: fact_job_run, fact_dq_check,
dim_pipeline, dim_failure_type, dim_date
(incidents live in Lakebase, NOT here — see §5)
        ▼
[Incident detection engine] runs as the final task of every
pipeline workflow (fires immediately), plus the 2-min poller
for crashes
→ opens incident row in Lakebase `incidents` (status=OPEN)
→ appends to Lakebase `incident_status_events`
→ Slack webhook: "🚨 incident opened"
        ▼
[AI Investigation Agent — LangGraph]
read tools: query_run_history · diff_schema · get_dq_failures ·
sample_bad_records · read_task_logs · search_runbooks(RAG) ·
time_travel_compare · correlate_github_commits
write tools: update_incident_status · log_agent_action ·
save_rca_report · propose_remediation
→ RCA report + remediation proposal; Slack: RCA summary posted
        ▼
[Databricks App (Streamlit)] incident console reads/writes
Lakebase; human approve/reject → approved fixes trigger a
remediation job; every action → Lakebase `audit_log`
        ▼
[Lakebase → Delta sync] synced tables (incidents,
incident_status_events, approvals, agent_actions, audit_log)
landing in the ops catalog
        ▼
[App analytics page] incident trends, MTTR, agent accuracy,
approval rates — read from the synced Delta tables
```

---

## 4b. Orchestration & Triggering (how work actually gets kicked off)

Lakebase cannot fire a Databricks job. Triggering is explicit:

**Agent dispatch — primary path**: the detection task, immediately after inserting an incident, calls the Jobs API (`jobs/run-now`) to launch the agent job with `incident_id` as a parameter. Synchronous handoff, no polling lag. The detection task does not wait for it.

**Agent dispatch — safety net**: a dispatcher job runs every 5 minutes over `incidents WHERE status='OPEN' AND agent_started_at IS NULL`, catching orphans (detection crashed after insert, agent job failed to launch). It claims each incident with an atomic `UPDATE ... SET agent_started_at = now() WHERE agent_started_at IS NULL RETURNING incident_id` — only one dispatcher can win, so double-dispatch is impossible. **This is a correctness backstop, not part of the SLA** — an incident that reaches the agent via the dispatcher has already blown the time budget by definition.

**Detection runs on two paths, and they can race:**
1. **In-workflow**: a final detection task on every pipeline workflow, configured `run_if = ALL_DONE` so it still executes when an upstream task fails. Covers everything except cluster/driver death.
2. **Jobs API poller** (every 2 min): covers exactly the case the in-workflow task can't — the compute died, so nothing in the workflow ran.

**Dedup rule — one incident per root event.** A single injected failure often surfaces as several signals: a null spike *and* a task failure, or the poller reporting `job_crash` while the in-workflow task reports `null_spike`. Keying on `(job_run_id, failure_type)` would open two incidents for one root cause. So:

- `incidents` has `UNIQUE(job_run_id)` — **one incident per pipeline run, period**
- Every detected signal is appended to a separate `incident_signals` table (incident_id, failure_type, detected_by, evidence_json)
- Insert order for any detecting path: `INSERT INTO incidents ... ON CONFLICT (job_run_id) DO NOTHING RETURNING incident_id` (falling back to a SELECT if the insert was a no-op), then always insert the signal, then recompute `incidents.primary_failure_type` from the signals by fixed priority:

  `job_crash > schema_drift > duplicate_explosion > null_spike > volume_anomaly > late_data`

  Rationale: the most upstream/structural cause wins. A crash explains a missing null-check; a null spike does not explain a crash.

- The agent receives *all* signals for the incident, not just the primary — "null rate spiked to 40% and the task then failed" is better evidence than either alone

This also fixes the race by construction: both detection paths can fire simultaneously and the result is one incident with two signals.

**Status writes are transactional**: updating `incidents.status` and appending to `incident_status_events` happen in one Postgres transaction. Never one without the other.

**Inter-job identity**: both the dispatch call and the poller authenticate with a PAT or service principal stored in Databricks secrets. Verify in Week 1 that Free Edition permits a job to call `jobs/run-now` and `runs/get-output`.

**Poller responsibilities** (one job, two sequential steps): step 1 lands raw run output into `ops.bronze.raw_task_logs`; step 2 evaluates crash conditions and opens incidents. Keeping ingestion and detection separate means a detection bug never costs you the logs.

**Two SLAs, stated honestly:**
- Happy path (DQ/schema/volume failures, in-workflow detection): failure → RCA in **< 5 min**
- Crash path (OOM/driver death, poller detection): failure → RCA in **< 8 min** (up to 2 min poller interval + agent cold start)

---

## 5. Data Design (the part evaluators grade hardest)

### Business data (the "monitored" pipelines) — catalog: `demo`
Pick one coherent domain, e.g., e-commerce:
- `bronze`: `raw_orders`, `raw_customers`, `raw_products`, `raw_events`, plus **`raw_reviews` (free-text customer reviews)** (generated with `Faker`/`dbldatagen`, ~1–5M rows total — small enough for Free Edition, big enough to be real)
- `silver`: cleaned, deduplicated, conformed; **`reviews_enriched`** — a Spark job batches review text through the LLM for sentiment/topic classification (unstructured-data requirement)
- `gold`: `fact_orders`, `dim_customer` (SCD Type 2 — bootcamp topic!), `dim_product`, daily aggregates

### Ops data — split by workload

**Pipeline telemetry (Delta, catalog `ops`)** — high-volume, append-heavy, analytical:
- `fact_job_run` (grain: one task run): run_id, pipeline_key, start/end, duration, status, rows_in/out, retries
- `fact_dq_check` (grain: one check per run): check_name, table, passed, observed_value, threshold
- `dim_pipeline`, `dim_failure_type`, `dim_date`
- `schema_snapshots`: per-run schema JSON for drift detection
- `raw_task_logs` → `parsed_error_signatures`: raw log text ingested to bronze, error patterns extracted with Spark (unstructured text the agent reads as evidence)

**App state (Lakebase / Postgres)** — low-volume, update-heavy, transactional; the App and agent read/write here:
- `incidents`: incident_id, **job_run_id (UNIQUE)**, detected_at, pipeline_key, **primary_failure_type**, severity, status (OPEN/INVESTIGATING/AWAITING_APPROVAL/RESOLVED), **agent_started_at (nullable)**, rca_report_path, linked_commit_sha
- `incident_signals` (append-only): signal_id, incident_id, failure_type, detected_by (`workflow` | `poller`), detected_at, evidence_json — every signal that fired for this run. `primary_failure_type` is derived from these by the priority order in §4b
- `incident_status_events` (append-only): incident_id, from_status, to_status, changed_at, changed_by — every status transition as an immutable event. This is what MTTR is computed from; never rely on a change feed for it.
- `approvals`: who approved/rejected which remediation, when
- `agent_actions`: every agent tool call — tool name, inputs, outputs summary, timestamp (this table alone makes great analytics)
- `audit_log`: every human + system action on the app

**Lakebase → Delta sync** — the required analytics bridge. **Terminology note**: the bootcamp README says "Change Data Feed (CDF) from Lakebase into a Delta table." Delta CDF (`delta.enableChangeDataFeed`) is a *Delta Lake* feature and does not apply to Postgres tables. The actual mechanism is **Lakebase synced tables** (Databricks' Postgres→Unity Catalog reverse sync). Verify in Week 1 which sync modes Free Edition exposes (snapshot / triggered / continuous). Fallback if unavailable: a scheduled Spark job reading Lakebase over JDBC and MERGEing into Delta — same outcome, document the substitution.

- Sync `incidents`, `incident_signals`, `incident_status_events`, `approvals`, `agent_actions`, `audit_log` into Delta tables in `ops`
- **Do not depend on change-feed semantics for MTTR.** Status history is modelled explicitly in `incident_status_events`, so MTTR is a plain SQL query on the synced table regardless of how the sync works. This removes the single riskiest dependency in the design.
- Optionally enable Delta CDF on the *synced Delta tables* (that is a legitimate use of the feature) for downstream incremental reads
- Analytics on the Delta side: incidents over time, MTTR, agent accuracy, approval rates, most-used agent tools

### Failure class → remediation mapping (pinned)
| Failure class | Remediation | Real knobs (serverless — no cluster tier to change) |
|---|---|---|
| Schema drift | Generate schema-evolution DDL | Human reviews the DDL before it runs |
| Null spike | Quarantine bad records | Bad rows → `_quarantine`, clean subset reprocessed |
| Duplicate explosion | Quarantine bad records | Same mechanism, dedup key applied |
| Job crash / OOM | Retry with adjusted config | `spark.sql.shuffle.partitions` increased, `maxRecordsPerFile` reduced, job-level `max_retries` bumped. **Not** "larger cluster" — that knob does not exist on serverless |
| Late-arriving data | Retry with widened window | Job parameter `lookback_hours` increased and the batch re-run. **Not** a streaming watermark — these are batch jobs |
| Volume anomaly | **No automated remediation** | Diagnosis only — a 10x volume spike may be legitimate. The agent reports; a human decides |

Five of six classes map to the three remediations. Volume anomaly is deliberately diagnosis-only, and saying so is better engineering than inventing a fix for it.

### Anomaly baseline cold start
Statistical rules need history that doesn't exist on day one. Rule: for any metric with **fewer than 7 prior runs**, use fixed thresholds (null rate > 20%, row count outside 0.5x–2x of the previous run, duration > 3x previous). Switch to z-score/IQR once N ≥ 7. Record which mode fired on each incident — it belongs in the evidence the agent cites.

### Failure injection module (`chaos.py`)
A config-driven injector that, on demand, corrupts a run: drop a column, spike nulls to 40%, duplicate 3x, inject 10x volume, delay data, throw an exception mid-task. **This is your test harness and your demo script.**

**Two distinct crash fixtures — this matters.** A caught exception mid-task is a *normal task failure*: the workflow completes with a failed task, so the `run_if = ALL_DONE` detection task still fires and the poller path never gets exercised. To actually test the poller you need the process to die: allocate until the driver OOMs, or call `os._exit(137)` to terminate without cleanup. Then no in-workflow task runs at all, and only the Jobs API poller can detect it. Include both fixtures in the demo — otherwise your hardest detection path ships untested.

### Demo reset (`reset_demo.py`)
Chaos mutates data and remediations change tables, so the demo is not idempotent without this. The reset script must: drop and regenerate all `demo` tables from the seeded generator, truncate `ops` telemetry, truncate all Lakebase app-state tables, and then **force the Postgres→Delta mirrors back to empty** — either by triggering a sync refresh (Path A/B) or by truncating the Delta mirrors directly and letting the next JDBC MERGE repopulate (Path C/D). Do not assume synced tables expose a checkpoint you can clear; verify the refresh mechanism in Week 1. One command, under 2 minutes. Build this in Week 3 alongside chaos — not Week 6, when you'll need it under time pressure.

### Evaluation rubric (define before measuring)
**Detection precision/recall** — automated: chaos writes ground truth (`run_id`, `injected_failure_type`) to `ops.injected_failures`; a scoring job joins it against `incidents`. True positive = incident opened for that run with the correct `primary_failure_type`. Wrong type on a real failure counts as both a false negative for the true class and a false positive for the predicted one.

**Agent root-cause accuracy** — manual grading against a 3-point scale, recorded in `docs/metrics/`:
- **2 (correct)**: names the actual injected cause and cites evidence that supports it
- **1 (partial)**: identifies the right affected table/column but the wrong mechanism, or right cause with no supporting evidence
- **0 (wrong)**: wrong cause, or hallucinated evidence

Report the full distribution, not just a pass rate — "14 correct, 3 partial, 1 wrong" is more honest and more interesting than "89%".

**Grades must become queryable**, or the app's "agent accuracy" metric has no source. Grade in a CSV (`docs/metrics/agent_grades.csv`: incident_id, score, grader, notes), loaded by a small job into `ops.agent_evaluations`. The analytics page joins that against the synced incidents table. Manual grading is fine; manual grading that never reaches a table is not.

---

## 6. Week-by-Week Implementation Plan

### Week 1 — Foundation & Simulated Platform
- Set up Free Edition workspace, Unity Catalog catalogs (`demo`, `ops`), GitHub repo, Git folder sync. **De-risk the four hard dependencies now**: (1) verify **Databricks Apps** works — deploy a hello-world Streamlit app; (2) verify **Lakebase** is available — create a database, one table, and confirm a **synced table into Delta** works end-to-end (including how to force a refresh); (3) verify the **embedding API** returns vectors from a Databricks job, that a job can call `jobs/run-now` and `runs/get-output` with a secret-stored identity, and that `run_if = ALL_DONE` works in DABs; (4) create the Slack webhook + a GitHub personal access token (read-only). Then pick your deployment path from §8b and record it. Repo scaffold:
  ```
  /notebooks /src/agent /src/detection /src/chaos /tests
  /docs/runbooks databricks.yml requirements.txt
  ```
- **If Week 1 runs long, cut in this order**: SCD2 on dim_customer → review enrichment → number of pipelines (5 → 3). Never cut the four dependency checks; everything downstream is built on their answers.
- Build data generator (including free-text `raw_reviews`) + bronze/silver/gold pipelines for the e-commerce domain (PySpark + Spark SQL, SCD2 on dim_customer).
- Wire pipelines into Databricks Workflows with dependencies + retries.
- **Exit criteria**: one scheduled workflow runs green end-to-end; repo has CI running `ruff` + one dummy test.

### Week 2 — Observability Layer (the ops warehouse)
- Instrument every task two ways: (a) a `telemetry.py` decorator writing run metadata, row counts, durations, and schema snapshots to `ops.bronze.*`; (b) a **Jobs API poller** (`/api/2.1/jobs/runs/list` → `runs/get-output`, every 2 min) that catches crashes and OOMs the decorator can't — the process is dead — and lands raw error text and stack traces in `ops.bronze.raw_task_logs`.
- Build DQ check framework: YAML-defined checks → executed as Spark SQL → results to `fact_dq_check`.
- Parse `raw_task_logs` into `parsed_error_signatures` with Spark (regex extraction of exception class, message, failing stage).
- Build the `reviews_enriched` job: batch review text through the LLM (LiteLLM) for sentiment/topic labels → silver.
- Model and build the `ops` dimensional gold layer (facts/dims above).
- **Exit criteria**: after any run, you can answer via SQL: "which runs slowed >2x vs 7-day median?", "which tables failed which checks?"; log text and review classifications land in Delta.

### Week 3 — Incident Detection Engine
- Build `detection/` rules: status failures, DQ failures, z-score anomalies on rows/duration/null-rate, schema diff vs last snapshot.
- Set up the Lakebase app-state schema (`incidents`, `incident_signals`, `incident_status_events`, `approvals`, `agent_actions`, `audit_log`) and stand up the **Lakebase → Delta synced tables** (see §5 terminology note).
- Deduplicate + correlate signals into one incident per root event (`UNIQUE(job_run_id)` + `incident_signals` + primary-type priority); write status transitions to `incident_status_events`; fire the Slack webhook on every new incident.
- Build `chaos.py` (writing ground truth to `ops.injected_failures`) and `reset_demo.py`. Prove it: inject each of the 6 failure classes; run the automated precision/recall scoring job; record numbers.
- **Exit criteria**: every injected failure creates exactly one OPEN incident whose `primary_failure_type` matches the injected class, visible in the synced Delta table; `reset_demo.py` returns the environment to a clean state in one command.

### Week 4 — AI Agent + RAG
- Write 8–10 runbooks in `docs/runbooks/` (one per failure class: symptoms, diagnosis steps, standard fixes). Chunk + embed into the `ops.runbook_embeddings` Delta table via an idempotent job; `search_runbooks` loads them and does cosine similarity in numpy.
- Build LangGraph agent with **read tools** (query_run_history, diff_schema, get_dq_failures, sample_bad_records, read_task_logs, search_runbooks, time_travel_compare, **correlate_github_commits** — pulls recent commits/PRs touching the failing pipeline's code and flags suspects) and **write tools** (update_incident_status, log_agent_action, save_rca_report, propose_remediation — all writing to Lakebase). Each tool is a plain Python function — deterministic, testable.
- Agent flow: triage → gather evidence (tool loop, max N steps) → hypothesize → verify → write structured RCA report (JSON + Markdown: summary, evidence with query results, suspected commit if any, root cause, blast radius, remediation proposal, cited runbook).
- Every tool call is logged to Lakebase `agent_actions` — this becomes your agent-behavior analytics for free.
- Post the RCA summary to Slack when the report is finalized.
- Evaluate: run all 6 failure classes ×3; grade each RCA on the 0/1/2 rubric in §5; iterate prompts.
- **Exit criteria**: mean rubric score ≥ 1.6/2 across 18 runs with no more than 1 zero; reports saved and linked from Lakebase `incidents.rca_report_path`.

### Week 5 — Remediation + Dashboard (Human-in-the-Loop)
- Implement 3 safe remediations as parameterized jobs: (a) retry with adjusted config, (b) quarantine bad records to `_quarantine` table + reprocess clean subset, (c) generate schema-evolution DDL for approval.
- Build the Streamlit incident console and **deploy it as a Databricks App**: incident feed, drill-down (evidence, report, timeline, suspected commit), Approve/Reject buttons — the app **reads and writes Lakebase directly**; approval flips status and triggers the remediation job; every action lands in `audit_log`. Add an **app analytics page** reading the **synced Delta tables** (incident trends, MTTR from `incident_status_events`, agent accuracy, approval rates, tool-usage stats).
- **Exit criteria**: full loop demo — inject failure → auto-detect → agent report → human approves → remediation runs → incident RESOLVED.

### Week 6 — Production Hardening + Submission
- Databricks Asset Bundles: `databricks bundle deploy` recreates all jobs from `databricks.yml`.
- Tests: unit tests for detection rules, chaos injector, agent tools (mock LLM); `pytest` in CI; `sqlfluff` on SQL.
- Docs: README with architecture (Mermaid), setup guide, metrics table (detection precision/recall, agent accuracy, simulated MTTR before/after), demo video (5–8 min: the full loop live).
- Buffer for breakage. Submit.

---

## 7. Definition of "Production-Level" — Checklist
- [ ] Everything as code in Git; no manual UI-only jobs (DABs deploys them)
- [ ] CI: lint + tests pass on every PR
- [ ] Secrets in secret stores, zero keys in code
- [ ] Idempotent pipelines (re-runs don't duplicate data — MERGE, not append)
- [ ] Retries + alerting on workflows
- [ ] Every incident, agent action, and approval auditable via Lakebase + its synced Delta mirror
- [ ] Human approval gate before any data-changing remediation
- [ ] `reset_demo.py` restores a clean demo state in one command
- [ ] Runbooks + README a stranger could operate from
- [ ] Measured results with a stated rubric, not claims (precision/recall, agent rubric distribution, MTTR)
- [ ] All 6 bootcamp requirements demonstrably met (Spark pipeline · GitHub + Slack + LLM APIs · logs/reviews as unstructured data · Databricks App · read/write AI agent · Lakebase → Delta analytics) — reference the coverage map in your README

---

## 8. Risks & Mitigations
| Risk | Mitigation |
|---|---|
| Free Edition compute limits | Keep data ≤ 5M rows; serverless SQL for transforms; schedule sparsely |
| Postgres + frontend availability | **Fallbacks compose — see §8b.** Apps and Lakebase are not independent choices; the app must reach whichever Postgres you end up with |
| Scope density over 6 weeks | Cut to 5 pipelines, 6 failure classes, 3 remediations. If Week 5 slips, ship the agent + approval loop and drop the third remediation — never thin the agent |
| LLM classification of reviews burns free-tier quota | Small batches (≤ 2–5k reviews), cache results, run once — it's a demo dataset, not a stream |
| LLM free-tier rate limits | LiteLLM retry/backoff across Groq and Gemini; cache agent tool results. Ollama is local-dev only and is NOT a runtime fallback |
| Agent hallucination | Force tool-cited evidence in report schema; verify step before finalizing |
| Inter-job auth blocked | Detection→agent dispatch and the Jobs API poller both need a workspace identity (PAT or service principal in Databricks secrets). Verify in Week 1 that a job can call `jobs/run-now` and `runs/get-output` on Free Edition |
| Scope creep | Non-goals section is your shield; 6 failure classes, 3 remediations, done |

---

## 8b. Composed Deployment Paths (fallbacks are not independent)

Two Week 1 questions decide everything: **is Lakebase available?** and **do Databricks Apps work?** The frontend must reach whichever Postgres you land on, so these cannot be chosen separately.

| Path | Postgres | Frontend | Postgres → Delta sync | Cost of the trade-off |
|---|---|---|---|---|
| **A (primary)** | Lakebase | Databricks App | Lakebase synced tables | None — this is the target |
| **B** | Lakebase | Streamlit Community Cloud | Lakebase synced tables | App reaches Lakebase over the public endpoint; needs credentials in Streamlit secrets. Document the security trade-off |
| **C** | Neon (free tier) | Databricks App | Scheduled Spark JDBC read + MERGE into Delta | Sync is batch, not continuous — set a 2-min schedule; note the lag in your README |
| **D (last resort, not a peer of A–C)** | Neon | Streamlit Community Cloud | Scheduled Spark JDBC read + MERGE | **Fails the "Databricks App with a frontend" requirement outright.** Only acceptable if both A/B and C are impossible; if you land here, raise it with the bootcamp before submitting rather than hoping it passes |

**Note what does *not* change across paths**: the app reads and writes Postgres directly (approvals work everywhere), and MTTR comes from `incident_status_events` regardless of sync mechanism. The "export Parquet and read it" fallback from an earlier draft is dead — it breaks approvals and is not a real fallback.

Decide the path in Week 1 from pre-flight results, write it in the README, and don't revisit.
