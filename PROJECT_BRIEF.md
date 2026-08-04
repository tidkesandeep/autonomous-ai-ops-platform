# Project Brief — Autonomous AI Ops Platform

**Repository:** `autonomous-ai-ops-platform`

> This file exists so any tool or person reading this repo understands what is
> being built and why, without needing external context. Cursor: read this
> first in any new session.

---

## What we are building

An AI agent that autonomously investigates Databricks pipeline failures.

When a data pipeline fails, an engineer normally spends hours: reading logs,
checking whether the schema changed, comparing row counts against yesterday,
digging through recent commits to see if someone shipped a breaking change.
This project automates that investigation loop.

The system watches a set of simulated data pipelines, detects when something
goes wrong, and dispatches an LLM agent to figure out *why*. The agent gathers
evidence using tools (query run history, diff schemas, read error logs, search
runbooks, check recent GitHub commits), writes a root-cause report, and
proposes a fix. A human approves or rejects the fix in a web app. Nothing
destructive happens without that approval.

## Why it exists

It is the capstone for the DataExpert.io "Rise of the AI Data Engineer"
bootcamp, and a portfolio project. Both audiences care about production
engineering quality, not just a working demo.

## The two halves of the system

This distinction drives most of the architecture, so it matters:

**The monitored system** — a simulated e-commerce data platform. Orders,
customers, products, events, and free-text reviews flowing through
bronze → silver → gold Delta tables. This is *fake data we generate*. Its only
job is to be realistic enough to break in interesting ways.

**The monitoring system** — the actual product. Telemetry capture, anomaly
detection, incident management, the AI agent, and the operator-facing app.

A failure-injection module (`chaos`) deliberately breaks the monitored system
so we can prove the monitoring system works and measure how well.

## The six failure classes

Everything — detection rules, runbooks, agent evaluation, the demo — is
organized around these:

1. **Schema drift** — a column is added, removed, or renamed upstream
2. **Null spike** — a column's null rate jumps far above its historical norm
3. **Volume anomaly** — row count is wildly higher or lower than expected
4. **Duplicate explosion** — a join or merge starts producing duplicate rows
5. **Late-arriving data** — event timestamps lag far behind processing time
6. **Job crash / OOM** — the task fails outright

## Where data lives, and why

Three storage layers, each with a specific job. Mixing them up is the most
likely way to break this project:

**`demo` catalog (Delta)** — the simulated e-commerce data being monitored.
Bronze/silver/gold medallion. High volume, append and merge patterns.

**`ops` catalog (Delta)** — pipeline telemetry. Every job run, every data
quality check result, schema snapshots, raw task logs and their parsed error
signatures. Append-heavy, analytical, never updated in place.

**Lakebase (OLTP / Postgres)** — application state. Incidents, approvals,
agent actions, audit log. This is where rows get *updated* — an incident moves
from OPEN to INVESTIGATING to RESOLVED. Row-level updates belong here, not in
Delta.

Change Data Feed syncs the Lakebase tables into Delta. The app's analytics page
reads only those CDF-synced Delta tables — that is how we get metrics like MTTR
(computed from the status-change history itself), agent accuracy, and approval
rates.

## The agent

A LangGraph state machine, not a chat wrapper. It runs a bounded tool-calling
loop: triage the incident, gather evidence, form a hypothesis, verify it, write
the report.

**Read tools:** query run history, diff schema across Delta versions, get failed
DQ checks, sample the offending records, read parsed task logs, search runbooks
semantically (RAG over ChromaDB), time-travel compare table versions, correlate
recent GitHub commits.

**Write tools:** update incident status, log every action taken, save the RCA
report, propose a remediation.

The write tools are the point. A recommendation engine that only reads is not
what this is — the agent changes state, and every change is auditable.

## Unstructured data

Three sources, all text:
- Raw task logs and stack traces, ingested and parsed with Spark
- Free-text customer reviews, batch-classified through an LLM into silver
- Runbook Markdown, chunked and embedded into the vector store the agent queries

## Third-party APIs

- **GitHub** — the agent pulls recent commits touching the failing pipeline's
  code, so an RCA can say "this followed PR #42"
- **Slack** — webhook notifications when an incident opens and when the RCA lands
- **Groq / Gemini** — the LLM itself, via LiteLLM so providers are swappable

## Frontend

A Streamlit app deployed as a **Databricks App** (not Streamlit Community
Cloud — it runs inside the workspace, next to the data). Two surfaces: an
incident console for drill-down and approval, and an analytics page reading the
CDF-synced tables.

## Hard constraints

- **Databricks Free Edition only.** No paid features, no AgentBricks, no
  enterprise tooling. If something requires a paid tier, it needs a documented
  free alternative or it doesn't go in.
- **Six-week timeline**, bootcamp-scoped.
- **Everything on GitHub**, structured as a portfolio piece: tests, CI,
  deployment as code (Databricks Asset Bundles), real documentation.

## Explicit non-goals

State these plainly; scope discipline is a feature:

- Not connecting to a real production workspace — simulated pipelines only
- Not auto-executing destructive remediations — human approval is required
- Not fine-tuning any model — prompted agents and RAG only
- Not a real-time streaming system — batch and micro-batch are fine

## What "done" looks like

A single command injects a failure into a pipeline. Within about three minutes:
the failure is detected, an incident opens in Lakebase, Slack fires, the agent
investigates and posts a root-cause report citing actual evidence, a human
approves the proposed fix in the app, the remediation job runs, and the incident
closes — with the entire sequence visible in the CDF-synced analytics.

Measured, not claimed: detection precision and recall against known injected
failures, agent root-cause accuracy across repeated runs, and mean
time-to-investigation.

---

## Full implementation plan

The week-by-week build order, data models, and task breakdown live in
`docs/planning/capstone-implementation-plan.md`. Architecture assumptions that
must be verified against Free Edition before building are in
`docs/planning/pre-flight-checklist.md`.
