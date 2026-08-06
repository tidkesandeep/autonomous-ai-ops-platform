# Console runbook — Databricks App + bulk seed

## Current goal

Populate **~100** Lakebase incidents across all 6 failure classes, with a mix of
statuses so every console filter is useful.

## One-time / repeatable bulk seed (recommended)

### Databricks UI
1. Open **Workflows → Jobs**
2. Find **`ops-bulk-seed-console`** (or `[dev …] ops-bulk-seed-console`)
3. **Run now with different settings** (optional):
   - `target_total` = `100`
   - `resolve_n` = `18`  → RESOLVED (absolute target)
   - `reject_n` = `8`   → INVESTIGATING (absolute target)
   - `mode` = `fast` (default — no LLM; use `agent` only for a few live RCAs)
4. Wait for SUCCESS (fast mode usually finishes in a few minutes)
5. Hard-refresh the app:
   https://aiops-console-7474653382320337.aws.databricksapps.com

### CLI
```bash
databricks jobs run-now --json '{
  "job_id": <BULK_SEED_JOB_ID>,
  "notebook_params": {
    "target_total": "100",
    "resolve_n": "18",
    "reject_n": "8",
    "mode": "fast",
    "write_evidence": "false"
  }
}'
```

## Using the console (aiops-console)

1. Open the app URL and complete Databricks OAuth login.
2. Sidebar:
   - **Status filter**: AWAITING_APPROVAL / ALL / OPEN / INVESTIGATING / RESOLVED
   - **Failure class**: All classes or one of the six
   - **Refresh list** after external job runs
3. Scan the **Incidents** table; pick a row via **Selected incident**.
4. Review:
   - Human-readable **Remediation proposal**
   - Tabs: RCA, Signals, Timeline, Agent actions, Approvals
5. Decision (enabled when not RESOLVED):
   - **Approve & remediate** → records approval and starts `ops-remediate` (or resolves diagnosis_only)
   - **Reject** → status INVESTIGATING
6. List auto-refreshes after a successful approve/reject.

## Optional: live chaos demo (1–2 classes)

For a realistic “break the pipeline” story (not for volume):
1. Run **`ops-chaos-inject`** with a `failure_type`
2. Run **`ops-incident-detection`**
3. Run **`ops-run-agent`** with the new `incident_id`
4. Approve in the console

Do **not** use **`ops-phase3-prove`** if you want to keep existing counts — it wipes Lakebase at the end.
