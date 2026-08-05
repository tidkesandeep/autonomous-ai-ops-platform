"""
# Demo script — full Autonomous AI Ops loop (5–8 min recording)

Record against workspace `https://dbc-da72c144-83db.cloud.databricks.com`
and app https://aiops-console-7474653382320337.aws.databricksapps.com.

## Prep (30s)

1. Run job `ops-reset-demo` (or `databricks bundle run ops_reset_demo -t dev`).
2. Confirm Lakebase `incidents` is empty / clean and demo gold tables exist.

## Script

| Time | Action | Show |
|---|---|---|
| 0:00 | Title card — "Autonomous AI Ops Platform" | README architecture diagram |
| 0:20 | Inject failure | Run `ops-chaos-inject` with `failure_type=null_spike` |
| 0:50 | Detect | Run `ops-incident-detection`; show OPEN incident in Lakebase / console |
| 1:20 | Investigate | Run `ops-run-agent` with the new `incident_id`; show tool actions |
| 2:20 | RCA | Open RCA markdown path + `agent_actions` rows |
| 3:00 | Approve | In **aiops-console**, filter `AWAITING_APPROVAL`, click Approve |
| 3:40 | Remediate | Show `ops-remediate` run SUCCESS; customers quarantine table |
| 4:20 | Resolved | Incident status `RESOLVED`; approvals + audit_log |
| 5:00 | Analytics | App **Analytics** page: status counts, MTTR, tool usage (Delta mirrors) |
| 5:40 | Metrics | Flash phase3/4/5 scorecards (precision 1.0, agent mean 2.0, loop OK) |
| 6:20 | Reset | Optional `ops-reset-demo` for idempotent demo close |

## Alternate one-click proof

If time is short, run `ops-phase5-prove` and narrate the printed JSON
(`exit_criteria_met=true`) while walking the audit trail in Lakebase.

## Artifacts to attach with the video

- `docs/metrics/phase3_scorecard.json`
- `docs/metrics/phase4_scorecard.json`
- `docs/metrics/phase5_scorecard.json`
- Link to this script
