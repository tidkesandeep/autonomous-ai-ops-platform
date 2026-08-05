# Null Spike

## Symptoms
- DQ check `*_null_rate` fails (null fraction above cold-start or z-score threshold)
- Downstream joins drop rows or produce NULL foreign keys
- Telemetry metadata may show elevated `null_rate`

## Diagnosis
1. Confirm which column spiked via `ops.gold.fact_dq_check` for the failing `run_id`
2. Sample silver/bronze rows where the column is NULL
3. Diff vs prior run null rate (cold-start threshold is 20% when history &lt; 7)
4. Check recent commits that touch the ingest mapping for that column

## Standard fix
Quarantine NULL-key rows to a `_quarantine` table and reprocess the clean subset.
Do not silently coalesce production facts without approval.
