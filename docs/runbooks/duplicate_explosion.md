# Duplicate Explosion

## Symptoms
- DQ `duplicate_rate` fails (non-distinct natural keys)
- Gold fact grain doubles or multiplies after a join/merge
- Telemetry may show `rows_out` much larger than `rows_in`

## Diagnosis
1. Measure `COUNT(*)` vs `COUNT(DISTINCT <natural_key>)` on the offending silver/gold table
2. Identify the join that lost a uniqueness constraint
3. Diff SCD2 / merge keys against the last good schema snapshot
4. Check commits that changed join predicates or dropped `ROW_NUMBER` dedupe

## Standard fix
Quarantine duplicate key groups, keep the latest row per key, and reprocess.
Require human approval before overwriting gold.
