# Volume Anomaly

## Symptoms
- Row counts collapse or explode vs the previous successful run
- Cold-start rule: outside 0.5x–2x of previous `rows_out`
- With history ≥ 7, absolute z-score ≥ 3 on row volume

## Diagnosis
1. Compare `ops.bronze.task_telemetry.rows_out` for this run vs history for the same pipeline
2. Check upstream bronze source freshness / filter predicates
3. Look for partial writes, truncated extracts, or duplicate fan-out
4. Correlate with deploy time of filter/partition changes on GitHub

## Standard fix
Volume anomalies are **diagnosis-only** in this platform — there is no safe automatic
remediation. Document blast radius and ask an operator to restore the source extract.
