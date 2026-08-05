# Late-Arriving Data

## Symptoms
- DQ `pct_late` / lag check fails (`lag_minutes` above threshold, typically &gt; 48h)
- Event timestamps far behind `process_ts`
- Downstream “today” metrics under-count recent activity

## Diagnosis
1. Inspect `lag_minutes` distribution on `demo.silver.events` (or the failing table)
2. Confirm whether producer clocks drifted or the batch window shifted
3. Compare partition dates in bronze vs expected watermark
4. Review scheduler changes / delayed upstream jobs in recent commits

## Standard fix
Reprocess the delayed window after correcting the watermark. Prefer a parameterized
replay job over mutating historical gold in place.
