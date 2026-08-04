# Schema Drift

## Symptoms
- Downstream job fails with `AnalysisException` / missing or unexpected column
- DQ schema checks fail against the last `schema_snapshots` entry
- Row counts may still look normal

## Diagnosis
1. Diff current table schema vs previous `ops` schema snapshot
2. Check recent GitHub commits touching the pipeline notebook / SQL
3. Sample bronze vs silver columns for the drifted field

## Standard fix
Generate schema-evolution DDL (add/rename mapping) for human approval; do not
auto-apply ALTER on gold tables.
