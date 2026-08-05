# GitHub Commit Correlation

## Symptoms
- Schema drift, join changes, or filter edits coincide with a recent merge
- Incident timing aligns with a push to the pipeline repo

## Diagnosis
1. List recent commits/PRs touching `src/demo`, `src/ops`, or the failing notebook path
2. Prefer commits after the last successful run for that pipeline
3. Flag suspects that rename columns, change join keys, or alter DQ thresholds
4. Record `linked_commit_sha` on the incident when confidence is high

## Standard fix
Cite the suspect commit in the RCA. Rollback or forward-fix is a human decision;
the agent only proposes.
