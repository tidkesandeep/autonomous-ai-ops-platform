# Job Crash / OOM

## Symptoms
- Task `result_state` is `FAILED` / `TIMEDOUT` or lifecycle `INTERNAL_ERROR`
- `ops.bronze.raw_task_logs` shows `OutOfMemoryError`, stack traces, or driver death
- In-workflow detection may be missing — rely on the Jobs API poller

## Diagnosis
1. Read the raw task log / error signature for the failing `run_id`
2. Note whether failure is OOM, timeout, or application exception
3. Check recent code changes that increased shuffle / broadcast size
4. Confirm sibling tasks for the same job run

## Standard fix
Propose a retry with adjusted job config (smaller partitions, longer timeout).
Human must approve before `jobs/run-now` remediation.
