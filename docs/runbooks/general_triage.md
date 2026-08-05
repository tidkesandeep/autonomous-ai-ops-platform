# General Incident Triage

## Symptoms
- An OPEN incident exists in Lakebase for a `job_run_id`
- One or more `incident_signals` are attached (possibly multiple failure types)

## Diagnosis
1. Load the incident and all signals; trust `primary_failure_type` priority:
   `job_crash > schema_drift > duplicate_explosion > null_spike > volume_anomaly > late_data`
2. Pull DQ failures, telemetry, and task logs for that run
3. Search runbooks for the primary failure class
4. Optionally correlate GitHub commits touching the pipeline package

## Standard fix
Do not remediate until an RCA names the cause and a human approves the proposal.
Update status `OPEN → INVESTIGATING → AWAITING_APPROVAL` as evidence is gathered.
