# Remediation Approval Gate

## Symptoms
- Agent proposes a remediation while incident is `INVESTIGATING` or `AWAITING_APPROVAL`
- No destructive job should run without an `approvals` row

## Diagnosis
1. Confirm `propose_remediation` wrote a structured proposal (type + parameters)
2. Ensure `incidents.status` is `AWAITING_APPROVAL`
3. Verify the proposal maps to an allowed remediation:
   - retry with adjusted config
   - quarantine bad records + reprocess
   - schema-evolution DDL proposal
4. Volume anomalies remain diagnosis-only

## Standard fix
Human approves or rejects in the app. On approval, the app triggers the remediation
job; the agent never applies fixes itself.
