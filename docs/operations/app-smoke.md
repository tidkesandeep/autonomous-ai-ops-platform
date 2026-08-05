# Databricks App smoke automation

## What it covers

`scripts/smoke_aiops_console.py` exercises the live **aiops-console** Databricks App without browser SSO:

| Check | Meaning |
|---|---|
| Apps API compute / app / deployment | Compute ACTIVE, app RUNNING, last deploy SUCCEEDED |
| `GET /healthz` | Public liveness (no OAuth) |
| `GET /` OAuth redirect | SSO gate is in place |
| Lakebase `list_incidents` + detail | Same PG path the Streamlit console uses |
| `ops.gold.incidents_delta` + PG fallback | Analytics page warehouse + Lakebase queries |
| `ops-remediate` job | `REMEDIATION_JOB_ID` still points at a real job |

Optional: `--approve-diagnosis-only` approves one `volume_anomaly` incident (no remediate job).

## Run locally (PAT / CLI already configured)

```bash
python scripts/smoke_aiops_console.py
python scripts/smoke_aiops_console.py --json
python scripts/smoke_aiops_console.py --approve-diagnosis-only
```

Writes `docs/metrics/app_smoke_latest.json`. Exit `0` = required checks passed.

## Unit tests (no Databricks)

```bash
pytest -q tests/test_app_services.py
```

## CI

GitHub Actions job `app-smoke` runs on `workflow_dispatch` when repo secrets
`DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set.

## UI note

The Streamlit UI itself is behind Databricks Apps OIDC. Automation covers the
service layer and app health; open
https://aiops-console-7474653382320337.aws.databricksapps.com while signed in
to click through Approve / Analytics.
