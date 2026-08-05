#!/usr/bin/env python3
"""Live smoke test for the Databricks App ``aiops-console``.

Validates compute/app status, public health endpoint, OAuth gate, Lakebase
reads used by the Streamlit console, SQL warehouse analytics queries, and the
wired remediation job — without needing a browser SSO session.

Usage:
  python scripts/smoke_aiops_console.py
  python scripts/smoke_aiops_console.py --json
  python scripts/smoke_aiops_console.py --approve-diagnosis-only   # optional write

Exit code 0 = all required checks passed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "app"
DEFAULT_APP_NAME = "aiops-console"
DEFAULT_APP_URL = "https://aiops-console-7474653382320337.aws.databricksapps.com"
DEFAULT_REMEDIATION_JOB_ID = "298394127011671"


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""
    required: bool = True


@dataclass
class Report:
    app_name: str
    app_url: str
    checks: list[Check] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def add(self, name: str, ok: bool, detail: str = "", required: bool = True) -> None:
        self.checks.append(Check(name=name, ok=ok, detail=detail, required=required))

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks if c.required)

    def to_dict(self) -> dict[str, Any]:
        self.finished_at = self.finished_at or time.time()
        return {
            "ok": self.ok,
            "app_name": self.app_name,
            "app_url": self.app_url,
            "duration_s": round(self.finished_at - self.started_at, 2),
            "checks": [asdict(c) for c in self.checks],
        }


def _cli_json(args: list[str]) -> Any:
    proc = subprocess.run(
        ["databricks", *args, "-o", "json"],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}")
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def _workspace_host_token() -> tuple[str, str]:
    host = (os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN") or ""
    if host and token:
        return host, token
    cfg = Path.home() / ".databrickscfg"
    if cfg.exists():
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        section = None
        values: dict[str, str] = {}
        for line in cfg.read_text().splitlines():
            line = line.strip()
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1]
                continue
            if section == profile and "=" in line:
                k, v = line.split("=", 1)
                values[k.strip()] = v.strip()
        host = host or values.get("host", "").rstrip("/")
        token = token or values.get("token", "")
    if not host or not token:
        raise RuntimeError("DATABRICKS_HOST/TOKEN (or ~/.databrickscfg) required")
    return host, token


def check_app_status(report: Report, app_name: str) -> dict[str, Any]:
    app = _cli_json(["apps", "get", app_name])
    compute = (app.get("compute_status") or {}).get("state")
    app_state = (app.get("app_status") or {}).get("state")
    url = app.get("url") or report.app_url
    report.app_url = url
    report.add(
        "apps_api_compute_active",
        compute == "ACTIVE",
        f"compute={compute} msg={(app.get('compute_status') or {}).get('message')}",
    )
    report.add(
        "apps_api_app_running",
        app_state == "RUNNING",
        f"app={app_state} msg={(app.get('app_status') or {}).get('message')}",
    )
    dep = app.get("active_deployment") or {}
    dep_ok = (dep.get("status") or {}).get("state") == "SUCCEEDED"
    report.add(
        "apps_api_deployment",
        dep_ok,
        f"deployment_id={dep.get('deployment_id')} state={(dep.get('status') or {}).get('state')}",
    )
    return app


def check_http(report: Report, app_url: str) -> None:
    base = app_url.rstrip("/")
    # Public liveness (no SSO)
    try:
        r = requests.get(f"{base}/healthz", timeout=30, allow_redirects=False)
        report.add("http_healthz", r.status_code == 200, f"status={r.status_code}")
    except Exception as exc:  # noqa: BLE001
        report.add("http_healthz", False, str(exc))

    # SSO gate should redirect unauthenticated browsers to OIDC
    try:
        r = requests.get(f"{base}/", timeout=30, allow_redirects=False)
        loc = r.headers.get("Location", "")
        oauth = r.status_code in (301, 302, 303, 307, 308) and (
            "oauth" in loc.lower() or "oidc" in loc.lower() or "authorize" in loc.lower()
        )
        report.add(
            "http_oauth_gate",
            oauth,
            f"status={r.status_code} location_host={urlparse(loc).netloc or 'n/a'}",
        )
    except Exception as exc:  # noqa: BLE001
        report.add("http_oauth_gate", False, str(exc))


def check_lakebase_console(report: Report) -> list[dict[str, Any]]:
    sys.path.insert(0, str(APP_DIR))
    os.environ.setdefault("LAKEBASE_INSTANCE", "aiops-lakebase")
    os.environ.setdefault("LAKEBASE_USER", "sandeeptidke.work@gmail.com")
    os.environ.setdefault("REMEDIATION_JOB_ID", DEFAULT_REMEDIATION_JOB_ID)
    os.environ.setdefault("SQL_WAREHOUSE_ID", "4a3ce36aae2d0b64")

    host, token = _workspace_host_token()
    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_TOKEN"] = token

    from services import (  # type: ignore  # noqa: PLC0415
        get_incident,
        list_agent_actions,
        list_incidents,
        list_signals,
        list_status_events,
        proposal_for,
    )

    try:
        awaiting = list_incidents(limit=20, status="AWAITING_APPROVAL")
        all_rows = list_incidents(limit=5, status="ALL")
        report.add(
            "lakebase_list_incidents",
            isinstance(awaiting, list) and isinstance(all_rows, list),
            f"awaiting={len(awaiting)} recent_all={len(all_rows)}",
        )
    except Exception as exc:  # noqa: BLE001
        report.add("lakebase_list_incidents", False, str(exc))
        return []

    sample = (awaiting or all_rows or [None])[0]
    if not sample:
        report.add("lakebase_incident_detail", True, "no incidents to detail-check", required=False)
        return awaiting

    iid = sample["incident_id"]
    try:
        detail = get_incident(iid)
        signals = list_signals(iid)
        events = list_status_events(iid)
        actions = list_agent_actions(iid, limit=10)
        prop = proposal_for(iid)
        report.add(
            "lakebase_incident_detail",
            bool(detail) and detail.get("incident_id") == iid,
            f"id={iid} signals={len(signals)} events={len(events)} "
            f"actions={len(actions)} proposal={'yes' if prop else 'no'}",
        )
    except Exception as exc:  # noqa: BLE001
        report.add("lakebase_incident_detail", False, f"id={iid} err={exc}")
    return awaiting


def check_analytics(report: Report) -> None:
    sys.path.insert(0, str(APP_DIR))
    from db import fetchall, sql_warehouse_query  # type: ignore  # noqa: PLC0415

    delta_ok = False
    try:
        rows = sql_warehouse_query(
            "SELECT status, COUNT(*) AS n FROM ops.gold.incidents_delta GROUP BY status ORDER BY n DESC"
        )
        delta_ok = isinstance(rows, list)
        report.add(
            "analytics_delta_warehouse",
            delta_ok,
            f"rows={len(rows)} sample={rows[:3]}",
        )
    except Exception as exc:  # noqa: BLE001
        # Analytics page falls back to Lakebase; warehouse miss is soft unless PG also fails.
        report.add("analytics_delta_warehouse", False, str(exc), required=False)
        delta_ok = False

    try:
        pg_rows = fetchall(
            "SELECT status, COUNT(*)::int AS n FROM incidents GROUP BY status ORDER BY n DESC"
        )
        # Empty result set is healthy (fresh reset); query errors are not.
        report.add(
            "analytics_lakebase_fallback",
            True,
            f"rows={len(pg_rows)} sample={pg_rows[:5]}",
        )
        report.add(
            "lakebase_has_incidents",
            sum(int(r.get("n") or 0) for r in pg_rows) > 0 or delta_ok,
            f"pg_status_rows={pg_rows[:5]} delta_ok={delta_ok}",
            required=False,
        )
    except Exception as exc:  # noqa: BLE001
        report.add("analytics_lakebase_fallback", False, str(exc))


def check_remediation_job(report: Report, job_id: str) -> None:
    try:
        job = _cli_json(["jobs", "get", job_id])
        name = (job.get("settings") or {}).get("name", "")
        report.add(
            "remediation_job_exists",
            "remediate" in name.lower() or name == "ops-remediate",
            f"job_id={job_id} name={name}",
        )
    except Exception as exc:  # noqa: BLE001
        report.add("remediation_job_exists", False, str(exc))


def optional_approve_diagnosis_only(report: Report, awaiting: list[dict[str, Any]]) -> None:
    """Exercise approve path for a volume_anomaly (diagnosis_only) without firing remediate."""
    sys.path.insert(0, str(APP_DIR))
    from services import do_approve  # type: ignore  # noqa: PLC0415

    candidate = next(
        (i for i in awaiting if (i.get("primary_failure_type") or "") == "volume_anomaly"),
        None,
    )
    if not candidate:
        report.add(
            "approve_diagnosis_only",
            True,
            "skipped — no AWAITING_APPROVAL volume_anomaly",
            required=False,
        )
        return
    try:
        result = do_approve(
            candidate["incident_id"],
            decided_by="smoke_aiops_console",
            notes="automated smoke approve (diagnosis_only)",
        )
        ok = bool(result.get("ok")) and result.get("remediation_type") == "diagnosis_only"
        report.add(
            "approve_diagnosis_only",
            ok,
            json.dumps(result, default=str)[:500],
            required=False,
        )
    except Exception as exc:  # noqa: BLE001
        report.add("approve_diagnosis_only", False, str(exc), required=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-name", default=os.environ.get("AIOPS_APP_NAME", DEFAULT_APP_NAME))
    parser.add_argument("--app-url", default=os.environ.get("AIOPS_APP_URL", DEFAULT_APP_URL))
    parser.add_argument(
        "--remediation-job-id",
        default=os.environ.get("REMEDIATION_JOB_ID", DEFAULT_REMEDIATION_JOB_ID),
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON report")
    parser.add_argument(
        "--approve-diagnosis-only",
        action="store_true",
        help="Also approve one volume_anomaly incident (write path)",
    )
    args = parser.parse_args()

    report = Report(app_name=args.app_name, app_url=args.app_url)
    try:
        check_app_status(report, args.app_name)
    except Exception as exc:  # noqa: BLE001
        report.add("apps_api_compute_active", False, str(exc))
        report.add("apps_api_app_running", False, str(exc))
        report.add("apps_api_deployment", False, str(exc))

    check_http(report, report.app_url)
    awaiting: list[dict[str, Any]] = []
    try:
        awaiting = check_lakebase_console(report)
    except Exception as exc:  # noqa: BLE001
        report.add("lakebase_list_incidents", False, str(exc))

    try:
        check_analytics(report)
    except Exception as exc:  # noqa: BLE001
        report.add("analytics_lakebase_fallback", False, str(exc))

    check_remediation_job(report, args.remediation_job_id)

    if args.approve_diagnosis_only:
        optional_approve_diagnosis_only(report, awaiting)

    report.finished_at = time.time()
    payload = report.to_dict()

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(f"aiops-console smoke: {'PASS' if report.ok else 'FAIL'} ({payload['duration_s']}s)")
        print(f"url: {report.app_url}")
        for c in report.checks:
            flag = "OK " if c.ok else "FAIL"
            req = "" if c.required else " (optional)"
            print(f"  [{flag}] {c.name}{req}: {c.detail}")

    out = ROOT / "docs" / "metrics" / "app_smoke_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
