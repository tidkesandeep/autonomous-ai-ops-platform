"""Local Lakebase + Spark integration: one synthetic null_spike investigation."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

import src.agent.rag as rag
import src.agent.synth as synth_mod
import src.chaos.evidence as evidence
import src.ops.dq as dq
from src.agent.evaluate import auto_grade_report
from src.agent.rag import rebuild_runbook_embeddings
from src.agent.runner import run_agent
from src.agent.synth import open_synthetic_incident, seed_synthetic_evidence
from src.common.spark_local import local_spark


def _lakebase_password() -> str | None:
    try:
        out = subprocess.check_output(
            [
                "databricks",
                "database",
                "generate-database-credential",
                "--json",
                '{"instance_names":["aiops-lakebase"]}',
                "-o",
                "json",
            ],
            text=True,
        )
        return json.loads(out)["token"]
    except Exception:  # noqa: BLE001
        return None


@pytest.mark.skipif(_lakebase_password() is None, reason="Lakebase credential unavailable")
def test_agent_null_spike_live_lakebase(tmp_path, monkeypatch):
    pw = _lakebase_password()
    assert pw
    monkeypatch.setenv("LAKEBASE_PASSWORD", pw)

    suffix = uuid4().hex[:8]
    ops_bronze = f"ops_bronze_{suffix}"
    ops_gold = f"ops_gold_{suffix}"
    for mod in (evidence, synth_mod, rag, dq):
        if hasattr(mod, "TABLE_FORMAT"):
            monkeypatch.setattr(mod, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(evidence, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(evidence, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(rag, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(dq, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(synth_mod, "OPS_GOLD", ops_gold)

    import src.agent.tools_read as tools_read

    monkeypatch.setattr(tools_read, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(tools_read, "OPS_GOLD", ops_gold)

    spark = local_spark(app_name=f"agent-live-{suffix}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ops_bronze}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ops_gold}")

    rb = Path("/workspace/docs/runbooks")
    rebuild_runbook_embeddings(spark, rb)

    run_id = f"agent-test-null-{suffix}"
    seed_synthetic_evidence(spark, "null_spike", run_id, "customers_scd2")

    from src.common.postgres import postgres_connection

    reports = tmp_path / "rca"
    with postgres_connection() as conn:
        iid = open_synthetic_incident(
            conn, job_run_id=run_id, pipeline_key="customers_scd2", failure_type="null_spike"
        )
        result = run_agent(spark, conn, incident_id=iid, reports_dir=str(reports))
        assert result["ok"] is True
        grade = auto_grade_report(result["report"], "null_spike")
        assert grade.score == 2

    spark.stop()
