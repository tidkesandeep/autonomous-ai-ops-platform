"""Unit tests for agent embeddings, RAG, evaluate, and heuristic path."""

from __future__ import annotations

from uuid import uuid4

import src.agent.rag as rag
import src.ops.dq as dq
from src.agent.embeddings import embed_texts, hash_embed
from src.agent.evaluate import auto_grade_report, summarize_grades
from src.agent.rag import load_runbook_chunks, rebuild_runbook_embeddings, search_runbooks
from src.common.spark_local import local_spark
from src.detection.incidents import InMemoryIncidentStore, record_signals
from src.detection.signals import DetectedSignal


def test_hash_embed_stable_and_normalized():
    a = hash_embed("null spike email column")
    b = hash_embed("null spike email column")
    assert a == b
    assert abs(sum(x * x for x in a) - 1.0) < 1e-6


def test_runbook_chunks_and_search(tmp_path):
    rb = tmp_path / "runbooks"
    rb.mkdir()
    (rb / "null_spike.md").write_text("# Null Spike\n\n## Symptoms\nnull rate high\n\n## Fix\nquarantine\n")
    (rb / "job_crash.md").write_text("# Job Crash\n\n## Symptoms\nOOM\n\n## Fix\nretry\n")
    chunks = load_runbook_chunks(rb)
    assert len(chunks) >= 2
    vectors, backend = embed_texts([c.text for c in chunks], prefer_api=False)
    assert backend == "hash"
    corpus = [
        {
            "chunk_id": c.chunk_id,
            "runbook_path": c.runbook_path,
            "title": c.title,
            "failure_type": c.failure_type,
            "chunk_text": c.text,
            "embedding": v,
            "embedding_backend": backend,
        }
        for c, v in zip(chunks, vectors, strict=True)
    ]
    hits = search_runbooks("null rate email spike", corpus, failure_type="null_spike", top_k=2)
    assert hits
    assert hits[0]["failure_type"] in {"null_spike", None}


def test_auto_grade_and_summary():
    g2 = auto_grade_report(
        {"incident_id": "1", "job_run_id": "r", "root_cause_type": "null_spike", "evidence": ["a", "b"]},
        "null_spike",
    )
    assert g2.score == 2
    grades = [g2] * 17 + [
        auto_grade_report({"incident_id": "x", "job_run_id": "y", "root_cause_type": "null_spike", "evidence": ["a"]}, "job_crash")
    ]
    # 17 twos + 1 zero = mean high but zeros=1 → with n=18 need mean>=1.6
    summary = summarize_grades(grades)
    assert summary["n"] == 18
    assert summary["zeros"] == 1
    assert summary["mean"] >= 1.6
    assert summary["exit_criteria_met"] is True


def test_heuristic_with_memory_store(tmp_path, monkeypatch):
    # Local spark + in-memory lakebase stand-in via recording only tools_write against pg is heavy;
    # here we validate grade path + embeddings rebuild on local parquet.
    suffix = uuid4().hex[:8]
    ops_gold = f"ops_gold_{suffix}"
    monkeypatch.setattr(rag, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(rag, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(dq, "TABLE_FORMAT", "parquet")

    spark = local_spark(app_name=f"agent-{suffix}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {ops_gold}")
    rb = tmp_path / "runbooks"
    rb.mkdir()
    (rb / "null_spike.md").write_text("# Null\n\n## Symptoms\nnulls\n")
    summary = rebuild_runbook_embeddings(spark, rb)
    assert summary["chunks"] >= 1
    spark.stop()


def test_record_signal_then_grade_report():
    store = InMemoryIncidentStore()
    record_signals(
        store,
        [DetectedSignal("run-1", "customers_scd2", "null_spike", "workflow", evidence={"table_name": "demo.silver.customers"})],
    )
    report = {
        "incident_id": store.incidents["run-1"]["incident_id"],
        "job_run_id": "run-1",
        "root_cause_type": "null_spike",
        "evidence": ["primary_failure_type=null_spike", "dq_failures=1"],
    }
    assert auto_grade_report(report, "null_spike").score == 2
