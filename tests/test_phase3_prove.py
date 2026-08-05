"""Local prove smoke: null_spike and job_crash hit matching primaries."""

from __future__ import annotations

from uuid import uuid4

import pytest

import src.chaos.evidence as evidence
import src.chaos.injector as injector
import src.demo.pipelines as pipelines
import src.ops.dq as dq
from src.chaos.evidence import seed_ops_evidence
from src.chaos.injector import inject
from src.common.spark_local import local_spark
from src.demo.generator import EcommerceGenerator
from src.detection.engine import run_detection
from src.detection.incidents import InMemoryIncidentStore


@pytest.fixture()
def spark_env(tmp_path, monkeypatch):
    suffix = uuid4().hex[:8]
    bronze_db = f"demo_bronze_{suffix}"
    silver_db = f"demo_silver_{suffix}"
    gold_db = f"demo_gold_{suffix}"
    ops_bronze = f"ops_bronze_{suffix}"
    ops_gold = f"ops_gold_{suffix}"

    bronze = {
        "customers": f"{bronze_db}.raw_customers",
        "products": f"{bronze_db}.raw_products",
        "orders": f"{bronze_db}.raw_orders",
        "events": f"{bronze_db}.raw_events",
        "reviews": f"{bronze_db}.raw_reviews",
    }
    silver = {
        "customers": f"{silver_db}.customers",
        "products": f"{silver_db}.products",
        "orders": f"{silver_db}.orders",
        "events": f"{silver_db}.events",
        "reviews": f"{silver_db}.reviews",
    }
    gold = {
        "fact_orders": f"{gold_db}.fact_orders",
        "dim_customer": f"{gold_db}.dim_customer",
        "dim_product": f"{gold_db}.dim_product",
        "daily_order_metrics": f"{gold_db}.daily_order_metrics",
    }
    for mod in (pipelines, injector, evidence):
        monkeypatch.setattr(mod, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(pipelines, "BRONZE_TABLES", bronze)
    monkeypatch.setattr(pipelines, "SILVER_TABLES", silver)
    monkeypatch.setattr(pipelines, "GOLD_TABLES", gold)
    monkeypatch.setattr(pipelines, "DEMO_BRONZE", bronze_db)
    monkeypatch.setattr(pipelines, "DEMO_SILVER", silver_db)
    monkeypatch.setattr(pipelines, "DEMO_GOLD", gold_db)
    monkeypatch.setattr(injector, "DEMO_BRONZE", bronze_db)
    monkeypatch.setattr(injector, "DEMO_SILVER", silver_db)
    monkeypatch.setattr(injector, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(evidence, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(evidence, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(dq, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(dq, "TABLE_FORMAT", "parquet")

    import src.detection.engine as engine

    monkeypatch.setattr(engine, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(engine, "OPS_GOLD", ops_gold)

    session = local_spark(app_name=f"prove-{suffix}")
    for db in (bronze_db, silver_db, gold_db, ops_bronze, ops_gold):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    gen = EcommerceGenerator(
        seed=3, n_customers=30, n_products=8, n_orders=40, n_events=60, n_reviews=10
    )
    pipelines.write_bronze(session, gen)
    pipelines.build_silver(session)
    pipelines.build_gold(session)

    checks = tmp_path / "dq.yml"
    checks.write_text(
        f"""
checks:
  - name: customers_email_null_rate
    table: {silver_db}.customers
    metric_field: null_rate
    comparator: "<="
    threshold: 0.20
    query: |
      SELECT COALESCE(AVG(CASE WHEN email IS NULL THEN 1.0 ELSE 0.0 END), 0.0) AS null_rate
      FROM {silver_db}.customers
  - name: orders_duplicate_rate
    table: {silver_db}.orders
    metric_field: duplicate_rate
    comparator: "<="
    threshold: 0.01
    query: |
      SELECT CASE WHEN COUNT(*) = 0 THEN 0.0
        ELSE (COUNT(*) - COUNT(DISTINCT order_id)) * 1.0 / COUNT(*) END AS duplicate_rate
      FROM {silver_db}.orders
  - name: events_lag_under_48h
    table: {silver_db}.events
    metric_field: pct_late
    comparator: "<="
    threshold: 0.05
    query: |
      SELECT COALESCE(AVG(CASE WHEN lag_minutes > 2880 THEN 1.0 ELSE 0.0 END), 0.0) AS pct_late
      FROM {silver_db}.events
"""
    )
    yield session, str(checks), ops_bronze, ops_gold
    session.stop()


def test_prove_null_spike_and_crash(spark_env):
    spark, checks_path, ops_bronze, ops_gold = spark_env
    store = InMemoryIncidentStore()

    inj = inject(spark, "null_spike", job_run_id="prove-null_spike")
    seed_ops_evidence(spark, inj, checks_path=checks_path)
    run_detection(spark, store, lookback_hours=48)
    assert store.incidents["prove-null_spike"]["primary_failure_type"] == "null_spike"

    # Clear evidence tables for crash path
    spark.sql(f"DROP TABLE IF EXISTS {ops_bronze}.task_telemetry")
    spark.sql(f"DROP TABLE IF EXISTS {ops_gold}.fact_dq_check")
    spark.sql(f"DROP TABLE IF EXISTS {ops_bronze}.raw_task_logs")

    inj2 = inject(spark, "job_crash", job_run_id="prove-job_crash")
    seed_ops_evidence(spark, inj2, checks_path=checks_path)
    run_detection(spark, store, lookback_hours=48)
    assert store.incidents["prove-job_crash"]["primary_failure_type"] == "job_crash"
