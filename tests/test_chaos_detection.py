"""Local Spark tests for chaos injectors + detection store integration."""

from __future__ import annotations

from uuid import uuid4

import pytest

import src.chaos.injector as injector
import src.demo.pipelines as pipelines
import src.ops.dq as dq
from src.chaos.injector import inject
from src.common.spark_local import local_spark
from src.demo.generator import EcommerceGenerator
from src.detection.incidents import InMemoryIncidentStore, record_signals
from src.detection.rules import detect_dq_failures


@pytest.fixture()
def spark():
    session = local_spark(app_name=f"chaos-{uuid4().hex[:6]}")
    yield session
    session.stop()


def test_chaos_null_spike_trips_dq(spark, tmp_path, monkeypatch):
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
    monkeypatch.setattr(pipelines, "BRONZE_TABLES", bronze)
    monkeypatch.setattr(pipelines, "SILVER_TABLES", silver)
    monkeypatch.setattr(pipelines, "GOLD_TABLES", gold)
    monkeypatch.setattr(pipelines, "DEMO_BRONZE", bronze_db)
    monkeypatch.setattr(pipelines, "DEMO_SILVER", silver_db)
    monkeypatch.setattr(pipelines, "DEMO_GOLD", gold_db)
    monkeypatch.setattr(pipelines, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(injector, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(injector, "DEMO_BRONZE", bronze_db)
    monkeypatch.setattr(injector, "DEMO_SILVER", silver_db)
    monkeypatch.setattr(injector, "OPS_BRONZE", ops_bronze)
    monkeypatch.setattr(dq, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(dq, "TABLE_FORMAT", "parquet")

    for db in (bronze_db, silver_db, gold_db, ops_bronze, ops_gold):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    gen = EcommerceGenerator(
        seed=7, n_customers=40, n_products=10, n_orders=80, n_events=100, n_reviews=20
    )
    pipelines.write_bronze(spark, gen)
    pipelines.build_silver(spark)

    result = inject(spark, "null_spike", job_run_id="chaos-null-test")
    assert result.failure_type == "null_spike"

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
"""
    )
    target = f"{ops_gold}.fact_dq_check"
    summary = dq.run_dq_checks(
        spark,
        str(checks),
        run_id="chaos-null-test",
        pipeline_key="customers_scd2",
        table_name=target,
    )
    assert summary["checks_failed"] == 1

    dq_rows = [r.asDict() for r in spark.table(target).collect()]
    signals = detect_dq_failures(dq_rows)
    store = InMemoryIncidentStore()
    record_signals(store, signals)
    assert len(store.incidents) == 1
    assert store.incidents["chaos-null-test"]["primary_failure_type"] == "null_spike"
