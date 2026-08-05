"""Unit tests for Phase 5 remediations (local Spark, no Lakebase)."""

from __future__ import annotations

from uuid import uuid4

import pytest

import src.demo.pipelines as pipelines
import src.remediation.quarantine as quarantine
import src.remediation.schema_ddl as schema_ddl
from src.common.spark_local import local_spark
from src.demo.generator import EcommerceGenerator
from src.remediation.mapping import remediation_for_failure
from src.remediation.retry_config import apply_retry_adjusted_config
from src.remediation.schema_ddl import generate_schema_ddl, run_schema_evolution


@pytest.fixture()
def spark(tmp_path, monkeypatch):
    suffix = uuid4().hex[:8]
    session = local_spark(app_name=f"remed-{suffix}")
    bronze_db = f"demo_bronze_{suffix}"
    silver_db = f"demo_silver_{suffix}"
    gold_db = f"demo_gold_{suffix}"
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
    monkeypatch.setattr(quarantine, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(quarantine, "DEMO_SILVER", silver_db)
    monkeypatch.setattr(quarantine, "OPS_GOLD", ops_gold)
    monkeypatch.setattr(schema_ddl, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(schema_ddl, "DEMO_BRONZE", bronze_db)
    monkeypatch.setattr(schema_ddl, "OPS_GOLD", ops_gold)
    import src.remediation.retry_config as retry_config

    monkeypatch.setattr(retry_config, "TABLE_FORMAT", "parquet")
    monkeypatch.setattr(retry_config, "OPS_GOLD", ops_gold)

    for db in (bronze_db, silver_db, gold_db, ops_gold):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    gen = EcommerceGenerator(
        seed=3, n_customers=30, n_products=8, n_orders=40, n_events=50, n_reviews=10
    )
    pipelines.write_bronze(session, gen)
    pipelines.build_silver(session)
    yield session, silver, bronze, ops_gold
    session.stop()


def test_mapping_covers_six_classes():
    for ft in (
        "job_crash",
        "schema_drift",
        "duplicate_explosion",
        "null_spike",
        "volume_anomaly",
        "late_data",
    ):
        rem, params = remediation_for_failure(ft)
        assert rem
        assert isinstance(params, dict)
    assert remediation_for_failure("volume_anomaly")[0] == "diagnosis_only"


def test_quarantine_null_keys(spark):
    session, silver, _bronze, _ops = spark
    from pyspark.sql import functions as F

    table = silver["customers"]
    # Force some nulls (materialize to avoid overwrite-while-read)
    df = session.table(table).withColumn(
        "email",
        F.when(F.hash("customer_id") % 3 == 0, F.lit(None)).otherwise(F.col("email")),
    )
    session.createDataFrame(df.collect(), schema=df.schema).write.format("parquet").mode(
        "overwrite"
    ).saveAsTable(table)
    before_nulls = session.table(table).filter("email IS NULL").count()
    assert before_nulls > 0
    result = quarantine.quarantine_null_keys(
        session, source_table=table, column="email", key="customer_id"
    )
    assert result["ok"]
    assert result["quarantined_rows"] == before_nulls
    assert session.table(table).filter("email IS NULL").count() == 0
    assert session.table(result["quarantine_table"]).count() == before_nulls


def test_quarantine_duplicates(spark):
    session, silver, _bronze, _ops = spark
    table = silver["orders"]
    df = session.table(table)
    exploded = session.createDataFrame(df.unionByName(df).collect(), schema=df.schema)
    exploded.write.format("parquet").mode("overwrite").saveAsTable(table)
    n_before = session.table(table).count()
    result = quarantine.quarantine_duplicates(session, source_table=table, key="order_id")
    assert result["ok"]
    assert result["quarantined_rows"] > 0
    assert session.table(table).count() < n_before


def test_retry_config_records(spark):
    session, _silver, _bronze, ops_gold = spark
    out = apply_retry_adjusted_config(
        session,
        incident_id="inc-1",
        pipeline_key="ops_force_fail",
        parameters={"lookback_hours": 168},
    )
    assert out["ok"]
    assert session.table(f"{ops_gold}.remediation_runs").count() >= 1


def test_schema_ddl_additive(spark):
    session, _silver, bronze, ops_gold = spark
    table = bronze["products"]
    df = session.table(table).withColumnRenamed("category", "product_category_v2")
    session.createDataFrame(df.collect(), schema=df.schema).write.format("parquet").mode(
        "overwrite"
    ).option("overwriteSchema", "true").saveAsTable(table)
    ddl = generate_schema_ddl(session, source_table=table)
    assert ddl["ok"]
    assert any("ADD COLUMN category" in s for s in ddl["ddl_statements"])
    result = run_schema_evolution(
        session,
        incident_id="inc-schema",
        pipeline_key="products_catalog",
        parameters={"table": table, "action": "generate_and_apply_additive"},
    )
    assert result["ok"]
    assert "category" in session.table(table).columns
    assert session.table(f"{ops_gold}.schema_evolution_proposals").count() >= 1
