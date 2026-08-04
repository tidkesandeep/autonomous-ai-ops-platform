"""Local Spark medallion smoke test using 2-level database.table names."""

from __future__ import annotations

from uuid import uuid4

import pytest

from src.common.spark_local import local_spark
from src.demo import pipelines


@pytest.fixture(scope="module")
def spark():
    session = local_spark()
    yield session
    session.stop()


def test_medallion_local(spark, monkeypatch):
    # Map UC 3-level names onto local Spark 2-level databases.
    suffix = uuid4().hex[:8]
    bronze_db = f"demo_bronze_{suffix}"
    silver_db = f"demo_silver_{suffix}"
    gold_db = f"demo_gold_{suffix}"
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
    # Local Spark has no Delta Lake extension; parquet is enough for smoke tests.
    monkeypatch.setattr(pipelines, "TABLE_FORMAT", "parquet")

    for db in (bronze_db, silver_db, gold_db):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    from src.demo.generator import EcommerceGenerator

    gen = EcommerceGenerator(
        seed=11,
        n_customers=30,
        n_products=8,
        n_orders=60,
        n_events=100,
        n_reviews=20,
    )
    pipelines.write_bronze(spark, gen)
    pipelines.build_silver(spark)
    pipelines.build_gold(spark)

    assert spark.table(f"{gold_db}.fact_orders").count() == 60
    assert spark.table(f"{gold_db}.dim_customer").count() == 30
    assert spark.table(f"{gold_db}.dim_product").count() > 0
    assert spark.table(f"{silver_db}.reviews").count() == 20
    # SCD2 initial load: all rows current
    current = spark.table(f"{gold_db}.dim_customer").filter("is_current = true").count()
    assert current == 30
