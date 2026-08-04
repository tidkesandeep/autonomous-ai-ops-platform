"""Local Spark medallion smoke test using 2-level database.table names."""

from __future__ import annotations

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
    bronze = {
        "customers": "demo_bronze.raw_customers",
        "products": "demo_bronze.raw_products",
        "orders": "demo_bronze.raw_orders",
        "events": "demo_bronze.raw_events",
        "reviews": "demo_bronze.raw_reviews",
    }
    silver = {
        "customers": "demo_silver.customers",
        "products": "demo_silver.products",
        "orders": "demo_silver.orders",
        "events": "demo_silver.events",
        "reviews": "demo_silver.reviews",
    }
    gold = {
        "fact_orders": "demo_gold.fact_orders",
        "dim_customer": "demo_gold.dim_customer",
        "dim_product": "demo_gold.dim_product",
        "daily_order_metrics": "demo_gold.daily_order_metrics",
    }
    monkeypatch.setattr(pipelines, "BRONZE_TABLES", bronze)
    monkeypatch.setattr(pipelines, "SILVER_TABLES", silver)
    monkeypatch.setattr(pipelines, "GOLD_TABLES", gold)
    monkeypatch.setattr(pipelines, "DEMO_BRONZE", "demo_bronze")
    monkeypatch.setattr(pipelines, "DEMO_SILVER", "demo_silver")
    monkeypatch.setattr(pipelines, "DEMO_GOLD", "demo_gold")
    # Local Spark has no Delta Lake extension; parquet is enough for smoke tests.
    monkeypatch.setattr(pipelines, "TABLE_FORMAT", "parquet")

    for db in ("demo_bronze", "demo_silver", "demo_gold"):
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

    assert spark.table("demo_gold.fact_orders").count() == 60
    assert spark.table("demo_gold.dim_customer").count() == 30
    assert spark.table("demo_gold.dim_product").count() > 0
    assert spark.table("demo_silver.reviews").count() == 20
    # SCD2 initial load: all rows current
    current = spark.table("demo_gold.dim_customer").filter("is_current = true").count()
    assert current == 30
