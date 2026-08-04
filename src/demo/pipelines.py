"""Spark helpers for writing generated e-commerce data into the `demo` catalog."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from src.common.constants import DEMO_BRONZE, DEMO_GOLD, DEMO_SILVER
from src.demo.generator import EcommerceGenerator, records_as_dicts

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


BRONZE_TABLES = {
    "customers": f"{DEMO_BRONZE}.raw_customers",
    "products": f"{DEMO_BRONZE}.raw_products",
    "orders": f"{DEMO_BRONZE}.raw_orders",
    "events": f"{DEMO_BRONZE}.raw_events",
    "reviews": f"{DEMO_BRONZE}.raw_reviews",
}

SILVER_TABLES = {
    "customers": f"{DEMO_SILVER}.customers",
    "products": f"{DEMO_SILVER}.products",
    "orders": f"{DEMO_SILVER}.orders",
    "events": f"{DEMO_SILVER}.events",
    "reviews": f"{DEMO_SILVER}.reviews",
}

GOLD_TABLES = {
    "fact_orders": f"{DEMO_GOLD}.fact_orders",
    "dim_customer": f"{DEMO_GOLD}.dim_customer",
    "dim_product": f"{DEMO_GOLD}.dim_product",
    "daily_order_metrics": f"{DEMO_GOLD}.daily_order_metrics",
}


def ensure_demo_schemas(spark: SparkSession) -> None:
    """Create Unity Catalog schemas used by the monitored platform."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEMO_BRONZE}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEMO_SILVER}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DEMO_GOLD}")


def _to_df(spark: SparkSession, rows: Sequence[object]) -> DataFrame:
    return spark.createDataFrame(records_as_dicts(rows))


def _ctas(spark: SparkSession, table: str, select_sql: str) -> None:
    """Create-or-replace table from a SELECT.

    Delta (Databricks) supports ``CREATE OR REPLACE TABLE ... AS``.
    Local parquet/Hive tables do not — drop then create instead.
    """
    if TABLE_FORMAT == "delta":
        spark.sql(f"CREATE OR REPLACE TABLE {table} AS {select_sql}")
        return
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"CREATE TABLE {table} USING {TABLE_FORMAT} AS {select_sql}")


# Databricks uses Delta; local Spark tests can override to parquet/default.
TABLE_FORMAT = "delta"


def write_bronze(spark: SparkSession, generator: EcommerceGenerator | None = None) -> dict[str, int]:
    """Generate and overwrite bronze raw tables. Returns row counts per table."""
    gen = generator or EcommerceGenerator()
    data = gen.generate_all()
    ensure_demo_schemas(spark)
    counts: dict[str, int] = {}
    for key, table in BRONZE_TABLES.items():
        df = _to_df(spark, data[key])
        if TABLE_FORMAT != "delta":
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        writer = df.write.format(TABLE_FORMAT).mode("overwrite")
        if TABLE_FORMAT == "delta":
            writer = writer.option("overwriteSchema", "true")
        writer.saveAsTable(table)
        counts[table] = df.count()
    return counts


def build_silver(spark: SparkSession) -> dict[str, int]:
    """Clean / conform bronze → silver with SQL (dedupe on natural keys)."""
    ensure_demo_schemas(spark)
    counts: dict[str, int] = {}

    _ctas(
        spark,
        SILVER_TABLES["customers"],
        f"""
        SELECT
          customer_id,
          lower(trim(email)) AS email,
          trim(full_name) AS full_name,
          upper(country) AS country,
          created_at,
          updated_at
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY customer_id ORDER BY updated_at DESC
          ) AS rn
          FROM {BRONZE_TABLES['customers']}
        )
        WHERE rn = 1
        """,
    )

    _ctas(
        spark,
        SILVER_TABLES["products"],
        f"""
        SELECT
          product_id,
          sku,
          name,
          lower(category) AS category,
          CAST(price_usd AS DOUBLE) AS price_usd,
          is_active,
          updated_at
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY product_id ORDER BY updated_at DESC
          ) AS rn
          FROM {BRONZE_TABLES['products']}
        )
        WHERE rn = 1 AND is_active = true
        """,
    )

    _ctas(
        spark,
        SILVER_TABLES["orders"],
        f"""
        SELECT
          order_id,
          customer_id,
          product_id,
          quantity,
          order_ts,
          lower(status) AS status,
          CAST(amount_usd AS DOUBLE) AS amount_usd,
          CAST(order_ts AS DATE) AS order_date
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY order_id ORDER BY order_ts DESC
          ) AS rn
          FROM {BRONZE_TABLES['orders']}
        )
        WHERE rn = 1
        """,
    )

    _ctas(
        spark,
        SILVER_TABLES["events"],
        f"""
        SELECT
          event_id,
          customer_id,
          product_id,
          lower(event_type) AS event_type,
          event_ts,
          process_ts,
          CAST(
            (unix_timestamp(process_ts) - unix_timestamp(event_ts)) / 60.0
            AS DOUBLE
          ) AS lag_minutes
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY event_id ORDER BY process_ts DESC
          ) AS rn
          FROM {BRONZE_TABLES['events']}
        )
        WHERE rn = 1
        """,
    )

    _ctas(
        spark,
        SILVER_TABLES["reviews"],
        f"""
        SELECT
          review_id,
          customer_id,
          product_id,
          rating,
          review_text,
          review_ts
        FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY review_id ORDER BY review_ts DESC
          ) AS rn
          FROM {BRONZE_TABLES['reviews']}
        )
        WHERE rn = 1
        """,
    )

    for table in SILVER_TABLES.values():
        counts[table] = spark.table(table).count()
    return counts


def build_gold(spark: SparkSession) -> dict[str, int]:
    """Build gold facts/dims including SCD Type 2 dim_customer."""
    ensure_demo_schemas(spark)
    counts: dict[str, int] = {}

    # SCD2: current snapshot with effective dating (initial load = all current)
    _ctas(
        spark,
        GOLD_TABLES["dim_customer"],
        f"""
        SELECT
          customer_id,
          email,
          full_name,
          country,
          created_at AS effective_from,
          CAST(NULL AS TIMESTAMP) AS effective_to,
          true AS is_current,
          updated_at AS source_updated_at
        FROM {SILVER_TABLES['customers']}
        """,
    )

    _ctas(
        spark,
        GOLD_TABLES["dim_product"],
        f"""
        SELECT
          product_id,
          sku,
          name,
          category,
          price_usd,
          is_active,
          updated_at
        FROM {SILVER_TABLES['products']}
        """,
    )

    _ctas(
        spark,
        GOLD_TABLES["fact_orders"],
        f"""
        SELECT
          o.order_id,
          o.customer_id,
          o.product_id,
          o.quantity,
          o.order_ts,
          o.order_date,
          o.status,
          o.amount_usd
        FROM {SILVER_TABLES['orders']} o
        """,
    )

    _ctas(
        spark,
        GOLD_TABLES["daily_order_metrics"],
        f"""
        SELECT
          order_date,
          COUNT(*) AS order_count,
          SUM(quantity) AS units_sold,
          ROUND(SUM(amount_usd), 2) AS revenue_usd,
          COUNT(DISTINCT customer_id) AS unique_customers
        FROM {GOLD_TABLES['fact_orders']}
        GROUP BY order_date
        """,
    )

    for table in GOLD_TABLES.values():
        counts[table] = spark.table(table).count()
    return counts


def run_medallion(
    spark: SparkSession,
    n_customers: int = 1_000,
    n_products: int = 200,
    n_orders: int = 5_000,
    n_events: int = 20_000,
    n_reviews: int = 2_000,
    seed: int = 42,
) -> dict[str, dict[str, int]]:
    """End-to-end bronze → silver → gold for the monitored e-commerce domain."""
    generator = EcommerceGenerator(
        seed=seed,
        n_customers=n_customers,
        n_products=n_products,
        n_orders=n_orders,
        n_events=n_events,
        n_reviews=n_reviews,
    )
    bronze = write_bronze(spark, generator)
    silver = build_silver(spark)
    gold = build_gold(spark)
    return {"bronze": bronze, "silver": silver, "gold": gold}
