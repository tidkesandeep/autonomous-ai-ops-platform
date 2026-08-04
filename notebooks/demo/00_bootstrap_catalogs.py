"""
# Bootstrap Unity Catalog catalogs

Creates `demo` and `ops` catalogs/schemas used by the platform.
Run once per workspace (Free Edition: may already have a default catalog —
adjust names if needed).
"""

# Databricks notebook source

dbutils.widgets.text("demo_catalog", "demo")
dbutils.widgets.text("ops_catalog", "ops")

demo = dbutils.widgets.get("demo_catalog")
ops = dbutils.widgets.get("ops_catalog")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {demo}")
spark.sql(f"CREATE CATALOG IF NOT EXISTS {ops}")

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {demo}.{schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ops}.{schema}")

print("Catalogs ready:")
spark.sql(f"SHOW SCHEMAS IN {demo}").show()
spark.sql(f"SHOW SCHEMAS IN {ops}").show()
