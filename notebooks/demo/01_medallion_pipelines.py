"""
# Demo Medallion — Bronze → Silver → Gold

Runs the monitored e-commerce pipelines end-to-end.

Widgets control scale so Free Edition stays comfortable.
"""

# Databricks notebook source
# MAGIC %pip install faker --quiet
# MAGIC
# COMMAND ----------

dbutils.widgets.text("n_customers", "1000")
dbutils.widgets.text("n_products", "200")
dbutils.widgets.text("n_orders", "5000")
dbutils.widgets.text("n_events", "20000")
dbutils.widgets.text("n_reviews", "2000")
dbutils.widgets.text("seed", "42")
dbutils.widgets.text("demo_catalog", "demo")

# COMMAND ----------

import sys

sys.path.append("/Workspace/Repos/autonomous-ai-ops-platform")

from src.demo.pipelines import run_medallion

counts = run_medallion(
    spark,
    n_customers=int(dbutils.widgets.get("n_customers")),
    n_products=int(dbutils.widgets.get("n_products")),
    n_orders=int(dbutils.widgets.get("n_orders")),
    n_events=int(dbutils.widgets.get("n_events")),
    n_reviews=int(dbutils.widgets.get("n_reviews")),
    seed=int(dbutils.widgets.get("seed")),
)

for layer, tables in counts.items():
    print(f"=== {layer} ===")
    for table, n in tables.items():
        print(f"  {table}: {n:,} rows")
