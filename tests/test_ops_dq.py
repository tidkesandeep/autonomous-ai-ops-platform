from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from src.common.spark_local import local_spark
from src.ops import dq


@pytest.fixture(scope="module")
def spark():
    session = local_spark(app_name="ops-dq-tests")
    yield session
    session.stop()


def test_run_dq_checks_local(spark, monkeypatch):
    # Map ops gold to local 2-level DB for spark local mode.
    demo_db = f"demo_silver_{uuid4().hex[:8]}"
    ops_db = f"ops_gold_{uuid4().hex[:8]}"
    monkeypatch.setattr(dq, "OPS_GOLD", ops_db)
    monkeypatch.setattr(dq, "TABLE_FORMAT", "parquet")
    spark.sql(f"CREATE DATABASE {demo_db}")
    spark.sql(f"CREATE DATABASE {ops_db}")

    spark.sql(
        f"""
        CREATE TABLE {demo_db}.orders (
          order_id STRING,
          customer_id STRING,
          amount_usd DOUBLE
        ) USING parquet
        """
    )
    spark.sql(
        f"""
        INSERT INTO {demo_db}.orders VALUES
        ('o1', 'c1', 10.0),
        ('o2', 'c2', 8.0)
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {demo_db}.events (
          lag_minutes DOUBLE
        ) USING parquet
        """
    )
    spark.sql(f"INSERT INTO {demo_db}.events VALUES (10.0), (20.0), (30.0)")

    spec = Path("/tmp/test_dq_checks.yml")
    spec.write_text(
        f"""
checks:
  - name: non_negative_amount
    table: {demo_db}.orders
    metric_field: bad_rows
    comparator: "=="
    threshold: 0
    query: |
      SELECT COUNT(*) AS bad_rows FROM {demo_db}.orders WHERE amount_usd < 0
  - name: events_lag_cap
    table: {demo_db}.events
    metric_field: pct_late
    comparator: "<="
    threshold: 0.05
    query: |
      SELECT AVG(CASE WHEN lag_minutes > 2880 THEN 1.0 ELSE 0.0 END) AS pct_late FROM {demo_db}.events
"""
    )

    target_table = f"{ops_db}.fact_dq_check"
    summary = dq.run_dq_checks(
        spark,
        checks_path=str(spec),
        run_id="unit_run_1",
        pipeline_key="orders_ingest",
        table_name=target_table,
    )
    assert summary["checks_total"] == 2
    assert summary["checks_failed"] == 0
    assert spark.table(target_table).count() == 2
