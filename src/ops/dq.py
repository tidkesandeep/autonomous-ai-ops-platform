"""YAML-driven DQ checks with results written to ops gold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from src.common.constants import OPS_GOLD

TABLE_FORMAT = "delta"


@dataclass
class DQResult:
    run_id: str
    pipeline_key: str
    check_name: str
    table_name: str
    metric_name: str
    observed_value: float
    threshold_value: float
    comparator: str
    passed: bool
    checked_at: datetime


def _eval(observed: float, comparator: str, threshold: float) -> bool:
    if comparator == "<=":
        return observed <= threshold
    if comparator == "<":
        return observed < threshold
    if comparator == ">=":
        return observed >= threshold
    if comparator == ">":
        return observed > threshold
    if comparator == "==":
        return observed == threshold
    raise ValueError(f"Unsupported comparator: {comparator}")


def load_checks(path: str) -> list[dict[str, Any]]:
    data = yaml.safe_load(Path(path).read_text())
    checks = data.get("checks", []) if isinstance(data, dict) else []
    if not checks:
        raise ValueError(f"No checks found in {path}")
    return checks


def run_dq_checks(
    spark: Any,
    checks_path: str,
    run_id: str,
    pipeline_key: str,
    table_name: str = f"{OPS_GOLD}.fact_dq_check",
) -> dict[str, int]:
    """Execute DQ checks and append results to ops.gold.fact_dq_check."""
    checks = load_checks(checks_path)
    now = datetime.now(UTC)
    rows: list[dict[str, Any]] = []

    for check in checks:
        result_df = spark.sql(check["query"])
        result_row = result_df.collect()[0].asDict()
        observed = float(result_row[check["metric_field"]])
        threshold = float(check["threshold"])
        comparator = check["comparator"]
        passed = _eval(observed, comparator, threshold)
        rows.append(
            {
                "run_id": run_id,
                "pipeline_key": pipeline_key,
                "check_name": check["name"],
                "table_name": check["table"],
                "metric_name": check["metric_field"],
                "observed_value": observed,
                "threshold_value": threshold,
                "comparator": comparator,
                "passed": passed,
                "checked_at": now.replace(tzinfo=None),
            }
        )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          run_id STRING,
          pipeline_key STRING,
          check_name STRING,
          table_name STRING,
          metric_name STRING,
          observed_value DOUBLE,
          threshold_value DOUBLE,
          comparator STRING,
          passed BOOLEAN,
          checked_at TIMESTAMP
        )
        USING {TABLE_FORMAT}
        """
    )
    spark.createDataFrame(rows).write.mode("append").format(TABLE_FORMAT).saveAsTable(table_name)
    passed_count = sum(1 for r in rows if r["passed"])
    return {"checks_total": len(rows), "checks_passed": passed_count, "checks_failed": len(rows) - passed_count}
