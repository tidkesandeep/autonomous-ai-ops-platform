"""Retry-with-adjusted-config remediation (serverless-safe knobs only)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.common.constants import OPS_GOLD

TABLE_FORMAT = "delta"


def apply_retry_adjusted_config(
    spark: Any,
    *,
    incident_id: str,
    pipeline_key: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    """Record adjusted job knobs and mark the remediation as applied.

    On serverless Free Edition there is no cluster-size knob. We persist the
    approved parameter overlay (shuffle partitions, maxRecordsPerFile,
    lookback_hours, max_retries) into ``ops.gold.remediation_runs`` so a
    subsequent pipeline re-run / Jobs API caller can consume them.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    table = f"{OPS_GOLD}.remediation_runs"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          incident_id STRING,
          remediation_type STRING,
          result_json STRING,
          ran_at TIMESTAMP
        ) USING {TABLE_FORMAT}
        """
    )
    payload = {
        "ok": True,
        "remediation_type": "retry_adjusted_config",
        "pipeline_key": pipeline_key,
        "adjusted_config": parameters,
        "note": "Config overlay recorded; re-run the pipeline job with these knobs.",
    }
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("remediation_type", StringType(), False),
            StructField("result_json", StringType(), False),
            StructField("ran_at", TimestampType(), False),
        ]
    )
    row = {
        "incident_id": incident_id,
        "remediation_type": "retry_adjusted_config",
        "result_json": json.dumps(payload, default=str),
        "ran_at": datetime.now(UTC).replace(tzinfo=None),
    }
    spark.createDataFrame([row], schema=schema).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)
    return payload
