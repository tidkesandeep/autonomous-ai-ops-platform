"""Schema-evolution DDL generation (and safe additive apply for demo drift)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from src.common.constants import DEMO_BRONZE, OPS_GOLD

TABLE_FORMAT = "delta"


def generate_schema_ddl(
    spark: Any,
    *,
    source_table: str | None = None,
    pipeline_key: str = "products_catalog",
) -> dict[str, Any]:
    """Inspect drifted bronze products and emit additive DDL + optional rename map."""
    table = source_table or f"{DEMO_BRONZE}.raw_products"
    try:
        cols = spark.table(table).columns
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc), "table": table}

    ddl_statements: list[str] = []
    notes: list[str] = []

    # Chaos renames category → product_category_v2; propose restoring alias column.
    if "product_category_v2" in cols and "category" not in cols:
        ddl = f"ALTER TABLE {table} ADD COLUMN category STRING COMMENT 'restored alias for product_category_v2'"
        ddl_statements.append(ddl)
        notes.append("Detected renamed category → product_category_v2; additive restore proposed.")
    elif "unexpected_col" in cols:
        notes.append("unexpected_col present — review before DROP; no auto-drop emitted.")
        ddl_statements.append(
            f"-- REVIEW: ALTER TABLE {table} DROP COLUMN unexpected_col;  -- destructive, not auto-applied"
        )
    else:
        notes.append("No known demo drift pattern; emit no-op documentation DDL.")
        ddl_statements.append(f"-- No additive DDL required for {table} (cols={cols})")

    return {
        "ok": True,
        "table": table,
        "pipeline_key": pipeline_key,
        "columns": cols,
        "ddl_statements": ddl_statements,
        "notes": notes,
    }


def apply_additive_ddl(spark: Any, ddl_result: dict[str, Any]) -> dict[str, Any]:
    """Apply only safe ADD COLUMN restores; skip comments and DROP."""
    from pyspark.sql import functions as F

    applied: list[str] = []
    skipped: list[str] = []
    table = ddl_result.get("table")
    cols = set(ddl_result.get("columns") or [])

    for stmt in ddl_result.get("ddl_statements") or []:
        s = stmt.strip()
        if not s or s.startswith("--"):
            skipped.append(stmt)
            continue
        upper = s.upper()
        if "ADD COLUMN" in upper and "DROP" not in upper and table:
            # Prefer DataFrame rewrite — works on Delta and local parquet.
            if "category" in s.lower() and "product_category_v2" in cols and "category" not in cols:
                src = spark.table(table)
                df = spark.createDataFrame(
                    src.withColumn("category", F.col("product_category_v2")).collect(),
                    schema=src.schema.add("category", "string"),
                )
                writer = df.write.format(TABLE_FORMAT).mode("overwrite")
                if TABLE_FORMAT == "delta":
                    writer = writer.option("overwriteSchema", "true")
                writer.saveAsTable(table)
                applied.append(s)
            else:
                try:
                    spark.sql(s)
                    applied.append(s)
                except Exception as exc:  # noqa: BLE001
                    skipped.append(f"{stmt} ({exc})")
        else:
            skipped.append(stmt)
    return {"ok": True, "applied": applied, "skipped": skipped}


def run_schema_evolution(
    spark: Any,
    *,
    incident_id: str,
    pipeline_key: str,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    ddl = generate_schema_ddl(
        spark,
        source_table=parameters.get("table"),
        pipeline_key=pipeline_key,
    )
    apply_result = {"ok": False, "skipped": ["not requested"]}
    if parameters.get("action") in ("generate_and_apply_additive", "apply_additive") and ddl.get("ok"):
        apply_result = apply_additive_ddl(spark, ddl)

    payload = {
        "ok": bool(ddl.get("ok")),
        "remediation_type": "schema_evolution_ddl",
        "ddl": ddl,
        "apply": apply_result,
    }
    _record(spark, incident_id, payload)
    return payload


def _record(spark: Any, incident_id: str, payload: dict[str, Any]) -> None:
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

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
    # Also persist human-readable DDL proposals for the console.
    proposals = f"{OPS_GOLD}.schema_evolution_proposals"
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {proposals} (
          incident_id STRING,
          table_name STRING,
          ddl_text STRING,
          created_at TIMESTAMP
        ) USING {TABLE_FORMAT}
        """
    )
    schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("remediation_type", StringType(), False),
            StructField("result_json", StringType(), False),
            StructField("ran_at", TimestampType(), False),
        ]
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    spark.createDataFrame(
        [
            {
                "incident_id": incident_id,
                "remediation_type": "schema_evolution_ddl",
                "result_json": json.dumps(payload, default=str),
                "ran_at": now,
            }
        ],
        schema=schema,
    ).write.mode("append").format(TABLE_FORMAT).saveAsTable(table)

    ddl = payload.get("ddl") or {}
    ddl_text = "\n".join(ddl.get("ddl_statements") or [])
    p_schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("table_name", StringType(), True),
            StructField("ddl_text", StringType(), False),
            StructField("created_at", TimestampType(), False),
        ]
    )
    spark.createDataFrame(
        [
            {
                "incident_id": incident_id,
                "table_name": ddl.get("table"),
                "ddl_text": ddl_text,
                "created_at": now,
            }
        ],
        schema=p_schema,
    ).write.mode("append").format(TABLE_FORMAT).saveAsTable(proposals)
