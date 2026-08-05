"""Deterministic read tools for the investigation agent."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from typing import Any

from src.agent.rag import load_embeddings_from_spark, search_runbooks
from src.common.constants import OPS_BRONZE, OPS_GOLD


def _rows(spark: Any, sql: str) -> list[dict[str, Any]]:
    try:
        return [r.asDict(recursive=True) for r in spark.sql(sql).collect()]
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc), "sql": sql[:200]}]


def query_run_history(spark: Any, pipeline_key: str, *, limit: int = 10) -> list[dict[str, Any]]:
    return _rows(
        spark,
        f"""
        SELECT run_id, pipeline_key, task_name, status, rows_in, rows_out, duration_ms, ended_at, error_class
        FROM {OPS_BRONZE}.task_telemetry
        WHERE pipeline_key = '{pipeline_key}'
        ORDER BY ended_at DESC
        LIMIT {int(limit)}
        """,
    )


def get_dq_failures(spark: Any, run_id: str) -> list[dict[str, Any]]:
    return _rows(
        spark,
        f"""
        SELECT run_id, pipeline_key, check_name, table_name, metric_name,
               observed_value, threshold_value, comparator, passed, checked_at
        FROM {OPS_GOLD}.fact_dq_check
        WHERE run_id = '{run_id}' AND passed = false
        ORDER BY checked_at DESC
        """,
    )


def read_task_logs(spark: Any, run_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
    return _rows(
        spark,
        f"""
        SELECT run_id, task_key, result_state, lifecycle_state, error_signature,
               substring(raw_output, 1, 1500) AS raw_output_excerpt, collected_at
        FROM {OPS_BRONZE}.raw_task_logs
        WHERE run_id = '{run_id}'
        ORDER BY collected_at DESC
        LIMIT {int(limit)}
        """,
    )


def diff_schema(spark: Any, pipeline_key: str) -> dict[str, Any]:
    rows = _rows(
        spark,
        f"""
        SELECT run_id, schema_snapshot_json, ended_at
        FROM {OPS_BRONZE}.task_telemetry
        WHERE pipeline_key = '{pipeline_key}' AND schema_snapshot_json IS NOT NULL
        ORDER BY ended_at DESC
        LIMIT 2
        """,
    )
    if len(rows) < 2 or "error" in rows[0]:
        return {"pipeline_key": pipeline_key, "diff": None, "reason": "insufficient schema snapshots", "rows": rows}
    try:
        current = json.loads(rows[0]["schema_snapshot_json"])
        previous = json.loads(rows[1]["schema_snapshot_json"])
    except (TypeError, json.JSONDecodeError) as exc:
        return {"pipeline_key": pipeline_key, "diff": None, "reason": str(exc)}
    cur_cols, prev_cols = set(current), set(previous)
    return {
        "pipeline_key": pipeline_key,
        "current_run_id": rows[0].get("run_id"),
        "previous_run_id": rows[1].get("run_id"),
        "added": sorted(cur_cols - prev_cols),
        "removed": sorted(prev_cols - cur_cols),
        "type_changes": [
            {"column": c, "from": previous.get(c), "to": current.get(c)}
            for c in sorted(cur_cols & prev_cols)
            if previous.get(c) != current.get(c)
        ],
    }


def sample_bad_records(spark: Any, table_name: str, *, limit: int = 5) -> list[dict[str, Any]]:
    """Best-effort sample — prefers rows with NULLs if present."""
    safe = table_name.replace("`", "")
    try:
        cols = [f.name for f in spark.table(safe).schema.fields]
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc), "table": table_name}]
    null_preds = " OR ".join([f"`{c}` IS NULL" for c in cols[:8]]) or "1=0"
    return _rows(
        spark,
        f"""
        SELECT * FROM {safe}
        WHERE {null_preds}
        LIMIT {int(limit)}
        """,
    )


def time_travel_compare(spark: Any, table_name: str) -> dict[str, Any]:
    """Compare current count vs ~1 day ago when Delta history exists."""
    safe = table_name.replace("`", "")
    try:
        current = spark.table(safe).count()
    except Exception as exc:  # noqa: BLE001
        return {"table": table_name, "error": str(exc)}
    ts = (datetime.now(UTC) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        past = spark.sql(f"SELECT COUNT(*) AS n FROM {safe} TIMESTAMP AS OF '{ts}'").collect()[0]["n"]
        return {"table": table_name, "current_count": current, "count_1d_ago": int(past), "as_of": ts}
    except Exception as exc:  # noqa: BLE001
        return {
            "table": table_name,
            "current_count": current,
            "count_1d_ago": None,
            "note": f"time travel unavailable: {exc}",
        }


def search_runbooks_tool(
    spark: Any,
    query: str,
    *,
    failure_type: str | None = None,
    top_k: int = 3,
    table: str | None = None,
) -> list[dict[str, Any]]:
    try:
        corpus = load_embeddings_from_spark(spark, table=table)
    except Exception as exc:  # noqa: BLE001
        return [{"error": f"embeddings unavailable: {exc}"}]
    return search_runbooks(query, corpus, top_k=top_k, failure_type=failure_type)


def correlate_github_commits(
    pipeline_key: str,
    *,
    repo: str | None = None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """List recent commits; requires GITHUB_TOKEN. Returns empty list if unset."""
    token = os.environ.get("GITHUB_TOKEN")
    repo = repo or os.environ.get("GITHUB_REPO", "tidkesandeep/autonomous-ai-ops-platform")
    if not token:
        return [
            {
                "note": "GITHUB_TOKEN unset — skipped live correlation",
                "pipeline_key": pipeline_key,
                "repo": repo,
            }
        ]
    try:
        from github import Github

        gh = Github(token)
        repository = gh.get_repo(repo)
        path_hints = {
            "orders_ingest": "src/demo",
            "customers_scd2": "src/demo",
            "products_catalog": "src/demo",
            "events_clickstream": "src/demo",
            "reviews_enrichment": "src/demo",
            "ops_force_fail": "src/ops",
        }
        hint = path_hints.get(pipeline_key, "src/")
        commits = repository.get_commits()[: limit * 3]
        out = []
        for c in commits:
            files = [f.filename for f in c.files] if c.files else []
            if hint and not any(hint in f for f in files):
                continue
            out.append(
                {
                    "sha": c.sha[:12],
                    "message": (c.commit.message or "").split("\n")[0][:200],
                    "author": c.commit.author.name if c.commit.author else None,
                    "date": c.commit.author.date.isoformat() if c.commit.author and c.commit.author.date else None,
                    "files": files[:10],
                }
            )
            if len(out) >= limit:
                break
        return out or [{"note": "no matching commits for path hint", "hint": hint, "repo": repo}]
    except Exception as exc:  # noqa: BLE001
        return [{"error": str(exc), "repo": repo}]
