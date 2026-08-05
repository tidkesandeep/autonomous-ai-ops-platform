"""Runbook chunking, Delta persistence, and numpy cosine search."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.agent.embeddings import backend_fingerprint, cosine_similarity, embed_texts
from src.common.constants import OPS_GOLD

TABLE_FORMAT = "delta"


def embeddings_table_name() -> str:
    return f"{OPS_GOLD}.runbook_embeddings"


@dataclass(frozen=True)
class RunbookChunk:
    chunk_id: str
    runbook_path: str
    failure_type: str | None
    title: str
    text: str


FAILURE_FROM_NAME = {
    "schema_drift": "schema_drift",
    "null_spike": "null_spike",
    "volume_anomaly": "volume_anomaly",
    "duplicate_explosion": "duplicate_explosion",
    "late_data": "late_data",
    "job_crash": "job_crash",
}


def chunk_markdown(path: Path, text: str, *, max_chars: int = 900) -> list[RunbookChunk]:
    title = path.stem.replace("_", " ").title()
    failure = FAILURE_FROM_NAME.get(path.stem)
    parts = re.split(r"\n(?=## )", text.strip())
    chunks: list[RunbookChunk] = []
    for i, part in enumerate(parts):
        body = part.strip()
        if not body:
            continue
        if len(body) > max_chars:
            body = body[: max_chars - 3] + "..."
        chunks.append(
            RunbookChunk(
                chunk_id=f"{path.stem}-{i}",
                runbook_path=str(path.as_posix()),
                failure_type=failure,
                title=title,
                text=body,
            )
        )
    return chunks


def load_runbook_chunks(runbooks_dir: str | Path) -> list[RunbookChunk]:
    root = Path(runbooks_dir)
    chunks: list[RunbookChunk] = []
    for path in sorted(root.glob("*.md")):
        chunks.extend(chunk_markdown(path, path.read_text(encoding="utf-8")))
    if not chunks:
        raise FileNotFoundError(f"No runbook markdown found in {root}")
    return chunks


def rebuild_runbook_embeddings(
    spark: Any,
    runbooks_dir: str | Path,
    *,
    table: str | None = None,
) -> dict[str, Any]:
    """Idempotent rebuild of ops.runbook_embeddings from docs/runbooks."""
    table = table or embeddings_table_name()
    chunks = load_runbook_chunks(runbooks_dir)
    vectors, backend = embed_texts([c.text for c in chunks])
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = []
    for chunk, vec in zip(chunks, vectors, strict=True):
        rows.append(
            {
                "chunk_id": chunk.chunk_id,
                "runbook_path": chunk.runbook_path,
                "failure_type": chunk.failure_type,
                "title": chunk.title,
                "chunk_text": chunk.text,
                "embedding_json": json.dumps(vec),
                "embedding_backend": backend,
                "embedded_at": now,
            }
        )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OPS_GOLD}")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    (
        spark.createDataFrame(rows)
        .write.format(TABLE_FORMAT)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    return {"chunks": len(rows), "backend": backend, "table": table}


def load_embeddings_from_spark(spark: Any, table: str | None = None) -> list[dict[str, Any]]:
    table = table or embeddings_table_name()
    rows = [r.asDict(recursive=True) for r in spark.table(table).collect()]
    for row in rows:
        row["embedding"] = json.loads(row["embedding_json"])
    return rows


def search_runbooks(
    query: str,
    corpus: list[dict[str, Any]],
    *,
    top_k: int = 3,
    failure_type: str | None = None,
) -> list[dict[str, Any]]:
    """Cosine search over precomputed embeddings (numpy)."""
    if not corpus:
        return []
    backend = corpus[0].get("embedding_backend") or backend_fingerprint()
    prefer_api = backend.startswith("api:")
    qvec, _ = embed_texts([query], prefer_api=prefer_api)
    query_vec = qvec[0]

    scored: list[dict[str, Any]] = []
    for row in corpus:
        row_ft = row.get("failure_type")
        if failure_type and row_ft not in {None, failure_type}:
            continue
        score = cosine_similarity(query_vec, row["embedding"])
        scored.append(
            {
                "chunk_id": row["chunk_id"],
                "runbook_path": row["runbook_path"],
                "title": row["title"],
                "failure_type": row.get("failure_type"),
                "chunk_text": row["chunk_text"],
                "score": score,
            }
        )
    scored.sort(key=lambda r: r["score"], reverse=True)
    if failure_type:
        # Boost exact failure-type matches
        scored.sort(key=lambda r: (r.get("failure_type") == failure_type, r["score"]), reverse=True)
    return scored[:top_k]
