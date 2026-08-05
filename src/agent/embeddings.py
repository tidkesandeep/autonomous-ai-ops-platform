"""Embedding helpers — LiteLLM API when keyed, else deterministic hash vectors.

Corpus and query MUST use the same backend. Hash embeddings keep RAG usable on
Free Edition without Gemini/Groq secrets; swap to API by setting GEMINI_API_KEY.
"""

from __future__ import annotations

import hashlib
import os
import re

import numpy as np

EMBED_DIM = 256
DEFAULT_API_MODEL = "gemini/text-embedding-004"


def _tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9_]+", text.lower())


def hash_embed(text: str, dim: int = EMBED_DIM) -> list[float]:
    """Deterministic bag-of-tokens hashing trick (same for corpus + query)."""
    vec = np.zeros(dim, dtype=np.float64)
    tokens = _tokenize(text)
    if not tokens:
        return vec.tolist()
    for tok in tokens:
        digest = hashlib.sha256(tok.encode("utf-8")).digest()
        idx = int.from_bytes(digest[:4], "little") % dim
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        vec[idx] += sign
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec.tolist()


def api_embed(text: str, model: str | None = None) -> list[float]:
    """Embed via LiteLLM (Gemini text-embedding-004 by default)."""
    from litellm import embedding

    model = model or os.environ.get("EMBEDDING_MODEL", DEFAULT_API_MODEL)
    resp = embedding(model=model, input=[text])
    # litellm returns OpenAI-like shape
    data = resp["data"] if isinstance(resp, dict) else resp.data
    first = data[0]
    vec = first["embedding"] if isinstance(first, dict) else first.embedding
    arr = np.asarray(vec, dtype=np.float64)
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr = arr / norm
    return arr.tolist()


def embed_texts(texts: list[str], *, prefer_api: bool | None = None) -> tuple[list[list[float]], str]:
    """Embed many texts. Returns (vectors, backend_name)."""
    if prefer_api is None:
        prefer_api = bool(os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    if prefer_api:
        try:
            return [api_embed(t) for t in texts], "api:" + os.environ.get("EMBEDDING_MODEL", DEFAULT_API_MODEL)
        except Exception:
            # Fall through to hash — never mix backends within one rebuild
            pass
    return [hash_embed(t) for t in texts], "hash"


def cosine_similarity(a: list[float] | np.ndarray, b: list[float] | np.ndarray) -> float:
    aa = np.asarray(a, dtype=np.float64)
    bb = np.asarray(b, dtype=np.float64)
    denom = float(np.linalg.norm(aa) * np.linalg.norm(bb))
    if denom == 0:
        return 0.0
    return float(np.dot(aa, bb) / denom)


def backend_fingerprint() -> str:
    if os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"):
        return "api:" + os.environ.get("EMBEDDING_MODEL", DEFAULT_API_MODEL)
    return "hash"
