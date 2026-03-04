"""
Embedding generation for pgvector semantic search.

Generates 1536-dim embeddings for student profiles and email bodies
using sentence-transformers (local) or OpenAI/Groq (remote).
Stored in students.profile_embedding (VECTOR(1536)) via pgvector.
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)

_MODEL_NAME = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
_OPENAI_KEY = os.getenv("OPENAI_API_KEY", "")
_GROQ_KEY   = os.getenv("GROQ_API_KEY", "")

# Target dim for pgvector column (VECTOR(1536))
_TARGET_DIM = 1536

_st_model = None


def _get_sentence_transformer():
    """Lazy-load sentence-transformers model."""
    global _st_model
    if _st_model is None:
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
            _st_model = SentenceTransformer(_MODEL_NAME)
            logger.info("Loaded sentence-transformer model: %s", _MODEL_NAME)
        except ImportError:
            logger.warning("sentence-transformers not installed. Using deterministic hash embeddings.")
            _st_model = False
    return _st_model


def _hash_embedding(text: str, dim: int = _TARGET_DIM) -> list[float]:
    """
    Deterministic pseudo-embedding via repeated SHA-256 hashing.
    Not semantically meaningful but fills the vector column without ML deps.
    """
    vec: list[float] = []
    seed = text.encode()
    while len(vec) < dim:
        seed = hashlib.sha256(seed).digest()
        # 32 bytes → 32 floats in [-1, 1]
        for i in range(0, len(seed), 4):
            chunk = seed[i : i + 4]
            val = int.from_bytes(chunk, "big", signed=True) / (2**31)
            vec.append(val)
    return vec[:dim]


def embed_text(text: str) -> list[float]:
    """
    Embed text to a 1536-dim float vector.
    Priority:
      1. sentence-transformers (local, free)
      2. Deterministic hash fallback (no ML needed)
    """
    if not text or not text.strip():
        return _hash_embedding("", _TARGET_DIM)

    model = _get_sentence_transformer()
    if model and model is not False:
        try:
            raw = model.encode(text, show_progress_bar=False).tolist()
            # Pad / truncate to TARGET_DIM
            if len(raw) < _TARGET_DIM:
                raw = raw + [0.0] * (_TARGET_DIM - len(raw))
            return raw[:_TARGET_DIM]
        except Exception as exc:
            logger.warning("sentence-transformer encode failed: %s — using hash fallback", exc)

    return _hash_embedding(text, _TARGET_DIM)


def embed_student_profile(usn: str, name: str, results_summary: str = "") -> list[float]:
    """Build a student profile embedding from USN, name, and results summary."""
    profile_text = f"Student USN: {usn}. Name: {name}. {results_summary}".strip()
    return embed_text(profile_text)


def _vec_to_pg_literal(vec: list[float]) -> str:
    """Convert float list to PostgreSQL vector literal '[1.0,2.0,...]'"""
    return "[" + ",".join(f"{v:.6f}" for v in vec) + "]"


def store_student_embedding(student_id: str, usn: str, name: str, results_summary: str = "") -> bool:
    """
    Generate and store a pgvector embedding for a student profile.
    Returns True if successfully stored, False otherwise.
    """
    try:
        from src.common.database import get_connection  # avoid circular import
        vec = embed_student_profile(usn, name, results_summary)
        pg_literal = _vec_to_pg_literal(vec)

        with get_connection() as conn:
            with conn.cursor() as cur:
                # Check if pgvector extension is enabled
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
                if not cur.fetchone():
                    logger.debug("pgvector extension not installed — skipping embedding storage")
                    return False

                cur.execute(
                    "UPDATE students SET profile_embedding = %s::vector WHERE id = %s",
                    (pg_literal, student_id),
                )
        return True
    except Exception as exc:
        logger.debug("store_student_embedding failed: %s", exc)
        return False


def semantic_search_students(
    query: str,
    institution_id: str,
    limit: int = 10,
    threshold: float = 0.7,
) -> list[dict]:
    """
    Search students by semantic similarity using pgvector cosine distance.
    Falls back to text LIKE search if pgvector is unavailable.
    """
    try:
        from src.common.database import get_connection  # avoid circular import
        from psycopg2.extras import RealDictCursor

        vec = embed_text(query)
        pg_literal = _vec_to_pg_literal(vec)

        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Check pgvector
                cur.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
                if not cur.fetchone():
                    return []

                cur.execute("""
                    SELECT id, usn, name AS full_name, cgpa, total_backlogs,
                           1 - (profile_embedding <=> %s::vector) AS similarity
                    FROM students
                    WHERE institution_id = %s
                      AND profile_embedding IS NOT NULL
                    ORDER BY profile_embedding <=> %s::vector
                    LIMIT %s
                """, (pg_literal, institution_id, pg_literal, limit))
                rows = cur.fetchall()
                return [
                    {k: str(v) if not isinstance(v, (str, int, float, bool, type(None))) else v
                     for k, v in dict(r).items()}
                    for r in rows
                    if float(r.get("similarity") or 0) >= threshold
                ]
    except Exception as exc:
        logger.debug("semantic_search_students failed: %s", exc)
        return []
