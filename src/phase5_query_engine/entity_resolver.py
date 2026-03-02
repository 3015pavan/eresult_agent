"""
Entity Resolver.

Resolves extracted entities (USN, student name, department, subject)
to canonical database identifiers using fuzzy matching.

Resolution strategies:
  1. Exact match: USN, subject code
  2. Fuzzy match: Student name (Levenshtein), subject name (trigram)
  3. Alias resolution: Department abbreviations, common misspellings
  4. Embedding similarity: Semantic matching for subject names
"""

from __future__ import annotations

from typing import Any

import jellyfish

from src.common.config import get_settings
from src.common.models import ParsedQuery
from src.common.observability import get_logger

logger = get_logger(__name__)


# Department alias mappings
DEPARTMENT_ALIASES: dict[str, str] = {
    "cs": "CSE",
    "cse": "CSE",
    "computer science": "CSE",
    "comp sci": "CSE",
    "is": "ISE",
    "ise": "ISE",
    "information science": "ISE",
    "ec": "ECE",
    "ece": "ECE",
    "electronics": "ECE",
    "ee": "EEE",
    "eee": "EEE",
    "electrical": "EEE",
    "me": "ME",
    "mechanical": "ME",
    "cv": "CVE",
    "civil": "CVE",
    "ai": "AIML",
    "aiml": "AIML",
    "ml": "AIML",
    "machine learning": "AIML",
    "it": "IT",
    "information technology": "IT",
}


class EntityResolver:
    """
    Resolve query entities to canonical database identifiers.

    Resolution pipeline:
      1. Normalize: lowercase, strip, expand abbreviations
      2. Exact match: Try direct lookup
      3. Fuzzy match: Levenshtein distance ≤ threshold
      4. Embedding match: Vector similarity for ambiguous cases
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self._db_pool = None

    async def resolve(
        self,
        parsed: ParsedQuery,
    ) -> ParsedQuery:
        """
        Resolve all entities in a parsed query.

        Mutates the ParsedQuery.entities with resolved IDs.
        """
        entities = parsed.entities

        # Resolve USN
        if entities.get("usn"):
            resolved = await self._resolve_usn(entities["usn"])
            if resolved:
                entities["student_id"] = resolved["student_id"]
                entities["usn"] = resolved["usn"]

        # Resolve student name
        if entities.get("student_name") and not entities.get("student_id"):
            resolved = await self._resolve_student_name(
                entities["student_name"],
            )
            if resolved:
                entities["student_id"] = resolved["student_id"]
                entities["usn"] = resolved["usn"]

        # Resolve department
        if entities.get("department"):
            entities["department"] = self._resolve_department(
                entities["department"],
            )

        # Resolve subject
        if entities.get("subject"):
            resolved = await self._resolve_subject(entities["subject"])
            if resolved:
                entities["subject_id"] = resolved["subject_id"]
                entities["subject_code"] = resolved["subject_code"]

        logger.info(
            "entities_resolved",
            original=parsed.entities,
            resolved=entities,
        )

        parsed.entities = entities
        return parsed

    def _resolve_department(self, dept_input: str) -> str:
        """Resolve department alias to canonical code."""
        normalized = dept_input.lower().strip()
        return DEPARTMENT_ALIASES.get(normalized, dept_input.upper())

    async def _resolve_usn(self, usn: str) -> dict[str, Any] | None:
        """Resolve USN to student record."""
        pool = await self._get_pool()
        if not pool:
            return None

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id as student_id, usn, name
                FROM students
                WHERE usn = $1
                """,
                usn.upper().strip(),
            )

        if row:
            return dict(row)
        return None

    async def _resolve_student_name(
        self,
        name: str,
    ) -> dict[str, Any] | None:
        """Resolve student name with fuzzy matching."""
        pool = await self._get_pool()
        if not pool:
            return None

        # Try exact match first
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id as student_id, usn, name
                FROM students
                WHERE LOWER(name) = LOWER($1)
                """,
                name.strip(),
            )
            if row:
                return dict(row)

            # Fuzzy match with pg_trgm
            rows = await conn.fetch(
                """
                SELECT id as student_id, usn, name,
                       similarity(name, $1) as sim
                FROM students
                WHERE similarity(name, $1) > 0.3
                ORDER BY sim DESC
                LIMIT 5
                """,
                name.strip(),
            )

        if not rows:
            return None

        # Apply Levenshtein as secondary filter
        best_match = None
        best_score = 0.0

        for row in rows:
            db_name = row["name"].lower()
            query_name = name.lower().strip()

            # Jaro-Winkler similarity (0-1, higher is better)
            jw = jellyfish.jaro_winkler_similarity(query_name, db_name)

            # Combined score
            combined = 0.6 * row["sim"] + 0.4 * jw

            if combined > best_score and combined > 0.5:
                best_score = combined
                best_match = dict(row)

        if best_match:
            logger.info(
                "fuzzy_name_match",
                query=name,
                matched=best_match["name"],
                score=best_score,
            )

        return best_match

    async def _resolve_subject(
        self,
        subject_input: str,
    ) -> dict[str, Any] | None:
        """Resolve subject by code or name with fuzzy matching."""
        pool = await self._get_pool()
        if not pool:
            return None

        async with pool.acquire() as conn:
            # Try exact code match
            row = await conn.fetchrow(
                """
                SELECT id as subject_id, code as subject_code, name
                FROM subjects
                WHERE UPPER(code) = UPPER($1)
                """,
                subject_input.strip(),
            )
            if row:
                return dict(row)

            # Fuzzy name match
            rows = await conn.fetch(
                """
                SELECT id as subject_id, code as subject_code, name,
                       similarity(name, $1) as sim
                FROM subjects
                WHERE similarity(name, $1) > 0.3
                ORDER BY sim DESC
                LIMIT 3
                """,
                subject_input.strip(),
            )

        if rows:
            return dict(rows[0])
        return None

    async def _get_pool(self):
        """Get or create database pool."""
        if self._db_pool is None:
            try:
                import asyncpg
                self._db_pool = await asyncpg.create_pool(
                    dsn=self.settings.database.url,
                    min_size=2,
                    max_size=5,
                )
            except Exception as e:
                logger.warning("db_pool_init_failed", error=str(e))
                return None
        return self._db_pool
