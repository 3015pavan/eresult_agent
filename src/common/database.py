"""
PostgreSQL database layer using psycopg2.

Column mapping for schema 001_core_schema.sql:
  students:           name, name_normalized (both required)
  email_metadata:     partitioned by received_at; many NOT NULL fields
  extractions:        attachment_id can be NULL (we relax this below)
  student_results:    status = 'PASS' | 'FAIL' | 'ABSENT' | 'WITHHELD'
  semester_aggregates: several NOT NULL columns
"""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ─── Connection pool ──────────────────────────────────────────────────────────
_pool: Optional[ThreadedConnectionPool] = None
_DEFAULT_INSTITUTION_ID: Optional[str] = None


def _get_dsn() -> str:
    # Use pydantic settings so the .env file is respected (DB_PORT=5434 etc.)
    try:
        from src.common.config import get_settings
        db_cfg = get_settings().database
        return (
            f"host={db_cfg.host} "
            f"port={db_cfg.port} "
            f"dbname={db_cfg.name} "
            f"user={db_cfg.user} "
            f"password={db_cfg.password}"
        )
    except Exception:
        # Fallback to raw env vars if settings import fails
        return (
            f"host={os.getenv('DB_HOST', 'localhost')} "
            f"port={os.getenv('DB_PORT', '5432')} "
            f"dbname={os.getenv('DB_NAME', 'email_agent')} "
            f"user={os.getenv('DB_USER', 'app')} "
            f"password={os.getenv('DB_PASSWORD', 'app_secret')}"
        )


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        dsn = _get_dsn()
        _pool = ThreadedConnectionPool(minconn=2, maxconn=10, dsn=dsn)
        logger.info("PostgreSQL connection pool created")
    return _pool


def reset_pool() -> None:
    """Force recreation of the connection pool (e.g. after config change)."""
    global _pool
    if _pool is not None:
        try:
            _pool.closeall()
        except Exception:
            pass
        _pool = None


@contextmanager
def get_connection():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _normalise_name(name: str) -> str:
    """Produce a searchable lowercase-nospace version of a name."""
    return re.sub(r'\s+', ' ', name.upper().strip())


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─── Initialisation ───────────────────────────────────────────────────────────

def init_db() -> str:
    """
    Verify connectivity, create missing email_metadata partitions, and
    upsert the default institution returning its UUID.
    """
    global _DEFAULT_INSTITUTION_ID

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Default institution
            cur.execute("""
                INSERT INTO institutions (code, name)
                VALUES ('DEFAULT', 'AcadExtract')
                ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """)
            row = cur.fetchone()
            _DEFAULT_INSTITUTION_ID = str(row["id"])

            # Ensure a default partition exists for email_metadata
            # email_metadata is non-partitioned — skip partition creation
            logger.info("DB init complete. Default institution: %s", _DEFAULT_INSTITUTION_ID)

    return _DEFAULT_INSTITUTION_ID


def get_default_institution_id() -> str:
    global _DEFAULT_INSTITUTION_ID
    if _DEFAULT_INSTITUTION_ID is None:
        init_db()
    return _DEFAULT_INSTITUTION_ID


# ─── Subjects ─────────────────────────────────────────────────────────────────

def get_or_create_subject(
    institution_id: str, code: str, name: str, semester: Optional[int] = None
) -> str:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO subjects (institution_id, code, name, credits, semester)
                VALUES (%s, %s, %s, 3, %s)
                ON CONFLICT (institution_id, code) DO UPDATE SET name = EXCLUDED.name
                RETURNING id
            """, (institution_id, code.upper(), name or code, semester))
            return str(cur.fetchone()["id"])


# ─── Students ─────────────────────────────────────────────────────────────────

def upsert_student(
    usn: str,
    name: str,
    email: Optional[str] = None,
    institution_id: Optional[str] = None,
    department_id: Optional[str] = None,
    source: str = "pipeline",
) -> str:
    """Insert or update a student record. source='seed' for test data, 'pipeline' for real extracted data."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    normalized = _normalise_name(name or usn)
    meta = json.dumps({"source": source})
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO students
                    (institution_id, usn, name, name_normalized, email, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (institution_id, usn) DO UPDATE SET
                    name            = COALESCE(EXCLUDED.name,  students.name),
                    name_normalized = COALESCE(EXCLUDED.name_normalized, students.name_normalized),
                    email           = COALESCE(EXCLUDED.email, students.email),
                    updated_at      = NOW()
                RETURNING id
            """, (institution_id, usn.upper(), name or usn, normalized, email, meta))
            return str(cur.fetchone()["id"])


# ─── Results ──────────────────────────────────────────────────────────────────

def upsert_result(
    student_id: str,
    subject_id: str,
    semester: int,
    marks_obtained: Optional[float] = None,
    max_marks: float = 100,
    grade: Optional[str] = None,
    grade_points: Optional[float] = None,
    status: Optional[str] = None,
    exam_type: str = "regular",
    attempt_number: int = 1,
) -> str:
    # Schema CHECK: status IN ('PASS','FAIL','ABSENT','WITHHELD')
    if status is None:
        passed = (grade_points or 0) >= 4.0 or (marks_obtained or 0) >= 40
        status = "PASS" if passed else "FAIL"
    status = status.upper()
    if status not in ("PASS", "FAIL", "ABSENT", "WITHHELD"):
        status = "PASS"

    # Schema CHECK: exam_type IN ('regular','supplementary','improvement')
    if exam_type.lower() not in ("regular", "supplementary", "improvement"):
        exam_type = "regular"
    exam_type = exam_type.lower()

    total = int(marks_obtained or 0)

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO student_results
                    (student_id, subject_id, semester, total_marks, max_marks,
                     grade, grade_points, status, exam_type, attempt_number)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (student_id, subject_id, semester, exam_type, attempt_number)
                DO UPDATE SET
                    total_marks  = COALESCE(EXCLUDED.total_marks,  student_results.total_marks),
                    grade        = COALESCE(EXCLUDED.grade,        student_results.grade),
                    grade_points = COALESCE(EXCLUDED.grade_points, student_results.grade_points),
                    status       = EXCLUDED.status
                RETURNING id
            """, (student_id, subject_id, semester, total, int(max_marks),
                  grade, grade_points, status, exam_type, attempt_number))
            return str(cur.fetchone()["id"])


def store_semester_aggregate(
    student_id: str,
    semester: int,
    sgpa: Optional[float] = None,
    total_marks: Optional[float] = None,
    max_marks: Optional[float] = None,
    credits_earned: Optional[int] = None,
    backlogs: int = 0,
):
    safe_sgpa    = sgpa    if sgpa    else None
    safe_credits = credits_earned or 0

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Count subjects passed/failed in this semester
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'PASS') AS passed,
                    COUNT(*) FILTER (WHERE status = 'FAIL') AS failed
                FROM student_results
                WHERE student_id = %s AND semester = %s
            """, (student_id, semester))
            row = cur.fetchone()
            subjects_passed = int(row["passed"]) if row else 0
            subjects_failed = int(row["failed"]) if row else backlogs

            cur.execute("""
                INSERT INTO semester_aggregates
                    (student_id, semester, sgpa, credits_earned, credits_attempted,
                     subjects_passed, subjects_failed, backlogs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (student_id, semester) DO UPDATE SET
                    sgpa              = EXCLUDED.sgpa,
                    credits_earned    = EXCLUDED.credits_earned,
                    subjects_passed   = EXCLUDED.subjects_passed,
                    subjects_failed   = EXCLUDED.subjects_failed,
                    backlogs          = EXCLUDED.backlogs,
                    updated_at        = NOW()
            """, (student_id, semester, safe_sgpa,
                  safe_credits, safe_credits,
                  subjects_passed, subjects_failed, subjects_failed))


def compute_and_store_cgpa(student_id: str) -> float:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Grade-point weighted average
            cur.execute("""
                SELECT AVG(grade_points) AS avg_gp FROM student_results
                WHERE student_id = %s AND grade_points IS NOT NULL AND grade_points > 0
            """, (student_id,))
            row  = cur.fetchone()
            cgpa = float(row["avg_gp"]) if row and row["avg_gp"] else None

            # 2. SGPA average fallback
            if cgpa is None:
                cur.execute("""
                    SELECT AVG(sgpa) AS avg_sg FROM semester_aggregates
                    WHERE student_id = %s AND sgpa IS NOT NULL AND sgpa > 0
                """, (student_id,))
                row  = cur.fetchone()
                cgpa = float(row["avg_sg"]) if row and row["avg_sg"] else None

            # 3. Marks percentage fallback
            if cgpa is None:
                cur.execute("""
                    SELECT SUM(total_marks) AS tot, SUM(max_marks) AS mx
                    FROM student_results
                    WHERE student_id = %s AND total_marks IS NOT NULL
                """, (student_id,))
                row = cur.fetchone()
                if row and row["tot"] and row["mx"] and float(row["mx"]) > 0:
                    cgpa = round(float(row["tot"]) / float(row["mx"]) * 10, 2)

            if cgpa is not None:
                cgpa = round(cgpa, 2)
                cur.execute(
                    "UPDATE students SET cgpa = %s, updated_at = NOW() WHERE id = %s",
                    (cgpa, student_id),
                )

            # Refresh total_backlogs
            cur.execute("""
                UPDATE students SET
                    total_backlogs = (
                        SELECT COUNT(*) FROM student_results
                        WHERE student_id = %s AND status = 'FAIL'
                    ),
                    active_backlogs = (
                        SELECT COUNT(*) FROM student_results
                        WHERE student_id = %s AND status = 'FAIL'
                    ),
                    updated_at = NOW()
                WHERE id = %s
            """, (student_id, student_id, student_id))

            return cgpa or 0.0


# ─── Emails ───────────────────────────────────────────────────────────────────

def upsert_email(
    message_id: str,
    subject: Optional[str] = None,
    sender: Optional[str] = None,
    received_at=None,
    raw_path: Optional[str] = None,
    institution_id: Optional[str] = None,
    body: Optional[str] = None,
) -> str:
    if institution_id is None:
        institution_id = get_default_institution_id()

    # email_metadata has many NOT NULL columns including received_at, body_hash, etc.
    if received_at is None:
        received_at = _now_utc()
    elif isinstance(received_at, str):
        try:
            from email.utils import parsedate_to_datetime
            received_at = parsedate_to_datetime(received_at)
        except Exception:
            received_at = _now_utc()

    # body_hash — real SHA-256 of body when available, sentinel zeros otherwise
    import hashlib as _hl
    body_hash = (
        _hl.sha256(body.encode("utf-8", errors="replace")).hexdigest()
        if body
        else "0" * 64
    )
    from_address = sender or "unknown@unknown"
    storage_path = raw_path or "s3://emails-raw/unknown"

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO email_metadata
                    (institution_id, message_id, account_id, from_address,
                     to_addresses, subject, received_at,
                     body_hash, raw_storage_path)
                VALUES (%s, %s, %s, %s, %s::text[], %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE SET
                    subject           = COALESCE(EXCLUDED.subject,          email_metadata.subject),
                    raw_storage_path  = COALESCE(EXCLUDED.raw_storage_path, email_metadata.raw_storage_path)
                RETURNING id
            """, (
                institution_id, message_id, "gmail",
                from_address, "{}", subject, received_at,
                body_hash, storage_path,
            ))
            return str(cur.fetchone()["id"])


def get_email_db_status(message_id: str) -> str | None:
    """Return the pipeline status of an email by message_id, or None if not found."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT status FROM email_metadata WHERE message_id = %s LIMIT 1",
                    (message_id,)
                )
                row = cur.fetchone()
                return row[0] if row else None
    except Exception:
        return None


def update_email_classification(
    email_id: str, classification: str, confidence: float
):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_metadata
                SET classification            = %s,
                    classification_confidence = %s,
                    status                    = 'processing'
                WHERE id = %s
            """, (classification, confidence, email_id))


def update_email_status(email_id: str, status: str, error: Optional[str] = None):
    valid_statuses = ('pending', 'processing', 'completed', 'failed', 'quarantined', 'skipped')
    if status not in valid_statuses:
        status = 'completed'
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE email_metadata
                SET status        = %s,
                    error_message = %s,
                    processed_at  = NOW()
                WHERE id = %s
            """, (status, error, email_id))


def save_extraction(
    email_id: str,
    records: list,
    confidence: float = 0.8,
    strategy: str = "regex",
    attachment_id: Optional[str] = None,
) -> str:
    """
    Save an extraction record.
    The core schema's extractions table requires attachment_id NOT NULL,
    so we use a simpler app-level table.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Ensure the app-level extractions table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS app_extractions (
                    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    email_id            UUID NOT NULL,
                    attachment_id       UUID,
                    extraction_strategy TEXT,
                    records_extracted   INTEGER DEFAULT 0,
                    confidence_score    DECIMAL(3,2),
                    extracted_data      JSONB,
                    created_at         TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO app_extractions
                    (email_id, attachment_id, extraction_strategy,
                     records_extracted, confidence_score, extracted_data)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
            """, (email_id, attachment_id, strategy, len(records),
                  confidence, json.dumps(records)))
            return str(cur.fetchone()["id"])


# ─── Queries ──────────────────────────────────────────────────────────────────

def get_student(
    usn: str, institution_id: Optional[str] = None
) -> Optional[dict]:
    if institution_id is None:
        institution_id = get_default_institution_id()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, usn, name AS full_name, email,
                       cgpa, total_backlogs, active_backlogs,
                       current_semester, created_at
                FROM students
                WHERE UPPER(usn) = UPPER(%s) AND institution_id = %s
            """, (usn, institution_id))
            row = cur.fetchone()
            return dict(row) if row else None


def get_student_results(
    usn: str,
    semester: Optional[int] = None,
    institution_id: Optional[str] = None,
) -> list[dict]:
    if institution_id is None:
        institution_id = get_default_institution_id()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if semester:
                cur.execute("""
                    SELECT sr.semester, sr.total_marks AS marks_obtained, sr.max_marks,
                           sr.grade, sr.grade_points, sr.status AS pass_status,
                           sr.exam_type, sr.attempt_number,
                           sub.code AS subject_code, sub.name AS subject_name
                    FROM student_results sr
                    JOIN students  s   ON sr.student_id = s.id
                    JOIN subjects  sub ON sr.subject_id = sub.id
                    WHERE UPPER(s.usn) = UPPER(%s)
                      AND s.institution_id = %s
                      AND sr.semester = %s
                    ORDER BY sr.semester, sub.code
                """, (usn, institution_id, semester))
            else:
                cur.execute("""
                    SELECT sr.semester, sr.total_marks AS marks_obtained, sr.max_marks,
                           sr.grade, sr.grade_points, sr.status AS pass_status,
                           sr.exam_type, sr.attempt_number,
                           sub.code AS subject_code, sub.name AS subject_name
                    FROM student_results sr
                    JOIN students  s   ON sr.student_id = s.id
                    JOIN subjects  sub ON sr.subject_id = sub.id
                    WHERE UPPER(s.usn) = UPPER(%s) AND s.institution_id = %s
                    ORDER BY sr.semester, sub.code
                """, (usn, institution_id))
            return [dict(r) for r in cur.fetchall()]


def search_students(
    query: str,
    institution_id: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    if institution_id is None:
        institution_id = get_default_institution_id()
    like = f"%{query.upper()}%"
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, usn, name AS full_name, cgpa, total_backlogs
                FROM students
                WHERE institution_id = %s
                  AND (UPPER(usn) LIKE %s OR UPPER(name) LIKE %s)
                ORDER BY name LIMIT %s
            """, (institution_id, like, like, limit))
            return [dict(r) for r in cur.fetchall()]


def get_all_students(
    institution_id: Optional[str] = None,
    limit: int = 200,
    exclude_seed: bool = True,
) -> list[dict]:
    """Fetch all students. By default excludes seed/test data (source='seed')."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    seed_filter = "AND (metadata->>'source' IS DISTINCT FROM 'seed')" if exclude_seed else ""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, usn, name AS full_name, cgpa, total_backlogs, current_semester
                FROM students
                WHERE institution_id = %s {seed_filter}
                ORDER BY cgpa DESC NULLS LAST LIMIT %s
            """, (institution_id, limit))
            return [dict(r) for r in cur.fetchall()]


def get_pipeline_stats(institution_id: Optional[str] = None, exclude_seed: bool = True) -> dict:
    """Aggregate stats. By default excludes seed/test student rows."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    sf = "AND (metadata->>'source' IS DISTINCT FROM 'seed')" if exclude_seed else ""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM students WHERE institution_id = %s {sf}",
                (institution_id,),
            )
            total_students = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT COUNT(*) AS cnt FROM student_results sr
                JOIN students s ON sr.student_id = s.id
                WHERE s.institution_id = %s {sf}
            """, (institution_id,))
            total_results = cur.fetchone()["cnt"]

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM email_metadata WHERE institution_id = %s",
                (institution_id,),
            )
            emails_processed = cur.fetchone()["cnt"]

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM email_metadata WHERE institution_id = %s AND classification = 'result_email'",
                (institution_id,),
            )
            result_emails = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT COALESCE(SUM(total_backlogs), 0) AS bl
                FROM students WHERE institution_id = %s {sf}
            """, (institution_id,))
            total_backlogs = cur.fetchone()["bl"]

            cur.execute(f"""
                SELECT COALESCE(AVG(cgpa), 0) AS avg_cgpa
                FROM students WHERE institution_id = %s AND cgpa > 0 {sf}
            """, (institution_id,))
            avg_cgpa = cur.fetchone()["avg_cgpa"]

            return {
                "total_students":   int(total_students),
                "total_results":    int(total_results),
                "emails_processed": int(emails_processed),
                "result_emails":    int(result_emails),
                "total_backlogs":   int(total_backlogs),
                "average_cgpa":     round(float(avg_cgpa), 2),
            }


def get_recent_extractions(limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Use app_extractions if it exists
            try:
                cur.execute("""
                    SELECT e.id, e.extraction_strategy, e.records_extracted,
                           e.confidence_score, e.created_at,
                           em.subject AS email_subject, em.from_address AS sender
                    FROM app_extractions e
                    JOIN email_metadata em ON e.email_id = em.id
                    ORDER BY e.created_at DESC LIMIT %s
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
            except Exception:
                return []


# ─── Query audit log ──────────────────────────────────────────────────────────

def store_audit_log(
    *,
    institution_id: Optional[str] = None,
    user_id: Optional[str] = None,
    raw_query: str,
    parsed_intent: Optional[str] = None,
    sql_generated: Optional[str] = None,
    response_summary: Optional[str] = None,
    result_count: int = 0,
    duration_ms: int = 0,
    error: Optional[str] = None,
) -> None:
    """
    Append one row to `query_audit_log`.
    Never raises — failures are logged and silently swallowed so they
    cannot break the query response path.
    """
    if institution_id is None:
        institution_id = get_default_institution_id()

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO query_audit_log
                        (institution_id, user_id, raw_query, parsed_intent,
                         sql_generated, response_summary, result_count,
                         duration_ms, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        institution_id, user_id, raw_query, parsed_intent,
                        sql_generated, response_summary, result_count,
                        duration_ms, error,
                    ),
                )
    except Exception as exc:
        logger.warning("store_audit_log failed: %s", exc)
