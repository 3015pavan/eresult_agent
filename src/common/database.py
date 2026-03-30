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
_tables_ensured: bool = False


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

REAL_STUDENT_SOURCES = ("pipeline", "upload")

def _normalise_name(name: str) -> str:
    """Produce a searchable lowercase-nospace version of a name."""
    return re.sub(r'\s+', ' ', name.upper().strip())


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def student_source_filter(alias: Optional[str] = None, include_and: bool = False) -> str:
    """SQL fragment that whitelists students coming from real pipeline sources."""
    prefix = f"{alias}." if alias else ""
    clause = (
        f"({prefix}metadata ? 'source') "
        f"AND {prefix}metadata->>'source' IN {REAL_STUDENT_SOURCES}"
    )
    return f"AND {clause}" if include_and else clause


def _ensure_app_support_tables(cur) -> None:
    """Create lightweight app-managed tables used by stats and uploads. Runs once per process."""
    global _tables_ensured
    if _tables_ensured:
        return
    # Add config JSONB column to institutions if it doesn't exist
    cur.execute("""
        ALTER TABLE institutions ADD COLUMN IF NOT EXISTS config JSONB DEFAULT '{}'
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_extractions (
            id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email_id            UUID NOT NULL,
            attachment_id       UUID,
            extraction_strategy TEXT,
            records_extracted   INTEGER DEFAULT 0,
            confidence_score    DECIMAL(3,2),
            extracted_data      JSONB,
            created_at          TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_uploads (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            institution_id    TEXT NOT NULL,
            filename          TEXT NOT NULL,
            content_type      TEXT,
            file_size         BIGINT DEFAULT 0,
            records_parsed    INTEGER DEFAULT 0,
            students_upserted INTEGER DEFAULT 0,
            results_stored    INTEGER DEFAULT 0,
            created_at        TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS app_pipeline_events (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            email_id          UUID,
            attachment_id     UUID,
            stage             TEXT NOT NULL,
            status            TEXT NOT NULL,
            message           TEXT,
            event_payload     JSONB DEFAULT '{}',
            created_at        TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    _tables_ensured = True


# ─── Initialisation ───────────────────────────────────────────────────────────

def init_db() -> str:
    """
    Verify connectivity, create missing email_metadata partitions, and
    upsert the default institution returning its UUID.
    """
    global _DEFAULT_INSTITUTION_ID

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_app_support_tables(cur)
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


# ─── Institution config ────────────────────────────────────────────────────────

def get_institution_config(institution_id: Optional[str] = None) -> dict:
    """
    Return the JSONB config for an institution.
    Falls back to the default institution when institution_id is None.
    Returns {} when the institution is not found or config is NULL.
    """
    inst_id = institution_id or get_default_institution_id()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT config FROM institutions WHERE id = %s", (inst_id,)
                )
                row = cur.fetchone()
                if row and row[0]:
                    return dict(row[0])
    except Exception as exc:
        logger.warning("get_institution_config failed: %s", exc)
    return {}


def set_institution_config(config: dict, institution_id: Optional[str] = None) -> None:
    """
    Merge-update the JSONB config for an institution.
    Only keys present in `config` are updated; other keys are preserved.
    """
    inst_id = institution_id or get_default_institution_id()
    from psycopg2.extras import Json as PgJson
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE institutions
                SET config = COALESCE(config, '{}') || %s
                WHERE id = %s
                """,
                (PgJson(config), inst_id),
            )


# ─── Subjects ─────────────────────────────────────────────────────────────────

def get_or_create_subject(
    institution_id: str,
    code: str,
    name: str,
    semester: Optional[int] = None,
    credits: int = 3,
    pass_marks: Optional[int] = None,
) -> str:
    """
    Insert or update a subject record.
    `credits` stores the VTU credit hours for this subject (used in SGPA computation).
    `pass_marks` is the minimum marks required to PASS this subject (DB default: 35).
    """
    safe_credits = max(1, int(credits or 3))
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if pass_marks is not None:
                safe_pass = max(1, int(pass_marks))
                cur.execute("""
                    INSERT INTO subjects (institution_id, code, name, credits, semester, pass_marks)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (institution_id, code) DO UPDATE SET
                        name       = COALESCE(NULLIF(EXCLUDED.name, ''),   subjects.name),
                        credits    = CASE WHEN EXCLUDED.credits != 3
                                         THEN EXCLUDED.credits
                                         ELSE subjects.credits END,
                        pass_marks = EXCLUDED.pass_marks
                    RETURNING id
                """, (institution_id, code.upper(), name or code, safe_credits, semester, safe_pass))
            else:
                cur.execute("""
                    INSERT INTO subjects (institution_id, code, name, credits, semester)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (institution_id, code) DO UPDATE SET
                        name    = COALESCE(NULLIF(EXCLUDED.name, ''), subjects.name),
                        credits = CASE WHEN EXCLUDED.credits != 3 THEN EXCLUDED.credits
                                       ELSE subjects.credits END
                    RETURNING id
                """, (institution_id, code.upper(), name or code, safe_credits, semester))
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
    """Insert or update a student record. source='seed' for test data, 'pipeline'/'upload' for real data."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    normalized = _normalise_name(name or usn)
    from psycopg2.extras import Json as PgJson
    meta = PgJson({"source": source})
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
                    metadata        = CASE
                        WHEN students.metadata->>'source' IN ('pipeline', 'upload')
                        THEN students.metadata
                        ELSE EXCLUDED.metadata
                    END,
                    updated_at      = NOW()
                RETURNING id
            """, (institution_id, usn.upper(), name or usn, normalized, email, meta))
            return str(cur.fetchone()["id"])


# ─── Results ──────────────────────────────────────────────────────────────────
def remove_seeded_students():
    """
    Remove all students and related data seeded for testing (source='seed').
    Deletes from student_results, semester_aggregates, students, and subjects if orphaned.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Find all seeded students
            cur.execute("SELECT id, usn FROM students WHERE metadata->>'source' = 'seed'")
            seeded = cur.fetchall()
            if not seeded:
                print("No seeded students found.")
                return 0
            student_ids = [str(r[0]) for r in seeded]
            usns = [str(r[1]) for r in seeded]

            # Remove results and aggregates
            cur.execute("DELETE FROM student_results WHERE student_id = ANY(%s)", (student_ids,))
            cur.execute("DELETE FROM semester_aggregates WHERE student_id = ANY(%s)", (student_ids,))
            cur.execute("DELETE FROM students WHERE id = ANY(%s)", (student_ids,))

            # Optionally remove orphaned subjects
            cur.execute("DELETE FROM subjects WHERE id NOT IN (SELECT subject_id FROM student_results)")

            print(f"Removed {len(student_ids)} seeded students and related data.")
            return len(student_ids)

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
        # Look up the subject's configured pass_marks threshold from DB
        pass_threshold = 35  # DB default
        try:
            with get_connection() as _conn:
                with _conn.cursor() as _cur:
                    _cur.execute(
                        "SELECT pass_marks FROM subjects WHERE id = %s", (subject_id,)
                    )
                    _row = _cur.fetchone()
                    if _row and _row[0] is not None:
                        pass_threshold = int(_row[0])
        except Exception:
            pass
        passed = (grade_points or 0) >= 4.0 or (marks_obtained or 0) >= pass_threshold
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
    """
    Recompute and persist SGPA (per semester) and CGPA for a student.

    Formula (VTU):
        SGPA_sem  = Σ(grade_points × credits) / Σ(credits)   [over subjects in semester]
        CGPA      = Σ(grade_points × credits) / Σ(credits)   [over all subjects all semesters]

    If no grade_points exist, falls back to:
        1. Average of semester_aggregates.sgpa
        2. (total_marks / max_marks) × 10

    Also refreshes semester_aggregates.sgpa and student backlog counts.
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:

            # ── 1. Credits-weighted CGPA (primary) ───────────────────────────
            cur.execute("""
                SELECT
                    ROUND(
                        SUM(sr.grade_points * COALESCE(sub.credits, 3))::numeric
                        / NULLIF(SUM(COALESCE(sub.credits, 3))::numeric, 0),
                        2
                    ) AS cgpa
                FROM student_results sr
                JOIN subjects sub ON sub.id = sr.subject_id
                WHERE sr.student_id = %s
                  AND sr.grade_points IS NOT NULL
                  AND sr.grade_points > 0
            """, (student_id,))
            row  = cur.fetchone()
            cgpa = float(row["cgpa"]) if row and row["cgpa"] else None

            # ── 1a. Refresh per-semester SGPA in semester_aggregates ─────────
            cur.execute("""
                SELECT
                    sr.semester,
                    ROUND(
                        SUM(sr.grade_points * COALESCE(sub.credits, 3))::numeric
                        / NULLIF(SUM(COALESCE(sub.credits, 3))::numeric, 0),
                        2
                    ) AS sgpa,
                    SUM(COALESCE(sub.credits, 3)) AS total_credits
                FROM student_results sr
                JOIN subjects sub ON sub.id = sr.subject_id
                WHERE sr.student_id = %s
                  AND sr.grade_points IS NOT NULL
                  AND sr.grade_points > 0
                GROUP BY sr.semester
            """, (student_id,))
            sem_rows = cur.fetchall() or []
            for sr in sem_rows:
                if sr.get("sgpa") is not None:
                    sgpa_val = min(10.0, max(0.0, float(sr["sgpa"])))
                    cur.execute("""
                        INSERT INTO semester_aggregates
                            (student_id, semester, sgpa, credits_earned, credits_attempted,
                             subjects_passed, subjects_failed, backlogs)
                        VALUES (%s, %s, %s, %s, %s, 0, 0, 0)
                        ON CONFLICT (student_id, semester) DO UPDATE SET
                            sgpa           = EXCLUDED.sgpa,
                            credits_earned = EXCLUDED.credits_earned,
                            updated_at     = NOW()
                    """, (student_id, sr["semester"], sgpa_val,
                          int(sr["total_credits"]), int(sr["total_credits"])))

            # ── 2. Fallback: average of semester SGPAs ────────────────────────
            if cgpa is None:
                cur.execute("""
                    SELECT AVG(sgpa) AS avg_sg FROM semester_aggregates
                    WHERE student_id = %s AND sgpa IS NOT NULL AND sgpa > 0
                """, (student_id,))
                row  = cur.fetchone()
                cgpa = float(row["avg_sg"]) if row and row["avg_sg"] else None

            # ── 3. Fallback: marks percentage scaled to 10 ───────────────────
            if cgpa is None:
                cur.execute("""
                    SELECT SUM(total_marks) AS tot, SUM(max_marks) AS mx
                    FROM student_results
                    WHERE student_id = %s AND total_marks IS NOT NULL
                """, (student_id,))
                row = cur.fetchone()
                if row and row["tot"] and row["mx"] and float(row["mx"]) > 0:
                    cgpa = round(float(row["tot"]) / float(row["mx"]) * 10, 2)

            # Clamp to valid range and persist
            if cgpa is not None:
                cgpa = round(min(10.0, max(0.0, cgpa)), 2)
                cur.execute(
                    "UPDATE students SET cgpa = %s, updated_at = NOW() WHERE id = %s",
                    (cgpa, student_id),
                )

            # ── Refresh backlog counts ────────────────────────────────────────
            cur.execute("""
                UPDATE students SET
                    total_backlogs = (
                        SELECT COUNT(DISTINCT subject_id) FROM student_results
                        WHERE student_id = %s AND status = 'FAIL'
                    ),
                    active_backlogs = (
                        SELECT COUNT(DISTINCT subject_id) FROM student_results sr
                        WHERE sr.student_id = %s
                          AND sr.status = 'FAIL'
                          AND NOT EXISTS (
                              SELECT 1 FROM student_results sr2
                              WHERE sr2.student_id = %s
                                AND sr2.subject_id = sr.subject_id
                                AND sr2.status = 'PASS'
                          )
                    ),
                    updated_at = NOW()
                WHERE id = %s
            """, (student_id, student_id, student_id, student_id))

            return cgpa or 0.0


def fix_corrupted_grade_data() -> dict:
    """
    One-time migration that fixes corrupted grade / grade_points values
    produced by earlier pipeline runs before strict validation was in place.

    VTU grade scale:  O=10, A+=9, A=8, B+=7, B=6, C=5, P=4, F=0

    Observed corruption patterns:
      • grade column contains raw numbers (e.g. 20, 9.9, 0, 198)
        instead of VTU letter grades
      • grade_points column contains marks values (e.g. 30, 40)
        instead of 0–10 scale values
      • total_marks stored as 0 even when marks exist

    This function applies five SQL UPDATE passes in order, then
    calls compute_and_store_cgpa() for every affected student.

    Returns a dict with per-step row counts.
    """
    stats: dict[str, int] = {}

    with get_connection() as conn:
        with conn.cursor() as cur:

            # ── Step 1: rows with valid VTU letter grades ────────────────────
            # Fix grade_points to exact VTU lookup value.
            cur.execute("""
                UPDATE student_results
                SET grade_points = CASE grade
                    WHEN 'O'  THEN 10.0
                    WHEN 'A+' THEN  9.0
                    WHEN 'A'  THEN  8.0
                    WHEN 'B+' THEN  7.0
                    WHEN 'B'  THEN  6.0
                    WHEN 'C'  THEN  5.0
                    WHEN 'P'  THEN  4.0
                    WHEN 'F'  THEN  0.0
                END
                WHERE grade IN ('O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F')
            """)
            stats["step1_valid_grade_fixed"] = cur.rowcount

            # ── Step 2: grade is a float in [0, 10] ──────────────────────────
            # Treat the numeric grade as a grade_point value stored in the
            # wrong column; move it to grade_points and derive letter grade.
            cur.execute("""
                UPDATE student_results
                SET
                    grade_points = grade::numeric,
                    grade = CASE
                        WHEN grade::numeric >= 10.0 THEN 'O'
                        WHEN grade::numeric >=  9.0 THEN 'A+'
                        WHEN grade::numeric >=  8.0 THEN 'A'
                        WHEN grade::numeric >=  7.0 THEN 'B+'
                        WHEN grade::numeric >=  6.0 THEN 'B'
                        WHEN grade::numeric >=  5.0 THEN 'C'
                        WHEN grade::numeric >=  4.0 THEN 'P'
                        ELSE 'F'
                    END,
                    status = CASE WHEN grade::numeric >= 4.0 THEN 'PASS' ELSE 'FAIL' END
                WHERE grade ~ '^[0-9]+([.][0-9]+)?$'
                  AND grade::numeric BETWEEN 0 AND 10
            """)
            stats["step2_gp_in_grade_fixed"] = cur.rowcount

            # ── Step 3: grade is a marks percentage in (10, 100] ─────────────
            # Treat as percentage marks; derive VTU letter grade and grade_points.
            cur.execute("""
                UPDATE student_results
                SET
                    grade_points = CASE
                        WHEN grade::numeric >= 90 THEN 10.0
                        WHEN grade::numeric >= 80 THEN  9.0
                        WHEN grade::numeric >= 70 THEN  8.0
                        WHEN grade::numeric >= 60 THEN  7.0
                        WHEN grade::numeric >= 55 THEN  6.0
                        WHEN grade::numeric >= 50 THEN  5.0
                        WHEN grade::numeric >= 40 THEN  4.0
                        ELSE 0.0
                    END,
                    grade = CASE
                        WHEN grade::numeric >= 90 THEN 'O'
                        WHEN grade::numeric >= 80 THEN 'A+'
                        WHEN grade::numeric >= 70 THEN 'A'
                        WHEN grade::numeric >= 60 THEN 'B+'
                        WHEN grade::numeric >= 55 THEN 'B'
                        WHEN grade::numeric >= 50 THEN 'C'
                        WHEN grade::numeric >= 40 THEN 'P'
                        ELSE 'F'
                    END,
                    status = CASE
                        WHEN grade::numeric >= 40 THEN 'PASS' ELSE 'FAIL'
                    END
                WHERE grade ~ '^[0-9]+([.][0-9]+)?$'
                  AND grade::numeric > 10
                  AND grade::numeric <= 100
            """)
            stats["step3_marks_in_grade_fixed"] = cur.rowcount

            # ── Step 4: grade > 100 — garbage, force to F ────────────────────
            cur.execute("""
                UPDATE student_results
                SET grade = 'F', grade_points = 0.0, status = 'FAIL'
                WHERE grade ~ '^[0-9]+([.][0-9]+)?$'
                  AND grade::numeric > 100
            """)
            stats["step4_garbage_grade_fixed"] = cur.rowcount

            # ── Step 5: remaining grade_points > 10 are invalid ─────────────
            # These are marks values that slipped into the grade_points column.
            # NULL them out so they don't skew CGPA computation.
            cur.execute("""
                UPDATE student_results
                SET grade_points = NULL
                WHERE grade_points > 10.0
            """)
            stats["step5_invalid_gp_nulled"] = cur.rowcount

            # ── Step 6: re-derive status from grade for any remaining rows ───
            cur.execute("""
                UPDATE student_results
                SET status = CASE
                    WHEN grade = 'F' THEN 'FAIL'
                    WHEN grade IN ('O', 'A+', 'A', 'B+', 'B', 'C', 'P') THEN 'PASS'
                    ELSE status
                END
                WHERE grade IN ('O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F')
                  AND status NOT IN ('ABSENT', 'WITHHELD')
            """)
            stats["step6_status_fixed"] = cur.rowcount

            # ── Step 7: enforce exact VTU grade_points for all valid grades ──
            # After numeric-grade conversions, grade_points may be non-standard
            # (e.g. 9.9 for A+ instead of exactly 9.0). Re-apply VTU lookup.
            cur.execute("""
                UPDATE student_results
                SET grade_points = CASE grade
                    WHEN 'O'  THEN 10.0
                    WHEN 'A+' THEN  9.0
                    WHEN 'A'  THEN  8.0
                    WHEN 'B+' THEN  7.0
                    WHEN 'B'  THEN  6.0
                    WHEN 'C'  THEN  5.0
                    WHEN 'P'  THEN  4.0
                    WHEN 'F'  THEN  0.0
                END
                WHERE grade IN ('O', 'A+', 'A', 'B+', 'B', 'C', 'P', 'F')
                  AND (grade_points IS NULL OR grade_points NOT IN (
                      10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 0.0
                  ))
            """)
            stats["step7_gp_normalized"] = cur.rowcount

        # Commit the grade/gp fixes before recomputing GPAs
        conn.commit()

    # ── Recompute CGPA for every student affected ────────────────────────────
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM students")
            student_ids = [str(r[0]) for r in cur.fetchall()]

    recomputed = 0
    for sid in student_ids:
        try:
            compute_and_store_cgpa(sid)
            recomputed += 1
        except Exception as exc:
            logger.warning("fix_grade_data: recompute_cgpa failed for %s: %s", sid, exc)

    stats["students_gpa_recomputed"] = recomputed
    logger.info("fix_corrupted_grade_data complete: %s", stats)
    return stats


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
    valid_statuses = (
        'pending',
        'processing',
        'completed',
        'failed',
        'quarantined',
        'skipped',
        'processed_no_records',
        'queued_for_review',
    )
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
            _ensure_app_support_tables(cur)
            cur.execute("""
                INSERT INTO app_extractions
                    (email_id, attachment_id, extraction_strategy,
                     records_extracted, confidence_score, extracted_data)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id
            """, (email_id, attachment_id, strategy, len(records),
                  confidence, json.dumps(records)))
            return str(cur.fetchone()["id"])


def save_attachment(
    *,
    email_id: str,
    filename: str,
    content_type: str,
    file_size: int,
    file_hash: str,
    storage_path: str,
    document_type: str = "",
    parse_status: str = "pending",
    metadata: Optional[dict] = None,
) -> str:
    """Persist attachment metadata to the core attachments table."""
    safe_status = parse_status if parse_status in ("pending", "processing", "completed", "failed", "quarantined") else "pending"
    from psycopg2.extras import Json as PgJson
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO attachments
                    (email_id, filename, content_type, file_size, file_hash,
                     storage_path, parse_status, document_type, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                email_id, filename, content_type or "application/octet-stream",
                int(file_size or 0), file_hash, storage_path, safe_status,
                document_type or None, PgJson(metadata or {}),
            ))
            return str(cur.fetchone()["id"])


def update_attachment_status(
    attachment_id: str,
    *,
    parse_status: str,
    page_count: Optional[int] = None,
    document_type: Optional[str] = None,
    metadata: Optional[dict] = None,
) -> None:
    """Update attachment processing status and parser metadata."""
    safe_status = parse_status if parse_status in ("pending", "processing", "completed", "failed", "quarantined") else "pending"
    from psycopg2.extras import Json as PgJson
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE attachments
                SET parse_status = %s,
                    page_count = COALESCE(%s, page_count),
                    document_type = COALESCE(%s, document_type),
                    metadata = CASE
                        WHEN %s IS NULL THEN metadata
                        ELSE COALESCE(metadata, '{}') || %s::jsonb
                    END
                WHERE id = %s
            """, (
                safe_status,
                page_count,
                document_type,
                json.dumps(metadata) if metadata is not None else None,
                json.dumps(metadata or {}),
                attachment_id,
            ))


def store_pipeline_event(
    *,
    stage: str,
    status: str,
    message: str = "",
    email_id: Optional[str] = None,
    attachment_id: Optional[str] = None,
    payload: Optional[dict] = None,
) -> None:
    """Append one pipeline audit event. Best-effort only."""
    try:
        from psycopg2.extras import Json as PgJson
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                _ensure_app_support_tables(cur)
                cur.execute("""
                    INSERT INTO app_pipeline_events
                        (email_id, attachment_id, stage, status, message, event_payload)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    email_id,
                    attachment_id,
                    stage,
                    status,
                    message,
                    PgJson(payload or {}),
                ))
    except Exception as exc:
        logger.warning("store_pipeline_event failed: %s", exc)


def save_admin_upload(
    institution_id: str,
    filename: str,
    content_type: Optional[str] = None,
    file_size: int = 0,
    records_parsed: int = 0,
    students_upserted: int = 0,
    results_stored: int = 0,
) -> str:
    """Persist one admin upload so dashboard file counts are based on real uploads."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_app_support_tables(cur)
            cur.execute("""
                INSERT INTO app_uploads
                    (institution_id, filename, content_type, file_size,
                     records_parsed, students_upserted, results_stored)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                institution_id,
                filename,
                content_type,
                file_size,
                records_parsed,
                students_upserted,
                results_stored,
            ))
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
                       created_at
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
                  {student_filter}
                ORDER BY name LIMIT %s
                        """.format(student_filter=student_source_filter(include_and=True)), (institution_id, like, like, limit))
            return [dict(r) for r in cur.fetchall()]


def get_all_students(
    institution_id: Optional[str] = None,
    limit: int = 200,
    exclude_seed: bool = True,
) -> list[dict]:
    """Fetch all students. By default excludes seed/test data (source='seed')."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    seed_filter = student_source_filter(include_and=True) if exclude_seed else ""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT id, usn, name AS full_name, cgpa, total_backlogs
                FROM students
                WHERE institution_id = %s {seed_filter}
                ORDER BY cgpa DESC NULLS LAST LIMIT %s
            """, (institution_id, limit))
            return [dict(r) for r in cur.fetchall()]


def get_pipeline_stats(institution_id: Optional[str] = None, exclude_seed: bool = True) -> dict:
    """Aggregate stats. By default excludes seed/test student rows."""
    if institution_id is None:
        institution_id = get_default_institution_id()
    # Whitelist: only count students inserted via real pipeline or admin upload
    sf = student_source_filter(include_and=True) if exclude_seed else ""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_app_support_tables(cur)
            cur.execute(
                f"SELECT COUNT(*) AS cnt FROM students WHERE institution_id = %s {sf}",
                (institution_id,),
            )
            total_students = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM students
                WHERE institution_id = %s
                AND (metadata ? 'source')
                AND metadata->>'source' = %s
            """, (institution_id, REAL_STUDENT_SOURCES[0]))
            email_students = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM students
                WHERE institution_id = %s
                AND (metadata ? 'source')
                AND metadata->>'source' = %s
            """, (institution_id, REAL_STUDENT_SOURCES[1]))
            admin_students = cur.fetchone()["cnt"]

            cur.execute(f"""
                SELECT COUNT(*) AS cnt FROM student_results sr
                JOIN students s ON sr.student_id = s.id
                WHERE s.institution_id = %s {sf}
            """, (institution_id,))
            total_results = cur.fetchone()["cnt"]

            cur.execute("""
                SELECT COUNT(*) AS cnt
                FROM app_extractions ae
                JOIN email_metadata em ON ae.email_id = em.id
                WHERE em.institution_id = %s
            """, (institution_id,))
            email_extractions = cur.fetchone()["cnt"]

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM app_uploads WHERE institution_id = %s",
                (institution_id,),
            )
            admin_upload_files = cur.fetchone()["cnt"]

            cur.execute(
                "SELECT COUNT(*) AS cnt FROM email_metadata WHERE institution_id = %s",
                (institution_id,),
            )
            emails_processed = cur.fetchone()["cnt"]

            # Check if classification column exists before querying
            try:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM email_metadata WHERE institution_id = %s AND classification = 'result_email'",
                    (institution_id,),
                )
                result_emails = cur.fetchone()["cnt"]
            except Exception:
                conn.rollback()
                # Column doesn't exist, set to 0
                result_emails = 0

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
                "email_students":   int(email_students),
                "admin_students":   int(admin_students),
                "total_results":    int(total_results),
                "email_extractions": int(email_extractions),
                "admin_upload_files": int(admin_upload_files),
                "emails_processed": int(emails_processed),
                "result_emails":    int(result_emails),
                "total_backlogs":   int(total_backlogs),
                "average_cgpa":     round(float(avg_cgpa), 2),
            }


def get_recent_extractions(limit: int = 10) -> list[dict]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            _ensure_app_support_tables(cur)
            cur.execute("""
                SELECT e.id, e.extraction_strategy, e.records_extracted,
                       e.confidence_score, e.created_at,
                       em.subject AS email_subject, em.from_address AS sender
                FROM app_extractions e
                JOIN email_metadata em ON e.email_id = em.id
                ORDER BY e.created_at DESC LIMIT %s
            """, (limit,))
            return [dict(r) for r in cur.fetchall()]


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
