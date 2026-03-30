"""
Celery tasks — Indexing (pgvector embeddings + Elasticsearch).
Queue: indexing
"""

from __future__ import annotations

import logging

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.index_student",
    bind=True,
    max_retries=2,
    queue="indexing",
)
def index_student(self, usn: str) -> dict:
    """
    (Re)generate embedding for a single student and push to Elasticsearch.
    Called after each successful extraction.
    """
    try:
        from src.common.database import get_connection
        from src.common.embeddings import store_student_embedding

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, usn, name FROM students WHERE usn = %s", (usn,)
                    )
                    row = cur.fetchone()
        except Exception:
            return {"status": "db_error"}

        if row is None:
            return {"status": "not_found", "usn": usn}

        student_id, usn_val, name = row
        store_student_embedding(str(student_id), usn_val, name)

        # Elasticsearch sync (best-effort)
        _es_index_student(usn=usn_val, name=name, student_id=str(student_id))

        return {"status": "ok", "usn": usn}

    except Exception as exc:
        logger.error("index_student failed for %s: %s", usn, exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.rebuild_all_embeddings",
    bind=True,
    queue="indexing",
)
def rebuild_all_embeddings(self) -> dict:
    """
    Nightly task: regenerate pgvector embeddings for ALL students.
    Beat schedule: 02:00 UTC daily.
    """
    try:
        from src.common.database import get_connection
        from src.common.embeddings import store_student_embedding

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id, usn, name FROM students")
                    rows = cur.fetchall()
        except Exception:
            return {"status": "db_error"}

        rebuilt = 0
        for student_id, usn, name in rows:
            try:
                store_student_embedding(str(student_id), usn, name)
                rebuilt += 1
            except Exception as exc:
                logger.warning("rebuild_all_embeddings: skip %s: %s", usn, exc)

        logger.info("rebuild_all_embeddings: rebuilt %d/%d", rebuilt, len(rows))
        return {"status": "ok", "rebuilt": rebuilt, "total": len(rows)}

    except Exception as exc:
        logger.error("rebuild_all_embeddings failed: %s", exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.refresh_elasticsearch",
    bind=True,
    queue="indexing",
)
def refresh_elasticsearch(self) -> dict:
    """
    Hourly task: push all students to Elasticsearch in bulk.
    Beat schedule: every 60 minutes.
    """
    try:
        from src.common.database import get_connection

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.id, s.usn, s.name, s.cgpa, s.total_backlogs
                        FROM students s
                        ORDER BY s.created_at DESC
                        LIMIT 5000
                        """
                    )
                    rows = cur.fetchall()
        except Exception:
            return {"status": "db_error"}

        docs = [
            {
                "id":             str(r[0]),
                "usn":            r[1],
                "name":           r[2],
                "cgpa":           float(r[3]) if r[3] else 0.0,
                "total_backlogs": int(r[4]) if r[4] else 0,
            }
            for r in rows
        ]

        indexed = _es_bulk_index(docs)
        logger.info("refresh_elasticsearch: indexed %d docs", indexed)
        return {"status": "ok", "indexed": indexed}

    except Exception as exc:
        logger.error("refresh_elasticsearch failed: %s", exc)
        raise self.retry(exc=exc)


# ── Elasticsearch helpers ─────────────────────────────────────────────────────

def _es_index_student(*, usn: str, name: str, student_id: str) -> None:
    """Index a single student document. Best-effort — never raises."""
    try:
        from src.common.elasticsearch_client import es_client
        es_client().index(
            index="students",
            id=student_id,
            document={"usn": usn, "name": name},
        )
    except Exception as exc:
        logger.debug("_es_index_student: %s", exc)


def _es_bulk_index(docs: list[dict]) -> int:
    """Bulk-index documents to Elasticsearch. Returns count indexed."""
    if not docs:
        return 0
    try:
        from src.common.elasticsearch_client import es_client, bulk_index
        return bulk_index(es_client(), "students", docs)
    except Exception as exc:
        logger.debug("_es_bulk_index: %s", exc)
        return 0


def _es_bulk_index_emails(docs: list[dict]) -> int:
    """Bulk-index email documents to Elasticsearch `emails` index."""
    if not docs:
        return 0
    try:
        from src.common.elasticsearch_client import es_client, bulk_index, ensure_email_index
        ensure_email_index()
        return bulk_index(es_client(), "emails", docs)
    except Exception as exc:
        logger.debug("_es_bulk_index_emails: %s", exc)
        return 0


@celery_app.task(
    name="tasks.refresh_email_index",
    bind=True,
    queue="indexing",
)
def refresh_email_index(self) -> dict:
    """
    Index all cached emails into Elasticsearch for full-text search.
    Reads from data/emails_cache.json — safe to run repeatedly.
    """
    try:
        import json, os
        cache_path = "data/emails_cache.json"
        if not os.path.exists(cache_path):
            return {"status": "no_cache"}

        emails = json.loads(open(cache_path).read())
        docs = [
            {
                "id":             e.get("id", ""),
                "subject":        e.get("subject", ""),
                "from":           e.get("from", ""),
                "body":           (e.get("body", "") or "")[:5000],
                "date":           e.get("date", ""),
                "snippet":        e.get("snippet", ""),
                "thread_id":      e.get("threadId", e.get("message_id", "")),
                "classification": e.get("classification", ""),
            }
            for e in emails
            if e.get("id")
        ]
        indexed = _es_bulk_index_emails(docs)
        logger.info("refresh_email_index: indexed %d/%d emails", indexed, len(docs))
        return {"status": "ok", "indexed": indexed, "total": len(docs)}

    except Exception as exc:
        logger.error("refresh_email_index failed: %s", exc)
        raise self.retry(exc=exc)
