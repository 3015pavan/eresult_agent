"""
Celery tasks — Email/attachment extraction.
Queue: extraction
"""

from __future__ import annotations

import logging
import tempfile
import os
from typing import Any

from src.common.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    name="tasks.extract_email",
    bind=True,
    max_retries=2,
    default_retry_delay=30,
    queue="extraction",
)
def extract_email(
    self,
    email: dict,
    institution_id: str | None = None,
) -> dict:
    """
    Full pipeline for a single cached email:
      1. Classify
      2. Security scan (body)
      3. Multi-strategy extraction (regex + LLM)
      4. Validate + correction loop
      5. Save to DB
      6. Enqueue indexing
    """
    try:
        from src.common.security import is_safe
        from src.phase3_extraction_engine.strategy_merger import extract_with_voting
        from src.phase3_extraction_engine.validator import validate_and_correct
        from src.phase3_extraction_engine.review_queue import enqueue_for_review, REVIEW_THRESHOLD
        from src.common.database import (
            upsert_student, upsert_result
        )

        msg_id  = email.get("id", "")
        subject = email.get("subject", "")
        body    = email.get("body", "")
        sender  = email.get("from", "")

        # 1. Safety
        if not is_safe(body.encode()):
            logger.warning("extract_email: unsafe content in %s", msg_id)
            return {"status": "unsafe", "message_id": msg_id}

        # 2. Classify
        from src.api.routes.pipeline import _classify_email  # local import to avoid circular
        classification = _classify_email(subject, body)
        if classification.get("type") != "result":
            return {"status": "skipped", "classification": classification.get("type")}

        # 3. Multi-strategy extract
        records = extract_with_voting(body, [], run_llm=True)

        # 4. Validate
        records = validate_and_correct(records, body)

        if not records:
            return {"status": "no_records", "message_id": msg_id}

        # 5. Compute confidence
        confidence = records[0].get("confidence", 0.5) if records else 0.0
        if confidence < REVIEW_THRESHOLD:
            queue_id = enqueue_for_review(
                email_id=msg_id, subject=subject, sender=sender,
                body=body, records=records, confidence=confidence,
            )
            return {"status": "queued_for_review", "queue_id": queue_id}

        # 6. Save
        saved = 0
        for rec in records:
            usn = rec.get("usn")
            if not usn:
                continue
            upsert_student(usn, usn, institution_id or "default", rec)
            for subj in rec.get("subjects", []):
                upsert_result(usn, rec.get("semester", 0), subj, institution_id or "default")
                saved += 1

        # 7. Enqueue indexing for each USN
        usns = {r.get("usn") for r in records if r.get("usn")}
        for usn in usns:
            index_student.apply_async(kwargs={"usn": usn}, queue="indexing")

        return {"status": "ok", "message_id": msg_id, "records": len(records), "saved": saved}

    except Exception as exc:
        logger.error("extract_email failed for %s: %s", email.get("id", "?"), exc)
        raise self.retry(exc=exc)


@celery_app.task(
    name="tasks.extract_attachment",
    bind=True,
    max_retries=2,
    queue="extraction",
)
def extract_attachment(
    self,
    attachment_id: str,
    filename: str,
    content_bytes_b64: str,
    email_id: str,
    institution_id: str | None = None,
) -> dict:
    """
    Extract student records from an email attachment (PDF/Excel/image).

    content_bytes_b64: base64-encoded attachment bytes.
    """
    try:
        import base64
        from src.common.security import is_safe
        from src.phase2_document_intelligence.router import route_to_parser
        from src.phase3_extraction_engine.strategy_merger import extract_with_voting
        from src.phase3_extraction_engine.validator import validate_and_correct
        from src.common.database import upsert_student, upsert_result

        data = base64.b64decode(content_bytes_b64)

        # Security scan
        if not is_safe(data):
            logger.warning("extract_attachment: unsafe attachment %s", filename)
            return {"status": "unsafe", "attachment_id": attachment_id}

        # Write to temp file
        suffix = os.path.splitext(filename)[1] or ".bin"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        try:
            # Phase 2 parse
            parsed = route_to_parser(tmp_path, None, None)
            text = parsed.flat_text()
            doc_records = [row for tbl in parsed.tables for row in tbl]

            # Phase 3 extract + validate
            records = extract_with_voting(text, doc_records, run_llm=True)
            records = validate_and_correct(records, text)

            saved = 0
            for rec in records:
                usn = rec.get("usn")
                if not usn:
                    continue
                upsert_student(usn, usn, institution_id or "default", rec)
                for subj in rec.get("subjects", []):
                    upsert_result(usn, rec.get("semester", 0), subj, institution_id or "default")
                    saved += 1

            for usn in {r.get("usn") for r in records if r.get("usn")}:
                index_student.apply_async(kwargs={"usn": usn}, queue="indexing")

            return {
                "status": "ok",
                "attachment_id": attachment_id,
                "parse_strategy": parsed.parse_strategy,
                "records": len(records),
                "saved": saved,
            }
        finally:
            os.unlink(tmp_path)

    except Exception as exc:
        logger.error("extract_attachment failed: %s", exc)
        raise self.retry(exc=exc)


# Circular-import workaround — import after definitions
from src.tasks.indexing import index_student  # noqa: E402
