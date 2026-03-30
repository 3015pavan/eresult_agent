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
        from src.common.database import upsert_student, upsert_result

        msg_id  = email.get("id", "")
        subject = email.get("subject", "")
        body    = email.get("body", "")
        sender  = email.get("from", "")
        received_at = email.get("date")

        from src.common.database import (
            compute_and_store_cgpa,
            get_default_institution_id,
            get_or_create_subject,
            save_extraction,
            update_email_classification,
            update_email_status,
            upsert_email,
        )

        inst_id = institution_id or get_default_institution_id()
        email_db_id = upsert_email(
            message_id=msg_id or email.get("message_id") or "",
            subject=subject,
            sender=sender,
            received_at=received_at,
            body=body,
            institution_id=inst_id,
        )

        # 1. Safety
        if not is_safe(body.encode()):
            logger.warning("extract_email: unsafe content in %s", msg_id)
            update_email_status(email_db_id, "quarantined", error="unsafe_content")
            return {"status": "unsafe", "message_id": msg_id}

        # 2. Classify
        from src.api.routes.pipeline import _classify_email  # local import to avoid circular
        classification, confidence = _classify_email({"subject": subject, "body": body})
        update_email_classification(email_db_id, classification, confidence)
        if classification != "result_email":
            update_email_status(email_db_id, "skipped")
            return {"status": "skipped", "classification": classification}

        # 3. Multi-strategy extract
        full_text = f"{subject}\n{body}".strip()
        records = extract_with_voting(full_text, [], run_llm=True)

        # 4. Validate
        records, _vr = validate_and_correct(records, full_text)

        if not records:
            save_extraction(
                email_id=email_db_id,
                records=[],
                confidence=0.0,
                strategy="multi_strategy",
            )
            update_email_status(email_db_id, "processed_no_records")
            return {"status": "no_records", "message_id": msg_id}

        # 5. Compute confidence
        confidence = (
            records[0].get("overall_confidence", records[0].get("confidence", 0.5))
            if records else 0.0
        )
        save_extraction(
            email_id=email_db_id,
            records=records,
            confidence=confidence,
            strategy="multi_strategy",
        )
        if confidence < REVIEW_THRESHOLD:
            queue_id = enqueue_for_review(
                email_id=msg_id,
                email_subject=subject,
                email_from=sender,
                raw_text=body,
                extracted_records=records,
                confidence=confidence,
            )
            update_email_status(email_db_id, "queued_for_review")
            return {"status": "queued_for_review", "queue_id": queue_id}

        # 6. Save
        saved = 0
        for rec in records:
            usn = rec.get("usn")
            if not usn:
                continue
            student_id = upsert_student(usn, rec.get("name", usn), institution_id=inst_id, source="pipeline")
            for subj in rec.get("subjects", []):
                subj_code = subj.get("subject_code", "UNKNOWN")
                subj_name = subj.get("subject_name", subj_code)
                subject_id = get_or_create_subject(inst_id, subj_code, subj_name, rec.get("semester"))
                upsert_result(
                    student_id=student_id,
                    subject_id=subject_id,
                    semester=rec.get("semester", 1),
                    marks_obtained=subj.get("total_marks"),
                    max_marks=subj.get("max_marks", 100),
                    grade=subj.get("grade"),
                    grade_points=subj.get("grade_points"),
                    status=subj.get("status"),
                )
                saved += 1
            compute_and_store_cgpa(student_id)

        update_email_status(
            email_db_id,
            "completed" if saved > 0 else "processed_no_records",
        )

        # 7. Enqueue indexing for each USN
        usns = {r.get("usn") for r in records if r.get("usn")}
        for usn in usns:
            index_student.apply_async(kwargs={"usn": usn}, queue="indexing")

        # 8. Send acknowledgement reply (best-effort, never blocks pipeline)
        if sender:
            try:
                from src.common.email_sender import send_extraction_confirmation
                send_extraction_confirmation(
                    sender_email=sender,
                    original_subject=subject,
                    records_saved=saved,
                    reply_to_message_id=email.get("message_id") or msg_id,
                    thread_id=email.get("threadId"),
                )
            except Exception as _reply_exc:
                logger.debug("extract_email: auto-reply failed (non-fatal): %s", _reply_exc)

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
        from src.common.database import (
            upsert_student, upsert_result,
            get_or_create_subject, get_default_institution_id, compute_and_store_cgpa,
        )

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
            # Convert raw Phase 2 table rows to structured dicts for the merger
            from src.phase3_extraction_engine.strategy_merger import raw_tables_to_doc_records
            doc_records = raw_tables_to_doc_records(parsed.tables)

            # Phase 3 extract + validate
            records = extract_with_voting(text, doc_records, run_llm=True)
            records, _vr = validate_and_correct(records, text)

            saved = 0
            inst_id = institution_id or get_default_institution_id()
            for rec in records:
                usn = rec.get("usn")
                if not usn:
                    continue
                student_id = upsert_student(usn, rec.get("name", usn), institution_id=inst_id, source="pipeline")
                for subj in rec.get("subjects", []):
                    subj_code = subj.get("subject_code", "UNKNOWN")
                    subj_name = subj.get("subject_name", subj_code)
                    subj_credits = int(subj.get("credits") or 3)
                    subject_id = get_or_create_subject(
                        inst_id, subj_code, subj_name,
                        rec.get("semester"),
                        credits=subj_credits,
                    )
                    upsert_result(
                        student_id=student_id,
                        subject_id=subject_id,
                        semester=rec.get("semester", 1),
                        marks_obtained=subj.get("total_marks"),
                        max_marks=subj.get("max_marks", 100),
                        grade=subj.get("grade"),
                        grade_points=subj.get("grade_points"),
                        status=subj.get("status"),
                    )
                    saved += 1
                compute_and_store_cgpa(student_id)

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
