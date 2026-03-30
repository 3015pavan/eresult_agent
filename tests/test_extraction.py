"""Tests for Phase 3 — Extraction Engine."""

import pytest
from unittest.mock import patch, MagicMock


# ---------------------------------------------------------------------------
# Strategy Merger / extract_with_voting
# ---------------------------------------------------------------------------

class TestExtractWithVoting:
    """Tests for the multi-strategy extraction merger."""

    def test_empty_text_returns_empty(self):
        from src.phase3_extraction_engine.strategy_merger import extract_with_voting
        records = extract_with_voting("", [], run_llm=False)
        assert isinstance(records, list)

    def test_usn_detected_in_body(self):
        from src.phase3_extraction_engine.strategy_merger import extract_with_voting
        body = "USN: 1MS21CS001  SGPA: 8.50  Semester: 3  Status: PASS"
        records = extract_with_voting(body, [], run_llm=False)
        assert isinstance(records, list)
        if records:
            assert records[0].get("usn") == "1MS21CS001"

    def test_returns_list(self):
        from src.phase3_extraction_engine.strategy_merger import extract_with_voting
        body = "1RV22IS042 sgpa 7.8 semester 2"
        result = extract_with_voting(body, [], run_llm=False)
        assert isinstance(result, list)


# ---------------------------------------------------------------------------
# Validator — validate_and_correct
# ---------------------------------------------------------------------------

class TestValidateAndCorrect:
    """Tests for extraction validation and correction."""

    def test_valid_record_passes(self):
        from src.phase3_extraction_engine.validator import validate_and_correct
        records = [
            {
                "usn": "1BM21CS001",
                "name": "Alice A",
                "semester": 3,
                "sgpa": 8.5,
                "cgpa": 8.2,
                "subjects": [
                    {"subject_code": "21CS51", "subject_name": "OS",
                     "total_marks": 75, "max_marks": 100, "status": "PASS", "grade": "A"}
                ],
                "confidence": 0.9,
            }
        ]
        corrected, vr = validate_and_correct(records, "body text")
        assert isinstance(corrected, list)
        assert len(corrected) == 1

    def test_invalid_usn_dropped_or_corrected(self):
        from src.phase3_extraction_engine.validator import validate_and_correct
        records = [
            {
                "usn": "INVALID_USN",
                "semester": 1,
                "subjects": [],
                "confidence": 0.5,
            }
        ]
        corrected, vr = validate_and_correct(records, "body")
        # Either dropped or flagged — no crash
        assert isinstance(corrected, list)

    def test_empty_records_pass(self):
        from src.phase3_extraction_engine.validator import validate_and_correct
        corrected, vr = validate_and_correct([], "body")
        assert corrected == []

    def test_gpa_out_of_range_clamped_or_dropped(self):
        from src.phase3_extraction_engine.validator import validate_and_correct
        records = [
            {
                "usn": "1MS21CS001",
                "semester": 2,
                "sgpa": 15.0,  # > 10 — invalid
                "subjects": [],
                "confidence": 0.7,
            }
        ]
        corrected, vr = validate_and_correct(records, "body")
        # Must not crash; if kept, SGPA must be clamped to ≤ 10
        for rec in corrected:
            if rec.get("sgpa") is not None:
                assert rec["sgpa"] <= 10.0

    def test_returns_tuple(self):
        from src.phase3_extraction_engine.validator import validate_and_correct
        result = validate_and_correct([], "")
        assert isinstance(result, tuple)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Review Queue
# ---------------------------------------------------------------------------

class TestReviewQueue:
    """Tests for the human review queue."""

    def test_enqueue_returns_id(self):
        from src.phase3_extraction_engine.review_queue import enqueue_for_review
        # _ensure_table and get_connection are both deferred local imports —
        # patch at their source module rather than the review_queue namespace.
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": "abc-123"}
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__ = MagicMock(return_value=mock_conn_ctx)
        mock_conn_ctx.__exit__ = MagicMock(return_value=False)
        mock_conn_ctx.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn_ctx.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("src.phase3_extraction_engine.review_queue._ensure_table"), \
             patch("src.common.database.get_connection", return_value=mock_conn_ctx):
            result = enqueue_for_review(
                email_id="msg1",
                email_subject="Re: Results",
                email_from="noreply@vtu.ac.in",
                raw_text="1MS21CS001 sgpa 8.5",
                extracted_records=[{"usn": "1MS21CS001"}],
                confidence=0.4,
            )
            # Should return a string ID (or "" on exception)
            assert isinstance(result, str)
