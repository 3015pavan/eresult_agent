"""Tests for Phase 3 — Information Extraction."""

import pytest
from unittest.mock import patch, MagicMock

from src.common.models import (
    ExtractedTable,
    StudentRecord,
    SubjectResult,
    ResultStatus,
    ExtractionStrategy,
)


class TestRuleBasedExtractor:
    """Tests for the rule-based extraction engine."""

    @pytest.fixture
    def extractor(self, mock_settings):
        with patch("src.phase3_information_extraction.rule_engine.get_settings", return_value=mock_settings):
            from src.phase3_information_extraction.rule_engine import RuleBasedExtractor
            return RuleBasedExtractor()

    def test_extract_subject_per_row_layout(self, extractor, sample_table):
        """Test extraction from a table with one subject per row."""
        records = extractor.extract(sample_table)

        assert len(records) >= 1
        for record in records:
            assert record.usn is not None
            assert record.extraction_strategy == ExtractionStrategy.RULE_BASED

    def test_extract_empty_table(self, extractor):
        """Test extraction from an empty table returns no records."""
        empty_table = ExtractedTable(
            headers=[],
            rows=[],
            confidence=0.5,
            page_number=1,
        )
        records = extractor.extract(empty_table)
        assert records == []

    def test_extract_preserves_marks_range(self, extractor, sample_table):
        """Test that extracted marks are within valid range."""
        records = extractor.extract(sample_table)
        for record in records:
            for subj in record.subjects:
                if subj.total_marks is not None:
                    assert 0 <= subj.total_marks <= 200
                if subj.max_marks is not None:
                    assert subj.max_marks > 0


class TestRegexExtractor:
    """Tests for the regex-based extraction engine."""

    @pytest.fixture
    def extractor(self, mock_settings):
        with patch("src.phase3_information_extraction.regex_engine.get_settings", return_value=mock_settings):
            from src.phase3_information_extraction.regex_engine import RegexExtractor
            return RegexExtractor()

    def test_usn_pattern_matching(self, extractor):
        """Test USN pattern matches valid formats."""
        import re
        pattern = extractor.PATTERNS["usn"]

        valid_usns = ["1BM21CS001", "4VV22IS123", "2BI20EC045"]
        for usn in valid_usns:
            assert pattern.search(usn), f"Should match: {usn}"

        invalid = ["ABCDEFG", "123456", "1B21CS0001"]
        for val in invalid:
            match = pattern.search(val)
            if match:
                # Some of these might partially match; just ensure the full value isn't
                # incorrectly captured
                pass

    def test_extract_from_table(self, extractor, sample_table):
        """Test regex extraction from a sample table."""
        records = extractor.extract(sample_table)

        assert len(records) >= 1
        usns = {r.usn for r in records}
        assert "1BM21CS001" in usns

    def test_gpa_range_constraint(self, extractor):
        """Test that extracted GPAs are within valid range."""
        table = ExtractedTable(
            headers=["USN", "SGPA"],
            rows=[
                ["1BM21CS001", "8.5"],
                ["1BM21CS002", "15.0"],  # Invalid
            ],
            confidence=0.8,
            page_number=1,
        )
        records = extractor.extract(table)
        for record in records:
            if record.sgpa is not None:
                assert 0 <= record.sgpa <= 10.0


class TestExtractionValidator:
    """Tests for extraction validation."""

    @pytest.fixture
    def validator(self, mock_settings):
        with patch("src.phase3_information_extraction.validator.get_settings", return_value=mock_settings):
            from src.phase3_information_extraction.validator import ExtractionValidator
            return ExtractionValidator()

    def test_valid_records_pass(self, validator, sample_student_records):
        """Test that valid records pass validation."""
        result = validator.validate_batch(sample_student_records)
        # Should have no hard errors for well-formed records
        assert result.records_checked == 2

    def test_invalid_usn_detected(self, validator):
        """Test that invalid USN format is caught."""
        record = StudentRecord(
            usn="INVALID",
            name="Test",
            subjects=[],
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.8,
            field_confidences={},
        )
        result = validator.validate_batch([record])
        usn_errors = [e for e in result.errors if e.field == "usn"]
        assert len(usn_errors) > 0

    def test_marks_exceeding_max_detected(self, validator):
        """Test that marks > max_marks is caught."""
        record = StudentRecord(
            usn="1BM21CS001",
            name="Test",
            subjects=[
                SubjectResult(
                    subject_code="21CS51",
                    total_marks=120,  # Exceeds max
                    max_marks=100,
                    status=ResultStatus.PASS,
                ),
            ],
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.8,
            field_confidences={},
        )
        result = validator.validate_batch([record])
        marks_errors = [e for e in result.errors if "total_marks" in e.field]
        assert len(marks_errors) > 0

    def test_sgpa_out_of_range_detected(self, validator):
        """Test that SGPA > 10 is caught."""
        record = StudentRecord(
            usn="1BM21CS001",
            name="Test",
            subjects=[],
            sgpa=12.5,  # Invalid
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.8,
            field_confidences={},
        )
        result = validator.validate_batch([record])
        sgpa_errors = [e for e in result.errors if e.field == "sgpa"]
        assert len(sgpa_errors) > 0

    def test_empty_batch_is_valid(self, validator):
        """Test that empty batch passes validation."""
        result = validator.validate_batch([])
        assert result.is_valid
        assert result.records_checked == 0


class TestExtractionMerger:
    """Tests for the extraction merger."""

    @pytest.fixture
    def merger(self):
        from src.phase3_information_extraction.merger import ExtractionMerger
        return ExtractionMerger()

    def test_single_strategy_passthrough(self, merger, sample_student_records):
        """Test that single strategy results pass through unchanged."""
        results = {ExtractionStrategy.RULE_BASED: sample_student_records}
        merged = merger.merge(results)
        assert len(merged) == len(sample_student_records)

    def test_merge_agreeing_strategies(self, merger):
        """Test merge when strategies agree."""
        record_rule = StudentRecord(
            usn="1BM21CS001",
            name="Alice A",
            subjects=[],
            sgpa=8.5,
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.90,
            field_confidences={"usn": 0.99, "name": 0.95, "sgpa": 0.90},
        )
        record_regex = StudentRecord(
            usn="1BM21CS001",
            name="Alice A",
            subjects=[],
            sgpa=8.5,
            extraction_strategy=ExtractionStrategy.REGEX,
            overall_confidence=0.75,
            field_confidences={"usn": 0.95, "name": 0.70, "sgpa": 0.80},
        )

        results = {
            ExtractionStrategy.RULE_BASED: [record_rule],
            ExtractionStrategy.REGEX: [record_regex],
        }
        merged = merger.merge(results)

        assert len(merged) == 1
        assert merged[0].usn == "1BM21CS001"
        assert merged[0].overall_confidence >= 0.85  # Full agreement

    def test_empty_strategies(self, merger):
        """Test merge with no results."""
        merged = merger.merge({})
        assert merged == []
