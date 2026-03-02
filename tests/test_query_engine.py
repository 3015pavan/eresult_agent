"""Tests for Phase 5 — Query Engine."""

import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from src.common.models import QueryIntent


class TestIntentParser:
    """Tests for the intent parser."""

    def test_usn_pattern_detection(self):
        """Test that USN patterns are detected as student_lookup."""
        from src.phase5_query_engine.intent_parser import IntentParser

        with patch("src.phase5_query_engine.intent_parser.get_settings") as mock:
            mock.return_value = MagicMock()
            parser = IntentParser()
            intent = parser._quick_pattern_match("Show results for 1BM21CS001")
            assert intent == QueryIntent.STUDENT_LOOKUP

    def test_top_n_detection(self):
        """Test that top-N queries are detected."""
        from src.phase5_query_engine.intent_parser import IntentParser

        with patch("src.phase5_query_engine.intent_parser.get_settings") as mock:
            mock.return_value = MagicMock()
            parser = IntentParser()
            intent = parser._quick_pattern_match("Top 10 students by SGPA")
            assert intent == QueryIntent.TOP_N

    def test_backlog_detection(self):
        """Test that backlog queries are detected."""
        from src.phase5_query_engine.intent_parser import IntentParser

        with patch("src.phase5_query_engine.intent_parser.get_settings") as mock:
            mock.return_value = MagicMock()
            parser = IntentParser()
            intent = parser._quick_pattern_match("Students with backlogs in CS")
            assert intent == QueryIntent.BACKLOGS

    def test_aggregation_detection(self):
        """Test that aggregation queries are detected."""
        from src.phase5_query_engine.intent_parser import IntentParser

        with patch("src.phase5_query_engine.intent_parser.get_settings") as mock:
            mock.return_value = MagicMock()
            parser = IntentParser()
            intent = parser._quick_pattern_match("What is the average CGPA?")
            assert intent == QueryIntent.AGGREGATION


class TestSQLGenerator:
    """Tests for the SQL generator."""

    @pytest.fixture
    def generator(self, mock_settings):
        with patch("src.phase5_query_engine.sql_generator.get_settings", return_value=mock_settings):
            from src.phase5_query_engine.sql_generator import SQLGenerator
            return SQLGenerator()

    def test_student_lookup_by_usn(self, generator):
        """Test SQL generation for student lookup by USN."""
        from src.common.models import ParsedQuery

        parsed = ParsedQuery(
            raw_query="Show results for 1BM21CS001",
            intent=QueryIntent.STUDENT_LOOKUP,
            entities={"usn": "1BM21CS001"},
            filters={},
            confidence=0.9,
        )

        sql, params = generator.generate(parsed)

        assert "students" in sql.lower()
        assert "$1" in sql
        assert params == ["1BM21CS001"]

    def test_top_n_with_limit(self, generator):
        """Test SQL generation for top-N query."""
        from src.common.models import ParsedQuery

        parsed = ParsedQuery(
            raw_query="Top 5 students",
            intent=QueryIntent.TOP_N,
            entities={"limit": 5, "metric": "sgpa"},
            filters={},
            confidence=0.8,
        )

        sql, params = generator.generate(parsed)

        assert "LIMIT" in sql.upper()
        assert 5 in params

    def test_dangerous_sql_blocked(self, generator):
        """Test that dangerous SQL keywords are blocked."""
        from src.phase5_query_engine.sql_generator import SecurityError

        with pytest.raises(SecurityError):
            generator._validate_sql("DROP TABLE students")

    def test_count_query(self, generator):
        """Test SQL generation for count query."""
        from src.common.models import ParsedQuery

        parsed = ParsedQuery(
            raw_query="How many students passed?",
            intent=QueryIntent.COUNT,
            entities={},
            filters={"status": "PASS"},
            confidence=0.8,
        )

        sql, params = generator.generate(parsed)
        assert "COUNT" in sql.upper()


class TestAggregationEngine:
    """Tests for the aggregation engine."""

    @pytest.fixture
    def engine(self):
        from src.phase5_query_engine.aggregation_engine import AggregationEngine
        return AggregationEngine()

    def test_sgpa_computation(self, engine):
        """Test SGPA is computed correctly."""
        subjects = [
            {"grade": "A", "credits": 4, "status": "PASS"},   # 8.0 * 4 = 32
            {"grade": "B+", "credits": 3, "status": "PASS"},  # 7.0 * 3 = 21
            {"grade": "A+", "credits": 4, "status": "PASS"},  # 9.0 * 4 = 36
        ]
        # Total: 89 / 11 = 8.09

        result = engine.compute_sgpa(subjects)

        assert result.sgpa == pytest.approx(8.09, abs=0.01)
        assert result.credits_earned == 11
        assert result.subjects_passed == 3
        assert result.subjects_failed == 0

    def test_sgpa_with_failure(self, engine):
        """Test SGPA with failed subject."""
        subjects = [
            {"grade": "A", "credits": 4, "status": "PASS"},  # 8.0 * 4 = 32
            {"grade": "F", "credits": 3, "status": "FAIL"},  # 0.0 * 3 = 0
        ]
        # Total: 32 / 7 = 4.57

        result = engine.compute_sgpa(subjects)

        assert result.sgpa == pytest.approx(4.57, abs=0.01)
        assert result.credits_earned == 4
        assert result.subjects_passed == 1
        assert result.subjects_failed == 1

    def test_cgpa_computation(self, engine):
        """Test CGPA from multiple semesters."""
        from src.phase5_query_engine.aggregation_engine import SGPAResult

        semesters = [
            SGPAResult(sgpa=8.0, credits_earned=20, credits_attempted=22, subjects_passed=5, subjects_failed=1, grade_point_sum=170),
            SGPAResult(sgpa=7.5, credits_earned=22, credits_attempted=22, subjects_passed=6, subjects_failed=0, grade_point_sum=165),
        ]

        result = engine.compute_cgpa(semesters)

        assert 7.0 <= result.cgpa <= 8.5
        assert result.semesters_completed == 2
        assert result.total_subjects_passed == 11

    def test_batch_statistics(self, engine):
        """Test batch statistics computation."""
        cgpas = [7.5, 8.0, 6.5, 9.0, 7.0, 8.5, 5.5, 7.8, 8.2, 6.0]

        stats = engine.compute_batch_statistics(cgpas)

        assert stats.total_students == 10
        assert 6.0 <= stats.avg_cgpa <= 8.5
        assert stats.min_cgpa == 5.5
        assert stats.max_cgpa == 9.0
        assert stats.std_cgpa > 0
        assert 50 in stats.percentiles

    def test_empty_batch_statistics(self, engine):
        """Test batch statistics with no data."""
        stats = engine.compute_batch_statistics([])
        assert stats.total_students == 0
        assert stats.avg_cgpa == 0.0
