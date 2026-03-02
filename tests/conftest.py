"""Test configuration and shared fixtures."""

import pytest
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_settings():
    """Mock application settings."""
    settings = MagicMock()
    settings.environment = "test"
    settings.email.poll_interval_seconds = 60
    settings.email.classification_threshold = 0.7
    settings.extraction.gpa_max = 10.0
    settings.extraction.marks_max_default = 100
    settings.extraction.usn_pattern = r"[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}"
    settings.extraction.llm_model = "gpt-4o"
    settings.agent.max_steps = 15
    settings.agent.planner_model = "gpt-4o"
    settings.query.intent_model = "gpt-4o"
    settings.query.answer_model = "gpt-4o"
    settings.database.url = "postgresql://test:test@localhost/test"
    settings.redis.host = "localhost"
    settings.redis.port = 6379
    settings.redis.db = 0
    settings.security.allowed_origins = ["*"]
    settings.llm.providers = {
        "openai": {"api_key": "test-key"},
        "google": {"api_key": "test-key"},
    }
    return settings


@pytest.fixture
def sample_table():
    """Sample extracted table for testing."""
    from src.common.models import ExtractedTable
    return ExtractedTable(
        headers=["USN", "Name", "Subject", "Marks", "Max", "Grade", "Status"],
        rows=[
            ["1BM21CS001", "Alice A", "21CS51", "85", "100", "A", "PASS"],
            ["1BM21CS002", "Bob B", "21CS51", "42", "100", "D", "PASS"],
            ["1BM21CS003", "Carol C", "21CS51", "30", "100", "F", "FAIL"],
        ],
        confidence=0.90,
        page_number=1,
    )


@pytest.fixture
def sample_student_records():
    """Sample student records for testing."""
    from src.common.models import StudentRecord, SubjectResult, ResultStatus, ExtractionStrategy

    return [
        StudentRecord(
            usn="1BM21CS001",
            name="Alice A",
            subjects=[
                SubjectResult(
                    subject_code="21CS51",
                    subject_name="Data Structures",
                    total_marks=85,
                    max_marks=100,
                    grade="A",
                    status=ResultStatus.PASS,
                ),
            ],
            sgpa=8.5,
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.92,
            field_confidences={"usn": 0.99, "name": 0.95, "subjects": 0.88, "sgpa": 0.90},
        ),
        StudentRecord(
            usn="1BM21CS002",
            name="Bob B",
            subjects=[
                SubjectResult(
                    subject_code="21CS51",
                    subject_name="Data Structures",
                    total_marks=42,
                    max_marks=100,
                    grade="D",
                    status=ResultStatus.PASS,
                ),
            ],
            sgpa=4.0,
            extraction_strategy=ExtractionStrategy.RULE_BASED,
            overall_confidence=0.90,
            field_confidences={"usn": 0.99, "name": 0.95, "subjects": 0.85, "sgpa": 0.88},
        ),
    ]
