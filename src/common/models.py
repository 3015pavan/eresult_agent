"""
Pydantic domain models shared across all phases.

These models enforce schema validation at every boundary in the pipeline,
preventing malformed data from propagating downstream.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


# =============================================================================
# ENUMS
# =============================================================================

class EmailClassification(str, Enum):
    RESULT_EMAIL = "result_email"
    SPAM = "spam"
    ADMINISTRATIVE = "administrative"
    OTHER = "other"


class DocumentType(str, Enum):
    PDF_NATIVE = "pdf_native"
    PDF_SCANNED = "pdf_scanned"
    EXCEL = "excel"
    CSV = "csv"
    UNKNOWN = "unknown"


class ExtractionStrategy(str, Enum):
    RULE_BASED = "rule_based"
    REGEX = "regex"
    LLM = "llm"
    HYBRID = "hybrid"


class ResultStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    ABSENT = "ABSENT"
    WITHHELD = "WITHHELD"


class ExamType(str, Enum):
    REGULAR = "regular"
    SUPPLEMENTARY = "supplementary"
    IMPROVEMENT = "improvement"


class AgentState(str, Enum):
    IDLE = "IDLE"
    PLANNING = "PLANNING"
    EXECUTING = "EXECUTING"
    VERIFYING = "VERIFYING"
    RETRYING = "RETRYING"
    ERROR = "ERROR"
    COMPLETED = "COMPLETED"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    QUARANTINED = "quarantined"
    SKIPPED = "skipped"


class QueryIntent(str, Enum):
    STUDENT_LOOKUP = "student_lookup"
    TOP_N = "top_n"
    BACKLOGS = "backlogs"
    AGGREGATION = "aggregation"
    COUNT = "count"
    STUDENT_GPA = "student_gpa"
    STUDENT_CGPA = "student_cgpa"
    SEMESTER_PERFORMANCE = "semester_performance"
    SUBJECT_STATUS = "subject_status"
    BACKLOG_CHECK = "backlog_check"
    STUDENT_LIST_FILTER = "student_list_filter"
    COMPARISON = "comparison"
    AGGREGATE = "aggregate"
    UNKNOWN = "unknown"


# =============================================================================
# EMAIL MODELS
# =============================================================================

class EmailMessage(BaseModel):
    """Represents an ingested email message."""
    id: UUID = Field(default_factory=uuid4)
    message_id: str
    account_id: str
    from_address: str
    to_addresses: list[str]
    subject: str | None = None
    body_text: str | None = None
    body_html: str | None = None
    received_at: datetime
    body_hash: str = ""
    body_simhash: int | None = None
    thread_id: str | None = None
    raw_storage_path: str = ""
    attachments: list[AttachmentInfo] = Field(default_factory=list)


class AttachmentInfo(BaseModel):
    """Metadata about an email attachment."""
    id: UUID = Field(default_factory=uuid4)
    filename: str
    content_type: str
    file_size: int
    file_hash: str = ""
    storage_path: str = ""
    document_type: DocumentType = DocumentType.UNKNOWN


class ClassificationResult(BaseModel):
    """Result of email classification."""
    email_id: UUID
    classification: EmailClassification
    confidence: float = Field(ge=0.0, le=1.0)
    uncertainty: float = Field(ge=0.0, le=1.0, default=0.0)
    model_name: str = "distilbert-email-classifier"


# =============================================================================
# DOCUMENT MODELS
# =============================================================================

class TableCell(BaseModel):
    """A single cell in an extracted table."""
    row: int
    col: int
    text: str
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)
    bbox: tuple[float, float, float, float] | None = None  # x0, y0, x1, y1


class ExtractedTable(BaseModel):
    """A complete extracted table from a document."""
    page_number: int
    table_index: int = 0
    headers: list[str] = Field(default_factory=list)
    rows: list[list[str]] = Field(default_factory=list)
    cells: list[TableCell] = Field(default_factory=list)
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    num_rows: int = 0
    num_cols: int = 0

    @model_validator(mode="after")
    def compute_dimensions(self) -> "ExtractedTable":
        if self.rows and not self.num_rows:
            self.num_rows = len(self.rows)
        if self.headers and not self.num_cols:
            self.num_cols = len(self.headers)
        return self


class DocumentParseResult(BaseModel):
    """Complete result of parsing a document."""
    attachment_id: UUID
    document_type: DocumentType
    tables: list[ExtractedTable] = Field(default_factory=list)
    raw_text: str = ""
    page_count: int = 0
    ocr_used: bool = False
    ocr_confidence: float | None = None
    parse_method: str = ""  # e.g., "pdfplumber+camelot", "paddleocr+layoutlmv3"
    parse_time_ms: int = 0
    errors: list[str] = Field(default_factory=list)


# =============================================================================
# EXTRACTION MODELS
# =============================================================================

class SubjectResult(BaseModel):
    """Extracted result for a single subject."""
    subject_code: str | None = None
    subject_name: str | None = None
    internal_marks: int | None = None
    external_marks: int | None = None
    total_marks: int
    max_marks: int = 100
    grade: str | None = None
    grade_points: float | None = None
    credits: int | None = None
    status: ResultStatus

    @field_validator("total_marks")
    @classmethod
    def validate_marks(cls, v: int, info: Any) -> int:
        if v < 0:
            raise ValueError(f"Marks cannot be negative: {v}")
        return v

    @field_validator("grade_points")
    @classmethod
    def validate_grade_points(cls, v: float | None) -> float | None:
        if v is not None and (v < 0 or v > 10):
            raise ValueError(f"Grade points must be in [0, 10]: {v}")
        return v


class StudentRecord(BaseModel):
    """Complete extracted record for one student from one result document."""
    usn: str
    name: str
    semester: int | None = None
    academic_year: str | None = None
    exam_type: ExamType = ExamType.REGULAR
    sgpa: float | None = None
    subjects: list[SubjectResult] = Field(default_factory=list)
    extraction_strategy: ExtractionStrategy = ExtractionStrategy.HYBRID
    field_confidences: dict[str, float] = Field(default_factory=dict)
    overall_confidence: float = Field(ge=0.0, le=1.0, default=0.0)

    @field_validator("usn")
    @classmethod
    def validate_usn(cls, v: str) -> str:
        v = v.strip().upper()
        # Common OCR corrections
        v = v.replace("O", "0").replace("l", "1").replace(" ", "")
        return v

    @field_validator("sgpa")
    @classmethod
    def validate_sgpa(cls, v: float | None) -> float | None:
        if v is not None and (v < 0 or v > 10):
            raise ValueError(f"SGPA must be in [0, 10]: {v}")
        return v


class ExtractionResult(BaseModel):
    """Complete extraction output for one document."""
    extraction_id: UUID = Field(default_factory=uuid4)
    attachment_id: UUID
    strategy: ExtractionStrategy
    records: list[StudentRecord] = Field(default_factory=list)
    overall_confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    validation_errors: list[str] = Field(default_factory=list)
    model_used: str = ""
    extraction_time_ms: int = 0
    llm_tokens_used: int = 0


# =============================================================================
# QUERY MODELS
# =============================================================================

class ParsedQuery(BaseModel):
    """LLM-parsed representation of a teacher's natural language query."""
    raw_query: str
    intent: QueryIntent
    entities: dict[str, Any] = Field(default_factory=dict)
    filters: dict[str, Any] = Field(default_factory=dict)
    # Resolved entities
    student_usn: str | None = None
    student_name: str | None = None
    semester: int | None = None
    subject_code: str | None = None
    # Filters
    cgpa_threshold: float | None = None
    backlog_threshold: int | None = None
    status_filter: ResultStatus | None = None
    # Confidence
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)


class QueryResult(BaseModel):
    """Result of a teacher query."""
    query_id: UUID = Field(default_factory=uuid4)
    query: ParsedQuery
    answer: str
    data: list[dict[str, Any]] = Field(default_factory=list)
    sql_generated: str | None = None
    records_returned: int = 0
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    citations: list[str] = Field(default_factory=list)
    response_time_ms: int = 0


# =============================================================================
# AGENT MODELS
# =============================================================================

class ToolCall(BaseModel):
    """Represents a tool invocation by the agent."""
    tool_name: str
    tool_input: dict[str, Any] = Field(default_factory=dict)
    tool_output: dict[str, Any] | None = None
    success: bool = False
    duration_ms: int = 0
    error: str | None = None


class AgentStep(BaseModel):
    """One step in the agent execution trace."""
    step_number: int
    state: AgentState
    plan: str | None = None
    tool_call: ToolCall | None = None
    reflection: str | None = None
    confidence: float = Field(ge=0.0, le=1.0, default=0.0)
    duration_ms: int = 0


class AgentRun(BaseModel):
    """Complete trace of one agent execution run."""
    run_id: UUID = Field(default_factory=uuid4)
    trigger: str  # e.g., "new_email", "manual", "scheduled"
    steps: list[AgentStep] = Field(default_factory=list)
    final_state: AgentState = AgentState.IDLE
    total_duration_ms: int = 0
    emails_processed: int = 0
    records_extracted: int = 0
    errors: list[str] = Field(default_factory=list)


# Forward reference resolution
EmailMessage.model_rebuild()
