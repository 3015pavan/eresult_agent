"""
Tool Registry.

Provides a typed registry of tools available to the agent.
Each tool has:
  - name: unique identifier
  - description: natural-language description for the planner
  - parameters: JSON schema of expected inputs
  - handler: async callable implementing the tool logic
  - cost: estimated execution cost (tokens, latency, $)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

from src.common.observability import get_logger

logger = get_logger(__name__)


@dataclass
class ToolSpec:
    """Specification of a single tool."""

    name: str
    description: str
    parameters: dict[str, Any]
    handler: Callable[..., Awaitable[Any]]
    cost_estimate: float = 0.0  # approx cost in USD per call
    timeout_seconds: float = 60.0
    requires_confirmation: bool = False
    max_retries: int = 2
    tags: list[str] = field(default_factory=list)

    def to_schema(self) -> dict[str, Any]:
        """Convert to OpenAI function-calling schema."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    """
    Registry of tools available to the planning agent.

    Tools span all pipeline phases:
      - Email: fetch_emails, classify_email, check_duplicate
      - Document: parse_document, detect_tables, run_ocr
      - Extraction: extract_records, validate_records, correct_record
      - Database: store_records, query_student, update_cgpa
      - Utility: send_notification, log_event
    """

    def __init__(self) -> None:
        self._tools: dict[str, ToolSpec] = {}
        self._register_builtin_tools()

    def register(self, spec: ToolSpec) -> None:
        """Register a new tool."""
        if spec.name in self._tools:
            logger.warning("tool_overwrite", name=spec.name)
        self._tools[spec.name] = spec
        logger.debug("tool_registered", name=spec.name)

    def get(self, name: str) -> ToolSpec | None:
        """Retrieve a tool by name."""
        return self._tools.get(name)

    def list_tools(self, tags: list[str] | None = None) -> list[ToolSpec]:
        """List all tools, optionally filtered by tags."""
        if tags:
            return [
                t for t in self._tools.values()
                if any(tag in t.tags for tag in tags)
            ]
        return list(self._tools.values())

    def get_schemas(self, tags: list[str] | None = None) -> list[dict]:
        """Get OpenAI function-calling schemas for tools."""
        return [t.to_schema() for t in self.list_tools(tags)]

    async def execute(
        self,
        name: str,
        arguments: dict[str, Any],
    ) -> Any:
        """Execute a tool by name with given arguments."""
        tool = self._tools.get(name)
        if tool is None:
            raise ValueError(f"Unknown tool: {name}")

        logger.info("tool_execute_start", tool=name)

        try:
            result = await asyncio.wait_for(
                tool.handler(**arguments),
                timeout=tool.timeout_seconds,
            )
            logger.info("tool_execute_success", tool=name)
            return result

        except asyncio.TimeoutError:
            logger.error("tool_execute_timeout", tool=name, timeout=tool.timeout_seconds)
            raise
        except Exception as e:
            logger.error("tool_execute_error", tool=name, error=str(e))
            raise

    def _register_builtin_tools(self) -> None:
        """Register all built-in pipeline tools."""

        # ── Email Intelligence Tools ────────────────────────────────
        self.register(ToolSpec(
            name="fetch_emails",
            description=(
                "Fetch new emails from configured email accounts. "
                "Returns a list of raw email messages with metadata."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "max_count": {
                        "type": "integer",
                        "description": "Maximum number of emails to fetch",
                        "default": 50,
                    },
                    "since_hours": {
                        "type": "integer",
                        "description": "Fetch emails from last N hours",
                        "default": 24,
                    },
                },
            },
            handler=self._fetch_emails,
            cost_estimate=0.0,
            tags=["email", "ingestion"],
        ))

        self.register(ToolSpec(
            name="classify_email",
            description=(
                "Classify an email as academic-result, administrative, "
                "or irrelevant. Returns classification label and confidence."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "email_id": {
                        "type": "string",
                        "description": "Email message ID",
                    },
                },
                "required": ["email_id"],
            },
            handler=self._classify_email,
            cost_estimate=0.001,
            tags=["email", "classification"],
        ))

        self.register(ToolSpec(
            name="check_duplicate",
            description="Check if an email is a duplicate using multi-strategy dedup.",
            parameters={
                "type": "object",
                "properties": {
                    "email_id": {"type": "string"},
                },
                "required": ["email_id"],
            },
            handler=self._check_duplicate,
            cost_estimate=0.0,
            tags=["email", "dedup"],
        ))

        # ── Document Intelligence Tools ────────────────────────────
        self.register(ToolSpec(
            name="parse_document",
            description=(
                "Parse an attachment (PDF/Excel/CSV) and extract tables. "
                "Returns structured table data."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "attachment_id": {"type": "string"},
                    "document_type": {
                        "type": "string",
                        "enum": ["pdf", "excel", "csv"],
                    },
                },
                "required": ["attachment_id"],
            },
            handler=self._parse_document,
            cost_estimate=0.0,
            timeout_seconds=120,
            tags=["document", "parsing"],
        ))

        self.register(ToolSpec(
            name="run_ocr",
            description="Run OCR on a scanned PDF/image attachment.",
            parameters={
                "type": "object",
                "properties": {
                    "attachment_id": {"type": "string"},
                },
                "required": ["attachment_id"],
            },
            handler=self._run_ocr,
            cost_estimate=0.01,
            timeout_seconds=180,
            tags=["document", "ocr"],
        ))

        # ── Extraction Tools ───────────────────────────────────────
        self.register(ToolSpec(
            name="extract_records",
            description=(
                "Run the full extraction pipeline (rule + regex + LLM) "
                "on parsed table data. Returns merged student records."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                },
                "required": ["table_id"],
            },
            handler=self._extract_records,
            cost_estimate=0.05,
            timeout_seconds=120,
            tags=["extraction"],
        ))

        self.register(ToolSpec(
            name="validate_records",
            description=(
                "Validate extracted records against domain constraints. "
                "Returns validation result with errors and warnings."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "records_json": {"type": "string"},
                },
                "required": ["records_json"],
            },
            handler=self._validate_records,
            cost_estimate=0.0,
            tags=["extraction", "validation"],
        ))

        self.register(ToolSpec(
            name="correct_record",
            description="Use LLM to correct specific field errors in a record.",
            parameters={
                "type": "object",
                "properties": {
                    "record_json": {"type": "string"},
                    "errors": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "table_id": {"type": "string"},
                },
                "required": ["record_json", "errors"],
            },
            handler=self._correct_record,
            cost_estimate=0.02,
            tags=["extraction", "correction"],
        ))

        # ── Database Tools ─────────────────────────────────────────
        self.register(ToolSpec(
            name="store_records",
            description="Store validated student records to the database.",
            parameters={
                "type": "object",
                "properties": {
                    "records_json": {"type": "string"},
                    "email_id": {"type": "string"},
                },
                "required": ["records_json"],
            },
            handler=self._store_records,
            cost_estimate=0.0,
            tags=["database", "storage"],
        ))

        self.register(ToolSpec(
            name="query_student",
            description="Query student records from the database by USN or name.",
            parameters={
                "type": "object",
                "properties": {
                    "usn": {"type": "string"},
                    "name": {"type": "string"},
                },
            },
            handler=self._query_student,
            cost_estimate=0.0,
            tags=["database", "query"],
        ))

        self.register(ToolSpec(
            name="recompute_cgpa",
            description="Recompute CGPA for a student after new results are stored.",
            parameters={
                "type": "object",
                "properties": {
                    "student_id": {"type": "integer"},
                },
                "required": ["student_id"],
            },
            handler=self._recompute_cgpa,
            cost_estimate=0.0,
            tags=["database", "aggregation"],
        ))

        # ── Utility Tools ──────────────────────────────────────────
        self.register(ToolSpec(
            name="send_notification",
            description="Send a notification about processing status.",
            parameters={
                "type": "object",
                "properties": {
                    "channel": {
                        "type": "string",
                        "enum": ["email", "slack", "webhook"],
                    },
                    "message": {"type": "string"},
                },
                "required": ["channel", "message"],
            },
            handler=self._send_notification,
            cost_estimate=0.0,
            tags=["utility"],
        ))

    # ── Tool Handler Stubs ──────────────────────────────────────────
    # In production, these delegate to actual phase modules.

    async def _fetch_emails(self, max_count: int = 50, since_hours: int = 24) -> dict:
        from src.phase1_email_intelligence.ingestion import EmailIngestionService
        svc = EmailIngestionService()
        emails = await svc.fetch_new_emails(max_count=max_count)
        return {"count": len(emails), "email_ids": [e.message_id for e in emails]}

    async def _classify_email(self, email_id: str) -> dict:
        return {"email_id": email_id, "status": "delegated_to_classifier"}

    async def _check_duplicate(self, email_id: str) -> dict:
        return {"email_id": email_id, "is_duplicate": False}

    async def _parse_document(self, attachment_id: str, document_type: str = "pdf") -> dict:
        return {"attachment_id": attachment_id, "tables_found": 0}

    async def _run_ocr(self, attachment_id: str) -> dict:
        return {"attachment_id": attachment_id, "status": "ocr_complete"}

    async def _extract_records(self, table_id: str) -> dict:
        return {"table_id": table_id, "records_extracted": 0}

    async def _validate_records(self, records_json: str) -> dict:
        return {"is_valid": True, "errors": [], "warnings": []}

    async def _correct_record(self, record_json: str, errors: list[str], table_id: str = "") -> dict:
        return {"status": "correction_attempted"}

    async def _store_records(self, records_json: str, email_id: str = "") -> dict:
        return {"status": "stored", "count": 0}

    async def _query_student(self, usn: str = "", name: str = "") -> dict:
        return {"results": []}

    async def _recompute_cgpa(self, student_id: int) -> dict:
        return {"student_id": student_id, "status": "recomputed"}

    async def _send_notification(self, channel: str, message: str) -> dict:
        logger.info("notification_sent", channel=channel)
        return {"status": "sent"}
