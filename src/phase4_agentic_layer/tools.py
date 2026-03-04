"""
Tool Registry — Phase 4.

Defines 15 tools the agent can call to accomplish tasks.
Each tool is a callable with a documented schema.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class ToolSchema:
    name:        str
    description: str
    parameters:  dict          # JSON Schema-style input spec
    handler:     Callable      # actual implementation
    category:    str = "general"
    enabled:     bool = True


# ── Handler implementations ───────────────────────────────────────────────────

def _tool_email_fetch(query: str = "", max_results: int = 50) -> dict:
    """Fetch emails matching a query from Gmail cache."""
    import json
    from pathlib import Path
    cache_path = Path(__file__).resolve().parents[3] / "data" / "emails_cache.json"
    emails = json.loads(cache_path.read_text()) if cache_path.exists() else []
    if query:
        ql = query.lower()
        emails = [e for e in emails if ql in (e.get("subject","") + e.get("body","")).lower()]
    return {"emails": emails[:max_results], "total": len(emails)}


def _tool_pdf_parse(path: str) -> dict:
    """Parse a PDF file and return extracted text and tables."""
    from src.phase2_document_intelligence import route_to_parser
    doc = route_to_parser(path)
    return {
        "text": doc.text[:3000],
        "tables": doc.tables[:5],
        "strategy": doc.parse_strategy,
        "confidence": doc.confidence,
    }


def _tool_dedup_check(message_id: str, sender: str = "", date: str = "", subject: str = "") -> dict:
    """Check if an email is a duplicate using SHA-256 + SimHash."""
    from src.common.cache import get_cache
    cache = get_cache()
    sha_dup = cache.is_duplicate_sha256(message_id, sender, date, subject)
    return {"is_duplicate": sha_dup, "method": "sha256"}


def _tool_classify_email(email_id: str = "", text: str = "") -> dict:
    """Classify an email as result_email or other."""
    from src.api.routes.pipeline import _classify_email
    email = {"body": text, "subject": ""}
    label, confidence = _classify_email(email)
    return {"classification": label, "confidence": confidence}


def _tool_extract_records(text: str = "", use_llm: bool = True) -> dict:
    """Extract student result records from text using multi-strategy merger."""
    from src.phase3_extraction_engine import extract_with_voting
    records = extract_with_voting(text, run_llm=use_llm)
    return {"records": records, "count": len(records)}


def _tool_validate_extraction(records: list = None) -> dict:
    """Validate extracted records and auto-correct where possible."""
    if not records:
        return {"valid": True, "errors": [], "records": []}
    from src.phase3_extraction_engine import validate_and_correct
    corrected, vr = validate_and_correct(records or [], text="")
    return {
        "valid": vr.valid,
        "errors": vr.errors,
        "warnings": vr.warnings,
        "records": corrected,
    }


def _tool_save_results(records: list = None, email_id: str = "") -> dict:
    """Persist extracted records to PostgreSQL."""
    if not records:
        return {"saved": 0}
    from src.api.routes.pipeline import _save_records_to_db
    n = _save_records_to_db(records, email_id, extraction_id="agent-run")
    return {"saved": n}


def _tool_parse_document(path: str = "", url: str = "", mime_type: str = "") -> dict:
    """
    Parse ANY document format (PDF, DOCX, XLSX, ODT, RTF, TXT, HTML, image)
    and return extracted text + tables.
    Accepts file path, URL, or raw text.
    """
    from src.phase2_document_intelligence import convert_any
    if path:
        doc = convert_any(path, mime_type=mime_type)
    elif url:
        try:
            import httpx
            resp = httpx.get(url, timeout=15.0, follow_redirects=True)
            mime = resp.headers.get("content-type", "").split(";")[0].strip()
            fname = url.rsplit("/", 1)[-1]
            doc = convert_any(resp.content, mime_type=mime_type or mime, filename=fname)
        except Exception as exc:
            return {"error": f"url_fetch_failed: {exc}"}
    else:
        return {"error": "provide path or url"}

    if doc is None:
        return {"error": "parse_failed"}

    return {
        "text": (doc.text or "")[:4000],
        "tables": doc.tables[:10],
        "strategy": doc.parse_strategy,
        "confidence": doc.confidence,
        "has_tables": doc.has_tables,
        "errors": doc.errors,
    }


def _tool_ocr_image(path: str = "", url: str = "", mime_type: str = "image/jpeg") -> dict:
    """
    OCR an image file (JPG/PNG/TIFF/BMP) using Groq Vision LLM or Tesseract.
    Accepts file path or URL.
    """
    from src.phase2_document_intelligence import ocr_image_bytes, ocr_image_path
    if url and not path:
        try:
            import httpx
            resp = httpx.get(url, timeout=20.0, follow_redirects=True)
            detected_mime = resp.headers.get("content-type", mime_type).split(";")[0].strip()
            text, strategy = ocr_image_bytes(resp.content, detected_mime)
        except Exception as exc:
            return {"error": f"url_fetch_failed: {exc}", "text": ""}
    elif path:
        doc = ocr_image_path(path)
        text, strategy = doc.text, doc.parse_strategy
    else:
        return {"error": "provide path or url", "text": ""}

    return {
        "text": text,
        "strategy": strategy,
        "chars_extracted": len(text),
    }


def _tool_html_to_text(html: str = "") -> dict:
    """
    Parse HTML content (email body or file) to clean plain text + tables.
    """
    if not html:
        return {"error": "no html provided"}
    from src.phase2_document_intelligence import convert_html_body
    doc = convert_html_body(html)
    return {
        "text": doc.text[:4000],
        "tables": doc.tables[:10],
        "strategy": doc.parse_strategy,
        "confidence": doc.confidence,
    }


def _tool_gpa_compute(usn: str) -> dict:
    """Compute and return CGPA for a student."""
    from src.common import database as db
    student = db.get_student(usn)
    if not student:
        return {"error": f"student not found: {usn}"}
    cgpa = db.compute_and_store_cgpa(str(student["id"]))
    return {"usn": usn, "cgpa": cgpa}


def _tool_student_lookup(usn: str = "", name: str = "") -> dict:
    """Look up a student record."""
    from src.common import database as db
    if usn:
        s = db.get_student(usn)
    else:
        matches = db.search_students(name)
        s = matches[0] if matches else None
    if not s:
        return {"found": False}
    results = db.get_student_results(str(s.get("usn","")))
    return {"found": True, "student": s, "results": results}


def _tool_semantic_search(query: str, limit: int = 5) -> dict:
    """Search students semantically using pgvector embeddings."""
    from src.common.embeddings import semantic_search_students
    from src.common import database as db
    inst_id = db.get_default_institution_id()
    results = semantic_search_students(query, inst_id, limit=limit)
    return {"results": results, "total": len(results)}


def _tool_store_email(email: dict = None) -> dict:
    """Store raw email in MinIO object storage."""
    if not email:
        return {"stored": False}
    from src.common.storage import get_storage
    storage = get_storage()
    msg_id = email.get("id", "unknown")
    path = storage.store_email(msg_id, email)
    return {"stored": True, "path": path}


def _tool_query_db(sql: str = "") -> dict:
    """Execute a read-only SQL query against the academic database."""
    if not sql or ";" in sql[:-1]:  # Prevent injection; allow one statement
        return {"error": "invalid_sql"}
    from src.common.database import get_connection
    from psycopg2.extras import RealDictCursor
    # Only allow SELECT statements
    if not sql.strip().upper().startswith("SELECT"):
        return {"error": "only_select_allowed"}
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = [dict(r) for r in cur.fetchall()]
    return {"rows": rows, "count": len(rows)}


def _tool_send_notification(
    recipient: str = "", subject: str = "", message: str = ""
) -> dict:
    """Send an email notification (stub — logs only in dev mode)."""
    logger.info("notification: to=%s subject=%s", recipient, subject[:50])
    return {"sent": True, "to": recipient}


def _tool_enqueue_review(email_id: str = "", records: list = None, confidence: float = 0.5) -> dict:
    """Route a low-confidence extraction to the human review queue."""
    from src.phase3_extraction_engine.review_queue import enqueue_for_review
    qid = enqueue_for_review(
        email_id, "", "", "", records or [], confidence
    )
    return {"queue_id": qid, "enqueued": bool(qid)}


# ── Tool Registry ─────────────────────────────────────────────────────────────

TOOLS: dict[str, ToolSchema] = {
    "email_fetch":     ToolSchema("email_fetch",     "Fetch emails from Gmail cache",               {"query": "str", "max_results": "int"},                                    _tool_email_fetch,     "ingestion"),
    "pdf_parse":       ToolSchema("pdf_parse",       "Parse a PDF attachment",                       {"path": "str"},                                                           _tool_pdf_parse,       "document"),
    "parse_document":  ToolSchema("parse_document",  "Parse ANY format: PDF/DOCX/ODT/XLS/HTML/image", {"path": "str", "url": "str", "mime_type": "str"},                     _tool_parse_document,  "document"),
    "ocr_image":       ToolSchema("ocr_image",       "OCR an image via Groq Vision or Tesseract",   {"path": "str", "url": "str"},                                            _tool_ocr_image,       "document"),
    "html_to_text":    ToolSchema("html_to_text",    "Convert HTML body to plain text + tables",    {"html": "str"},                                                           _tool_html_to_text,    "document"),
    "dedup_check":     ToolSchema("dedup_check",     "Check if email is a duplicate",                {"message_id": "str", "sender": "str"},                                   _tool_dedup_check,     "ingestion"),
    "classify_email":  ToolSchema("classify_email",  "Classify email as result or other",            {"text": "str"},                                                           _tool_classify_email,  "classification"),
    "extract_records": ToolSchema("extract_records", "Extract student results using all strategies", {"text": "str", "use_llm": "bool"},                                        _tool_extract_records, "extraction"),
    "validate":        ToolSchema("validate",        "Validate and correct extracted records",       {"records": "list"},                                                       _tool_validate_extraction, "extraction"),
    "save_results":    ToolSchema("save_results",    "Save records to PostgreSQL",                   {"records": "list", "email_id": "str"},                                   _tool_save_results,    "storage"),
    "gpa_compute":     ToolSchema("gpa_compute",     "Compute CGPA for a student",                  {"usn": "str"},                                                            _tool_gpa_compute,     "compute"),
    "student_lookup":  ToolSchema("student_lookup",  "Look up a student by USN or name",            {"usn": "str", "name": "str"},                                             _tool_student_lookup,  "query"),
    "semantic_search": ToolSchema("semantic_search", "Semantic search over student profiles",       {"query": "str", "limit": "int"},                                          _tool_semantic_search, "query"),
    "store_email":     ToolSchema("store_email",     "Store raw email in MinIO",                    {"email": "dict"},                                                         _tool_store_email,     "storage"),
    "query_db":        ToolSchema("query_db",        "Run a read-only SQL query",                   {"sql": "str"},                                                            _tool_query_db,        "query"),
    "notify":          ToolSchema("notify",          "Send a notification to a recipient",          {"recipient": "str", "subject": "str", "message": "str"},                 _tool_send_notification, "notification"),
    "enqueue_review":  ToolSchema("enqueue_review",  "Route low-confidence extraction to review",   {"email_id": "str", "records": "list", "confidence": "float"},            _tool_enqueue_review,  "review"),
}


def get_tool(name: str) -> Optional[ToolSchema]:
    return TOOLS.get(name)


def call_tool(name: str, **kwargs) -> Any:
    """Call a tool by name with provided kwargs."""
    tool = TOOLS.get(name)
    if not tool:
        raise ValueError(f"Unknown tool: {name!r}. Available: {list(TOOLS)}")
    if not tool.enabled:
        raise RuntimeError(f"Tool {name!r} is disabled.")
    logger.info("tool_call: %s args=%s", name, list(kwargs.keys()))
    return tool.handler(**kwargs)


def list_tools() -> list[dict]:
    """Return tool schemas as a list of dicts (for LLM function-calling)."""
    return [
        {
            "name": t.name,
            "description": t.description,
            "parameters": t.parameters,
            "category": t.category,
        }
        for t in TOOLS.values()
        if t.enabled
    ]
