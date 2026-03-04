"""
Planner — Phase 4.

Converts a high-level goal into a sequence of tool calls.
Uses LLM for dynamic planning or rule-based templates for known goals.
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger(__name__)

_GROQ_KEY = os.getenv("GROQ_API_KEY", "")

# ── Rule-based plan templates ─────────────────────────────────────────────────

_TEMPLATES: dict[str, list[dict]] = {
    "process_emails": [
        {"tool": "email_fetch",     "args": {"query": "result OR marks OR grade", "max_results": 100}},
        {"tool": "dedup_check",     "args": {"message_id": "{step_0.emails[0].id}"}},
        {"tool": "classify_email",  "args": {"text": "{step_0.emails[0].body}"}},
        {"tool": "extract_records", "args": {"text": "{step_0.emails[0].body}", "use_llm": True}},
        {"tool": "validate",        "args": {"records": "{step_3.records}"}},
        {"tool": "save_results",    "args": {"records": "{step_4.records}"}},
    ],
    "lookup_student": [
        {"tool": "student_lookup",  "args": {"usn": "{usn}"}},
        {"tool": "gpa_compute",     "args": {"usn": "{usn}"}},
    ],
    "find_backlogs": [
        {"tool": "query_db", "args": {"sql": "SELECT usn, name, total_backlogs FROM students WHERE total_backlogs > 0 ORDER BY total_backlogs DESC LIMIT 20"}},
    ],
    "semantic_search": [
        {"tool": "semantic_search", "args": {"query": "{query}", "limit": 10}},
    ],
    "parse_attachment": [
        {"tool": "parse_document",  "args": {"path": "{path}", "mime_type": ""}},
        {"tool": "extract_records", "args": {"text": "{step_0.text}", "use_llm": True}},
        {"tool": "validate",        "args": {"records": "{step_1.records}"}},
        {"tool": "save_results",    "args": {"records": "{step_2.records}"}},
    ],
    "ocr_image": [
        {"tool": "ocr_image",       "args": {"path": "{path}"}},
        {"tool": "extract_records", "args": {"text": "{step_0.text}", "use_llm": True}},
        {"tool": "validate",        "args": {"records": "{step_1.records}"}},
        {"tool": "save_results",    "args": {"records": "{step_2.records}"}},
    ],
    "parse_html_body": [
        {"tool": "html_to_text",    "args": {"html": "{html}"}},
        {"tool": "extract_records", "args": {"text": "{step_0.text}", "use_llm": True}},
        {"tool": "validate",        "args": {"records": "{step_1.records}"}},
        {"tool": "save_results",    "args": {"records": "{step_2.records}"}},
    ],
}


def _match_template(goal: str, context: dict) -> list[dict] | None:
    """Try to match the goal to a known template."""
    gl = goal.lower()

    if any(w in gl for w in ("process", "email", "extract result", "run pipeline")):
        plan = _TEMPLATES["process_emails"].copy()
        return plan

    usn_m = re.search(r"\b([1-4][a-z]{2}\d{2}[a-z]{2,3}\d{3})\b", gl, re.IGNORECASE)
    if usn_m and any(w in gl for w in ("lookup", "find", "show", "get", "result", "grade")):
        usn = usn_m.group(1).upper()
        return [{"tool": t["tool"], "args": {k: v.replace("{usn}", usn) for k, v in t["args"].items()}}
                for t in _TEMPLATES["lookup_student"]]

    if any(w in gl for w in ("backlog", "fail", "arrear")):
        return _TEMPLATES["find_backlogs"]

    if any(w in gl for w in ("search", "similar", "semantic")):
        query = context.get("query", goal)
        return [{"tool": "semantic_search", "args": {"query": query, "limit": 10}}]

    # ── Image / OCR ───────────────────────────────────────────────────────────
    if any(w in gl for w in ("ocr", "image", "photo", "scan", "jpg", "jpeg", "png")):
        path = context.get("path", "")
        url  = context.get("url", "")
        if path or url:
            tmpl = _TEMPLATES["ocr_image"]
            return [{"tool": t["tool"],
                     "args": {k: v.replace("{path}", path) for k, v in t["args"].items()}}
                    for t in tmpl]

    # ── HTML body ─────────────────────────────────────────────────────────────
    if "html" in gl:
        html = context.get("html", "")
        if html:
            tmpl = _TEMPLATES["parse_html_body"]
            return [{"tool": t["tool"],
                     "args": {k: v.replace("{html}", html) for k, v in t["args"].items()}}
                    for t in tmpl]

    # ── Generic document / attachment ─────────────────────────────────────────
    if any(w in gl for w in ("pdf", "docx", "doc", "spreadsheet", "excel", "odt",
                              "attachment", "document", "parse", "convert")):
        path = context.get("path", "")
        url  = context.get("url", "")
        if path or url:
            tmpl = _TEMPLATES["parse_attachment"]
            return [{"tool": t["tool"],
                     "args": {k: v.replace("{path}", path).replace("{url}", url)
                               for k, v in t["args"].items()}}
                    for t in tmpl]

    return None


def _llm_plan(goal: str, context: dict) -> list[dict]:
    """Generate a plan using Groq LLM when no template matches."""
    import json
    import httpx

    from .tools import list_tools
    tools_desc = json.dumps(list_tools(), indent=2)[:3000]

    system_prompt = f"""You are a planning agent for an academic result extraction system.
Available tools: {tools_desc}

Return ONLY a JSON array of steps, each with:
{{"tool": "tool_name", "args": {{"arg_key": "value_or_{{step_N.field}}"}}, "reason": "why"}}

Resolve dynamic values using {{step_N.field}} syntax to reference previous step outputs.
Keep plans short (≤8 steps). Return [] if goal cannot be accomplished with available tools."""

    try:
        resp = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {_GROQ_KEY}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": f"Goal: {goal}\nContext: {json.dumps(context)[:500]}"},
                ],
                "temperature": 0.0,
                "max_tokens": 1024,
            },
            timeout=20.0,
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"]
        # Extract JSON array
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except Exception as exc:
        logger.warning("llm_planner failed: %s", exc)

    return []


def create_plan(goal: str, context: dict | None = None) -> list[dict]:
    """
    Generate a tool-call plan for a goal.
    Returns list of {"tool": str, "args": dict} dicts.
    """
    ctx = context or {}

    # 1. Try rule-based template
    plan = _match_template(goal, ctx)
    if plan:
        logger.info("planner: template match for goal=%r, steps=%d", goal[:60], len(plan))
        return plan

    # 2. LLM planning
    if _GROQ_KEY:
        plan = _llm_plan(goal, ctx)
        if plan:
            logger.info("planner: llm plan for goal=%r, steps=%d", goal[:60], len(plan))
            return plan

    # 3. Fallback: generic email processing
    logger.warning("planner: no plan found for goal=%r, using default", goal[:60])
    return _TEMPLATES["process_emails"]
