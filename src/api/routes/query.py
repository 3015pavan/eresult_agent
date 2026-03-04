"""
Query Endpoints — Phase 5 Query Engine.

Natural language query interface for teachers.
Backend: PostgreSQL (psycopg2)
"""

from __future__ import annotations

import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor

from src.common.observability import get_logger
from src.common import database as db

logger = get_logger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    query: str = Field(..., min_length=3, max_length=500)
    context: dict[str, Any] | None = None


class QueryResponse(BaseModel):
    query: str
    intent: str
    text_answer: str
    summary: str | None = None
    data: list[dict[str, Any]] = []
    chart_spec: dict[str, Any] | None = None
    confidence: float
    caveats: list[str] = []


class StudentSummary(BaseModel):
    usn: str
    name: str
    email: str | None
    department: str
    batch_year: int | None
    current_cgpa: float
    active_backlogs: int
    total_results: int
    class_rank: int | None
    total_students: int
    semesters: list[dict[str, Any]]
    results_by_semester: dict[str, list[dict[str, Any]]]


class TrendResponse(BaseModel):
    usn: str
    name: str
    trend: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Helpers — normalise row values for JSON serialisation
# ---------------------------------------------------------------------------

def _serialise(row: dict) -> dict:
    return {
        k: str(v) if not isinstance(v, (str, int, float, bool, type(None), list, dict)) else v
        for k, v in row.items()
    }


def _student_display_name(s: dict) -> str:
    return s.get("full_name") or s.get("name") or s.get("usn") or ""


# ---------------------------------------------------------------------------
# Phase 5 — intent parsing
# ---------------------------------------------------------------------------

def _parse_intent_local(query: str) -> dict[str, Any]:
    q = query.lower()
    usn_m = re.search(r'\b[1-4][a-z]{2}\d{2}[a-z]{2,3}\d{3}\b', q, re.IGNORECASE)
    usn   = usn_m.group(0).upper() if usn_m else None
    sem_m = re.search(r'\b(\d)\s*(st|nd|rd|th)?\s*sem', q)
    semester = int(sem_m.group(1)) if sem_m else None

    if usn and any(w in q for w in ["result", "marks", "grade", "score", "show", "get"]):
        intent = "student_lookup"
    elif any(w in q for w in ["backlog", "fail", "failed", "arrear"]):
        intent = "backlogs" if usn else "backlogs_list"
    elif any(w in q for w in ["top", "best", "rank", "topper"]):
        intent = "top_n"
    elif any(w in q for w in ["average", "avg", "mean", "cgpa", "overall"]):
        intent = "aggregation"
    elif any(w in q for w in ["trend", "progress", "across semester"]):
        intent = "trend"
    elif usn:
        intent = "student_lookup"
    else:
        intent = "aggregation"

    return {"intent": intent, "usn": usn, "semester": semester, "confidence": 0.75}


# ---------------------------------------------------------------------------
# Query execution (PostgreSQL)
# ---------------------------------------------------------------------------

def _get_institution_id() -> str:
    return db.get_default_institution_id()


def _execute_query(
    intent: str,
    usn: str | None,
    semester: int | None,
    raw_query: str,
) -> tuple[list[dict], str, str]:
    db.init_db()
    inst_id = _get_institution_id()

    # ── student_lookup ───────────────────────────────────────────────────────
    if intent == "student_lookup" and usn:
        student = db.get_student(usn)
        if not student:
            matches = db.search_students(usn)
            if not matches:
                return [], "", f"No student found with USN {usn}."
            student = matches[0]
            usn = student["usn"]

        results = db.get_student_results(usn, semester)
        name = _student_display_name(student)

        if not results:
            return [_serialise(student)], "", (
                f"Found student {name} (USN: {usn}) but no result records yet. "
                "Run the pipeline to extract results."
            )

        passes = sum(1 for r in results if str(r.get("pass_status", "")).lower() == "pass")
        fails  = sum(1 for r in results if str(r.get("pass_status", "")).lower() == "fail")
        text   = f"Results for {name} (USN: {usn})"
        text  += f" - Sem {semester}: " if semester else ": "
        text  += f"{len(results)} subject(s), {passes} passed, {fails} failed."
        if student.get("cgpa"):
            text += f" CGPA: {float(student['cgpa']):.2f}."

        return (
            [_serialise(r) for r in results],
            f"SELECT * FROM student_results WHERE student_id = '{student['id']}'",
            text,
        )

    # ── backlogs_list ────────────────────────────────────────────────────────
    if intent == "backlogs_list":
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT usn, name AS full_name, total_backlogs, cgpa
                    FROM students
                    WHERE institution_id = %s AND total_backlogs > 0
                    ORDER BY total_backlogs DESC LIMIT 50
                """, (inst_id,))
                data = [_serialise(dict(r)) for r in cur.fetchall()]
        if not data:
            return [], "", "No students with backlogs found."
        top = data[0]
        text = (
            f"{len(data)} student(s) have backlogs. "
            f"Most: {top.get('full_name', top.get('usn'))} ({top['total_backlogs']})."
        )
        return data, "SELECT usn, full_name, total_backlogs FROM students WHERE total_backlogs > 0", text

    # ── backlogs (single student) ────────────────────────────────────────────
    if intent == "backlogs" and usn:
        student = db.get_student(usn)
        if not student:
            return [], "", f"Student {usn} not found."
        results = db.get_student_results(usn)
        failed  = [_serialise(r) for r in results if str(r.get("pass_status", "")).lower() == "fail"]
        name    = _student_display_name(student)
        return (
            failed,
            f"SELECT * FROM student_results WHERE student_id = '{student['id']}' AND pass_status='fail'",
            f"{name} has {len(failed)} backlog(s).",
        )

    # ── top_n ────────────────────────────────────────────────────────────────
    if intent == "top_n":
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT usn, name AS full_name, cgpa, total_backlogs
                    FROM students
                    WHERE institution_id = %s AND cgpa > 0
                    ORDER BY cgpa DESC LIMIT 10
                """, (inst_id,))
                data = [_serialise(dict(r)) for r in cur.fetchall()]
        if not data:
            return [], "", "No CGPA data yet. Run the pipeline first."
        top  = data[0]
        name = top.get("full_name", top.get("usn", ""))
        return (
            data,
            "SELECT usn, full_name, cgpa FROM students ORDER BY cgpa DESC LIMIT 10",
            f"Top {len(data)} students by CGPA: {name} leads with {float(top['cgpa']):.2f}.",
        )

    # ── aggregation ──────────────────────────────────────────────────────────
    if intent in ("aggregation", "comparison"):
        stats = db.get_pipeline_stats()
        text  = (
            f"DB: {stats['total_students']} students, {stats['total_results']} records. "
            f"Avg CGPA: {stats['average_cgpa']:.2f}. Backlogs: {stats['total_backlogs']}."
        )
        return [stats], "SELECT COUNT(*), AVG(cgpa) FROM students", text

    # ── trend ────────────────────────────────────────────────────────────────
    if intent == "trend" and usn:
        student = db.get_student(usn)
        if not student:
            return [], "", f"Student {usn} not found."
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT semester, sgpa, credits_earned, backlogs
                    FROM semester_aggregates
                    WHERE student_id = %s
                    ORDER BY semester
                """, (student["id"],))
                trend = [_serialise(dict(r)) for r in cur.fetchall()]
        name = _student_display_name(student)
        if not trend:
            return [_serialise(student)], "", f"No semester data for {name} yet."
        text = "Trend: " + ", ".join(
            f"S{r['semester']}:{float(r['sgpa']):.2f}" for r in trend if r.get("sgpa")
        )
        return (
            trend,
            f"SELECT semester, sgpa FROM semester_aggregates WHERE student_id = '{student['id']}'",
            text,
        )

    # ── fallback: name search ────────────────────────────────────────────────
    for word in [w for w in raw_query.split() if len(w) > 3]:
        matches = db.search_students(word, limit=5)
        if matches:
            return (
                [_serialise(m) for m in matches],
                f"SELECT * FROM students WHERE full_name ILIKE '%{word}%'",
                f"Found {len(matches)} student(s) matching '{word}'.",
            )

    stats = db.get_pipeline_stats()
    return (
        [stats],
        "",
        f"DB: {stats['total_students']} students, avg CGPA {stats['average_cgpa']}.",
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/query", response_model=QueryResponse)
async def submit_query(request: QueryRequest) -> QueryResponse:
    """Natural language academic query (Phase 5)."""
    start    = time.perf_counter()
    db.init_db()

    parsed   = _parse_intent_local(request.query)
    intent   = parsed["intent"]
    usn      = parsed.get("usn")
    semester = parsed.get("semester")

    rows, sql, text_answer = _execute_query(intent, usn, semester, request.query)
    elapsed  = int((time.perf_counter() - start) * 1000)

    chart_spec = None
    if intent == "trend" and rows and isinstance(rows[0], dict) and rows[0].get("sgpa"):
        chart_spec = {
            "type": "line",
            "x": [r.get("semester") for r in rows],
            "y": [r.get("sgpa") for r in rows],
            "xlabel": "Semester", "ylabel": "SGPA",
        }
    elif intent == "top_n" and rows:
        chart_spec = {
            "type": "bar",
            "labels": [r.get("full_name", r.get("usn", "")) for r in rows],
            "values": [float(r.get("cgpa", 0) or 0) for r in rows],
        }

    # Write to query_audit_log (best-effort — never fails the response)
    db.store_audit_log(
        raw_query=request.query,
        parsed_intent=intent,
        sql_generated=sql or None,
        response_summary=(text_answer or "")[:500],
        result_count=len(rows),
        duration_ms=elapsed,
    )

    return QueryResponse(
        query=request.query,
        intent=intent,
        text_answer=text_answer or "No results found.",
        summary=f"{len(rows)} record(s) in {elapsed}ms.",
        data=rows,
        chart_spec=chart_spec,
        confidence=parsed.get("confidence", 0.75),
        caveats=(["No data found. Run the pipeline on synced emails."] if not rows else []),
    )


@router.get("/student/{usn}", response_model=StudentSummary)
async def get_student(usn: str) -> StudentSummary:
    """Full student profile: aggregates, per-subject results, class rank."""
    db.init_db()
    student = db.get_student(usn.upper())
    if not student:
        raise HTTPException(status_code=404, detail=f"Student {usn} not found")

    sid = student["id"]
    inst_id = _get_institution_id()

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Semester aggregates
            cur.execute("""
                SELECT semester, sgpa, credits_earned, credits_attempted,
                       subjects_passed, subjects_failed, backlogs
                FROM semester_aggregates WHERE student_id = %s ORDER BY semester
            """, (sid,))
            sems = [_serialise(dict(r)) for r in cur.fetchall()]

            # All subject results
            cur.execute("""
                SELECT sr.semester, sr.total_marks AS marks_obtained, sr.max_marks,
                       sr.grade, sr.grade_points, sr.status AS pass_status,
                       sub.code AS subject_code, sub.name AS subject_name
                FROM student_results sr
                JOIN subjects sub ON sr.subject_id = sub.id
                WHERE sr.student_id = %s
                ORDER BY sr.semester, sub.code
            """, (sid,))
            all_results = [_serialise(dict(r)) for r in cur.fetchall()]

            # Class rank (non-seed only)
            cur.execute("""
                SELECT COUNT(*) AS rnk FROM students
                WHERE institution_id = %s
                  AND (metadata->>'source' IS DISTINCT FROM 'seed')
                  AND cgpa > %s
            """, (inst_id, float(student.get("cgpa") or 0)))
            rnk_row = cur.fetchone()
            class_rank = int(rnk_row["rnk"]) + 1 if rnk_row else None

            # Total non-seed students
            cur.execute("""
                SELECT COUNT(*) AS cnt FROM students
                WHERE institution_id = %s
                  AND (metadata->>'source' IS DISTINCT FROM 'seed')
            """, (inst_id,))
            total_students = int(cur.fetchone()["cnt"])

    # Group results by semester for easy frontend rendering
    results_by_sem: dict[str, list] = {}
    for r in all_results:
        key = str(r.get("semester", "?"))
        results_by_sem.setdefault(key, []).append(r)

    # Derive batch_year and department from USN  (e.g. 1MS21CS001 → batch=2021, dept=CS)
    u = student["usn"].upper()
    batch_year = None
    department = ""
    try:
        year_2d = int(u[3:5])
        batch_year = 2000 + year_2d
        department = u[5:7]  # e.g. CS, IS, EC
    except Exception:
        pass

    return StudentSummary(
        usn=student["usn"],
        name=_student_display_name(student),
        email=student.get("email"),
        department=department,
        batch_year=batch_year,
        current_cgpa=float(student.get("cgpa") or 0),
        active_backlogs=int(student.get("active_backlogs") or 0),
        total_results=len(all_results),
        class_rank=class_rank,
        total_students=total_students,
        semesters=sems,
        results_by_semester=results_by_sem,
    )


@router.get("/student/{usn}/trend", response_model=TrendResponse)
async def get_student_trend(usn: str) -> TrendResponse:
    """SGPA trend across semesters."""
    db.init_db()
    student = db.get_student(usn.upper())
    if not student:
        raise HTTPException(status_code=404, detail=f"Student {usn} not found")

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT semester, sgpa, credits_earned, backlogs
                FROM semester_aggregates
                WHERE student_id = %s
                ORDER BY semester
            """, (student["id"],))
            trend = [_serialise(dict(r)) for r in cur.fetchall()]

    return TrendResponse(
        usn=student["usn"],
        name=_student_display_name(student),
        trend=trend,
    )


@router.get("/student/{usn}/report")
async def export_student_report(usn: str, format: str = "pdf"):
    """Generate a downloadable PDF or DOCX academic report for a student."""
    from fastapi.responses import StreamingResponse
    import io

    db.init_db()
    student = db.get_student(usn.upper())
    if not student:
        raise HTTPException(status_code=404, detail=f"Student {usn} not found")

    sid = student["id"]
    all_results = db.get_student_results(usn.upper())

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT semester, sgpa, credits_earned, subjects_passed, subjects_failed, backlogs
                FROM semester_aggregates WHERE student_id = %s ORDER BY semester
            """, (sid,))
            sems = [dict(r) for r in cur.fetchall()]

    # Derive department + batch from USN
    u = student["usn"].upper()
    try:
        batch_year = 2000 + int(u[3:5])
        department = u[5:7]
    except Exception:
        batch_year, department = None, ""

    name      = _student_display_name(student)
    cgpa      = float(student.get("cgpa") or 0)
    backlogs  = int(student.get("active_backlogs") or 0)
    email_str = student.get("email") or "—"

    if format.lower() == "docx":
        buf = _build_docx_report(u, name, email_str, department, batch_year, cgpa, backlogs, sems, all_results)
        fname = f"{u}_report.docx"
        media  = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    else:
        buf = _build_pdf_report(u, name, email_str, department, batch_year, cgpa, backlogs, sems, all_results)
        fname = f"{u}_report.pdf"
        media  = "application/pdf"

    return StreamingResponse(
        buf,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


def _build_pdf_report(usn, name, email_str, dept, batch_year, cgpa, backlogs, sems, results):
    """Build a PDF academic report using ReportLab."""
    import io
    from datetime import date
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             leftMargin=2*cm, rightMargin=2*cm,
                             topMargin=2*cm,  bottomMargin=2*cm)
    styles = getSampleStyleSheet()
    accent = colors.HexColor("#4f8cff")
    dark   = colors.HexColor("#1a1d27")
    mid    = colors.HexColor("#2a2d37")
    light  = colors.HexColor("#e4e6eb")
    muted  = colors.HexColor("#8b8fa3")

    title_style  = ParagraphStyle("Title",  parent=styles["Heading1"], fontSize=20, textColor=dark,   spaceAfter=4)
    sub_style    = ParagraphStyle("Sub",    parent=styles["Normal"],   fontSize=10, textColor=muted,  spaceAfter=12)
    section_style= ParagraphStyle("Sec",    parent=styles["Heading2"], fontSize=13, textColor=accent, spaceBefore=14, spaceAfter=6)
    body_style   = ParagraphStyle("Body",   parent=styles["Normal"],   fontSize=10, textColor=dark,   leading=16)

    W = A4[0] - 4*cm  # usable width

    story = []

    # ── Header ──────────────────────────────────────────────────────
    story.append(Paragraph("AcadExtract", title_style))
    story.append(Paragraph(f"Academic Report  ·  Generated {date.today().strftime('%d %B %Y')}", sub_style))
    story.append(HRFlowable(width="100%", thickness=1, color=mid, spaceAfter=14))

    # ── Student Profile ─────────────────────────────────────────────
    story.append(Paragraph("Student Profile", section_style))
    profile_data = [
        ["USN",           usn],
        ["Name",          name],
        ["Email",         email_str],
        ["Department",    dept if dept else "—"],
        ["Batch Year",    str(batch_year) if batch_year else "—"],
        ["CGPA",          f"{cgpa:.2f}"],
        ["Active Backlogs", str(backlogs)],
    ]
    pt = Table(profile_data, colWidths=[4*cm, W-4*cm])
    pt.setStyle(TableStyle([
        ("FONTSIZE",    (0,0), (-1,-1), 10),
        ("FONTNAME",    (0,0), (0,-1), "Helvetica-Bold"),
        ("TEXTCOLOR",   (0,0), (0,-1), muted),
        ("TEXTCOLOR",   (1,0), (1,-1), dark),
        ("TOPPADDING",  (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0),(-1,-1), 4),
        ("ROWBACKGROUNDS",(0,0),(-1,-1),[colors.white, colors.HexColor("#f5f6fa")]),
        ("LINEBELOW",   (0,-1),(-1,-1), 0.5, mid),
    ]))
    story.append(pt)
    story.append(Spacer(1, 14))

    # ── Semester Summary ────────────────────────────────────────────
    if sems:
        story.append(Paragraph("Semester Summary", section_style))
        sem_hdr = ["Sem", "SGPA", "Credits", "Passed", "Failed", "Backlogs"]
        sem_rows = [[
            str(s.get("semester", "")),
            f"{float(s['sgpa']):.2f}" if s.get("sgpa") else "—",
            str(s.get("credits_earned", "—")),
            str(s.get("subjects_passed", "—")),
            str(s.get("subjects_failed", "—")),
            str(s.get("backlogs", 0)),
        ] for s in sems]
        st = Table([sem_hdr] + sem_rows, repeatRows=1,
                   colWidths=[1.2*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.5*cm])
        st.setStyle(TableStyle([
            ("BACKGROUND",  (0,0), (-1,0), accent),
            ("TEXTCOLOR",   (0,0), (-1,0), colors.white),
            ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
            ("FONTSIZE",    (0,0), (-1,-1), 9),
            ("ALIGN",       (0,0), (-1,-1), "CENTER"),
            ("TOPPADDING",  (0,0), (-1,-1), 4),
            ("BOTTOMPADDING",(0,0),(-1,-1), 4),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, colors.HexColor("#f5f6fa")]),
            ("GRID",        (0,0), (-1,-1), 0.4, mid),
        ]))
        story.append(st)
        story.append(Spacer(1, 14))

    # ── Detailed Results per Semester ───────────────────────────────
    by_sem: dict[int, list] = {}
    for r in results:
        by_sem.setdefault(int(r.get("semester", 0)), []).append(r)

    if by_sem:
        story.append(Paragraph("Detailed Results", section_style))
        for sem_num in sorted(by_sem):
            story.append(Paragraph(f"Semester {sem_num}", body_style))
            hdr = ["Code", "Subject", "Marks", "Max", "Grade", "Status"]
            rows = [[
                r.get("subject_code", ""),
                r.get("subject_name", "")[:35],
                str(int(r.get("marks_obtained", 0) or 0)),
                str(int(r.get("max_marks", 100) or 100)),
                r.get("grade", "—") or "—",
                r.get("pass_status", "—") or "—",
            ] for r in by_sem[sem_num]]
            rt = Table([hdr]+rows, repeatRows=1,
                       colWidths=[2*cm, (W-11*cm), 1.8*cm, 1.8*cm, 1.8*cm, 2*cm])
            rt.setStyle(TableStyle([
                ("BACKGROUND",  (0,0), (-1,0), mid),
                ("TEXTCOLOR",   (0,0), (-1,0), light),
                ("FONTNAME",    (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE",    (0,0), (-1,-1), 8.5),
                ("ALIGN",       (2,0), (-1,-1), "CENTER"),
                ("TOPPADDING",  (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1),3),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, colors.HexColor("#f5f6fa")]),
                ("GRID",        (0,0), (-1,-1), 0.3, mid),
                # red FAIL rows
                *[("TEXTCOLOR", (5, i+1), (5, i+1),
                   colors.HexColor("#e55353") if row[5] == "FAIL" else colors.HexColor("#2ecc71"))
                  for i, row in enumerate(rows)],
            ]))
            story.append(rt)
            story.append(Spacer(1, 10))

    doc.build(story)
    buf.seek(0)
    return buf


def _build_docx_report(usn, name, email_str, dept, batch_year, cgpa, backlogs, sems, results):
    """Build a DOCX academic report using python-docx."""
    import io
    from datetime import date
    from docx import Document
    from docx.shared import Pt, RGBColor, Inches, Cm
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    ACCENT = RGBColor(0x4f, 0x8c, 0xff)
    DARK   = RGBColor(0x1a, 0x1d, 0x27)
    MUTED  = RGBColor(0x8b, 0x8f, 0xa3)
    FAIL_C = RGBColor(0xe5, 0x53, 0x53)
    PASS_C = RGBColor(0x2e, 0xcc, 0x71)

    doc = Document()
    # Set narrow margins
    for sec in doc.sections:
        sec.top_margin    = Cm(2)
        sec.bottom_margin = Cm(2)
        sec.left_margin   = Cm(2.5)
        sec.right_margin  = Cm(2.5)

    def add_heading(text, level=1, color=DARK):
        p = doc.add_heading(text, level=level)
        for run in p.runs:
            run.font.color.rgb = color
        return p

    def add_field(label, value):
        p = doc.add_paragraph()
        r = p.add_run(f"{label}: ")
        r.bold = True; r.font.color.rgb = MUTED
        r2 = p.add_run(str(value))
        r2.font.color.rgb = DARK
        p.paragraph_format.space_after = Pt(2)
        return p

    # Header
    h = doc.add_heading("AcadExtract", 0)
    for run in h.runs:
        run.font.color.rgb = DARK
    sub = doc.add_paragraph(f"Academic Report  ·  Generated {date.today().strftime('%d %B %Y')}")
    sub.runs[0].font.color.rgb = MUTED
    sub.runs[0].font.size = Pt(9)
    doc.add_paragraph("─" * 80).runs[0].font.color.rgb = MUTED

    # Student Profile
    add_heading("Student Profile", 1, ACCENT)
    add_field("USN",            usn)
    add_field("Name",           name)
    add_field("Email",          email_str)
    add_field("Department",     dept if dept else "—")
    add_field("Batch Year",     str(batch_year) if batch_year else "—")
    add_field("CGPA",           f"{cgpa:.2f}")
    add_field("Active Backlogs", str(backlogs))

    # Semester Summary
    if sems:
        add_heading("Semester Summary", 1, ACCENT)
        tbl = doc.add_table(rows=1, cols=6)
        tbl.style = "Light Shading Accent 1"
        for i, hdr_txt in enumerate(["Sem", "SGPA", "Credits", "Passed", "Failed", "Backlogs"]):
            cell = tbl.rows[0].cells[i]
            cell.text = hdr_txt
            cell.paragraphs[0].runs[0].bold = True
        for s in sems:
            row = tbl.add_row()
            row.cells[0].text = str(s.get("semester",""))
            row.cells[1].text = f"{float(s['sgpa']):.2f}" if s.get("sgpa") else "—"
            row.cells[2].text = str(s.get("credits_earned","—"))
            row.cells[3].text = str(s.get("subjects_passed","—"))
            row.cells[4].text = str(s.get("subjects_failed","—"))
            row.cells[5].text = str(s.get("backlogs", 0))

    # Detailed Results
    by_sem: dict[int, list] = {}
    for r in results:
        by_sem.setdefault(int(r.get("semester", 0)), []).append(r)

    if by_sem:
        add_heading("Detailed Results", 1, ACCENT)
        for sem_num in sorted(by_sem):
            add_heading(f"Semester {sem_num}", 2, DARK)
            tbl = doc.add_table(rows=1, cols=6)
            tbl.style = "Light Grid Accent 1"
            for i, hdr_txt in enumerate(["Code","Subject","Marks","Max","Grade","Status"]):
                cell = tbl.rows[0].cells[i]
                cell.text = hdr_txt
                cell.paragraphs[0].runs[0].bold = True
            for r in by_sem[sem_num]:
                row = tbl.add_row()
                row.cells[0].text = r.get("subject_code","")
                row.cells[1].text = r.get("subject_name","")
                row.cells[2].text = str(int(r.get("marks_obtained",0) or 0))
                row.cells[3].text = str(int(r.get("max_marks",100) or 100))
                row.cells[4].text = r.get("grade","—") or "—"
                status = r.get("pass_status","—") or "—"
                row.cells[5].text = status
                if status == "FAIL":
                    for cell in row.cells:
                        for p in cell.paragraphs:
                            for run in p.runs:
                                run.font.color.rgb = FAIL_C

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf


@router.get("/students")
async def list_students(q: str = "", limit: int = 50) -> dict:
    """List or search students."""
    db.init_db()
    students = db.search_students(q, limit=limit) if q else db.get_all_students(limit=limit)
    inst_id  = _get_institution_id()

    result = []
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for s in students:
                cur.execute("""
                    SELECT COUNT(*) AS n FROM student_results sr
                    JOIN students st ON sr.student_id = st.id
                    WHERE st.usn = %s AND st.institution_id = %s
                """, (s["usn"], inst_id))
                cnt = cur.fetchone()["n"]
                row = _serialise(s)
                row["result_count"] = cnt
                result.append(row)

    return {"total": len(result), "students": result}


# ---------------------------------------------------------------------------
# AI Assistant — /chat  (Gemini-powered)
# ---------------------------------------------------------------------------

AI_SYSTEM_PROMPT = """You are AcadAssist, an AI assistant embedded in AcadExtract — an academic result extraction platform for teachers.

Your role: Help teachers understand student performance, answer questions about results, identify students needing attention, and summarize trends.

You have real-time access to the PostgreSQL database. All relevant data is provided in the DATABASE CONTEXT section below.

Rules:
- ALWAYS answer from the database context provided — do NOT say data is missing if it appears in context
- If a student is found in the context, show all their available details (USN, name, CGPA, subject marks, grades)
- For semester-specific questions, filter and show only that semester's subjects
- Format results as a clear table or bullet list with: Subject Code | Subject Name | Marks | Grade | Status
- When asked for a "grade card" or "marks sheet", show all subjects for that semester in a table format
- Be precise with numbers, student names, and USNs
- If a student truly does not appear anywhere in the context, say "Student not found in the database"
- Never tell the user to sync or run the pipeline — the data is already loaded
- Use bullet points for lists, markdown bold for emphasis
- Understand variations: 'backlog' = 'fail', 'topper' = highest CGPA, 'marks sheet' = 'grade card'"""


class ChatMessage(BaseModel):
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=2000)
    history: list[ChatMessage] = []


class ChatResponse(BaseModel):
    reply: str
    context_used: bool = False


def _local_chat_answer(message: str) -> str:
    """Generate a direct answer from the DB using the query engine (no LLM needed)."""
    try:
        db.init_db()
        parsed      = _parse_intent_local(message)
        intent      = parsed["intent"]
        usn         = parsed.get("usn")
        semester    = parsed.get("semester")
        rows, _, text_answer = _execute_query(intent, usn, semester, message)

        reply_lines = [text_answer]

        if rows:
            # Show first 10 rows as a simple summary
            for row in rows[:10]:
                parts = []
                for k in ("usn", "full_name", "subject_name", "subject_code",
                          "marks_obtained", "total_marks", "grade", "status",
                          "semester", "sgpa", "cgpa", "total_backlogs"):
                    v = row.get(k)
                    if v is not None and str(v) not in ("", "None"):
                        parts.append(f"{k}: {v}")
                if parts:
                    reply_lines.append("  • " + "  |  ".join(parts))

        if len(rows) > 10:
            reply_lines.append(f"  … and {len(rows) - 10} more record(s).")

        reply_lines.append(
            "\n_(No AI provider configured — showing direct database results. "
            "Add a GROQ_API_KEY, GEMINI_API_KEY, or OPENAI_API_KEY in your .env for natural-language responses.)_"
        )
        return "\n".join(reply_lines)
    except Exception as exc:
        logger.warning("local_chat_fallback_failed", error=str(exc))
        return (
            "No AI provider configured and the database query also failed. "
            "Please add a GROQ_API_KEY, GEMINI_API_KEY, or OPENAI_API_KEY to your .env file."
        )


@router.post("/chat", response_model=ChatResponse)
async def ai_chat(req: ChatRequest) -> ChatResponse:
    """AI assistant endpoint — answers teacher questions using Gemini + live DB context."""
    from src.common.config import get_settings
    settings = get_settings()

    no_llm = (
        not settings.llm.groq_api_key
        and not settings.llm.secondary_api_key
        and not settings.llm.primary_api_key
    )
    if no_llm:
        # No LLM configured — fall back to local DB query engine
        reply = _local_chat_answer(req.message)
        return ChatResponse(reply=reply, context_used=True)

    # ── Gather live DB context ──────────────────────────────────────
    context_lines: list[str] = []
    try:
        db.init_db()
        stats = db.get_pipeline_stats()
        total_students = stats.get('total_students', 0)
        context_lines.append(f"DATABASE SUMMARY:")
        context_lines.append(f"- Total students: {total_students}")
        context_lines.append(f"- Total result records: {stats.get('total_results', 0)}")
        context_lines.append(f"- Emails processed: {stats.get('emails_processed', 0)}")
        if stats.get("average_cgpa"):
            context_lines.append(f"- Average CGPA: {stats['average_cgpa']:.2f}")
        if stats.get("total_backlogs") is not None:
            context_lines.append(f"- Total active backlogs: {stats['total_backlogs']}")

        inst_id = _get_institution_id()
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # Full student list (up to 100) — lets AI answer any student question
                if total_students > 0:
                    cur.execute("""
                        SELECT s.usn, s.name AS full_name, s.cgpa, s.active_backlogs
                        FROM students s
                        WHERE s.institution_id = %s
                          AND (s.metadata->>'source' IS DISTINCT FROM 'seed')
                        ORDER BY s.cgpa DESC NULLS LAST
                        LIMIT 100
                    """, (inst_id,))
                    all_students = cur.fetchall()
                    if all_students:
                        context_lines.append(f"\nALL STUDENTS ({len(all_students)}):")
                        for st in all_students:
                            cgpa_str = f", CGPA {float(st['cgpa']):.2f}" if st['cgpa'] else ""
                            back_str = f", backlogs {st['active_backlogs']}" if st['active_backlogs'] else ""
                            context_lines.append(f"  - {st['usn']} {st['full_name'] or ''}{cgpa_str}{back_str}")

                # Students with backlogs
                cur.execute("""
                    SELECT s.usn, s.name AS full_name, s.active_backlogs
                    FROM students s
                    WHERE s.institution_id = %s AND s.active_backlogs > 0
                      AND (s.metadata->>'source' IS DISTINCT FROM 'seed')
                    ORDER BY s.active_backlogs DESC
                """, (inst_id,))
                backlog_students = cur.fetchall()
                if backlog_students:
                    context_lines.append(f"\nSTUDENTS WITH BACKLOGS ({len(backlog_students)}):")
                    for bs in backlog_students:
                        context_lines.append(f"  - {bs['usn']} {bs['full_name'] or ''}: {bs['active_backlogs']} backlog(s)")

                # Subject pass rates (hardest subjects) — exclude seed student results
                cur.execute("""
                    SELECT sub.name AS subject_name, sub.code,
                           COUNT(*) AS total,
                           SUM(CASE WHEN sr.status='PASS' THEN 1 ELSE 0 END) AS passed
                    FROM student_results sr
                    JOIN subjects sub ON sr.subject_id = sub.id
                    JOIN students s ON sr.student_id = s.id
                    WHERE (s.metadata->>'source' IS DISTINCT FROM 'seed')
                    GROUP BY sub.id, sub.name, sub.code
                    ORDER BY (SUM(CASE WHEN sr.status='PASS' THEN 1 ELSE 0 END)::float / COUNT(*)) ASC
                    LIMIT 5
                """, ())
                hard_subjects = cur.fetchall()
                if hard_subjects:
                    context_lines.append("\nSUBJECTS WITH LOWEST PASS RATE:")
                    for hs in hard_subjects:
                        rate = round(100 * hs["passed"] / hs["total"]) if hs["total"] else 0
                        context_lines.append(f"  - {hs['code']} {hs['subject_name']}: {rate}% pass rate ({hs['passed']}/{hs['total']})")

                # Specific student detail if USN or name found in question OR in history
                usn_match = re.search(r'\b[1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3}\b', req.message.upper())
                # Also try name-based lookup if no USN matched
                name_match_usn = None
                if not usn_match:
                    # Extract 2+ consecutive capitalized words as potential name
                    name_candidates = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', req.message.title())
                    for candidate in name_candidates:
                        cur.execute("""
                            SELECT usn FROM students
                            WHERE LOWER(name) LIKE LOWER(%s)
                              AND (metadata->>'source' IS DISTINCT FROM 'seed')
                            LIMIT 1
                        """, (f'%{candidate}%',))
                        row = cur.fetchone()
                        if row:
                            name_match_usn = row[0] if isinstance(row, (list, tuple)) else row['usn']
                            break

                # Fall back: scan recent history for a USN (handles follow-up questions)
                if not usn_match and not name_match_usn:
                    for hist_msg in reversed(req.history[-6:]):
                        hist_usn = re.search(r'\b[1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3}\b', hist_msg.content.upper())
                        if hist_usn:
                            name_match_usn = hist_usn.group(0)
                            break
                    # Also scan history for names if still not found
                    if not name_match_usn:
                        for hist_msg in reversed(req.history[-6:]):
                            name_candidates = re.findall(r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+', hist_msg.content.title())
                            for candidate in name_candidates:
                                cur.execute("""
                                    SELECT usn FROM students
                                    WHERE LOWER(name) LIKE LOWER(%s)
                                      AND (metadata->>'source' IS DISTINCT FROM 'seed')
                                    LIMIT 1
                                """, (f'%{candidate}%',))
                                row = cur.fetchone()
                                if row:
                                    name_match_usn = row[0] if isinstance(row, (list, tuple)) else row['usn']
                                    break
                            if name_match_usn:
                                break

                target_usn = usn_match.group(0) if usn_match else name_match_usn

                # Extract semester number from message (limit to 1-2 digits, avoid matching USN digits)
                sem_match = re.search(r'\bsem(?:ester)?\s*(\d{1,2})\b|\b(\d{1,2})(?:st|nd|rd|th)?\s*sem\b', req.message.lower())
                filter_sem = int(sem_match.group(1) or sem_match.group(2)) if sem_match else None

                if target_usn:
                    usn = target_usn.upper()
                    cur.execute("""
                        SELECT s.usn, s.name AS full_name, s.cgpa, s.active_backlogs
                        FROM students s WHERE UPPER(s.usn) = %s
                    """, (usn,))
                    st = cur.fetchone()
                    if st:
                        context_lines.append(f"\nSTUDENT DETAIL for {usn}:")
                        context_lines.append(f"  Name: {st['full_name']}")
                        context_lines.append(f"  CGPA: {st['cgpa']}")
                        context_lines.append(f"  Active backlogs: {st['active_backlogs']}")
                        sem_sql = "AND sr.semester = %s" if filter_sem else ""
                        sem_params = (usn, filter_sem) if filter_sem else (usn,)
                        cur.execute(f"""
                            SELECT sub.code, sub.name AS subject_name, sr.total_marks,
                                   sr.max_marks, sr.grade, sr.status, sr.semester
                            FROM student_results sr
                            JOIN subjects sub ON sr.subject_id = sub.id
                            JOIN students s ON sr.student_id = s.id
                            WHERE UPPER(s.usn) = %s {sem_sql}
                            ORDER BY sr.semester, sub.code
                        """, sem_params)
                        results = cur.fetchall()
                        if results:
                            sem_label = f" (Semester {filter_sem})" if filter_sem else " (all semesters)"
                            context_lines.append(f"  Results{sem_label}:")
                            for r in results:
                                context_lines.append(
                                    f"    Sem{r['semester']} | {r['code']} | {r['subject_name']} | "
                                    f"{r['total_marks']}/{r['max_marks']} | {r['grade'] or '-'} | {r['status']}"
                                )
                        else:
                            context_lines.append(f"  No results found{' for semester ' + str(filter_sem) if filter_sem else ''}.")
    except Exception as e:
        logger.warning("chat_db_context_failed", error=str(e))

    db_context = "\n".join(context_lines) if context_lines else "No database context available."
    context_used = bool(context_lines)

    # ── Build conversation for Gemini ──────────────────────────────
    # Gemini uses a simple prompt approach (not multi-turn in this flow)
    convo_history = ""
    for msg in req.history[-6:]:  # last 3 turns (6 messages)
        role = "Teacher" if msg.role == "user" else "AcadAssist"
        convo_history += f"{role}: {msg.content}\n"

    full_prompt = (
        f"{AI_SYSTEM_PROMPT}\n\n"
        f"{db_context}\n\n"
        + (f"CONVERSATION HISTORY:\n{convo_history}\n" if convo_history else "")
        + f"Teacher: {req.message}\nAcadAssist:"
    )

    provider = settings.llm.active_provider
    try:
        if provider == "groq":
            from groq import Groq
            gclient = Groq(api_key=settings.llm.groq_api_key)
            completion = gclient.chat.completions.create(
                model=settings.llm.groq_model,
                messages=[{"role": "user", "content": full_prompt}],
                temperature=0.3,
                max_tokens=1024,
            )
            reply = completion.choices[0].message.content.strip()
        elif provider == "gemini":
            import google.generativeai as genai
            genai.configure(api_key=settings.llm.secondary_api_key)
            gmodel = genai.GenerativeModel(settings.llm.secondary_model)
            response = await gmodel.generate_content_async(
                full_prompt,
                generation_config={"temperature": 0.3, "max_output_tokens": 1024},
            )
            reply = response.text.strip()
        elif provider == "openai":
            from openai import AsyncOpenAI
            oclient = AsyncOpenAI(api_key=settings.llm.primary_api_key)
            resp = await oclient.chat.completions.create(
                model=settings.llm.primary_model,
                messages=[{"role": "user", "content": full_prompt}],
                temperature=0.3,
                max_tokens=1024,
            )
            reply = resp.choices[0].message.content.strip()
        else:
            # provider is "none" but we got past the no_llm check — shouldn't happen,
            # but gracefully fall back to the local DB query engine
            reply = _local_chat_answer(req.message)
            return ChatResponse(reply=reply, context_used=bool(context_lines))
    except HTTPException:
        raise
    except Exception as e:
        logger.error("ai_chat_failed", error=str(e))
        raise HTTPException(status_code=500, detail=f"AI assistant error: {str(e)}")

    return ChatResponse(reply=reply, context_used=context_used)
