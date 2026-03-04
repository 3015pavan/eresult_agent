"""
Admin Endpoints.

System statistics, pipeline management, and document upload for administrators.
"""

from __future__ import annotations

import re
import io
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field
from psycopg2.extras import RealDictCursor

from src.common.observability import get_logger
from src.common import database as db

logger = get_logger(__name__)
router = APIRouter()


# ── Request/Response Models ─────────────────────────────────────────


class IngestionTrigger(BaseModel):
    """Trigger email ingestion."""
    max_emails: int = Field(default=50, ge=1, le=500)
    since_hours: int = Field(default=24, ge=1, le=168)


class ReprocessRequest(BaseModel):
    """Reprocess a failed document."""
    attachment_id: str
    force: bool = False


# ── Endpoints ───────────────────────────────────────────────────────


@router.post("/ingest")
async def trigger_ingestion(request: IngestionTrigger) -> dict:
    """Redirect to the pipeline endpoint to process cached emails."""
    return {
        "detail": "Use POST /api/v1/pipeline/run to process emails.",
        "max_emails": request.max_emails,
        "since_hours": request.since_hours,
    }


@router.post("/reprocess")
async def reprocess_document(request: ReprocessRequest) -> dict:
    """Re-run pipeline with force=true to reprocess all emails."""
    return {
        "detail": "Use POST /api/v1/pipeline/run with force=true to re-process all emails.",
        "attachment_id": request.attachment_id,
    }


@router.get("/status")
async def pipeline_status() -> dict:
    """Get current pipeline status and database statistics."""
    try:
        db.init_db()
        stats = db.get_pipeline_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        logger.error("admin_status_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def system_statistics() -> dict:
    """Get system-wide academic statistics."""
    try:
        db.init_db()
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        (SELECT COUNT(*)  FROM students)               AS total_students,
                        (SELECT COUNT(*)  FROM student_results)        AS total_results,
                        (SELECT COUNT(*)  FROM email_metadata)         AS total_emails,
                        (SELECT COUNT(*)  FROM extractions)            AS total_extractions,
                        (SELECT ROUND(AVG(cgpa)::numeric, 2)
                           FROM students WHERE cgpa > 0)               AS avg_cgpa,
                        (SELECT COUNT(*)
                           FROM students WHERE active_backlogs > 0)    AS students_with_backlogs
                """)
                row = cur.fetchone()
        return {k: (float(v) if v is not None else 0) for k, v in dict(row).items()}
    except Exception as e:
        logger.error("admin_stats_failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


# ── Document Upload ──────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_document(file: UploadFile = File(...)) -> dict:
    """
    Upload an Excel, CSV, PDF, DOCX, or TXT result document.
    Parses student records and stores them directly to the database.
    Returns a summary of what was extracted.
    """
    filename = (file.filename or "").lower()
    content  = await file.read()

    db.init_db()
    inst_id = db.get_default_institution_id()

    try:
        if filename.endswith((".xlsx", ".xls")):
            records = _parse_excel(content, filename)
        elif filename.endswith(".csv"):
            records = _parse_csv(content)
        elif filename.endswith(".pdf"):
            records = _parse_pdf_text(content)
        elif filename.endswith(".docx"):
            records = _parse_docx_text(content)
        elif filename.endswith((".txt", ".text")):
            records = _parse_raw_text(content.decode("utf-8", errors="ignore"))
        else:
            raise HTTPException(
                status_code=415,
                detail="Unsupported file type. Upload Excel (.xlsx), CSV, PDF, DOCX, or TXT."
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("document_parse_failed", filename=filename, error=str(e))
        raise HTTPException(status_code=422, detail=f"Failed to parse file: {e}")

    if not records:
        return {
            "status": "no_data",
            "message": "No student records found in this file. Ensure it contains USN and result data.",
            "filename": file.filename,
        }

    # Store records
    students_upserted = 0
    results_stored    = 0
    errors: list[str] = []

    for rec in records:
        try:
            usn  = str(rec.get("usn", "")).strip().upper()
            name = str(rec.get("name", usn)).strip()
            if not usn:
                continue

            student_id = db.upsert_student(usn, name, institution_id=inst_id, source="upload")
            students_upserted += 1

            # Semester aggregate (SGPA-only records)
            if rec.get("sgpa") and not rec.get("subject_code"):
                sem = int(rec.get("semester", 1))
                sgpa = float(rec["sgpa"])
                cgpa = float(rec.get("cgpa", 0)) or None
                db.store_semester_aggregate(student_id=student_id, semester=sem, sgpa=sgpa, backlogs=0)
                if cgpa:
                    with db.get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute("UPDATE students SET cgpa=%s WHERE id=%s", (cgpa, student_id))

            # Full subject result
            elif rec.get("subject_code"):
                sem     = int(rec.get("semester", 1))
                code    = str(rec.get("subject_code", "UNKNOWN")).strip()
                subname = str(rec.get("subject_name", code)).strip()
                marks   = float(rec.get("marks", 0) or 0)
                max_m   = float(rec.get("max_marks", 100) or 100)
                grade   = str(rec.get("grade", "")).strip() or None
                status  = str(rec.get("status", "PASS" if marks >= 40 else "FAIL")).upper()
                gp      = float(rec.get("grade_points", 0) or 0) or None

                subject_id = db.get_or_create_subject(inst_id, code, subname, sem)
                db.upsert_result(
                    student_id=student_id,
                    subject_id=subject_id,
                    semester=sem,
                    marks_obtained=marks,
                    max_marks=max_m,
                    grade=grade,
                    grade_points=gp,
                    status=status,
                )
                results_stored += 1

            db.compute_and_store_cgpa(student_id)

        except Exception as e:
            errors.append(f"USN {rec.get('usn','?')}: {e}")

    return {
        "status": "ok",
        "filename": file.filename,
        "students_upserted": students_upserted,
        "results_stored": results_stored,
        "records_parsed": len(records),
        "errors": errors[:10],  # cap error list
    }


# ── Parsing helpers ───────────────────────────────────────────────────────────

_USN_RE  = re.compile(r"\b([1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3})\b", re.I)
_SGPA_RE = re.compile(r"(?:s\.?g\.?p\.?a|semester\s+gpa)[^\d]*([\d]{1,2}\.[\d]{1,2})", re.I)
_CGPA_RE = re.compile(r"(?:c\.?g\.?p\.?a|cumulative\s+gpa|overall\s+gpa)[^\d]*([\d]{1,2}\.[\d]{1,2})", re.I)


def _parse_raw_text(text: str) -> list[dict]:
    """Extract student records from free-form text using regex."""
    records: list[dict] = []
    # Split into per-student blocks at each USN
    blocks = re.split(r"(?=\b[1-4][A-Z]{2}\d{2}[A-Z]{2,4}\d{3}\b)", text, flags=re.I)
    for block in blocks:
        usn_m = _USN_RE.search(block)
        if not usn_m:
            continue
        usn  = usn_m.group(1).upper()
        sgpa = _SGPA_RE.search(block)
        cgpa = _CGPA_RE.search(block)
        sem_m = re.search(r"semester[:\s]+(\d)", block, re.I)
        sem = int(sem_m.group(1)) if sem_m else 1
        # Name heuristic: first ≥2 capitalized words after USN
        post = block[usn_m.end():].strip()
        name_m = re.match(r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,3})", post)
        name = name_m.group(1).strip() if name_m else usn
        rec: dict[str, Any] = {"usn": usn, "name": name, "semester": sem}
        if sgpa:
            rec["sgpa"] = float(sgpa.group(1))
        if cgpa:
            rec["cgpa"] = float(cgpa.group(1))
        # Subject results: CODE  Name  Marks  Grade  PASS/FAIL
        subj_re = re.compile(
            r"\b([A-Z]{2,4}\d{2,4})\b[^\n]*?(\d{2,3})\s*/?\s*(\d{2,3})?\s+([A-F][+-]?)\s+(PASS|FAIL)",
            re.I
        )
        subs = subj_re.findall(block)
        if subs:
            for code, marks, max_m, grade, status in subs:
                records.append({
                    "usn": usn, "name": name, "semester": sem,
                    "subject_code": code.upper(), "subject_name": code.upper(),
                    "marks": float(marks), "max_marks": float(max_m or 100),
                    "grade": grade.upper(), "status": status.upper(),
                })
        else:
            records.append(rec)
    return records


def _parse_pdf_text(content: bytes) -> list[dict]:
    import pdfplumber
    import pandas as pd

    text_parts: list[str] = []
    table_records: list[dict] = []

    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            # ── 1. Try table extraction first (grade cards / mark sheets) ──
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue
                try:
                    raw_headers = table[0]
                    headers = [
                        str(c).strip().replace("\n", " ") if c else f"col_{i}"
                        for i, c in enumerate(raw_headers)
                    ]
                    rows = [
                        [str(cell).strip() if cell is not None else "" for cell in row]
                        for row in table[1:]
                    ]
                    df = pd.DataFrame(rows, columns=headers)
                    recs = _parse_dataframe(df)
                    if recs:
                        table_records.extend(recs)
                except Exception:
                    pass

            # ── 2. Also collect plain text for USN/SGPA/CGPA header info ──
            t = page.extract_text()
            if t:
                text_parts.append(t)

    # If table extraction found subject-level records, use them
    if table_records:
        # Enrich with header-level USN/SGPA/CGPA from text if missing
        text_all = "\n".join(text_parts)
        usn_m  = _USN_RE.search(text_all)
        sgpa_m = _SGPA_RE.search(text_all)
        cgpa_m = _CGPA_RE.search(text_all)
        for rec in table_records:
            if not rec.get("usn") and usn_m:
                rec["usn"] = usn_m.group(1).upper()
            if not rec.get("sgpa") and sgpa_m:
                rec["sgpa"] = float(sgpa_m.group(1))
            if not rec.get("cgpa") and cgpa_m:
                rec["cgpa"] = float(cgpa_m.group(1))
        # Filter out any records still missing a valid USN
        valid = [r for r in table_records if r.get("usn") and _USN_RE.match(str(r["usn"]))]
        if valid:
            return valid

    # ── Fallback: regex over plain-text ──────────────────────────────────────
    return _parse_raw_text("\n".join(text_parts))


def _parse_docx_text(content: bytes) -> list[dict]:
    from docx import Document
    doc = Document(io.BytesIO(content))
    text = "\n".join(p.text for p in doc.paragraphs)
    # Also extract from tables
    for table in doc.tables:
        for row in table.rows:
            text += "\n" + "\t".join(cell.text for cell in row.cells)
    return _parse_raw_text(text)


def _parse_csv(content: bytes) -> list[dict]:
    import pandas as pd
    try:
        df = pd.read_csv(io.BytesIO(content), dtype=str)
    except Exception:
        df = pd.read_csv(io.BytesIO(content), dtype=str, encoding="latin-1")
    return _parse_dataframe(df)


def _parse_excel(content: bytes, filename: str) -> list[dict]:
    import pandas as pd
    engine = "xlrd" if filename.endswith(".xls") else "openpyxl"
    try:
        df = pd.read_excel(io.BytesIO(content), dtype=str, engine=engine)
    except Exception:
        # Try reading all sheets and pick the first non-empty one
        sheets = pd.read_excel(io.BytesIO(content), dtype=str, engine=engine, sheet_name=None)
        df = next((v for v in sheets.values() if not v.empty), None)
        if df is None:
            return []
    return _parse_dataframe(df)


def _parse_dataframe(df) -> list[dict]:
    """
    Parse a DataFrame where each row may be a student+semester aggregate
    or a student+subject result.

    Column name aliases (case-insensitive) supported:
      USN column    : usn, roll, roll_no, roll_number, university_seat_number, id
      Name column   : name, student_name, full_name
      Semester col  : semester, sem, semno
      SGPA col      : sgpa, semester_gpa, sem_gpa
      CGPA col      : cgpa
      Subject code  : subject_code, code, sub_code
      Subject name  : subject, subject_name, sub_name, paper
      Marks col     : marks, marks_obtained, total_marks, score
      Max marks     : max_marks, max, total, out_of
      Grade col     : grade, letter_grade
      Status col    : status, result, pass_fail
      Grade points  : grade_points, gp, points
    """
    import pandas as pd

    # Normalize column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    def _pick(cols: list[str]):
        for c in cols:
            if c in df.columns:
                return c
        return None

    usn_col    = _pick(["usn","roll","roll_no","roll_number","university_seat_number","id","seat_no"])
    name_col   = _pick(["name","student_name","full_name","student"])
    sem_col    = _pick(["semester","sem","semno","sem_no"])
    sgpa_col   = _pick(["sgpa","semester_gpa","sem_gpa","gpa"])
    cgpa_col   = _pick(["cgpa","cumulative_gpa"])
    code_col   = _pick(["subject_code","code","sub_code","course_code"])
    sname_col  = _pick(["subject","subject_name","sub_name","paper","course_name","course"])
    marks_col  = _pick(["marks","marks_obtained","total_marks","score","obtained"])
    max_col    = _pick(["max_marks","max","total","out_of","full_marks"])
    grade_col  = _pick(["grade","letter_grade","grade_letter"])
    status_col = _pick(["status","result","pass_fail","outcome"])
    gp_col     = _pick(["grade_points","gp","points","grade_point"])

    if not usn_col:
        return []

    records: list[dict] = []
    for _, row in df.iterrows():
        usn = str(row.get(usn_col, "")).strip().upper()
        # Validate USN format
        if not _USN_RE.match(usn):
            # Try to find USN pattern anywhere in the column value
            m = _USN_RE.search(usn)
            if m:
                usn = m.group(1)
            else:
                continue

        name = str(row.get(name_col, usn)).strip() if name_col else usn
        sem  = int(float(row.get(sem_col, 1) or 1)) if sem_col else 1
        rec: dict[str, Any] = {"usn": usn, "name": name, "semester": sem}

        if sgpa_col and pd.notna(row.get(sgpa_col)):
            try:
                rec["sgpa"] = float(str(row[sgpa_col]).replace(",", "."))
            except (ValueError, TypeError):
                pass

        if cgpa_col and pd.notna(row.get(cgpa_col)):
            try:
                rec["cgpa"] = float(str(row[cgpa_col]).replace(",", "."))
            except (ValueError, TypeError):
                pass

        if code_col and pd.notna(row.get(code_col)):
            rec["subject_code"] = str(row[code_col]).strip().upper()
            if sname_col and pd.notna(row.get(sname_col)):
                rec["subject_name"] = str(row[sname_col]).strip()
            else:
                rec["subject_name"] = rec["subject_code"]
            if marks_col and pd.notna(row.get(marks_col)):
                try:
                    rec["marks"] = float(str(row[marks_col]).replace(",","."))
                except (ValueError, TypeError):
                    pass
            if max_col and pd.notna(row.get(max_col)):
                try:
                    rec["max_marks"] = float(str(row[max_col]).replace(",","."))
                except (ValueError, TypeError):
                    pass
            if grade_col and pd.notna(row.get(grade_col)):
                rec["grade"] = str(row[grade_col]).strip()
            if status_col and pd.notna(row.get(status_col)):
                raw_s = str(row[status_col]).strip().upper()
                rec["status"] = "PASS" if raw_s in ("PASS","P","1","TRUE","YES","OK") else "FAIL"
            if gp_col and pd.notna(row.get(gp_col)):
                try:
                    rec["grade_points"] = float(str(row[gp_col]).replace(",","."))
                except (ValueError, TypeError):
                    pass

        records.append(rec)
    return records
