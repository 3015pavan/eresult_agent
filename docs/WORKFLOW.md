# Document Processing Workflow

This markdown describes the end-to-end workflow for the intelligent document analysis system, as specified in the provided paragraph. It replaces all previous .md files.

## 1. Input Ingestion Layer
- Documents enter via frontend upload or incoming email (SMTP/IMAP).
- Email attachments are automatically downloaded and stored in MinIO or Supabase Storage.

## 2. Document Classification
- Lightweight classification using rules or LLM (GPT/Gemini) checks if the document is a valid result/marksheet.

## 3. Asynchronous Queue System
- Valid documents are pushed into a Redis/Celery queue for scalable, parallel processing.

## 4. Document Processing Stage
- Background worker (AI agent pipeline) consumes tasks from the queue.
- YOLOv8 detects table regions in the document.
- PaddleOCR extracts text from scanned regions.
- LlamaParse extracts structured content from digital PDFs.

## 5. Core Intelligence Layer
- Large Language Model (GPT/Gemini) converts unstructured text into standardized JSON (student details, marks, totals, results).

## 6. Validation and Correction
- Rule-based checks verify numerical correctness.
- LLM reasoning detects logical inconsistencies.
- Retry mechanism reprocesses failed documents until accuracy or retry limit.
- Confidence scoring flags low-quality outputs for review.

## 7. Report Generation
- Structured data is transformed into HTML templates and converted to PDF.
- Reports include computed insights (percentages, grades, AI summary).

## 8. Automated Email Response
- LLM composes reply and sends report as attachment via SMTP/AWS SES.

## 9. Data Storage and Audit Logging
- All results, metadata, and confidence scores stored in PostgreSQL (Supabase).
- Audit logs track ingestion, processing, retries, and email delivery.

## 10. Role-Based Dashboard
- Next.js dashboard for uploads, report viewing, status monitoring, and admin log review.

## 11. End-to-End Automation
- Single intelligent AI agent pipeline orchestrated through queues and modular stages.
- Fully automated, production-ready document analysis solution.
# AcadExtract — Workflows

## 1. User: Upload a VTU Grade Report (Excel)

```
Teacher (Browser)              Admin API              Database
       │                           │                      │
       │  1. Navigate to Admin tab  │                      │
       │  2. Drag-drop .xlsx file  │                      │
       │──POST /api/v1/admin/upload▶                      │
       │                           │                      │
       │                    ┌──────▼──────────────┐       │
       │                    │  _parse_excel()      │       │
       │                    │    ▼                 │       │
       │                    │  _parse_dataframe()  │       │
       │                    │    → empty (VTU fmt) │       │
       │                    │    ▼                 │       │
       │                    │  _parse_vtu_grade_   │       │
       │                    │    report()          │       │
       │                    │    ├─ find USN row   │       │
       │                    │    ├─ detect columns │       │
       │                    │    ├─ infer semester │       │
       │                    │    └─ parse students │       │
       │                    └──────┬──────────────┘       │
       │                           │ records[]            │
       │                           │──upsert_student()────▶
       │                           │──upsert_result()─────▶
       │                           │──store_semester_agg()▶
       │                           │──compute_and_store_  │
       │                           │   cgpa()             ▶
       │                           │──save_admin_upload() ▶
       │                           │                      │
       │◀─{ students_upserted,     │                      │
       │    results_stored,        │                      │
       │    records_parsed }───────│                      │
       │                           │                      │
       │  3. Toast: "Upload        │                      │
       │     successful"           │                      │
```

---

## 2. User: Run Email Pipeline

```
Teacher               sync.py                  pipeline.py                 PostgreSQL
   │                     │                          │                          │
   │  1. Connect Gmail   │                          │                          │
   │──GET /auth/login────▶ → Google OAuth           │                          │
   │◀─redirect to consent│                          │                          │
   │──callback → token───▶                          │                          │
   │                     │                          │                          │
   │  2. Sync emails     │                          │                          │
   │──POST /sync─────────▶                          │                          │
   │                     │ Gmail API fetch           │                          │
   │                     │ → classify subjects       │                          │
   │                     │ → save to emails_cache.json                          │
   │◀─{ fetched, cached }│                          │                          │
   │                     │                          │                          │
   │  3. Run pipeline    │                          │                          │
   │──POST /pipeline/run─────────────────────────── ▶                          │
   │                     │                          │                          │
   │                     │              ┌───────────▼───────────┐              │
   │                     │              │  _run_pipeline_sync()  │              │
   │                     │              │                        │              │
   │                     │              │  for each email:       │              │
   │                     │              │   ├─ SHA256 dedup ────▶Redis          │
   │                     │              │   ├─ _classify_email() │              │
   │                     │              │   │   skip if 'other'  │              │
   │                     │              │   │                    │              │
   │                     │              │   ├─ _extract_from_body()             │
   │                     │              │   │   (regex patterns) │              │
   │                     │              │   │                    │              │
   │                     │              │   ├─ extract_with_     │              │
   │                     │              │   │   voting()         │              │
   │                     │              │   │   (LLM + regex)    ├────▶ Groq    │
   │                     │              │   │                    │              │
   │                     │              │   ├─ validate_and_     │              │
   │                     │              │   │   correct()        │              │
   │                     │              │   │                    │              │
   │                     │              │   └─ _save_records_    │              │
   │                     │              │       to_db()─────────▶              │
   │                     │              │                        │              │
   │                     │              │  store raw email ──────▶ MinIO        │
   │                     │              └────────────────────────┘              │
   │◀─{ processed, extracted, stored }──│                          │
```

---

## 3. User: Look Up a Student

```
Teacher (Browser)           query.py               PostgreSQL
       │                        │                       │
       │  Type USN or name in   │                       │
       │  Student Lookup panel  │                       │
       │                        │                       │
       │  ── if USN entered ──  │                       │
       │──GET /student/1MS21CS001▶                      │
       │                        │──get_student(usn)────▶│
       │                        │──get_student_results()│
       │                        │──compute rank─────────│
       │◀─{ StudentSummary }────│                       │
       │  render: semesters,    │                       │
       │  subjects, SGPA chart  │                       │
       │                        │                       │
       │  ── if name entered ── │                       │
       │──GET /students?q=Alice─▶                       │
       │                        │──search_students()───▶│
       │                        │  (pg_trgm LIKE)       │
       │◀─[{ usn, name, cgpa }]─│                       │
       │  display disambiguation│                       │
       │  list; pick one→       │                       │
       │──GET /student/{usn}────▶                       │
       │  (same as USN path)    │                       │
```

---

## 4. User: Chat with AI Assistant

```
Teacher (Browser)          query.py          PostgreSQL        LLM API
       │                      │                   │                │
       │  Type question in    │                   │                │
       │  AcadAssist chat box │                   │                │
       │──POST /chat──────────▶                   │                │
       │  { message,          │                   │                │
       │    history[] }       │                   │                │
       │                      │                   │                │
       │              ┌───────▼───────────────┐   │                │
       │              │  1. Intent detection   │   │                │
       │              │     _parse_intent_     │   │                │
       │              │     local(message)     │   │                │
       │              │                        │   │                │
       │              │  2. Name lookup        │   │                │
       │              │     search_students()──────▶               │
       │              │                        │   │                │
       │              │  3. DB context fetch   │   │                │
       │              │     _execute_query()───────▶               │
       │              │                        │   │                │
       │              │  4. LLM call           │   │                │
       │              │     _llm_synthesize_   │   │     ┌──────────│
       │              │     query_answer()─────────────── ▶Groq API │
       │              │◀───────────────────────────────── LLM reply─│
       │              │                        │   │                │
       │              │  5. Verification       │   │                │
       │              │     _verify_chat_      │   │                │
       │              │     reply()────────────────▶ check CGPA     │
       │              │     ± 0.15 tolerance   │   │                │
       │              └───────────────────────┘   │                │
       │◀─{ reply }───────────│                   │                │
       │  render markdown     │                   │                │
```

---

## 5. User: Export / Email a Student Report

```
Teacher                  query.py                   SMTP
   │                        │                         │
   │──GET /student/{usn}/   │                         │
   │    report?fmt=pdf  ─── ▶                         │
   │         or xlsx        │ _build_pdf_report()      │
   │         or docx        │   or _build_xlsx_report()│
   │                        │   or _build_docx_report()│
   │◀─ StreamingResponse ───│                         │
   │   (binary download)    │                         │
   │                        │                         │
   │─── OR ──────────────── │                         │
   │                        │                         │
   │──POST /student/{usn}/  │                         │
   │    email-report        │                         │
   │  { recipient, fmt }    │                         │
   │                        │ build report bytes       │
   │                        │──_send_email_with_       │
   │                        │   attachment()───────────▶
   │                        │                         │ SMTP connect
   │                        │                         │ send MIME msg
   │◀─{ sent: true }────────│                         │
```

---

## 6. Document Intelligence Pipeline (Phase 2)

```
Input (bytes / path / Gmail attachment)
          │
          ▼
  universal_converter.py
  convert_any() / convert_bytes() / convert_gmail_attachment()
          │
    ┌─────▼─────┐
    │ router.py  │  route_to_parser() — MIME type detection
    └─────┬─────┘
          │
    ┌─────▼───────────────────────────────────────────┐
    │                                                  │
    ├─ PDF with text layer ──▶ pdf_parser.py           │
    │    pdfplumber → camelot(lattice) → camelot(stream)
    │    → tabula → table_stitcher                     │
    │                                                  │
    ├─ PDF scanned / image ──▶ ocr_pipeline.py         │
    │    pdf2image → PaddleOCR → Tesseract             │
    │                                                  │
    ├─ Excel / CSV ──────────▶ excel_parser.py         │
    │    openpyxl → xlrd → pandas                      │
    │                                                  │
    ├─ DOCX / ODT / RTF ────▶ docx_odf_parser.py      │
    │    python-docx / odfpy / striprtf                │
    │                                                  │
    ├─ HTML / email body ────▶ html_parser.py          │
    │    BeautifulSoup lxml                            │
    │                                                  │
    └─ Image (PNG/JPG/TIFF) ─▶ Groq Vision LLM        │
         → ocr_pipeline.py (Tesseract fallback)        │
    └───────────────────────────────────┘
          │
          ▼
    ParsedDocument
    ├─ flat_text: str
    ├─ tables: list[list[dict]]
    └─ parse_strategy: str
```

---

## 7. Phase 3 — Multi-Strategy Extraction Voting

```
Input text + doc_records
          │
    ┌─────▼──────────────────────────────────────────┐
    │  strategy_merger.py :: extract_with_voting()    │
    │                                                  │
    │  ┌───────────────┐  ┌────────────┐  ┌─────────┐  │
    │  │ _extract_from │  │ LLM extract│  │doc table│  │
    │  │  _body()      │  │ (Groq/OAI) │  │ records │  │
    │  │  regex A-G    │  │            │  │         │  │
    │  └───────┬───────┘  └─────┬──────┘  └────┬────┘  │
    │          │                │               │       │
    │          └────────────────┴───────────────┘       │
    │                      field-level vote              │
    │           (highest-confidence field wins)          │
    └────────────────────────┬───────────────────────────┘
                             │
                    records[] (merged)
                             │
                    validator.py :: validate_and_correct()
                    ├─ USN format check
                    ├─ semester range 1-8
                    ├─ marks 0-200
                    ├─ SGPA/CGPA 0-10
                    ├─ status vs marks consistency
                    └─ LLM re-extraction (up to 3 retries)
                             │
                    if confidence < REVIEW_THRESHOLD:
                        enqueue_for_review()
                    else:
                        save to PostgreSQL
```

---

## 8. Phase 4 — Agentic Loop

```
Goal string
"Process all unextracted MSRIT emails and save validated records"
          │
          ▼
  planner.py :: create_plan()
  ├─ Match against 6 named templates
  │    process_emails, lookup_student, parse_attachment,
  │    extract_from_text, save_results, validate_extraction
  └─ LLM fallback (Groq) if no template matches
          │
          ▼
  Plan: [{ tool, args }, ...]
          │
          ▼
  executor.py :: execute_step()  (for each step)
  ├─ Resolve {step_N.field} templates from previous outputs
  └─ tools.py :: call_tool(name, args)
       ├─ email_fetch       → sync.py
       ├─ parse_document    → phase2 universal_converter
       ├─ extract_records   → phase3 strategy_merger
       ├─ validate          → phase3 validator
       ├─ save_results      → database.upsert_*
       ├─ student_lookup    → database.get_student
       ├─ semantic_search   → embeddings.semantic_search_students
       └─ ... (17 tools total)
          │
          ▼
  critic.py :: evaluate(agent_run)
  ├─ Rule-based: penalise failed steps, missing extract/save
  ├─ LLM scoring (optional)
  └─ CriticResult(score, passed, feedback)
          │
          ▼
  memory.py :: store(event)
  └─ episodic_memory PostgreSQL table
```

---

## 9. Development Workflow

```
1. Start infrastructure:
   docker compose -f docker/docker-compose.infra.yml up -d

2. Apply DB schema:
   psql -U acadextract -d acadextract -f schemas/001_core_schema.sql
   psql -U acadextract -d acadextract -f schemas/002_app_partitions.sql

3. Seed test data (optional):
   python scripts/seed_test_data.py

4. Start API:
   .venv/Scripts/python -m uvicorn src.api.app:app \
     --host 127.0.0.1 --port 8002 --reload

5. Open browser:
   http://127.0.0.1:8002

6. Upload a result sheet:
   Admin tab → drag-drop d:\RESULT_SHEETS_21_to_25\CS_UG_1_SEM.xlsx

7. Run pipeline (if emails synced):
   Pipeline tab → Run Pipeline

8. Query results:
   Student Lookup: enter "1MS21CS001" or "ALEKHYA"
   AI Chat: "Show me all students with CGPA above 8.5"

9. (Optional) Start Celery worker for background tasks:
   celery -A src.common.celery_app worker --loglevel=info \
     -Q email_ingestion,extraction,indexing
```

---

## 10. Iterative Development Checklist

| Phase | Feature | Status |
|---|---|---|
| Infra | PostgreSQL + pgvector + Redis + MinIO | ✅ Done |
| Phase 2 | PDF/Excel/DOCX/OCR parsing | ✅ Done |
| Phase 2+ | VTU wide-format Excel parser | ✅ Done |
| Phase 3 | Regex + LLM extraction with voting | ✅ Done |
| Phase 3+ | Multi-semester extraction | ✅ Done |
| Phase 4 | Agentic state machine + 17 tools | ✅ Done |
| Phase 5 | NL query → SQL + AI chat | ✅ Done |
| Phase 5+ | Verification agent (CGPA cross-check) | ✅ Done |
| UI | Student lookup by name (disambiguation) | ✅ Done |
| UI | SGPA trend chart | ✅ Done |
| UI | PDF/Excel/DOCX report export + email | ✅ Done |
| Ops | Prometheus metrics + OTel tracing | ✅ Done |
| Ops | /healthz + /readyz probes | ✅ Done |
| Phase 3+ | Review queue approve/reject endpoints (HTTP + UI) | ✅ Done |
| Phase 4 | Webhook push-to-queue integration (SMTP + MSGraph) | ✅ Done |
| Phase 3+ | Active backlog tracking (cleared via supplementary) | ✅ Done |
| Phase 3+ | Gemini LLM provider in extraction pipeline | ✅ Done |
| Missing | Multi-institution UI switcher | ⬜ Backlog |
