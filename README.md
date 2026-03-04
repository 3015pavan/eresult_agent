# AcadExtract — Autonomous Academic Result Extraction & Student Profiling

> **Status: Work In Progress** — Core pipeline is functional end-to-end. Several planned components (Phase 2 heavy OCR, Phase 4 Agentic layer, Celery task queues, pgvector search) are stubbed and not yet wired in.

An AI-powered system that automatically ingests emails from Gmail, extracts student academic result data from email bodies and attachments, structures it into a PostgreSQL database, and exposes a natural-language chat interface for querying results.

---

## Table of Contents

- [Quick Start](#quick-start)
- [What Works Today](#what-works-today)
- [What Is Not Yet Complete](#what-is-not-yet-complete)
- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [File-by-File Reference](#file-by-file-reference)
- [Configuration](#configuration)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Tech Stack](#tech-stack)

---

## Quick Start

### Prerequisites
- Python 3.11+
- PostgreSQL 16 (port 5434)
- Redis (port 6379)
- Docker (optional, for infra)
- A Groq API key (free at [console.groq.com](https://console.groq.com))

### 1. Clone & install

```bash
git clone https://github.com/3015pavan/eresult_agent.git
cd eresult_agent
python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements-api.txt
```

### 2. Set up environment

Create a `.env` file in the project root:

```env
GROQ_API_KEY=gsk_xxxxxxxxxxxxxxxxxxxx
DATABASE_URL=postgresql://postgres:password@localhost:5434/email_agent
REDIS_URL=redis://localhost:6379/0
SECRET_KEY=your-secret-key-here
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
```

### 3. Set up the database

```bash
# Start infrastructure (PostgreSQL, Redis, MinIO)
docker compose -f docker/docker-compose.infra.yml up -d

# Run schema migrations
psql -h localhost -p 5434 -U postgres -d email_agent -f schemas/001_core_schema.sql
psql -h localhost -p 5434 -U postgres -d email_agent -f schemas/002_app_partitions.sql
```

### 4. Connect Gmail

Place your Google OAuth2 `credentials.json` in `config/secrets/credentials.json`, then:

1. Open `http://localhost:8002`
2. Go to **Settings → Connect Gmail Account**
3. Complete OAuth flow — `token.json` is saved to `config/secrets/token.json`

### 5. Run the server

```bash
.venv\Scripts\uvicorn.exe src.api.app:app --host 0.0.0.0 --port 8002 --log-level warning
```

Open `http://localhost:8002` in your browser.

---

## What Works Today

| Feature | Status |
|---|---|
| Gmail OAuth2 sync (fetch emails + attachments) | ✅ Working |
| SHA-256 exact dedup + SimHash near-dedup | ✅ Working |
| Email classification (result / spam / other) — keyword heuristics | ✅ Working |
| Multi-strategy extraction: regex + LLM (Groq llama-3.3-70b) | ✅ Working |
| Multi-semester email parsing (5 semesters → 5 DB records) | ✅ Working |
| PostgreSQL student/results/subjects/semester_aggregates storage | ✅ Working |
| Natural language chat / AI assistant (Groq) | ✅ Working |
| Grade card by student name or USN | ✅ Working |
| Semester-specific filtering in chat | ✅ Working |
| Force reprocess with stale-data cleanup | ✅ Working |
| React-style single-page web UI | ✅ Working |
| Inbox filter (Results only / All) | ✅ Working |
| Pipeline live counters (synced / result emails / records / students) | ✅ Working |
| Prometheus `/metrics` endpoint | ✅ Working |
| PDF / DOCX / HTML / ODT attachment parsing | ✅ Working (inline, not Phase 2 router) |
| Excel / CSV parsing via admin upload | ✅ Working |

---

## What Is Not Yet Complete

| Planned Feature | Notes |
|---|---|
| DistilBERT email classifier | Currently keyword regex; ML model not trained/integrated |
| MinHash LSH (3rd dedup layer) | Only SHA256 + SimHash implemented |
| ClamAV virus scan | Installed in Dockerfile, never called in code |
| Thread reconstructor (In-Reply-To headers) | Not implemented |
| Raw email upload to MinIO/S3 | `storage.py` exists, pipeline never calls it |
| Phase 2 Document Router | `router.py` is a stub — camelot, PaddleOCR, Tesseract, LayoutLMv3 not wired |
| Phase 4 Agentic Layer | All stub files (`agent.py`, `critic.py`, `planner.py`, etc.) — not wired into any endpoint |
| Celery async task queues | Pipeline runs synchronously in-process; Celery config exists but unused |
| pgvector semantic search | Column + index in schema, zero embeddings generated |
| Elasticsearch full-text index | Docker service runs, no code touches it |
| Users / RBAC / login system | Schema has users table with 5 roles; no auth beyond Google OAuth |
| Row-level security enforcement | DB policies exist; app doesn't enforce them |
| OpenTelemetry distributed tracing | `observability.py` uses structlog only |
| Kubernetes HPA | `deployment.yaml` exists, no HPA configured |

---

## Architecture Overview

```
Gmail API / IMAP
      │
      ▼
Phase 1 — Email Ingestion
  • OAuth2 sync (sync.py)
  • SHA256 + SimHash dedup
  • Keyword classification
  • Attachment extraction
      │
      ▼
Phase 2 — Document Intelligence  [PARTIAL]
  • HTML, DOCX, ODT parsers (working)
  • PDF text via pdfplumber (working)
  • Image OCR via Groq Vision (working)
  • camelot / PaddleOCR / LayoutLMv3 (NOT WIRED)
      │
      ▼
Phase 3 — Information Extraction
  • Regex engine (30+ patterns: USN, SGPA, CGPA, subjects)
  • LLM extractor (Groq llama-3.3-70b, multi-semester aware)
  • Strategy merger (regex + LLM voting, multi-sem fast-path)
  • Validator + correction loop
  • Human review queue (low-confidence records)
      │
      ▼
Phase 4 — Agentic Layer  [STUB]
  • agent.py, critic.py, planner.py, executor.py, memory.py, tools.py
  • Not yet connected to any endpoint
      │
      ▼
Phase 5 — Query Engine
  • NL → SQL via Groq (llama-3.3-70b)
  • Student lookup by name or USN
  • Semester-specific filtering
  • History-aware follow-up questions
      │
      ▼
PostgreSQL (students, student_results, subjects, semester_aggregates)
```

---

## Project Structure

```
email_agent/
├── config/
│   ├── system.yaml              # Global app configuration
│   └── secrets/                 # Git-ignored — holds credentials.json + token.json
├── data/
│   ├── accounts.json            # Git-ignored — connected Gmail accounts
│   ├── emails_cache.json        # Git-ignored — local email cache
│   └── state/                   # Git-ignored — pipeline + sync state files
├── docker/
│   ├── Dockerfile               # Python 3.11 + system deps (Tesseract, ClamAV, etc.)
│   ├── docker-compose.yml       # Full stack: API + workers + all infra
│   └── docker-compose.infra.yml # Infra only: PostgreSQL, Redis, MinIO, Elasticsearch
├── docs/
│   └── ARCHITECTURE.md          # Full 1148-line architecture blueprint
├── infra/
│   ├── k8s/deployment.yaml      # Kubernetes deployment spec
│   └── monitoring/prometheus.yml
├── schemas/
│   ├── 001_core_schema.sql      # All tables, indexes, RLS policies, views
│   └── 002_app_partitions.sql   # Table partitioning for scale
├── scripts/
│   ├── seed_test_data.py        # Inserts 25 test students with result records
│   └── _debug_classify.py       # CLI tool to test email classification
├── src/
│   ├── api/                     # FastAPI application
│   ├── common/                  # Shared utilities (DB, cache, storage, etc.)
│   ├── phase2_document_intelligence/
│   ├── phase3_extraction_engine/
│   ├── phase4_agentic_layer/    # STUB — not yet functional
│   ├── tasks/                   # Celery task definitions (not yet used)
│   └── frontend/                # Single-page HTML/CSS/JS UI
└── tests/
```

---

## File-by-File Reference

### `src/api/`

| File | Purpose |
|---|---|
| `app.py` | FastAPI application factory. Registers all routers, sets up CORS, mounts the static frontend, initialises DB/Redis/MinIO on startup. |
| `routes/auth.py` | Gmail OAuth2 flow — `/auth/google`, `/auth/callback`, `/auth/status`. Saves `token.json` to `config/secrets/`. |
| `routes/sync.py` | Email sync — `/sync/trigger` fetches new Gmail messages, runs dedup, classifies, caches to `emails_cache.json` and `email_metadata` DB. `/sync/emails` returns inbox with optional `classification` filter. |
| `routes/pipeline.py` | The main extraction pipeline — `/pipeline/run` processes cached emails through Phase 2 → Phase 3 → DB save. Contains all regex patterns (USN, SGPA, grades, multi-semester blocks), `_extract_from_body()`, `_save_records_to_db()`. **Largest and most important file (~1040 lines).** |
| `routes/query.py` | AI chat endpoint — `/chat` takes a natural language message, builds DB context (student lookup by name/USN, semester filter, history scan), calls Groq LLM, returns markdown reply. Also contains `/query` for structured NL→SQL queries. |
| `routes/admin.py` | Manual file upload endpoint — accepts PDF, Excel, CSV, DOCX; runs extraction pipeline inline. Also `/admin/review-queue` for low-confidence records. |
| `routes/accounts.py` | Lists and removes connected Gmail accounts. |
| `routes/health.py` | `/api/v1/health` — checks DB, Redis, MinIO connectivity. Returns JSON with status of each service. |
| `routes/agent.py` | Placeholder endpoints for the Phase 4 agentic layer (`/agent/run`, `/agent/status`, `/admin/traces/{run_id}`). Returns stubs. |
| `routes/webhook.py` | Gmail push notification webhook receiver. Not yet fully wired. |

### `src/common/`

| File | Purpose |
|---|---|
| `database.py` | All PostgreSQL operations — `upsert_student()`, `upsert_result()`, `get_pipeline_stats()`, `store_semester_aggregate()`, `compute_and_store_cgpa()`, `save_extraction()`, etc. Uses `psycopg2` with `RealDictCursor`. **Second most important file.** |
| `config.py` | Loads `config/system.yaml` + `.env` via Pydantic Settings. Exposes `settings` singleton used everywhere. |
| `models.py` | Pydantic request/response models for the API. |
| `cache.py` | Redis wrapper — `get()`, `set()`, `delete()`, `exists()`. Used for dedup hashes and session state. |
| `storage.py` | MinIO/S3 wrapper — `upload_file()`, `download_file()`, `get_presigned_url()`. Configured but not yet called by the pipeline. |
| `observability.py` | structlog configuration + Prometheus metrics counter/histogram setup. |
| `embeddings.py` | Sentence-transformer embedding generation for pgvector. Called by `index_student` task but pgvector writes not yet active. |
| `security.py` | JWT token creation/validation, password hashing (bcrypt). For future RBAC; not yet enforced. |
| `celery_app.py` | Celery app configuration pointing at Redis broker. Tasks defined in `src/tasks/` but Celery workers not started. |
| `elasticsearch_client.py` | Elasticsearch client wrapper. Present but never called. |

### `src/phase2_document_intelligence/`

| File | Purpose |
|---|---|
| `router.py` | **STUB** — intended central `route_to_parser(file_type)` dispatcher. Not yet implemented. |
| `pdf_parser.py` | **STUB** — pdfplumber + camelot integration. Actual PDF parsing happens inline in `pipeline.py`. |
| `ocr_pipeline.py` | **STUB** — PaddleOCR + Tesseract fallback. Not called. Groq Vision used instead for image attachments. |
| `excel_parser.py` | openpyxl / pandas Excel parsing. Partially working via `admin.py` upload route. |
| `table_stitcher.py` | Multi-page table continuation detection. Has regex patterns, not called by main pipeline. |
| `html_parser.py` | BeautifulSoup HTML email body parser. Used for HTML attachment stripping. |
| `docx_odf_parser.py` | python-docx + odfpy DOCX/ODT parser. Wired into pipeline attachment handler. |
| `universal_converter.py` | Attempts to convert any document type to plain text. Used as fallback in attachment processing. |

### `src/phase3_extraction_engine/`

| File | Purpose |
|---|---|
| `llm_extractor.py` | Calls Groq API (llama-3.3-70b) with a JSON-mode system prompt to extract structured result data. Multi-semester aware — returns one JSON object per semester. Retries on parse failure with a targeted prompt. |
| `strategy_merger.py` | Runs regex + LLM strategies, then merges results. If LLM returns >1 distinct semester, returns them as-is (multi-sem fast-path). Otherwise does field-level voting for USN, semester, SGPA. |
| `validator.py` | Validates each extracted record — checks marks range, grade validity, PASS/FAIL consistency. Returns `(records, ValidationResult)` tuple. Attempts auto-correction on common errors. |
| `review_queue.py` | `enqueue_for_review()` saves low-confidence records (below 0.75 threshold) to a Redis-backed review queue. `/admin/review-queue` endpoint serves pending items. |

### `src/phase4_agentic_layer/` — **ALL STUBS**

| File | Purpose (Planned) |
|---|---|
| `agent.py` | State machine: IDLE → PLANNING → EXECUTING → VERIFYING → COMPLETED. Not wired. |
| `planner.py` | Breaks a user goal into sub-tasks using LLM function calling. Not wired. |
| `executor.py` | Executes planned tool calls. Not wired. |
| `critic.py` | Scores execution quality (0.0–1.0 `critic_score`). Not wired. |
| `memory.py` | Episodic memory store for past extraction patterns. Not wired. |
| `tools.py` | 15 tool definitions: `email_fetch`, `pdf_parse`, `dedup_check`, `gpa_compute`, etc. Not wired. |

### `src/tasks/` — **Defined, Celery not running**

| File | Purpose |
|---|---|
| `extraction.py` | `extract_email` Celery task — Phase 2+3 for a single email. |
| `indexing.py` | `index_student` Celery task — generates pgvector embedding for a student profile. |
| `ingestion.py` | `ingest_batch` Celery task — bulk email ingestion. |

### `src/frontend/`

| File | Purpose |
|---|---|
| `index.html` | Single-page application shell. Contains all HTML sections: Dashboard, Pipeline Control, Inbox, AI Assistant, Settings. |
| `static/js/app.js` | ~2000-line vanilla JavaScript SPA. All UI logic: `loadDashboard()`, `syncEmails()`, `runPipeline(force)`, `loadPipelineSummary()`, `setInboxFilter()`, `sendChat()`, `_mdToHtml()` (table/list/bold renderer), toast notifications. |
| `static/css/styles.css` | ~1700-line dark-theme stylesheet. CSS variables for theming, card layouts, inbox rows, pipeline status, chat bubbles, grade card tables. |

### `schemas/`

| File | Purpose |
|---|---|
| `001_core_schema.sql` | All 15 tables: `institutions`, `departments`, `students`, `subjects`, `student_results`, `semester_aggregates`, `email_metadata`, `attachments`, `extractions`, `agent_traces`, `query_audit_log`, `review_queue`, `users`, etc. Includes indexes, pgvector ivfflat index, Row-Level Security policies, and two materialized views. |
| `002_app_partitions.sql` | Range partitioning on `email_metadata` and `student_results` by year/semester for query performance at scale. |

### `scripts/`

| File | Purpose |
|---|---|
| `seed_test_data.py` | Inserts 25 synthetic students (names, USNs, results across 8 semesters) into the DB. Run once to populate test data: `.venv\Scripts\python.exe scripts/seed_test_data.py` |
| `_debug_classify.py` | CLI that reads an email body from stdin and prints the classification result. Useful for tuning keyword heuristics. |

### `docker/`

| File | Purpose |
|---|---|
| `Dockerfile` | Builds the API image: Python 3.11-slim + Tesseract OCR + ClamAV + Poppler + system libs. |
| `docker-compose.yml` | Full stack: API server + Celery worker + Celery beat + PostgreSQL + Redis + MinIO + Elasticsearch + Prometheus + Grafana. |
| `docker-compose.infra.yml` | Infrastructure only (no app containers). Use this for local dev with the server running outside Docker. |

### Other root files

| File | Purpose |
|---|---|
| `.env` | **Git-ignored.** `GROQ_API_KEY`, `DATABASE_URL`, `REDIS_URL`, `SECRET_KEY`. |
| `config/system.yaml` | Non-secret app config: institution name, sync intervals, extraction thresholds, feature flags. |
| `config/secrets/credentials.json` | **Git-ignored.** Google OAuth2 client credentials downloaded from Google Cloud Console. |
| `config/secrets/token.json` | **Git-ignored.** Gmail access + refresh token. Auto-generated on first OAuth login. |
| `pyproject.toml` | Project metadata, full dependency list, ruff/mypy/pytest configuration. |
| `requirements-api.txt` | Pinned subset of `pyproject.toml` deps — what's actually installed in the venv. |

---

## Configuration

`config/system.yaml` key settings:

```yaml
institution:
  name: "MSRIT"
  code: "MSRIT"

sync:
  max_emails_per_run: 100
  lookback_days: 90

pipeline:
  extraction_confidence_threshold: 0.75   # Below this → human review queue
  force_reprocess: false

groq:
  model: "llama-3.3-70b-versatile"
  extraction_model: "llama-3.3-70b-versatile"
```

---

## API Reference

All endpoints are prefixed with `/api/v1`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Service health check (DB, Redis, MinIO) |
| `GET` | `/auth/google` | Start Gmail OAuth2 flow |
| `GET` | `/auth/callback` | OAuth2 callback — saves token |
| `GET` | `/auth/status` | Check if Gmail is connected |
| `POST` | `/sync/trigger` | Fetch new emails from Gmail |
| `GET` | `/sync/emails` | List cached emails (`?classification=result_email`) |
| `GET` | `/sync/status` | Last sync time, account info, new emails count |
| `POST` | `/pipeline/run` | Run extraction pipeline (`{"force": true/false}`) |
| `GET` | `/pipeline/status` | DB totals: students, records, CGPA, backlogs |
| `POST` | `/chat` | AI assistant (`{"message": "...", "history": [...]}`) |
| `POST` | `/query` | Structured NL→SQL query |
| `GET` | `/admin/review-queue` | List low-confidence records pending review |
| `POST` | `/admin/upload` | Upload PDF/Excel/CSV for manual extraction |
| `GET` | `/metrics` | Prometheus metrics |

---

## Database Schema

Core tables and their role:

| Table | Description |
|---|---|
| `institutions` | Multi-tenant root — one row per college/university |
| `students` | Student master: USN, name, CGPA, backlogs, pgvector embedding column |
| `subjects` | Subject catalogue per institution (code, name, credits, semester) |
| `student_results` | One row per (student, subject, semester) — marks, grade, status |
| `semester_aggregates` | Pre-computed SGPA, pass/fail counts per (student, semester) |
| `email_metadata` | Every processed email: classification, pipeline status, sender |
| `attachments` | Files extracted from emails: PDF, DOCX, images |
| `extractions` | Extraction run records with confidence scores and raw LLM output |
| `review_queue` | Low-confidence records awaiting human verification |
| `users` | Planned: admin / teacher / HOD / principal roles (schema ready, not used) |
| `agent_traces` | Planned: step-by-step agentic execution log (schema ready, not used) |
| `query_audit_log` | Planned: every NL query log for analytics (schema ready, not used) |

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Framework | FastAPI 0.104+ / Uvicorn |
| Database | PostgreSQL 16 + pgvector |
| Cache / Broker | Redis 7 |
| Object Storage | MinIO (S3-compatible) |
| LLM | Groq (llama-3.3-70b-versatile) |
| Email | Gmail API (OAuth2) / IMAPClient |
| PDF Parsing | pdfplumber (active), camelot (planned) |
| OCR | Groq Vision (active), PaddleOCR / Tesseract (planned) |
| Observability | Prometheus + structlog |
| Frontend | Vanilla HTML/CSS/JS (no framework) |
| Containerisation | Docker + Docker Compose |
| Python | 3.11+ |

---

## Known Limitations

1. **Single institution only** — `get_default_institution_id()` returns a hardcoded UUID. Multi-tenancy is schema-ready but not enforced.
2. **Synchronous pipeline** — Large inboxes (500+ emails) will block the HTTP request. Celery task queue is configured but not running.
3. **Subject codes are auto-generated** — When the email doesn't include a subject code, an MD5-based code is generated (`DMSD867`, etc.). These are stable but not human-readable.
4. **No user authentication** — The API has no login system beyond Gmail OAuth. Anyone with network access to port 8002 can use all endpoints.
5. **LLM extraction cost** — Every force-reprocess call sends each email to Groq. With 75 emails, this is ~75 API calls.
6. **pgvector search not active** — Semantic similarity search (find students by embedding similarity) is designed but not implemented.
