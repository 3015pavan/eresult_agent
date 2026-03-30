# EMAIL_AGENT Architecture

This document describes the current working architecture of the `EMAIL_AGENT` repository as it exists in code today. It focuses on the real runtime path first, then calls out optional or partially wired components so the architecture stays accurate.

Related docs:

- `docs/ARCHITECTURE_DIAGRAM.md` for Mermaid-based visual diagrams
- `docs/IMPLEMENTATION_ROADMAP.md` for a step-by-step delivery plan

## 1. System Purpose

`EMAIL_AGENT` is an academic result ingestion and query system. It:

- ingests academic result data from Gmail, IMAP, webhooks, and manual uploads
- classifies result-bearing emails and documents
- extracts student, semester, and marks data from bodies and attachments
- validates and persists extracted records into PostgreSQL
- exposes lookup, analytics, report export, and AI-assisted query endpoints
- stores raw artifacts in object storage and operational state in Redis

## 2. High-Level Runtime Architecture

```text
Browser UI
  |
  v
FastAPI app (`src/api/app.py`)
  |
  +--> Auth routes (`src/api/routes/auth.py`)
  |     Gmail OAuth login/callback/status/logout
  |
  +--> Sync routes (`src/api/routes/sync.py`)
  |     Gmail fetch, inbox cache, thread views, IMAP trigger
  |
  +--> Pipeline routes (`src/api/routes/pipeline.py`)
  |     classify -> parse -> extract -> validate -> persist
  |
  +--> Admin routes (`src/api/routes/admin.py`)
  |     manual uploads, review queue, config helpers
  |
  +--> Query routes (`src/api/routes/query.py`)
  |     student lookup, reports, NL query, AI chat
  |
  +--> Agent routes (`src/api/routes/agent.py`)
  |     Phase 4 agent execution
  |
  +--> Health routes
  |
  +--> Static frontend (`src/frontend/`)
        plain HTML/CSS/JS served by FastAPI

Core dependencies
  |
  +--> PostgreSQL (`src/common/database.py`)      source of truth
  +--> Redis (`src/common/cache.py`)              dedup, checkpoints, pipeline state
  +--> MinIO/Supabase (`src/common/storage.py`)   raw emails and attachments
  +--> Celery (`src/common/celery_app.py`)        async workers and scheduled jobs
  +--> LLM providers (`src/common/config.py`)     Groq/OpenAI/Gemini
  +--> Embeddings / Elasticsearch                 optional search/index layers
```

## 3. Main Code Modules

### API and frontend

- `src/api/app.py`
  - creates the FastAPI app
  - initializes DB, storage, Redis, Sentry, and OpenTelemetry on startup
  - mounts routers and serves the frontend
- `src/frontend/index.html`
  - single-page UI shell
- `src/frontend/static/js/app.js`
  - dashboard logic, sync triggers, pipeline actions, lookup, and chat

### Shared infrastructure

- `src/common/config.py`
  - loads all environment-driven settings
  - centralizes DB, Redis, storage, LLM, SMTP, webhook, and document-AI config
- `src/common/database.py`
  - psycopg2-based data access layer
  - holds source-of-truth filters and most persistence logic
- `src/common/cache.py`
  - Redis wrapper for dedup, checkpoints, and pipeline state
- `src/common/storage.py`
  - unified object storage abstraction
  - supports MinIO as active default, Supabase as optional backend
- `src/common/celery_app.py`
  - Celery broker/backend configuration and periodic tasks

### Processing pipeline

- `src/api/routes/pipeline.py`
  - operational center of the email extraction flow
  - queues a Celery task when workers are available
  - falls back to synchronous execution if queue execution is unavailable
- `src/phase2_document_intelligence/`
  - converts attachments into normalized parsed text/tables
- `src/phase3_extraction_engine/`
  - combines regex and LLM extraction strategies
  - validates and corrects extracted records
  - routes low-confidence results into review
- `src/tasks/`
  - Celery entry points for ingestion, extraction, indexing, IMAP sync, and pipeline batch runs

### Query and agentic layers

- `src/api/routes/query.py`
  - exposes student lookup, report export, email delivery, NL query, and chat
- `src/phase5_query_engine/`
  - intent parsing, SQL generation, and aggregation helpers
- `src/phase4_agentic_layer/`
  - planner/executor/critic/tool registry for agent runs
  - callable today via `/api/v1/agent/run`, but not the mainline ingestion path

## 4. Data Stores and Their Roles

### PostgreSQL

Primary system of record for:

- students
- subjects
- student results
- semester aggregates
- email metadata
- attachments
- extraction audit records
- review queue and agent traces
- institution configuration and operational audit data

Schema sources:

- `schemas/001_core_schema.sql`
- `schemas/002_app_partitions.sql`

### Redis

Used for fast operational state:

- SHA-256 and similarity dedup
- sync checkpoints
- pipeline state cache
- queue support for Celery
- review-queue and transient orchestration helpers

### Object storage

Used for raw artifacts:

- raw synced emails as JSON
- downloaded attachments

Backends:

- active default: MinIO
- optional: Supabase Storage

### External providers

- Gmail OAuth + Gmail API for mailbox access
- IMAP for alternate inbox sync
- SMTP for report delivery
- Groq/OpenAI/Gemini for extraction and answer synthesis
- optional AWS SES flags exist in config but are not the default runtime path

## 5. End-to-End Operational Flows

### A. Gmail sync flow

```text
User connects Gmail
  -> `/api/v1/auth/login`
  -> Google OAuth callback
  -> credentials stored under `config/secrets/`

User triggers sync
  -> `/api/v1/sync`
  -> Gmail messages fetched and normalized
  -> emails cached under `data/emails_cache.json`
  -> sync state updated
  -> inbox exposed through `/api/v1/sync/emails` and `/api/v1/sync/threads`
```

### B. Email processing pipeline

```text
Cached emails
  -> `/api/v1/pipeline/run`
  -> Celery task `run_pipeline_batch` if available
  -> else synchronous `_run_pipeline_sync()`

Per email:
  1. Dedup using Redis
  2. Persist email metadata
  3. Classify as result email or other
  4. Store raw email in object storage
  5. Fetch and parse attachments if present
  6. Extract records from body + attachment text
  7. Merge regex and LLM outputs
  8. Validate/correct extracted records
  9. Save extraction audit row
  10. Route low-confidence output to review queue
  11. Upsert students/results/semester aggregates
  12. Update pipeline state and optional embeddings
```

### C. Manual admin upload flow

```text
User uploads PDF / Excel / CSV / DOCX / ZIP
  -> `/api/v1/admin/upload`
  -> parser selected by file type
  -> records parsed from file contents
  -> students/results/aggregates upserted
  -> upload audit saved
```

This path is especially important for structured Excel grade reports and VTU-style result sheets.

### D. Query and reporting flow

```text
Student lookup
  -> `/api/v1/student/{usn}`
  -> DB fetch of student, results, aggregates

Search
  -> `/api/v1/students?q=...`
  -> DB search using filtered visible records

Report export
  -> `/api/v1/student/{usn}/report?format=pdf|xlsx|docx`
  -> report built in-memory
  -> streamed to caller

Email report
  -> `/api/v1/student/{usn}/email-report`
  -> report generated
  -> sent through SMTP
```

### E. AI query and chat flow

```text
User asks question
  -> `/api/v1/query` or `/api/v1/chat`
  -> local intent parsing / DB context fetch
  -> optional SQL generation / aggregation helpers
  -> LLM synthesizes grounded response
  -> reply optionally verified against DB-derived values
```

## 6. Document Intelligence Pipeline

Phase 2 normalizes many input formats into a parsed-document representation.

Core files:

- `src/phase2_document_intelligence/universal_converter.py`
- `src/phase2_document_intelligence/router.py`
- `src/phase2_document_intelligence/pdf_parser.py`
- `src/phase2_document_intelligence/ocr_pipeline.py`
- `src/phase2_document_intelligence/excel_parser.py`
- `src/phase2_document_intelligence/docx_odf_parser.py`
- `src/phase2_document_intelligence/html_parser.py`

Current parser routing supports:

- PDF with text layers
- scanned PDFs and images via OCR
- Excel and CSV
- DOCX, ODT, and RTF
- HTML and email body content

Optional richer integrations exist in config and code:

- YOLO-driven table detection
- LlamaParse for digital PDFs

These are present as capabilities but are not the guaranteed default path for every pipeline run.

## 7. Extraction and Validation Architecture

Phase 3 combines multiple strategies rather than trusting one extractor.

Primary flow:

```text
normalized text
  -> regex/body extractors
  -> LLM extractor
  -> strategy merger with field-level voting
  -> validator and auto-correction
  -> confidence scoring
  -> low-confidence review queue or direct persistence
```

Important files:

- `src/phase3_extraction_engine/strategy_merger.py`
- `src/phase3_extraction_engine/llm_extractor.py`
- `src/phase3_extraction_engine/enhanced_llm_extractor.py`
- `src/phase3_extraction_engine/validator.py`
- `src/phase3_extraction_engine/review_queue.py`
- `src/phase3_extraction_engine/universal_extractor.py`

Validation checks include:

- USN format
- semester ranges
- marks bounds
- SGPA/CGPA ranges
- status consistency
- retry or correction for invalid outputs

## 8. Queue and Background Processing

Celery is configured with these queues:

- `email_ingestion`
- `extraction`
- `indexing`
- `notifications`

Defined workers/tasks:

- `src/tasks/pipeline_runner.py`
- `src/tasks/ingestion.py`
- `src/tasks/extraction.py`
- `src/tasks/indexing.py`
- `src/tasks/imap_sync.py`

Scheduled jobs include:

- periodic Gmail sync
- periodic IMAP sync
- nightly embedding rebuild
- hourly Elasticsearch refresh

Important architectural note:

- the app is queue-capable
- the `/api/v1/pipeline/run` route prefers queue-first execution
- if Celery is unavailable, the route falls back to in-process synchronous execution

## 9. Visibility and Source-of-Truth Rules

User-visible academic data is intentionally filtered.

Visible sources:

- `pipeline`
- `upload`

Excluded from normal product views:

- `seed`
- rows with missing or non-visible source metadata

This rule is enforced centrally in the database layer so dashboard counts, search, lookup, and AI responses stay aligned.

## 10. Deployment Architecture

### Local development

Typical dev stack:

- FastAPI app running locally via Uvicorn
- infrastructure via `docker/docker-compose.infra.yml`
- PostgreSQL, Redis, and MinIO running in Docker

### Containerized stack

`docker/docker-compose.yml` defines a fuller stack including:

- API
- Celery worker
- Celery beat
- PostgreSQL
- Redis
- MinIO
- Elasticsearch
- Prometheus
- Grafana

### Kubernetes and monitoring

Repo support exists for:

- `infra/k8s/deployment.yaml`
- `infra/monitoring/prometheus.yml`
- Grafana dashboard assets under `infra/monitoring/grafana/`

## 11. Security and Observability

### Security-related components

- CORS configuration in `src/api/app.py`
- Gmail OAuth token flow in `src/api/routes/auth.py`
- webhook secret support in config
- encryption and JWT config placeholders in `src/common/config.py`

Current limitation:

- there is no full end-user auth/authorization layer protecting the app beyond the Gmail OAuth integration for mailbox access

### Observability

The app includes:

- structured logging
- readiness and liveness endpoints
- Prometheus metrics endpoint
- optional Sentry
- OpenTelemetry instrumentation hooks

## 12. Current Architecture Status

### Fully working today

- FastAPI app and frontend
- Gmail OAuth flow
- Gmail sync routes and inbox cache
- IMAP task entry points
- pipeline orchestration
- PostgreSQL persistence
- Redis dedup and state
- MinIO-backed raw storage
- manual upload processing
- student lookup, reports, and chat
- review queue and admin approval endpoints
- agent endpoint with planner/executor/critic flow

### Present but conditional or partial

- Celery depends on workers actually running
- queue-first orchestration falls back to sync if workers are unavailable
- Supabase storage/database paths are optional, not active defaults
- YOLO and LlamaParse are configurable but not the default guaranteed path
- Elasticsearch and embedding flows exist but are secondary to core DB queries
- multi-tenant schema exists, but the product effectively operates as a single-institution deployment today

## 13. Recommended Mental Model

The easiest way to think about this project is:

1. FastAPI is the control plane and user-facing API.
2. PostgreSQL is the canonical academic record store.
3. Redis is the operational memory for dedup, checkpoints, queueing, and state.
4. MinIO stores the raw evidence that the pipeline processed.
5. Phase 2 converts documents into text and tables.
6. Phase 3 turns that text into validated student-result records.
7. Query/report/chat routes sit on top of the stored structured data.
8. Celery and the agent layer extend the system, but the core business value already comes from the API plus pipeline plus database path.

## 14. Key Files to Read First

If you are onboarding to the codebase, start here in order:

1. `src/api/app.py`
2. `src/api/routes/pipeline.py`
3. `src/common/database.py`
4. `src/api/routes/admin.py`
5. `src/api/routes/sync.py`
6. `src/api/routes/query.py`
7. `src/phase2_document_intelligence/universal_converter.py`
8. `src/phase3_extraction_engine/strategy_merger.py`
9. `src/phase3_extraction_engine/validator.py`
10. `src/common/celery_app.py`
