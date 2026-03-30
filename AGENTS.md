# Copilot Workspace Instructions for EMAIL_AGENT

## Purpose
This file provides essential conventions, build/test commands, and project-specific guidance for AI agents and developers working in the EMAIL_AGENT repository. It is designed to maximize automation, reduce onboarding time, and ensure consistent, production-grade contributions.

---

## Key Documentation
- **Project workflow:** See [docs/WORKFLOW.md](docs/WORKFLOW.md)
- **Audit and architecture notes:** See [docs/TARGET_WORKFLOW_AUDIT.md](docs/TARGET_WORKFLOW_AUDIT.md)
- **Database schema:** See [schemas/001_core_schema.sql](schemas/001_core_schema.sql) and [schemas/002_app_partitions.sql](schemas/002_app_partitions.sql)

---

## Build & Run
- **Create virtual environment:**
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  pip install -r requirements-api.txt
  ```
- **Install dev dependencies:**
  ```powershell
  pip install -e ".[dev]"
  ```
- **Start infrastructure:**
  ```powershell
  docker compose -f docker/docker-compose.infra.yml up -d
  ```
- **Run the API server:**
  ```powershell
  .venv\Scripts\python.exe -m uvicorn src.api.app:app --host 127.0.0.1 --port 8002 --reload
  ```
- **Run tests:**
  ```powershell
  pytest
  ```

---

## Project Structure
- **API:** `src/api/` (FastAPI app, routes)
- **Core logic:** `src/common/`, `src/phase2_document_intelligence/`, `src/phase3_extraction_engine/`
- **Agentic layer (future):** `src/phase4_agentic_layer/`
- **Query engine:** `src/phase5_query_engine/`
- **Tasks:** `src/tasks/` (Celery tasks)
- **Frontend:** `src/frontend/` (Vanilla JS/HTML/CSS)
- **Schemas:** `schemas/` (PostgreSQL)
- **Docker:** `docker/` (compose, Dockerfile)
- **Config:** `config/` (YAML, secrets)

---

## Conventions & Pitfalls
- **Pipeline orchestration:** Main logic in `src/api/routes/pipeline.py`.
- **Database access:** Use `src/common/database.py` for queries and filters.
- **Visibility rules:** Only `pipeline` and `upload` sources are user-visible.
- **Celery:** Task modules exist, but Celery is not always running; pipeline is synchronous by default.
- **Frontend:** No React/Next.js; plain HTML/JS/CSS only.
- **Secrets:** Never commit `.env` or files in `config/secrets/`.
- **Gmail OAuth:** Place credentials in `config/secrets/credentials.json`.

---

## API Endpoints
- See [README.md](README.md) for a full list of endpoints and their descriptions.

---

## Known Limitations
- Single institution only (multi-tenant schema, but not enforced)
- Synchronous pipeline (Celery not default)
- No user authentication beyond Gmail OAuth
- LLM extraction cost per email
- pgvector search not active

---

## Example Prompts
- "How do I run the extraction pipeline on new emails?"
- "Where is the main logic for document ingestion?"
- "How do I add a new admin upload endpoint?"
- "What are the required environment variables?"

---

## Next Steps / Customizations
- Consider agent instructions for:
  - `src/phase4_agentic_layer/` (agentic logic, not wired)
  - `src/frontend/` (UI/UX conventions)
  - `src/tasks/` (Celery task orchestration)
- For complex workflows, use `applyTo` patterns to scope instructions to relevant directories.

---

Link, don't embed: Always reference docs/WORKFLOW.md and docs/TARGET_WORKFLOW_AUDIT.md for workflow/architecture details instead of duplicating content here.
