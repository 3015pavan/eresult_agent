# EMAIL_AGENT

Academic result ingestion and query platform built with FastAPI, PostgreSQL, Redis, MinIO, and a vanilla HTML/JS frontend.

It supports:

- Gmail OAuth and IMAP-based email sync
- email and attachment processing pipelines
- manual admin uploads for result sheets
- structured student/result storage in PostgreSQL
- student lookup, report export, and AI-assisted query endpoints

## Quick Start

### 1. Create the virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-api.txt
pip install -e ".[dev]"
```

### 2. Start local infrastructure

```powershell
docker compose -f docker/docker-compose.infra.yml up -d
```

### 3. Apply the database schema

```powershell
psql -h localhost -p 5434 -U acadextract -d acadextract -f schemas/001_core_schema.sql
psql -h localhost -p 5434 -U acadextract -d acadextract -f schemas/002_app_partitions.sql
```

Adjust host, port, user, and password to match your `.env`.

### 4. Configure secrets

Core values are loaded from `.env`.

For Gmail OAuth, place Google credentials at:

```text
config/secrets/credentials.json
```

### 5. Run the API

```powershell
.\.venv\Scripts\python.exe -m uvicorn src.api.app:app --host 127.0.0.1 --port 8002 --reload
```

Open:

```text
http://127.0.0.1:8002
```

## Testing

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Main Endpoints

- `GET /healthz`
- `GET /readyz`
- `POST /api/v1/sync`
- `GET /api/v1/sync/emails`
- `POST /api/v1/pipeline/run`
- `GET /api/v1/pipeline/status`
- `POST /api/v1/admin/upload`
- `GET /api/v1/student/{usn}`
- `POST /api/v1/query`
- `POST /api/v1/chat`

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Architecture Diagram](docs/ARCHITECTURE_DIAGRAM.md)
- [Implementation Roadmap](docs/IMPLEMENTATION_ROADMAP.md)
- [Workflow](docs/WORKFLOW.md)
- [Target Workflow Audit](docs/TARGET_WORKFLOW_AUDIT.md)

## Notes

- The frontend is plain HTML/CSS/JS, not React or Next.js.
- PostgreSQL is the source of truth for academic records.
- Redis is used for dedup, queue support, and pipeline state.
- MinIO is the active default object storage backend.
- Sensitive control-plane routes can be protected by setting `APP_API_KEY` in `.env`.
