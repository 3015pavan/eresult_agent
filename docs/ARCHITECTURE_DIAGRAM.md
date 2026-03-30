# EMAIL_AGENT Visual Architecture

This visual diagram matches the current working runtime in the repository.

## System Diagram

```mermaid
flowchart TB
    UI[Browser UI<br/>src/frontend] --> API[FastAPI App<br/>src/api/app.py]

    subgraph API_ROUTES[API Surface]
        AUTH[Auth Routes<br/>auth.py]
        SYNC[Sync Routes<br/>sync.py]
        PIPE[Pipeline Routes<br/>pipeline.py]
        ADMIN[Admin Routes<br/>admin.py]
        QUERY[Query Routes<br/>query.py]
        AGENT[Agent Routes<br/>agent.py]
        HEALTH[Health Routes]
    end

    API --> AUTH
    API --> SYNC
    API --> PIPE
    API --> ADMIN
    API --> QUERY
    API --> AGENT
    API --> HEALTH

    GMAIL[Gmail OAuth + Gmail API]
    IMAP[IMAP Accounts]
    WEBHOOK[Webhook / SMTP Entry]
    MANUAL[Manual Uploads]

    GMAIL --> AUTH
    GMAIL --> SYNC
    IMAP --> SYNC
    WEBHOOK --> PIPE
    MANUAL --> ADMIN

    SYNC --> CACHEFILE[data/emails_cache.json]
    SYNC --> REDIS[(Redis<br/>dedup + state + broker)]

    PIPE --> CLASSIFY[Email Classification]
    CLASSIFY --> STOREMAIL[Store Raw Email]
    STOREMAIL --> OBJECT[(MinIO / Supabase Storage)]

    PIPE --> PARSE[Phase 2 Document Intelligence<br/>universal_converter + parsers]
    PARSE --> EXTRACT[Phase 3 Extraction Engine<br/>regex + LLM + voting]
    EXTRACT --> VALIDATE[Validation + Correction]
    VALIDATE --> REVIEW[Review Queue]
    VALIDATE --> DB[(PostgreSQL<br/>source of truth)]
    REVIEW --> DB

    ADMIN --> PARSE
    ADMIN --> DB

    QUERY --> DB
    QUERY --> REPORTS[PDF / XLSX / DOCX Reports]
    QUERY --> SMTP[SMTP Email Delivery]
    QUERY --> LLM[LLM Providers<br/>Groq / OpenAI / Gemini]

    AGENT --> TOOLS[Phase 4 Agent Tools]
    TOOLS --> SYNC
    TOOLS --> PARSE
    TOOLS --> EXTRACT
    TOOLS --> VALIDATE
    TOOLS --> DB
    AGENT --> DB

    PIPE --> CELERY[Celery Queue-First Execution]
    CELERY --> TASKS[Tasks<br/>pipeline_runner / ingestion / extraction / indexing / imap_sync]
    TASKS --> REDIS
    TASKS --> DB
    TASKS --> OBJECT

    DB --> EMBED[Embeddings / Search]
    EMBED --> ES[Elasticsearch<br/>optional]

    HEALTH --> OBS[Observability<br/>Prometheus / OTel / Sentry]
    API --> OBS
```

## Pipeline Sequence

```mermaid
sequenceDiagram
    participant User
    participant Sync as /api/v1/sync
    participant Cache as emails_cache.json
    participant Pipe as /api/v1/pipeline/run
    participant Redis as Redis
    participant Storage as Object Storage
    participant P2 as Phase 2 Parsers
    participant P3 as Phase 3 Extraction
    participant DB as PostgreSQL

    User->>Sync: Trigger inbox sync
    Sync->>Cache: Save normalized emails
    User->>Pipe: Run pipeline
    Pipe->>Redis: Check dedup + pipeline state
    Pipe->>DB: Save email metadata
    Pipe->>Storage: Save raw email JSON
    Pipe->>P2: Parse attachments and body
    P2-->>Pipe: Parsed text/tables
    Pipe->>P3: Extract + merge + validate
    P3-->>Pipe: Structured result records
    Pipe->>DB: Upsert students/results/aggregates
    Pipe->>Redis: Update seen hashes + pipeline state
    Pipe-->>User: Processing summary
```

## Notes

- `Pipeline` prefers Celery-backed execution and falls back to synchronous execution when workers are unavailable.
- `PostgreSQL` remains the canonical academic record store.
- `Redis` carries both operational state and queue support.
- `MinIO` is the active default object storage backend.
