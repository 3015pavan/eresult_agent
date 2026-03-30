# EMAIL_AGENT Implementation Roadmap

This roadmap turns the current architecture into a practical execution plan. It is ordered to improve correctness, reliability, and product readiness before deeper platform expansion.

## Phase 0: Stabilize the Current Baseline

Goal: make the existing system predictable and easy to run.

1. Standardize local setup and startup scripts.
2. Verify `.env` expectations against `src/common/config.py`.
3. Clean up `README.md` so setup, architecture, and workflow docs do not overlap.
4. Confirm schema bootstrap works end-to-end with `schemas/001_core_schema.sql` and `schemas/002_app_partitions.sql`.
5. Add a single documented happy-path smoke test:
   sync -> pipeline -> student lookup -> report export.

Definition of done:

- a new developer can boot infra, start the API, and run one successful ingestion flow without tribal knowledge

## Phase 1: Make Ingestion Production-Safe

Goal: harden the inbound email and upload edges.

1. Move all ingestion paths to a shared normalization contract for email bodies, metadata, and attachments.
2. Ensure webhook, Gmail, and IMAP paths all write consistent `email_metadata` and pipeline events.
3. Add stronger attachment validation:
   content type, file size, parse status, and storage-path auditing.
4. Centralize duplicate detection policy so sync and pipeline use the same exact rules.
5. Add retry-safe idempotency for repeated sync runs and repeated pipeline runs.

Definition of done:

- repeated ingestion does not create duplicate academic records
- every ingested artifact is traceable from source email to DB rows

## Phase 2: Finish Queue-First Processing

Goal: make Celery the normal runtime, not just a fallback option.

1. Treat `/api/v1/pipeline/run` as a queue-submission endpoint only.
2. Ensure `src/tasks/pipeline_runner.py` owns batch execution and status reporting.
3. Add task-level progress updates into Redis and `pipeline_events`.
4. Expose worker/task health clearly in `/pipeline/status`.
5. Add worker startup docs and a compose profile that launches API + worker + beat cleanly.
6. Add retry and dead-letter behavior for extraction failures.

Definition of done:

- large inboxes are processed asynchronously without blocking API requests
- task state is visible from the UI and API

## Phase 3: Strengthen Document Intelligence

Goal: improve parser reliability and make parser selection explicit.

1. Introduce an explicit parse-decision layer:
   digital PDF, scanned PDF, image, spreadsheet, HTML, text email.
2. Record parser choice, confidence, and fallback chain on every attachment.
3. Add parser-level tests for:
   PDF text, OCR images, VTU Excel, CSV, DOCX, and mixed ZIP uploads.
4. Wire optional features behind flags cleanly:
   YOLO, LlamaParse, richer OCR paths.
5. Normalize the `ParsedDocument` contract across all parsers.

Definition of done:

- parser behavior is inspectable and reproducible
- fallback paths are visible instead of implicit

## Phase 4: Improve Extraction Quality and Review Workflow

Goal: reduce bad data persistence and make review usable.

1. Consolidate extraction strategy outputs into one typed record schema.
2. Separate extraction confidence from validation confidence.
3. Expand validator coverage for totals, pass/fail consistency, SGPA/CGPA, and semester conflicts.
4. Add extraction snapshots so reviewers can compare raw source, parsed text, and final structured output.
5. Improve review queue endpoints with approve, reject, edit, and replay semantics.
6. Add metrics for:
   extraction success rate, review rate, auto-correction rate, and failure causes.

Definition of done:

- low-confidence records are routed safely
- reviewer actions are auditable and reversible

## Phase 5: Tighten Database and Query Guarantees

Goal: ensure product-visible answers always come from trusted stored data.

1. Review all source-visibility filters in `src/common/database.py`.
2. Add constraints and indexes for high-frequency lookups:
   USN, semester, institution, message_id, attachment hash.
3. Expand query-layer tests for search, student summary, trend, and report export.
4. Ensure chat and NL query endpoints always cite DB-derived facts before LLM synthesis.
5. Add audit logging for user queries and generated reports.

Definition of done:

- lookup, dashboard, and chat answers remain consistent across routes
- DB integrity guards catch malformed writes earlier

## Phase 6: Productize Reporting and Notifications

Goal: make outputs polished and operationally useful.

1. Move report rendering toward one canonical report view model.
2. Align PDF, XLSX, and DOCX exports around the same computed metrics.
3. Improve SMTP delivery tracking and error reporting.
4. Add optional HTML-template-based PDF rendering if exact formatting matters.
5. Add notification templates for:
   report sent, pipeline complete, review required, sync failed.

Definition of done:

- all report formats agree on the same student metrics
- outbound delivery is observable and debuggable

## Phase 7: Secure the App Properly

Goal: close the biggest operational risk in the current architecture.

1. Add real user authentication for app access.
2. Introduce authorization by role for admin, operator, and read-only users.
3. Protect admin and pipeline endpoints first.
4. Move secrets handling to a stricter operational model.
5. Add audit logs for sensitive actions:
   upload, approve review, rerun pipeline, config changes, report email.

Definition of done:

- the app is no longer effectively open to anyone who can reach the server

## Phase 8: Expand Search, Agent, and Multi-Tenant Capabilities

Goal: build the advanced platform features after the core is reliable.

1. Finish embedding refresh and semantic search UX.
2. Decide whether Elasticsearch is required or optional long-term.
3. Promote the Phase 4 agent from experimental to operational only after tool contracts are stable.
4. Replace single-institution assumptions with institution-aware routing and auth scoping.
5. Add tenant-aware dashboards, configs, and storage prefixes.

Definition of done:

- advanced features sit on top of a stable, observable core

## Testing Roadmap

Testing should grow with the phases, not after them.

1. Add smoke tests for the main user journeys.
2. Add parser fixture tests for representative documents.
3. Add extraction regression tests for known tricky emails and attachments.
4. Add API tests for sync, pipeline, admin review, lookup, report, and chat.
5. Add worker integration tests for queue-first execution.

## Suggested Execution Order

If we want the shortest path to a stronger product, do the work in this order:

1. Phase 0
2. Phase 1
3. Phase 2
4. Phase 4
5. Phase 3
6. Phase 5
7. Phase 6
8. Phase 7
9. Phase 8

That order prioritizes correctness and operations before deeper feature expansion.
