# Target Workflow Audit

This file captures the requested end-to-end workflow and the current repository alignment after the audit pass.

## Requested Workflow

The system starts from scratch with an input ingestion layer where documents enter either through a frontend upload or an incoming email connection configured using SMTP/IMAP. When an email arrives, the system automatically downloads attachments and stores them in MinIO or Supabase Storage, ensuring low-cost and scalable storage. Immediately after ingestion, a lightweight classification step using rules or an LLM (GPT/Gemini) checks whether the document is a valid result/marksheet to avoid unnecessary processing. Once validated, the document is pushed into an asynchronous queue system using Redis and Celery, which acts as the backbone of the system, enabling scalable and parallel processing of multiple documents.

A background worker (AI agent pipeline) consumes tasks from the queue and begins the document processing stage. First, YOLOv8 detects table regions in the document to isolate structured data. Then, PaddleOCR extracts text from detected regions for scanned documents, while LlamaParse is used in parallel for digital PDFs to directly extract structured content with higher accuracy. The extracted raw data is then passed to the core intelligence layer, where a Large Language Model (GPT/Gemini) converts noisy, unstructured text into a clean and standardized JSON format containing all required fields such as student details, subject marks, totals, and results.

Next, the system enters a validation and correction phase, which is critical for reliability. Rule-based checks verify numerical correctness (e.g., sum of marks equals total, no missing values), while optional LLM reasoning detects logical inconsistencies. If validation fails, a retry mechanism is triggered automatically, sending the document back through the processing pipeline until acceptable accuracy is achieved or a retry limit is reached. Alongside this, a confidence scoring mechanism evaluates the quality of extraction based on OCR clarity, missing fields, and validation success, flagging low-confidence outputs for review.

Once the data is validated, the system proceeds to report generation, where structured data is transformed into a clean, professional report using HTML templates converted to PDF. This report may include computed insights such as percentages, grades, and an optional AI-generated summary. After generating the report, the system triggers the automated email response module, which composes a reply using an LLM for natural language formatting and sends the report as an attachment via SMTP or AWS SES, completing the automation loop without human intervention.

All processed results, metadata, and confidence scores are stored in a PostgreSQL database (via Supabase), while audit logs track every step of the pipeline, including ingestion time, processing status, retries, and email delivery status. A role-based dashboard (built with Next.js) allows users to upload documents, view generated reports, monitor processing status, and manage system operations, while admins can oversee logs and flagged low-confidence cases.

Overall, the system operates as a single intelligent AI agent pipeline, orchestrated through queues and modular stages, capable of autonomously ingesting documents, extracting and validating information, generating reports, and communicating results via email, making it a fully automated, production-ready document analysis solution from scratch to deployment.

## Audit Outcome

- Working in the repo today:
  - Frontend uploads
  - Gmail sync plus IMAP task entry points
  - Redis cache, Celery task modules, PostgreSQL persistence, MinIO storage
  - Regex plus LLM extraction voting, validation, review queue, query/chat, and report export
- Improved in this audit:
  - Celery extraction now writes email metadata, extraction audit rows, and explicit completion or review statuses
  - Email statuses now preserve `processed_no_records` and `queued_for_review`
  - Report and email outputs now include deterministic metrics based on stored marks: percentage, pass count, and best SGPA
- Still partial relative to the requested architecture:
  - The main `/pipeline/run` path is still synchronous even though Celery workers exist
  - The dashboard is currently vanilla HTML/JS rather than Next.js
  - YOLOv8 and LlamaParse are not wired as first-class runtime stages in the current code path
  - Supabase-specific database and storage paths are not the active defaults

## Recommended Next Steps

- Convert `/pipeline/run` into a queue-first orchestration endpoint
- Add attachment-level retry counters and richer pipeline audit logs
- Introduce explicit scanned-vs-digital parser routing with confidence thresholds
- Replace the current direct PDF builders with HTML-template-to-PDF rendering if exact workflow parity is required
