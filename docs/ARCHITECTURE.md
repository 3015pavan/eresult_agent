# Autonomous Academic Result Extraction and Student Profiling from Email Streams

## Full System Architecture Blueprint — Production-Grade Implementation

---

## 1. FULL TEXT-BASED SYSTEM ARCHITECTURE DIAGRAM

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              EXTERNAL EMAIL SOURCES                                         │
│   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                │
│   │  Gmail API   │   │  IMAP/SMTP   │   │  MS Graph    │   │  Webhook     │                │
│   │  (OAuth2)    │   │  (TLS)       │   │  (Azure AD)  │   │  (Custom)    │                │
│   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘                │
│          │                  │                   │                  │                         │
└──────────┼──────────────────┼───────────────────┼──────────────────┼─────────────────────────┘
           │                  │                   │                  │
           ▼                  ▼                   ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1 — EMAIL INTELLIGENCE PIPELINE                               │
│                                                                                             │
│  ┌─────────────────┐   ┌─────────────────────┐   ┌──────────────────────┐                   │
│  │ Email Ingestion │──▶│ Dedup Engine         │──▶│ Classification       │                   │
│  │ Service         │   │ (SHA256 + SimHash +  │   │ Engine               │                   │
│  │                 │   │  MinHash LSH)        │   │ (DistilBERT finetuned│                   │
│  │ • Rate limiter  │   │                      │   │  on email corpus)    │                   │
│  │ • Backpressure  │   │ • Exact hash match   │   │                      │                   │
│  │ • Retry queue   │   │ • Near-dup (0.92)    │   │ Classes:             │                   │
│  │ • Health checks │   │ • Attachment hash     │   │  • result_email      │                   │
│  └────────┬────────┘   └─────────┬───────────┘   │  • spam              │                   │
│           │                      │                │  • administrative    │                   │
│           ▼                      ▼                │  • other            s │                   │
│  ┌─────────────────┐   ┌─────────────────────┐   │                      │                   │
│  │ Raw Email Store │   │ Thread Reconstructor │   │ Confidence: [0,1]    │                   │
│  │ (S3/MinIO +     │   │ (Message-ID /        │   │ Uncertainty: σ       │                   │
│  │  metadata in PG)│   │  In-Reply-To /       │   └──────────┬───────────┘                   │
│  └─────────────────┘   │  References headers) │              │                               │
│                        └─────────────────────┘              │                               │
│                                                              ▼                               │
│                                                 ┌──────────────────────┐                     │
│                                                 │ Attachment Extractor │                     │
│                                                 │ • MIME parser        │                     │
│                                                 │ • PDF / XLSX / CSV   │                     │
│                                                 │ • Virus scan (ClamAV)│                     │
│                                                 │ • Size gating        │                     │
│                                                 └──────────┬───────────┘                     │
│                                                            │                                 │
└────────────────────────────────────────────────────────────┼─────────────────────────────────┘
                                                             │
                          ┌──────────────────────────────────┼──────────────────┐
                          │                                  ▼                  │
┌─────────────────────────┼──────────────────────────────────────────────────────┼──────────────┐
│                         │  PHASE 2 — DOCUMENT INTELLIGENCE                    │              │
│                         │                                                     │              │
│  ┌──────────────────────▼──────────────────────────────────────────────┐       │              │
│  │                    Document Router                                  │       │              │
│  │  Input → file_type_detect() → is_scanned_pdf() → route_to_parser() │       │              │
│  └────┬──────────────────┬─────────────────────┬──────────────────────┘       │              │
│       │                  │                     │                              │              │
│       ▼                  ▼                     ▼                              │              │
│  ┌──────────┐     ┌─────────────┐      ┌───────────────┐                      │              │
│  │ Native   │     │ OCR Path    │      │ Excel/CSV     │                      │              │
│  │ PDF Path │     │             │      │ Parser        │                      │              │
│  │          │     │ ┌─────────┐ │      │               │                      │              │
│  │ pdfplumb │     │ │PaddleOCR│ │      │ openpyxl /    │                      │              │
│  │ + camelot│     │ │(primary)│ │      │ pandas        │                      │              │
│  │          │     │ └────┬────┘ │      │               │                      │              │
│  │ Table    │     │      │      │      │ Header detect │                      │              │
│  │ detect:  │     │      ▼      │      │ Schema map    │                      │              │
│  │ camelot  │     │ ┌─────────┐ │      │ Type coerce   │                      │              │
│  │ lattice  │     │ │Tesseract│ │      └───────┬───────┘                      │              │
│  │ + stream │     │ │(fallbck)│ │              │                              │              │
│  └────┬─────┘     │ └────┬────┘ │              │                              │              │
│       │           │      │      │              │                              │              │
│       │           │      ▼      │              │                              │              │
│       │           │ ┌─────────┐ │              │                              │              │
│       │           │ │LayoutLMv│ │              │                              │              │
│       │           │ │3 / Donut│ │              │                              │              │
│       │           │ │(VLM)    │ │              │                              │              │
│       │           │ └────┬────┘ │              │                              │              │
│       │           └──────┼──────┘              │                              │              │
│       │                  │                     │                              │              │
│       ▼                  ▼                     ▼                              │              │
│  ┌────────────────────────────────────────────────────────┐                    │              │
│  │              Unified Table Representation               │                    │              │
│  │  • Normalized cell grid                                 │                    │              │
│  │  • Header detection (heuristic + ML)                    │                    │              │
│  │  • Multi-page table stitching                           │                    │              │
│  │  • Confidence per cell                                  │                    │              │
│  └──────────────────────┬─────────────────────────────────┘                    │              │
│                         │                                                      │              │
└─────────────────────────┼──────────────────────────────────────────────────────┘              │
                          │                                                                    │
                          ▼                                                                    │
┌─────────────────────────────────────────────────────────────────────────────────────────────┐│
│                    PHASE 3 — INFORMATION EXTRACTION ENGINE                                   ││
│                                                                                             ││
│  ┌───────────────────────────────────────────────────────────────────────────────┐           ││
│  │                         Extraction Pipeline                                   │           ││
│  │                                                                               │           ││
│  │  ┌────────────────┐   ┌────────────────┐   ┌────────────────┐                 │           ││
│  │  │ Rule Engine    │   │ Regex Fallback │   │ LLM Structured │                 │           ││
│  │  │                │   │                │   │ Extraction     │                 │           ││
│  │  │ • Header map   │   │ • USN pattern  │   │                │                 │           ││
│  │  │ • Column index │   │ • Name pattern │   │ GPT-4o / Gemini│                 │           ││
│  │  │ • Known schema │   │ • Grade regex  │   │ with JSON mode │                 │           ││
│  │  │   matching     │   │ • GPA decimal  │   │ + Pydantic     │                 │           ││
│  │  └───────┬────────┘   └───────┬────────┘   │ schema enforce │                 │           ││
│  │          │ (primary)          │(secondary)  └───────┬────────┘                 │           ││
│  │          │                    │                     │(tertiary)                │           ││
│  │          ▼                    ▼                     ▼                          │           ││
│  │  ┌────────────────────────────────────────────────────────────────┐            │           ││
│  │  │                    Extraction Merger                           │            │           ││
│  │  │  • Voting across 3 strategies                                 │            │           ││
│  │  │  • Field-level confidence = max(rule, regex, llm)             │            │           ││
│  │  │  • Conflict resolution: rule > regex > llm for numbers        │            │           ││
│  │  └────────────────────────────┬───────────────────────────────────┘            │           ││
│  │                               │                                               │           ││
│  │                               ▼                                               │           ││
│  │  ┌────────────────────────────────────────────────────────────────┐            │           ││
│  │  │                Validation & Verification Layer                 │            │           ││
│  │  │                                                                │            │           ││
│  │  │  ✓ GPA ∈ [0, 10]                                              │            │           ││
│  │  │  ✓ Marks ∈ [0, 100]  (or [0, max_marks])                      │            │           ││
│  │  │  ✓ USN format regex match                                      │            │           ││
│  │  │  ✓ Subject code format                                         │            │           ││
│  │  │  ✓ Pass/Fail consistent with marks >= threshold                │            │           ││
│  │  │  ✓ SGPA consistent with individual subject grades              │            │           ││
│  │  │  ✓ Cross-record: same USN → same student name                  │            │           ││
│  │  │  ✓ Semester number monotonically increasing per student         │            │           ││
│  │  │                                                                │            │           ││
│  │  │  If validation fails → enter error correction loop (max 3)     │            │           ││
│  │  └────────────────────────────┬───────────────────────────────────┘            │           ││
│  │                               │                                               │           ││
│  └───────────────────────────────┼───────────────────────────────────────────────┘           ││
│                                  │                                                           ││
└──────────────────────────────────┼───────────────────────────────────────────────────────────┘│
                                   │                                                            │
                                   ▼                                                            │
┌──────────────────────────────────────────────────────────────────────────────────────────────┐│
│                    PHASE 4 — AGENTIC LLM ORCHESTRATION LAYER                                 ││
│                                                                                              ││
│  ┌──────────────────────────────────────────────────────────────────────────────────┐        ││
│  │                          Agent State Machine                                     │        ││
│  │                                                                                  │        ││
│  │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │        ││
│  │   │  IDLE    │───▶│ PLANNING │───▶│EXECUTING │───▶│VERIFYING │───▶│COMPLETED │  │        ││
│  │   └──────────┘    └─────┬────┘    └────┬─────┘    └────┬─────┘    └──────────┘  │        ││
│  │        ▲                │              │               │               │         │        ││
│  │        │                │              ▼               ▼               │         │        ││
│  │        │                │         ┌──────────┐   ┌──────────┐         │         │        ││
│  │        │                └────────▶│  ERROR   │◀──│ RETRYING │         │         │        ││
│  │        │                          └────┬─────┘   └──────────┘         │         │        ││
│  │        │                               │                              │         │        ││
│  │        └───────────────────────────────┴──────────────────────────────┘         │        ││
│  │                                                                                  │        ││
│  │  ┌──────────────────────────────────────────────────────────────────────┐        │        ││
│  │  │                       Tool Registry                                  │        │        ││
│  │  │                                                                      │        │        ││
│  │  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐    │        │        ││
│  │  │  │ email_fetch     │ │ pdf_parse        │ │ excel_parse         │    │        │        ││
│  │  │  │ email_classify  │ │ ocr_extract      │ │ db_query            │    │        │        ││
│  │  │  │ attachment_get  │ │ table_detect     │ │ student_upsert      │    │        │        ││
│  │  │  │ dedup_check     │ │ vlm_extract      │ │ profile_build       │    │        │        ││
│  │  │  │ thread_resolve  │ │ schema_validate  │ │ gpa_compute         │    │        │        ││
│  │  │  └─────────────────┘ └─────────────────┘ └─────────────────────┘    │        │        ││
│  │  └──────────────────────────────────────────────────────────────────────┘        │        ││
│  │                                                                                  │        ││
│  │  ┌─────────────────────────────────────────────────────┐                        │        ││
│  │  │              Memory System                           │                        │        ││
│  │  │                                                      │                        ││       ││
│  │  │  Short-term: Redis (current batch context)           │                        │        ││
│  │  │  Long-term:  PostgreSQL (processed email IDs,        │                        │        ││
│  │  │              student profiles, extraction history)    │                        │        ││
│  │  │  Episodic:   Vector DB (past extraction patterns)    │                        │        ││
│  │  └─────────────────────────────────────────────────────┘                        │        ││
│  └──────────────────────────────────────────────────────────────────────────────────┘        ││
│                                                                                              ││
└──────────────────────────────────┬───────────────────────────────────────────────────────────┘│
                                   │                                                            │
                                   ▼                                                            │
┌──────────────────────────────────────────────────────────────────────────────────────────────┐│
│                    PHASE 5 — TEACHER QUERY ENGINE                                            ││
│                                                                                              ││
│  ┌────────────────────────────────────────────────────────────────────────────────────┐      ││
│  │                                                                                    │      ││
│  │  Teacher NL Query ──▶ Intent Extractor ──▶ Entity Resolver ──▶ Query Planner       │      ││
│  │                        (LLM JSON mode)     (fuzzy USN/Name)    (SQL vs Vector)     │      ││
│  │                                                                                    │      ││
│  │       ┌───────────────────────────────┐  ┌───────────────────────────────┐         │      ││
│  │       │ SQL Generation Path           │  │ Vector Retrieval Path         │         │      ││
│  │       │                               │  │                               │         │      ││
│  │       │ • Parameterized only          │  │ • pgvector similarity search  │         │      ││
│  │       │ • Whitelist tables/columns    │  │ • Student profile embeddings  │         │      ││
│  │       │ • Read-only connection        │  │ • Reranking with cross-encoder│         │      ││
│  │       │ • sqlglot validation          │  │                               │         │      ││
│  │       │ • Row limit enforced          │  │                               │         │      ││
│  │       └──────────────┬────────────────┘  └──────────────┬────────────────┘         │      ││
│  │                      │                                  │                          │      ││
│  │                      ▼                                  ▼                          │      ││
│  │              ┌────────────────────────────────────────────────┐                    │      ││
│  │              │          Aggregation Engine                     │                    │      ││
│  │              │                                                 │                    │      ││
│  │              │  • Deterministic CGPA computation               │                    │      ││
│  │              │  • Backlog counting                             │                    │      ││
│  │              │  • Semester-wise aggregation                    │                    │      ││
│  │              │  • Percentile ranking                           │                    │      ││
│  │              │  • NO LLM arithmetic — all Python/SQL computed  │                    │      ││
│  │              └────────────────────┬───────────────────────────┘                    │      ││
│  │                                   │                                                │      ││
│  │                                   ▼                                                │      ││
│  │              ┌────────────────────────────────────────────────┐                    │      ││
│  │              │       RAG Answer Generator                      │                    │      ││
│  │              │                                                 │                    │      ││
│  │              │  • Grounded in retrieved data only              │                    │      ││
│  │              │  • Citations to source records                  │                    │      ││
│  │              │  • Confidence score in response                 │                    │      ││
│  │              │  • "I don't know" for insufficient data         │                    │      ││
│  │              └────────────────────────────────────────────────┘                    │      ││
│  │                                                                                    │      ││
│  └────────────────────────────────────────────────────────────────────────────────────┘      ││
│                                                                                              ││
└──────────────────────────────────────────────────────────────────────────────────────────────┘│
                                                                                                │
┌──────────────────────────────────────────────────────────────────────────────────────────────┐│
│                              DATA STORES                                                     ││
│                                                                                              ││
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   ││
│  │ PostgreSQL   │  │ Redis        │  │ MinIO / S3   │  │ pgvector     │  │ ElasticSearch│   ││
│  │              │  │              │  │              │  │              │  │              │   ││
│  │ • Students   │  │ • Job queues │  │ • Raw emails │  │ • Student    │  │ • Email full │   ││
│  │ • Results    │  │ • Rate limits│  │ • PDFs       │  │   profile    │  │   text index │   ││
│  │ • Subjects   │  │ • Agent state│  │ • Excel files│  │   embeddings │  │ • Attachment │   ││
│  │ • Extractions│  │ • Dedup cache│  │ • Extraction │  │ • Document   │  │   metadata   │   ││
│  │ • Audit logs │  │ • Session    │  │   artifacts  │  │   embeddings │  │              │   ││
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   ││
│                                                                                              ││
└──────────────────────────────────────────────────────────────────────────────────────────────┘│
                                                                                                │
┌──────────────────────────────────────────────────────────────────────────────────────────────┐│
│                         OBSERVABILITY & INFRASTRUCTURE                                       ││
│                                                                                              ││
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                     ││
│  │ Prometheus   │  │ Grafana      │  │ OpenTelemetry│  │ Sentry       │                     ││
│  │ + metrics    │  │ dashboards   │  │ traces       │  │ error track  │                     ││
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘                     ││
│                                                                                              ││
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                                       ││
│  │ Kubernetes   │  │ Celery /     │  │ Nginx /      │                                       ││
│  │ (EKS/GKE)   │  │ Dramatiq     │  │ Traefik LB   │                                       ││
│  └──────────────┘  └──────────────┘  └──────────────┘                                       ││
│                                                                                              ││
└──────────────────────────────────────────────────────────────────────────────────────────────┘│
```

---

## 2. DETAILED END-TO-END DATA FLOW

### 2.1 Email Ingestion Flow

```
1. Scheduler triggers every 60s (configurable)
2. EmailIngestionService authenticates via OAuth2 (Gmail) or IMAP credentials
3. Fetches new emails since last checkpoint (stored in Redis as `last_uid:<account_id>`)
4. For each email:
   a. Compute SHA256(message_id + from + date + subject) → exact dedup
   b. Compute SimHash(body_text) → near-duplicate detection (threshold: 0.92 Hamming similarity)
   c. If duplicate → log + skip
   d. Store raw email in S3/MinIO: `s3://emails/raw/{year}/{month}/{day}/{message_id}.eml`
   e. Store metadata in PostgreSQL: `email_metadata` table
   f. Publish to Celery task queue: `email.classify`
```

### 2.2 Classification Flow

```
1. Worker picks up `email.classify` task
2. Concatenate: subject + first 512 tokens of body
3. Pass through fine-tuned DistilBERT classifier:
   - Input: [CLS] subject [SEP] body_truncated [SEP]
   - Output: softmax over {result_email, spam, administrative, other}
   - Confidence = max(softmax)
   - Uncertainty = entropy of softmax distribution
4. If class == result_email AND confidence >= 0.85:
   → Publish to `email.extract_attachments`
5. If 0.60 <= confidence < 0.85:
   → Flag for human review queue
6. If confidence < 0.60 or class != result_email:
   → Archive, update metadata
```

### 2.3 Attachment Extraction Flow

```
1. Worker picks up `email.extract_attachments`
2. Parse MIME tree using Python `email` stdlib
3. For each attachment:
   a. Check file type (magic bytes, not extension)
   b. Reject if size > 50MB
   c. ClamAV virus scan
   d. Compute SHA256(file_bytes) → attachment dedup
   e. Store in S3: `s3://attachments/{email_id}/{filename}`
   f. Create `attachment_metadata` record
   g. Route to document parser:
      - .pdf → `document.parse_pdf`
      - .xlsx/.xls → `document.parse_excel`
      - .csv → `document.parse_csv`
```

### 2.4 Document Parsing Flow

```
PDF Path:
1. Attempt native text extraction via pdfplumber
2. If extracted_text_length / page_count < 100 chars → classified as scanned PDF
3. For native PDFs:
   a. Use camelot (lattice mode first, then stream mode) for table detection
   b. If camelot confidence < 0.5 → fallback to tabula-py
   c. Build cell grid with positional metadata
4. For scanned PDFs:
   a. Convert to images (pdf2image, 300 DPI)
   b. Deskew using OpenCV (Hough transform)
   c. Primary OCR: PaddleOCR (higher accuracy on structured documents)
   d. Fallback OCR: Tesseract 5 (if PaddleOCR confidence < 0.7)
   e. Pass OCR output + image to LayoutLMv3 for layout-aware extraction
   f. Alternative: Donut model for end-to-end extraction without OCR
5. For multi-page tables:
   a. Detect continuation headers (repeated column names)
   b. Stitch tables across pages using column alignment heuristics

Excel Path:
1. Load workbook via openpyxl (preserving formatting)
2. Detect header row: first row where ≥3 cells match known academic column names
3. Map detected columns to canonical schema
4. Handle merged cells by forward-fill
5. Extract data rows, apply type coercion (str→int for marks, str→float for GPA)
```

### 2.5 Information Extraction Flow

```
For each extracted table:

Strategy 1 — Rule-Based (Priority: Highest for structured data):
  1. Match column headers against known schema mappings
     (e.g., "Subject Code" → subject_code, "Marks Obtained" → marks, "SGPA" → sgpa)
  2. Extract data row by row using column index
  3. Confidence = 0.95 if header match is exact, 0.80 if fuzzy match (Levenshtein ≤ 2)

Strategy 2 — Regex Fallback (Priority: Medium):
  1. USN: r'[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}'
  2. GPA: r'\b\d{1,2}\.\d{1,2}\b' with constraint ≤ 10.0
  3. Marks: r'\b\d{1,3}\b' with constraint ≤ max_marks

Strategy 3 — LLM Structured Extraction (Priority: Lowest, used for ambiguous layouts):
  1. Construct prompt with table text + Pydantic schema
  2. Use GPT-4o with JSON mode OR Gemini 1.5 Pro
  3. Enforce schema via function calling / structured output
  4. Temperature = 0 (deterministic)

Merger:
  - For each field, collect outputs from all 3 strategies
  - If all agree → confidence = 0.98
  - If 2 agree → take majority, confidence = 0.85
  - If all disagree → take rule-based if available, else flag for review

Validation Loop (max 3 iterations):
  1. Check all constraints (GPA ≤ 10, marks ≤ max, USN format, etc.)
  2. Cross-field checks (marks < pass_threshold → status should be FAIL)
  3. Cross-record checks (same USN → same name)
  4. If violations found → re-extract with targeted prompt including violation context
  5. If still failing after 3 rounds → quarantine record for human review
```

### 2.6 Student Profile Building Flow

```
1. For each validated extraction:
   a. Resolve student identity: USN exact match → existing student, else create new
   b. Fuzzy name matching (Jaro-Winkler ≥ 0.92) for name variant resolution
   c. UPSERT into student_results table
   d. Recompute aggregates:
      - CGPA = Σ(SGPA_i × credits_i) / Σ(credits_i)
      - Total backlogs = count(status == 'FAIL')
      - Active backlogs = backlogs not yet cleared
   e. Generate embedding: text-embedding-3-small on student profile summary
   f. Store in pgvector for semantic retrieval
```

---

## 3. MODEL CHOICES AND JUSTIFICATION

### 3.1 Email Classification

| Component | Model | Justification |
|-----------|-------|---------------|
| Primary classifier | **DistilBERT** fine-tuned on academic email corpus | 6x faster than BERT-base with ~97% accuracy retention. 66M params fits on CPU for inference. Academic emails have distinct vocabulary (marks, semester, results) making fine-tuning highly effective. |
| Embedding for dedup | **all-MiniLM-L6-v2** (sentence-transformers) | 384-dim embeddings, 80ms/sentence on CPU. Perfect for SimHash-based near-duplicate detection. |
| Spam filter | **sklearn LinearSVC** ensemble with DistilBERT | Lightweight pre-filter reduces load on transformer model by 40%. |

**Latency**: DistilBERT inference ≈ 15ms/email on V100 GPU, 80ms on CPU. At 100K students × ~50 emails/student/year = 5M emails/year ≈ 14K emails/day. Single CPU worker handles this with headroom.

### 3.2 Document Understanding

| Component | Model | Justification |
|-----------|-------|---------------|
| OCR (primary) | **PaddleOCR v4** | State-of-art accuracy on structured documents (F1: 0.96 on FUNSD). Handles Hindi/English mixed text. Apache-2.0 license. |
| OCR (fallback) | **Tesseract 5** | Mature, widely deployed. Lower accuracy but higher robustness on degraded scans. |
| Layout understanding | **LayoutLMv3-base** | Multimodal (text + layout + image). Pre-trained on IIT-CDIP. Excels at table structure recognition. 133M params. |
| End-to-end extraction | **Donut** (Document Understanding Transformer) | OCR-free approach. Used when OCR pipelines produce noisy output on heavily degraded scans. |
| Table detection | **Table Transformer** (DETR-based) | Microsoft's DETR fine-tuned on PubTables-1M. 0.97 mAP on table detection, 0.93 on structure recognition. |

**Hybrid Strategy**:
```
if is_native_pdf(doc):
    tables = camelot.read_pdf(doc, flavor='lattice')
    if avg_confidence(tables) < 0.5:
        tables = camelot.read_pdf(doc, flavor='stream')
elif is_scanned_pdf(doc):
    if image_quality(doc) > 150_DPI:
        text = paddleocr.extract(doc)
        if confidence(text) > 0.7:
            structured = layoutlmv3.extract(text, images)
        else:
            structured = donut.extract(images)  # end-to-end fallback
    else:
        enhanced = opencv_enhance(doc)  # denoise, deskew, sharpen
        text = tesseract.extract(enhanced)
        structured = layoutlmv3.extract(text, images)
```

### 3.3 Information Extraction

| Component | Model | Justification |
|-----------|-------|---------------|
| Structured extraction | **GPT-4o** (via API) | Best JSON-mode accuracy. Function calling enforces Pydantic schema. Near-zero hallucination with constrained output. |
| Backup LLM | **Gemini 1.5 Pro** | Competitive accuracy, lower cost. 1M token context handles large transcripts. |
| Local fallback | **Mistral-7B-Instruct** (quantized) | For air-gapped deployments. GGUF Q4_K_M quantization, runs on single GPU. |
| Embeddings | **text-embedding-3-small** (OpenAI) | 1536-dim, $0.02/1M tokens. Best cost/performance for profile embeddings. |

### 3.4 Query Engine

| Component | Model | Justification |
|-----------|-------|---------------|
| Intent extraction | **GPT-4o-mini** | Fast, cheap ($0.15/1M input). Sufficient for intent classification. |
| SQL generation | **GPT-4o** | Highest accuracy on text-to-SQL benchmarks. Used with few-shot prompting + schema context. |
| Answer generation | **GPT-4o** | Grounded generation with citations. |
| Reranking | **cross-encoder/ms-marco-MiniLM-L-6-v2** | Fast reranking of vector search results. |

---

## 4. DATABASE SCHEMA DESIGN

```sql
-- =============================================================================
-- CORE ACADEMIC SCHEMA
-- =============================================================================

-- University/Institution multi-tenancy
CREATE TABLE institutions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    code            VARCHAR(20) UNIQUE NOT NULL,
    config          JSONB DEFAULT '{}',  -- institution-specific settings
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Department reference
CREATE TABLE departments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id),
    name            TEXT NOT NULL,
    code            VARCHAR(10) NOT NULL,
    UNIQUE(institution_id, code)
);

-- Student master record
CREATE TABLE students (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id),
    department_id   UUID REFERENCES departments(id),
    usn             VARCHAR(20) NOT NULL,      -- University Seat Number
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL,              -- lowercase, stripped
    email           TEXT,
    batch_year      SMALLINT,
    current_semester SMALLINT,
    cgpa            DECIMAL(4,2),              -- computed aggregate
    total_credits   INTEGER DEFAULT 0,
    total_backlogs  INTEGER DEFAULT 0,
    active_backlogs INTEGER DEFAULT 0,
    profile_embedding VECTOR(1536),            -- pgvector
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(institution_id, usn)
);

CREATE INDEX idx_students_usn ON students(usn);
CREATE INDEX idx_students_name_trgm ON students USING gin(name_normalized gin_trgm_ops);
CREATE INDEX idx_students_embedding ON students USING ivfflat(profile_embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_students_cgpa ON students(cgpa);
CREATE INDEX idx_students_institution ON students(institution_id);

-- Subject catalog
CREATE TABLE subjects (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id),
    code            VARCHAR(20) NOT NULL,
    name            TEXT NOT NULL,
    credits         SMALLINT NOT NULL DEFAULT 4,
    max_marks       SMALLINT NOT NULL DEFAULT 100,
    pass_marks      SMALLINT NOT NULL DEFAULT 35,
    semester        SMALLINT,
    department_id   UUID REFERENCES departments(id),
    metadata        JSONB DEFAULT '{}',
    UNIQUE(institution_id, code)
);

-- Individual result records
CREATE TABLE student_results (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    student_id      UUID NOT NULL REFERENCES students(id),
    subject_id      UUID NOT NULL REFERENCES subjects(id),
    semester        SMALLINT NOT NULL,
    academic_year   VARCHAR(10),               -- e.g., "2024-25"
    exam_type       VARCHAR(20) DEFAULT 'regular',  -- regular, supplementary, improvement
    internal_marks  SMALLINT,
    external_marks  SMALLINT,
    total_marks     SMALLINT NOT NULL,
    max_marks       SMALLINT NOT NULL DEFAULT 100,
    grade           VARCHAR(5),                -- A+, A, B+, B, C, P, F
    grade_points    DECIMAL(3,1),              -- 10, 9, 8, ...
    status          VARCHAR(10) NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ABSENT', 'WITHHELD')),
    attempt_number  SMALLINT DEFAULT 1,
    extraction_id   UUID REFERENCES extractions(id),
    confidence      DECIMAL(3,2) DEFAULT 1.0,  -- extraction confidence
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(student_id, subject_id, semester, exam_type, attempt_number)
);

CREATE INDEX idx_results_student ON student_results(student_id);
CREATE INDEX idx_results_semester ON student_results(student_id, semester);
CREATE INDEX idx_results_status ON student_results(status);

-- Semester-level aggregates (materialized for query speed)
CREATE TABLE semester_aggregates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    student_id      UUID NOT NULL REFERENCES students(id),
    semester        SMALLINT NOT NULL,
    academic_year   VARCHAR(10),
    sgpa            DECIMAL(4,2) NOT NULL,
    credits_earned  SMALLINT NOT NULL,
    credits_attempted SMALLINT NOT NULL,
    subjects_passed SMALLINT NOT NULL,
    subjects_failed SMALLINT NOT NULL,
    total_marks     INTEGER,
    percentage      DECIMAL(5,2),
    rank_in_class   INTEGER,
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(student_id, semester, academic_year)
);

-- =============================================================================
-- EMAIL & EXTRACTION PIPELINE SCHEMA
-- =============================================================================

-- Raw email metadata
CREATE TABLE email_metadata (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id      TEXT UNIQUE NOT NULL,      -- RFC 2822 Message-ID
    institution_id  UUID REFERENCES institutions(id),
    account_id      TEXT NOT NULL,             -- which email account
    from_address    TEXT NOT NULL,
    to_addresses    TEXT[] NOT NULL,
    subject         TEXT,
    received_at     TIMESTAMPTZ NOT NULL,
    body_hash       CHAR(64) NOT NULL,         -- SHA256 of body
    body_simhash    BIGINT,                    -- SimHash for near-dedup
    classification  VARCHAR(30),
    classification_confidence DECIMAL(3,2),
    classification_uncertainty DECIMAL(4,3),
    thread_id       TEXT,
    raw_storage_path TEXT NOT NULL,            -- S3 path
    status          VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed, quarantined
    retry_count     SMALLINT DEFAULT 0,
    error_message   TEXT,
    processed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_email_message_id ON email_metadata(message_id);
CREATE INDEX idx_email_body_hash ON email_metadata(body_hash);
CREATE INDEX idx_email_status ON email_metadata(status);
CREATE INDEX idx_email_received ON email_metadata(received_at);

-- Attachment metadata
CREATE TABLE attachments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email_id        UUID NOT NULL REFERENCES email_metadata(id),
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    file_size       BIGINT NOT NULL,
    file_hash       CHAR(64) NOT NULL,         -- SHA256
    storage_path    TEXT NOT NULL,              -- S3 path
    parse_status    VARCHAR(20) DEFAULT 'pending',
    document_type   VARCHAR(20),               -- pdf_native, pdf_scanned, excel, csv
    page_count      SMALLINT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_attachments_email ON attachments(email_id);
CREATE INDEX idx_attachments_hash ON attachments(file_hash);

-- Extraction jobs and results
CREATE TABLE extractions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attachment_id   UUID NOT NULL REFERENCES attachments(id),
    strategy        VARCHAR(20) NOT NULL,      -- rule_based, regex, llm, hybrid
    raw_output      JSONB NOT NULL,            -- raw extraction output
    validated_output JSONB,                    -- post-validation output
    records_extracted INTEGER DEFAULT 0,
    records_valid   INTEGER DEFAULT 0,
    records_quarantined INTEGER DEFAULT 0,
    overall_confidence DECIMAL(3,2),
    validation_errors JSONB DEFAULT '[]',
    extraction_model TEXT,                     -- which model was used
    extraction_time_ms INTEGER,
    llm_tokens_used INTEGER DEFAULT 0,
    retry_count     SMALLINT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_extractions_attachment ON extractions(attachment_id);
CREATE INDEX idx_extractions_status ON extractions(status);

-- =============================================================================
-- AGENT & OBSERVABILITY SCHEMA
-- =============================================================================

-- Agent execution trace
CREATE TABLE agent_traces (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL,             -- groups steps in one execution
    step_number     SMALLINT NOT NULL,
    state           VARCHAR(20) NOT NULL,      -- PLANNING, EXECUTING, VERIFYING, etc.
    tool_name       TEXT,
    tool_input      JSONB,
    tool_output     JSONB,
    reflection      TEXT,                      -- agent's self-assessment
    confidence      DECIMAL(3,2),
    duration_ms     INTEGER,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_traces_run ON agent_traces(run_id, step_number);

-- Audit log for queries
CREATE TABLE query_audit_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL,
    user_role       VARCHAR(30) NOT NULL,
    query_text      TEXT NOT NULL,
    intent          JSONB,
    generated_sql   TEXT,
    result_summary  TEXT,
    records_returned INTEGER,
    confidence      DECIMAL(3,2),
    response_time_ms INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- ACCESS CONTROL
-- =============================================================================

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id),
    email           TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    role            VARCHAR(30) NOT NULL CHECK (role IN ('admin', 'teacher', 'hod', 'principal', 'readonly')),
    department_id   UUID REFERENCES departments(id),
    password_hash   TEXT NOT NULL,
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Role-based data access: teachers see only their department's students
CREATE POLICY department_isolation ON students
    FOR SELECT
    USING (
        department_id IN (
            SELECT department_id FROM users WHERE id = current_setting('app.current_user_id')::UUID
        )
        OR EXISTS (
            SELECT 1 FROM users WHERE id = current_setting('app.current_user_id')::UUID
            AND role IN ('admin', 'principal')
        )
    );
```

---

## 5. FAILURE MODES AND MITIGATION STRATEGIES

### 5.1 Email Pipeline Failures

| Failure Mode | Detection | Mitigation | Recovery |
|-------------|-----------|------------|----------|
| Gmail API rate limit (250 quota units/sec) | HTTP 429 response | Exponential backoff: 1s, 2s, 4s, 8s, max 60s. Token bucket rate limiter. | Auto-retry with jitter. Alert if >5 consecutive failures. |
| IMAP connection drop | Socket timeout (30s) | Connection pooling with health checks every 30s. | Reconnect from last UID checkpoint. |
| OAuth2 token expiry | 401 response code | Proactive refresh at 80% of token lifetime. | Refresh token stored encrypted in Vault/KMS. |
| Malformed email (no Message-ID) | Validation on parse | Generate synthetic ID: SHA256(from + date + subject + body[:200]) | Log and continue. |
| Attachment virus detected | ClamAV positive scan | Quarantine file, alert admin, do not process. | Manual review queue. |
| Duplicate email storm (mailing list) | SimHash cluster detection | Rate-limit processing per sender. Dedup window: 24 hours. | Skip duplicates, process only canonical copy. |

### 5.2 Document Parsing Failures

| Failure Mode | Detection | Mitigation | Recovery |
|-------------|-----------|------------|----------|
| Password-protected PDF | pdfplumber exception | Attempt common institutional passwords (configurable). | Flag for manual decryption. |
| Corrupted PDF | PyPDF2 read error | Try alternative parsers: pikepdf → pymupdf → pdfminer | If all fail, quarantine with error context. |
| OCR accuracy < 0.5 | PaddleOCR confidence score | Switch to Tesseract with LSTM mode. Apply image enhancement. | If still failing, send to VLM (Donut) for end-to-end extraction. |
| Multi-page table split mid-row | Row count mismatch between pages | Detect partial rows: last row on page N has fewer columns than header. Stitch with first row of page N+1. | Heuristic stitching + LLM verification of merged row. |
| Rotated/skewed scan | OpenCV Hough transform angle > 0.5° | Auto-deskew before OCR. Apply affine transformation. | If deskew fails, try 0°, 90°, 180°, 270° rotations. Pick highest OCR confidence. |
| Excel with macros | xlrd/openpyxl warning | Open in read-only mode with macros disabled. | If macro-dependent, extract VBA, identify data ranges, extract directly. |
| Merged cells in Excel | openpyxl merged_cells attribute | Unmerge and forward-fill values. | Log merge regions for audit. |

### 5.3 Extraction Failures

| Failure Mode | Detection | Mitigation | Recovery |
|-------------|-----------|------------|----------|
| LLM hallucination (invented marks) | Cross-validation with source text | Constrain LLM output to exact strings found in source. Verify every number appears in original text. | Fall back to regex/rule extraction. Never trust LLM numbers unsupported by source. |
| Schema mismatch | Pydantic validation error | Try alternative schema mappings (5 common Indian university formats pre-loaded). | LLM-based schema inference with human confirmation for new formats. |
| GPA > 10 extracted | Constraint check | Flag as error. Re-extract from source cell. | If persistent, mark as decimal point error (e.g., 82 → 8.2). |
| USN format violation | Regex validation | Common corrections: O→0, l→1, whitespace removal. | Fuzzy match against known USN list (Levenshtein ≤ 2). |
| Name encoding issues (Unicode) | NFC normalization check | Apply NFKD normalization, strip diacritics for matching. | Store both normalized and original forms. |

### 5.4 Agent Failures

| Failure Mode | Detection | Mitigation | Recovery |
|-------------|-----------|------------|----------|
| Agent stuck in loop | Step count > max_steps (20) | Force transition to ERROR state. | Log trace, alert, manual intervention. |
| Tool call failure | Tool returns error status | Retry with exponential backoff (max 3 attempts). | Try alternative tool (e.g., OCR fallback). |
| LLM API timeout | 30s timeout | Circuit breaker pattern: open after 3 failures in 60s. | Queue task for later processing. Switch to backup LLM provider. |
| Memory overflow | Redis memory alert (>80%) | LRU eviction on short-term memory. | Archive old entries to PostgreSQL. |
| Confidence below threshold | Confidence < 0.5 after all retries | Route to human-in-the-loop queue. | Store partial results with confidence for review. |

### 5.5 Query Engine Failures

| Failure Mode | Detection | Mitigation | Recovery |
|-------------|-----------|------------|----------|
| SQL injection attempt | sqlglot parse tree analysis | Reject queries with DDL, DML, or system table access. Only SELECT allowed. | Log attempt, increment user risk score. |
| Ambiguous entity (multiple "Rahul") | Entity resolution returns >1 match | Return disambiguation prompt: "Did you mean Rahul K (1BM21CS089) or Rahul S (1BM21CS102)?" | Teacher selects correct entity. |
| Query timeout (>10s) | PostgreSQL statement_timeout | EXPLAIN ANALYZE pre-check. Reject full table scans. | Suggest more specific query. |
| No data found | Empty result set | Distinguish: student exists but no data vs student not found. | Clear error message with suggestion. |

---

## 6. EVALUATION METRICS

### 6.1 Email Classification

| Metric | Target | Measurement |
|--------|--------|-------------|
| Precision (result_email class) | ≥ 0.97 | Cross-validation on labeled corpus (n≥5000) |
| Recall (result_email class) | ≥ 0.95 | Must not miss result emails |
| F1-score (macro) | ≥ 0.94 | Balanced across all classes |
| Classification latency (p99) | ≤ 200ms | End-to-end per email on CPU |
| Duplicate detection precision | ≥ 0.99 | Near-zero false positive dedup |
| Duplicate detection recall | ≥ 0.95 | Catch most duplicates |

### 6.2 Document Parsing

| Metric | Target | Measurement |
|--------|--------|-------------|
| Table detection F1 | ≥ 0.95 | IoU ≥ 0.8 threshold on test set |
| Cell extraction accuracy | ≥ 0.93 | Exact match on cell text |
| OCR character accuracy | ≥ 0.97 | On 300 DPI scans |
| Multi-page table stitch accuracy | ≥ 0.90 | Correct row continuity |
| Parse throughput | ≥ 50 pages/min | Single worker with GPU |

### 6.3 Information Extraction

| Metric | Target | Measurement |
|--------|--------|-------------|
| USN extraction accuracy | ≥ 0.99 | Exact match |
| Name extraction accuracy | ≥ 0.97 | Fuzzy match (Jaro-Winkler ≥ 0.95) |
| Marks extraction accuracy | ≥ 0.98 | Exact numeric match |
| GPA extraction accuracy | ≥ 0.98 | Exact to 2 decimal places |
| Status extraction accuracy | ≥ 0.99 | Exact class match |
| End-to-end record accuracy | ≥ 0.95 | All fields correct for a record |
| Hallucination rate | ≤ 0.01 | Numbers not in source document |

### 6.4 Query Engine

| Metric | Target | Measurement |
|--------|--------|-------------|
| Intent classification accuracy | ≥ 0.95 | On 500+ labeled queries |
| Entity resolution accuracy | ≥ 0.97 | Correct student identification |
| SQL generation correctness | ≥ 0.92 | Execution produces correct result |
| Answer factual accuracy | 1.00 | All numbers from DB, never LLM-generated |
| Query latency (p95) | ≤ 3s | End-to-end including LLM calls |
| User satisfaction (CSAT) | ≥ 4.2/5 | Teacher survey |

### 6.5 System-Level

| Metric | Target | Measurement |
|--------|--------|-------------|
| End-to-end pipeline latency | ≤ 5 min | Email received → profile updated |
| System availability | ≥ 99.5% | Uptime over 30-day window |
| Data freshness | ≤ 15 min | Time from email to queryable data |
| Error rate (unrecoverable) | ≤ 2% | Emails requiring human intervention |
| Cost per email processed | ≤ $0.05 | Infrastructure + API costs |

---

## 7. SCALABILITY & DEPLOYMENT ARCHITECTURE

### 7.1 Deployment Topology

```
┌─────────────────────────────────────────────────────────┐
│                  Kubernetes Cluster (EKS/GKE)           │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │              Ingress (Nginx/Traefik)             │    │
│  │              + Rate Limiting + WAF               │    │
│  └──────────────────────┬──────────────────────────┘    │
│                         │                               │
│  ┌──────────────────────▼──────────────────────────┐    │
│  │              API Gateway (FastAPI)               │    │
│  │              Pods: 2-8 (HPA on CPU/RPS)         │    │
│  └──────────────────────┬──────────────────────────┘    │
│                         │                               │
│  ┌──────────┬───────────┼───────────┬──────────┐        │
│  ▼          ▼           ▼           ▼          ▼        │
│ ┌────┐   ┌────┐    ┌────────┐   ┌────┐   ┌────────┐    │
│ │Eml │   │Doc │    │Extract │   │Agnt│   │Query   │    │
│ │Wrkr│   │Wrkr│    │Worker  │   │Wrkr│   │Service │    │
│ │2-10│   │2-8 │    │2-6     │   │1-4 │   │2-6     │    │
│ │pods│   │pods│    │pods    │   │pods│   │pods    │    │
│ └────┘   └────┘    └────────┘   └────┘   └────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │         GPU Node Pool (for OCR/LayoutLM)         │    │
│  │         Spot instances: 1-4 T4/A10G              │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │         Monitoring Stack                          │    │
│  │         Prometheus + Grafana + OpenTelemetry      │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
└─────────────────────────────────────────────────────────┘

External Services:
  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │PostgreSQL│  │  Redis    │  │  MinIO   │  │OpenAI API│
  │(RDS/     │  │  Cluster  │  │  /S3     │  │/Azure    │
  │ managed) │  │  (3 nodes)│  │          │  │OpenAI    │
  └──────────┘  └──────────┘  └──────────┘  └──────────┘
```

### 7.2 Scaling Strategy

**Horizontal Scaling (per phase):**

| Component | Scaling Trigger | Min Pods | Max Pods | Resource Request |
|-----------|----------------|----------|----------|-----------------|
| Email Worker | Queue depth > 100 | 2 | 10 | 512Mi RAM, 0.5 CPU |
| Document Worker | Queue depth > 20 | 2 | 8 | 2Gi RAM, 1 CPU (+ GPU for OCR) |
| Extraction Worker | Queue depth > 30 | 2 | 6 | 1Gi RAM, 1 CPU |
| Agent Worker | Queue depth > 10 | 1 | 4 | 1Gi RAM, 0.5 CPU |
| Query Service | RPS > 50/pod | 2 | 6 | 512Mi RAM, 0.5 CPU |

**Database Scaling:**
- PostgreSQL: Read replicas (2-4) for query engine. Primary for writes.
- Connection pooling via PgBouncer (max 200 connections).
- Partitioning `student_results` by institution_id for multi-tenant isolation.
- `email_metadata` partitioned by received_at (monthly).

**Queue Scaling:**
- Celery with Redis broker. Separate queues per phase with priority levels.
- Dead letter queue for failed tasks (max 3 retries with exponential backoff).

### 7.3 Multi-University Deployment

```
University A ─── Account Config ─── ┐
University B ─── Account Config ─── ├──▶ Shared Infrastructure
University C ─── Account Config ─── ┘      (tenant-isolated at data layer)

Tenant isolation:
  - Row-level security (RLS) on all tables via institution_id
  - Separate S3 prefixes per institution
  - Separate Redis keyspaces per institution
  - API authentication scoped to institution
```

### 7.4 Cost Estimation (100K students, 5M emails/year)

| Component | Monthly Cost (USD) |
|-----------|-------------------|
| Kubernetes (3 nodes + 2 GPU spot) | $600-900 |
| PostgreSQL RDS (db.r6g.large + 1 replica) | $300-400 |
| Redis (cache.r6g.large) | $150-200 |
| S3/MinIO storage (500GB) | $12-15 |
| OpenAI API (GPT-4o: ~2M tokens/month) | $40-60 |
| OpenAI Embeddings (~10M tokens/month) | $2-5 |
| Monitoring (Grafana Cloud free tier) | $0 |
| **Total** | **$1,100-1,580/month** |

---

## 8. SECURITY & PRIVACY CONSIDERATIONS

### 8.1 Data Protection

| Layer | Mechanism |
|-------|-----------|
| Transport | TLS 1.3 everywhere. mTLS between microservices. |
| Storage | AES-256 encryption at rest (S3 SSE, RDS encryption). |
| Credentials | HashiCorp Vault / AWS KMS for OAuth tokens, API keys. Never in code/config. |
| PII | Student names, USNs are PII. Column-level encryption for email addresses. |
| Anonymization | Query audit logs anonymize student identifiers in dev/staging environments. |
| Data retention | Raw emails: 2 years. Extracted data: indefinite. Configurable per institution. |

### 8.2 Access Control

```
Role Hierarchy:
  admin ─────────── Full system access, all institutions
    │
  principal ─────── Read all departments within their institution
    │
  hod ───────────── Read all students in their department
    │
  teacher ────────── Read students in their assigned sections/subjects
    │
  readonly ──────── Read-only access to aggregate dashboards
```

- **Authentication**: OAuth2 / OIDC (integration with university SSO).
- **Authorization**: RBAC + ABAC (Attribute-Based Access Control).
- **API Security**: JWT tokens, 15-min expiry, refresh token rotation.
- **SQL Injection Prevention**: Parameterized queries only. sqlglot AST validation before execution.
- **Rate Limiting**: 100 queries/min/user. 1000/min/institution.

### 8.3 Compliance

| Framework | How Addressed |
|-----------|---------------|
| FERPA (US) | Role-based access. Audit logging of all student data access. Data minimization. |
| GDPR (EU) | Right to erasure support. Data processing agreements. Consent management. |
| Indian IT Act / DPDP Act 2023 | Data localization option (deploy in Indian cloud region). Consent-based processing. Grievance officer designation. |
| SOC 2 Type II | Audit trail. Access controls. Encryption. Availability monitoring. |

### 8.4 LLM-Specific Security

| Risk | Mitigation |
|------|-----------|
| Prompt injection via email content | Sanitize email content before LLM input. Separate data channel from instruction channel. |
| Data leakage to LLM provider | Use Azure OpenAI (no data retention). Or self-hosted Mistral for sensitive data. |
| Model output manipulation | Never trust LLM for final numeric values. All calculations are deterministic (Python/SQL). |
| PII in LLM context | Mask USNs and names before sending to external LLM. Unmask after response. |

---

## 9. RESEARCH GAP THIS SYSTEM FILLS

### 9.1 Current State of the Art — Gaps Identified

**Gap 1: No End-to-End Academic Result Pipelines**
Existing work treats email processing, document understanding, and information extraction as separate problems. Systems like DocAI (Google), Azure Form Recognizer, and Amazon Textract provide general-purpose document extraction but lack academic domain adaptation. No published system handles the full email→extraction→profiling→querying pipeline autonomously.

**Gap 2: Limited Tool-Using Agents for Structured Data Extraction**
Current agentic frameworks (LangChain, AutoGen, CrewAI) focus on conversational and research tasks. No published work applies Planner-Executor-Critic agent architectures specifically to multi-format academic document processing with self-verification loops.

**Gap 3: Hybrid Neuro-Symbolic Extraction with Domain Constraints**
Pure LLM extraction hallucinates numbers. Pure rule-based systems break on format changes. No existing system combines rule-based, regex, and LLM extraction with a voting mechanism and domain-specific constraint validation (GPA bounds, mark ranges, pass/fail consistency) in a unified pipeline.

**Gap 4: Longitudinal Student Profiling from Unstructured Sources**
Student Information Systems (SIS) require manual data entry. No system autonomously builds longitudinal academic profiles from heterogeneous email attachments with identity resolution across semesters.

**Gap 5: Grounded Natural Language Academic Querying**
Existing text-to-SQL systems (DIN-SQL, DAIL-SQL) generate SQL but don't enforce factual grounding — LLMs may still hallucinate aggregates. No system combines safe SQL generation with deterministic mathematical computation for academic queries.

### 9.2 How This System Advances the State of the Art

1. **First integrated email-to-profile pipeline** — Demonstrates that autonomous document processing can replace manual SIS data entry with ≥95% accuracy at scale.

2. **Hybrid extraction with confidence propagation** — Introduces a novel three-strategy extraction merger with field-level confidence that propagates through the pipeline to inform downstream decisions.

3. **Neuro-symbolic validation** — Combines neural extraction with symbolic constraint checking in a closed-loop error correction architecture, reducing hallucination to ≤1%.

4. **Agentic self-verification** — Implements a Planner-Executor-Critic architecture where the Critic verifies extraction accuracy using orthogonal methods (re-extraction from source + constraint validation).

5. **Grounded academic querying** — Ensures all numeric answers come from deterministic computation (SQL aggregates, Python arithmetic) rather than LLM generation, achieving 100% numeric accuracy.

---

## 10. POTENTIAL PUBLICATION ANGLE

### 10.1 Primary Paper

**Title**: "From Inbox to Insight: An Autonomous Multi-Agent System for Academic Result Extraction and Natural Language Student Profiling"

**Venue**: ACL / EMNLP / AAAI (Systems track) or Journal of Artificial Intelligence Research (JAIR)

**Key Contributions**:
1. A production-grade multi-agent architecture for autonomous academic result extraction from email streams
2. A hybrid neuro-symbolic extraction pipeline with three-strategy voting and domain constraint validation
3. A grounded natural language query engine that guarantees numeric accuracy through deterministic computation
4. Comprehensive evaluation on a multi-university dataset (to be constructed)

### 10.2 Secondary Papers (Component-Level)

| Paper | Venue | Focus |
|-------|-------|-------|
| "Hybrid Table Extraction from Academic Documents: Combining Rule-Based, Regex, and LLM Strategies" | ICDAR / Document AI Workshop | Phase 2-3: extraction pipeline |
| "Confidence-Aware Information Extraction with Self-Verification Agents" | NAACL / ACL Findings | Phase 3-4: confidence propagation + agent verification |
| "Grounded Text-to-SQL for Academic Querying: Preventing LLM Hallucination in Numeric Responses" | EMNLP / NeurIPS (Datasets & Benchmarks) | Phase 5: query engine |
| "Autonomous Email Triage and Attachment Processing at Scale: An Empirical Study" | CIKM / WWW | Phase 1: email pipeline |

### 10.3 Benchmark Dataset

Construct and release:
- **AcadEmail-100K**: 100,000 synthetic academic emails with labeled classifications
- **AcadDoc-10K**: 10,000 academic documents (PDFs, Excel) with ground-truth extractions
- **AcadQuery-1K**: 1,000 natural language academic queries with SQL translations and expected answers

---

## 11. FUTURE EXTENSIONS

### 11.1 Short-Term (3-6 months)

| Extension | Description |
|-----------|------------|
| **Multi-language support** | Support Hindi, Kannada, Telugu medium result sheets. PaddleOCR already supports 80+ languages. Add language detection and script-aware OCR. |
| **Predictive analytics** | Train models on historical data: dropout prediction (logistic regression on GPA trajectory), grade prediction (LSTM on semester sequences). |
| **Alert system** | Proactive notifications: "5 students in CSE dropped below 5.0 CGPA this semester." Push to teachers via email/Slack/WhatsApp. |
| **Batch comparison** | Compare current semester results with previous: class-level analytics, subject difficulty analysis. |

### 11.2 Medium-Term (6-12 months)

| Extension | Description |
|-----------|------------|
| **WhatsApp/Telegram bot** | Natural language querying via messaging apps. Useful for teachers on mobile. |
| **Student self-service portal** | Students query their own data. Strict access control (only own records). |
| **Curriculum analytics** | Cross-institutional analysis of subject pass rates, grade distributions. Inform curriculum redesign. |
| **Automated report generation** | Generate department-level performance reports (PDF) with charts. LaTeX template system. |
| **Knowledge graph** | Build a knowledge graph: Student→Takes→Subject→BelongsTo→Department. Enable graph queries and path analysis. |

### 11.3 Long-Term (12-24 months)

| Extension | Description |
|-----------|------------|
| **Federated learning** | Train shared extraction models across universities without sharing raw data. Privacy-preserving multi-institutional learning. |
| **Autonomous curriculum optimization** | Reinforcement learning agent that suggests timetable/curriculum changes based on aggregate performance data. |
| **Real-time streaming** | Replace batch email polling with streaming (Gmail push notifications / IMAP IDLE). Event-driven architecture with Kafka. |
| **Multi-modal interaction** | Voice queries for teachers. Camera input for student ID card scanning for quick lookup. |
| **Regulatory compliance engine** | Auto-check student eligibility for graduation, lateral entry, scholarship criteria against university regulations. |

---

## APPENDIX A: TECHNOLOGY STACK SUMMARY

| Layer | Technology | Version |
|-------|-----------|---------|
| Language | Python | 3.11+ |
| Web Framework | FastAPI | 0.104+ |
| Task Queue | Celery + Redis | 5.3+ |
| Database | PostgreSQL + pgvector | 16+ |
| Object Storage | MinIO / AWS S3 | - |
| Search | Elasticsearch | 8.x |
| PDF Parsing | pdfplumber + camelot-py + pymupdf | - |
| OCR | PaddleOCR + Tesseract 5 | - |
| Layout Understanding | LayoutLMv3 (HuggingFace) | - |
| VLM | Donut (HuggingFace) | - |
| Table Detection | Table Transformer (Microsoft) | - |
| Excel Parsing | openpyxl + pandas | - |
| LLM (Primary) | GPT-4o (OpenAI / Azure OpenAI) | - |
| LLM (Secondary) | Gemini 1.5 Pro | - |
| LLM (Local) | Mistral-7B-Instruct (GGUF) | - |
| Embeddings | text-embedding-3-small | - |
| Classifier | DistilBERT (fine-tuned) | - |
| Agent Framework | Custom (no LangChain dependency) | - |
| Containerization | Docker + Kubernetes | - |
| CI/CD | GitHub Actions | - |
| Monitoring | Prometheus + Grafana + OpenTelemetry | - |
| Error Tracking | Sentry | - |
| Secrets | HashiCorp Vault / AWS KMS | - |

## APPENDIX B: KEY CONFIGURATION PARAMETERS

```yaml
# config/system.yaml
email:
  poll_interval_seconds: 60
  max_attachment_size_mb: 50
  dedup_simhash_threshold: 0.92
  classification_confidence_threshold: 0.85
  classification_review_threshold: 0.60
  max_retries: 3
  backoff_base_seconds: 2

document:
  ocr_confidence_threshold: 0.7
  table_detection_confidence: 0.5
  max_pages_per_document: 200
  image_dpi: 300
  deskew_angle_threshold_degrees: 0.5

extraction:
  llm_temperature: 0
  llm_max_tokens: 4096
  max_validation_retries: 3
  gpa_max: 10.0
  marks_max_default: 100
  usn_pattern: '[1-4][A-Z]{2}\d{2}[A-Z]{2,3}\d{3}'
  name_similarity_threshold: 0.92
  confidence_threshold_auto_accept: 0.85
  confidence_threshold_quarantine: 0.50

agent:
  max_steps: 20
  tool_timeout_seconds: 30
  circuit_breaker_threshold: 3
  circuit_breaker_window_seconds: 60
  memory_ttl_seconds: 3600

query:
  sql_statement_timeout_seconds: 10
  max_results_per_query: 1000
  rate_limit_per_user_per_minute: 100
  rate_limit_per_institution_per_minute: 1000
```
