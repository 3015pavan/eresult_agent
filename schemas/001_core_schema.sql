-- =============================================================================
-- Autonomous Academic Result Extraction System
-- Core Database Schema — PostgreSQL 16+ with pgvector
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "vector";         -- pgvector for embeddings
CREATE EXTENSION IF NOT EXISTS "pg_trgm";        -- trigram similarity for fuzzy search

-- =============================================================================
-- MULTI-TENANT: INSTITUTIONS
-- =============================================================================
CREATE TABLE institutions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    code            VARCHAR(20) UNIQUE NOT NULL,
    config          JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE departments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    code            VARCHAR(10) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(institution_id, code)
);

-- =============================================================================
-- STUDENT MASTER
-- =============================================================================
CREATE TABLE students (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id      UUID NOT NULL REFERENCES institutions(id),
    department_id       UUID REFERENCES departments(id),
    usn                 VARCHAR(20) NOT NULL,
    name                TEXT NOT NULL,
    name_normalized     TEXT NOT NULL,
    email               TEXT,
    batch_year          SMALLINT,
    current_semester    SMALLINT,
    cgpa                DECIMAL(4,2),
    total_credits       INTEGER DEFAULT 0,
    total_backlogs      INTEGER DEFAULT 0,
    active_backlogs     INTEGER DEFAULT 0,
    profile_embedding   VECTOR(1536),
    metadata            JSONB DEFAULT '{}',
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(institution_id, usn)
);

CREATE INDEX idx_students_usn ON students(usn);
CREATE INDEX idx_students_name_trgm ON students USING gin(name_normalized gin_trgm_ops);
CREATE INDEX idx_students_embedding ON students USING ivfflat(profile_embedding vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_students_cgpa ON students(cgpa);
CREATE INDEX idx_students_institution ON students(institution_id);
CREATE INDEX idx_students_department ON students(department_id);
CREATE INDEX idx_students_batch ON students(batch_year);

-- =============================================================================
-- SUBJECTS
-- =============================================================================
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
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(institution_id, code)
);

CREATE INDEX idx_subjects_code ON subjects(code);
CREATE INDEX idx_subjects_institution ON subjects(institution_id);

-- =============================================================================
-- EMAIL PIPELINE
-- =============================================================================
CREATE TABLE email_metadata (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id                  TEXT UNIQUE NOT NULL,
    institution_id              UUID REFERENCES institutions(id),
    account_id                  TEXT NOT NULL,
    from_address                TEXT NOT NULL,
    to_addresses                TEXT[] NOT NULL,
    subject                     TEXT,
    received_at                 TIMESTAMPTZ NOT NULL,
    body_hash                   CHAR(64) NOT NULL,
    body_simhash                BIGINT,
    classification              VARCHAR(30),
    classification_confidence   DECIMAL(3,2),
    classification_uncertainty  DECIMAL(4,3),
    thread_id                   TEXT,
    raw_storage_path            TEXT NOT NULL,
    status                      VARCHAR(20) DEFAULT 'pending'
                                CHECK (status IN ('pending','processing','completed','failed','quarantined','skipped')),
    retry_count                 SMALLINT DEFAULT 0,
    error_message               TEXT,
    processed_at                TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (received_at);

-- Monthly partitions (create dynamically or pre-create)
CREATE TABLE email_metadata_2025_01 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE email_metadata_2025_02 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE email_metadata_2025_03 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
-- ... extend as needed

CREATE INDEX idx_email_message_id ON email_metadata(message_id);
CREATE INDEX idx_email_body_hash ON email_metadata(body_hash);
CREATE INDEX idx_email_status ON email_metadata(status);
CREATE INDEX idx_email_received ON email_metadata(received_at);
CREATE INDEX idx_email_account ON email_metadata(account_id);

-- =============================================================================
-- ATTACHMENTS
-- =============================================================================
CREATE TABLE attachments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email_id        UUID NOT NULL REFERENCES email_metadata(id) ON DELETE CASCADE,
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL,
    file_size       BIGINT NOT NULL,
    file_hash       CHAR(64) NOT NULL,
    storage_path    TEXT NOT NULL,
    parse_status    VARCHAR(20) DEFAULT 'pending'
                    CHECK (parse_status IN ('pending','processing','completed','failed','quarantined')),
    document_type   VARCHAR(20),
    page_count      SMALLINT,
    metadata        JSONB DEFAULT '{}',
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_attachments_email ON attachments(email_id);
CREATE INDEX idx_attachments_hash ON attachments(file_hash);
CREATE INDEX idx_attachments_status ON attachments(parse_status);

-- =============================================================================
-- EXTRACTIONS
-- =============================================================================
CREATE TABLE extractions (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    attachment_id           UUID NOT NULL REFERENCES attachments(id),
    strategy                VARCHAR(20) NOT NULL,
    raw_output              JSONB NOT NULL,
    validated_output        JSONB,
    records_extracted       INTEGER DEFAULT 0,
    records_valid           INTEGER DEFAULT 0,
    records_quarantined     INTEGER DEFAULT 0,
    overall_confidence      DECIMAL(3,2),
    validation_errors       JSONB DEFAULT '[]',
    extraction_model        TEXT,
    extraction_time_ms      INTEGER,
    llm_tokens_used         INTEGER DEFAULT 0,
    retry_count             SMALLINT DEFAULT 0,
    status                  VARCHAR(20) DEFAULT 'pending'
                            CHECK (status IN ('pending','processing','completed','failed','quarantined')),
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    completed_at            TIMESTAMPTZ
);

CREATE INDEX idx_extractions_attachment ON extractions(attachment_id);
CREATE INDEX idx_extractions_status ON extractions(status);

-- =============================================================================
-- STUDENT RESULTS
-- =============================================================================
CREATE TABLE student_results (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    student_id      UUID NOT NULL REFERENCES students(id),
    subject_id      UUID NOT NULL REFERENCES subjects(id),
    semester        SMALLINT NOT NULL,
    academic_year   VARCHAR(10),
    exam_type       VARCHAR(20) DEFAULT 'regular'
                    CHECK (exam_type IN ('regular','supplementary','improvement')),
    internal_marks  SMALLINT,
    external_marks  SMALLINT,
    total_marks     SMALLINT NOT NULL,
    max_marks       SMALLINT NOT NULL DEFAULT 100,
    grade           VARCHAR(5),
    grade_points    DECIMAL(3,1),
    status          VARCHAR(10) NOT NULL
                    CHECK (status IN ('PASS','FAIL','ABSENT','WITHHELD')),
    attempt_number  SMALLINT DEFAULT 1,
    extraction_id   UUID REFERENCES extractions(id),
    confidence      DECIMAL(3,2) DEFAULT 1.0,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(student_id, subject_id, semester, exam_type, attempt_number)
);

CREATE INDEX idx_results_student ON student_results(student_id);
CREATE INDEX idx_results_semester ON student_results(student_id, semester);
CREATE INDEX idx_results_status ON student_results(status);
CREATE INDEX idx_results_subject ON student_results(subject_id);

-- =============================================================================
-- SEMESTER AGGREGATES (materialized)
-- =============================================================================
CREATE TABLE semester_aggregates (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    student_id          UUID NOT NULL REFERENCES students(id),
    semester            SMALLINT NOT NULL,
    academic_year       VARCHAR(10),
    sgpa                DECIMAL(4,2) NOT NULL,
    credits_earned      SMALLINT NOT NULL,
    credits_attempted   SMALLINT NOT NULL,
    subjects_passed     SMALLINT NOT NULL,
    subjects_failed     SMALLINT NOT NULL,
    total_marks         INTEGER,
    percentage          DECIMAL(5,2),
    rank_in_class       INTEGER,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(student_id, semester, academic_year)
);

CREATE INDEX idx_sem_agg_student ON semester_aggregates(student_id);

-- =============================================================================
-- AGENT TRACES
-- =============================================================================
CREATE TABLE agent_traces (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL,
    step_number     SMALLINT NOT NULL,
    state           VARCHAR(20) NOT NULL,
    tool_name       TEXT,
    tool_input      JSONB,
    tool_output     JSONB,
    reflection      TEXT,
    confidence      DECIMAL(3,2),
    duration_ms     INTEGER,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_traces_run ON agent_traces(run_id, step_number);
CREATE INDEX idx_traces_created ON agent_traces(created_at);

-- =============================================================================
-- QUERY AUDIT LOG
-- =============================================================================
CREATE TABLE query_audit_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             UUID NOT NULL,
    user_role           VARCHAR(30) NOT NULL,
    query_text          TEXT NOT NULL,
    intent              JSONB,
    generated_sql       TEXT,
    result_summary      TEXT,
    records_returned    INTEGER,
    confidence          DECIMAL(3,2),
    response_time_ms    INTEGER,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_audit_user ON query_audit_log(user_id);
CREATE INDEX idx_audit_created ON query_audit_log(created_at);

-- =============================================================================
-- USERS & ACCESS CONTROL
-- =============================================================================
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    institution_id  UUID NOT NULL REFERENCES institutions(id),
    email           TEXT UNIQUE NOT NULL,
    name            TEXT NOT NULL,
    role            VARCHAR(30) NOT NULL
                    CHECK (role IN ('admin','teacher','hod','principal','readonly')),
    department_id   UUID REFERENCES departments(id),
    password_hash   TEXT NOT NULL,
    is_active       BOOLEAN DEFAULT true,
    last_login      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_institution ON users(institution_id);
CREATE INDEX idx_users_role ON users(role);

-- =============================================================================
-- ROW-LEVEL SECURITY
-- =============================================================================
ALTER TABLE students ENABLE ROW LEVEL SECURITY;
ALTER TABLE student_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE semester_aggregates ENABLE ROW LEVEL SECURITY;

-- Teachers can see their department's students
CREATE POLICY department_isolation_students ON students
    FOR SELECT USING (
        department_id IN (
            SELECT department_id FROM users
            WHERE id = current_setting('app.current_user_id', true)::UUID
        )
        OR EXISTS (
            SELECT 1 FROM users
            WHERE id = current_setting('app.current_user_id', true)::UUID
            AND role IN ('admin', 'principal')
        )
    );

-- Results follow student visibility
CREATE POLICY department_isolation_results ON student_results
    FOR SELECT USING (
        student_id IN (
            SELECT s.id FROM students s
            JOIN users u ON u.department_id = s.department_id
            WHERE u.id = current_setting('app.current_user_id', true)::UUID
        )
        OR EXISTS (
            SELECT 1 FROM users
            WHERE id = current_setting('app.current_user_id', true)::UUID
            AND role IN ('admin', 'principal')
        )
    );

-- =============================================================================
-- HELPER FUNCTIONS
-- =============================================================================

-- Recompute CGPA for a student
CREATE OR REPLACE FUNCTION recompute_student_cgpa(p_student_id UUID)
RETURNS DECIMAL(4,2) AS $$
DECLARE
    v_cgpa DECIMAL(4,2);
BEGIN
    SELECT
        CASE
            WHEN SUM(s.credits) > 0 THEN
                ROUND(SUM(sr.grade_points * s.credits) / SUM(s.credits), 2)
            ELSE 0
        END INTO v_cgpa
    FROM student_results sr
    JOIN subjects s ON sr.subject_id = s.id
    WHERE sr.student_id = p_student_id
    AND sr.status = 'PASS'
    AND sr.attempt_number = (
        SELECT MAX(sr2.attempt_number)
        FROM student_results sr2
        WHERE sr2.student_id = sr.student_id
        AND sr2.subject_id = sr.subject_id
        AND sr2.status = 'PASS'
    );

    UPDATE students SET
        cgpa = v_cgpa,
        updated_at = NOW()
    WHERE id = p_student_id;

    RETURN v_cgpa;
END;
$$ LANGUAGE plpgsql;

-- Count backlogs for a student
CREATE OR REPLACE FUNCTION recompute_student_backlogs(p_student_id UUID)
RETURNS TABLE(total_backlogs INTEGER, active_backlogs INTEGER) AS $$
BEGIN
    RETURN QUERY
    WITH subject_status AS (
        SELECT
            sr.subject_id,
            bool_or(sr.status = 'PASS') AS ever_passed,
            bool_and(sr.status = 'FAIL') AS always_failed
        FROM student_results sr
        WHERE sr.student_id = p_student_id
        GROUP BY sr.subject_id
    )
    SELECT
        COUNT(*)::INTEGER AS total_backlogs,
        COUNT(*) FILTER (WHERE always_failed)::INTEGER AS active_backlogs
    FROM subject_status
    WHERE NOT ever_passed;

    UPDATE students SET
        total_backlogs = (SELECT COUNT(*) FROM (
            SELECT sr.subject_id FROM student_results sr
            WHERE sr.student_id = p_student_id
            GROUP BY sr.subject_id
            HAVING NOT bool_or(sr.status = 'PASS')
        ) t),
        active_backlogs = (SELECT COUNT(*) FROM (
            SELECT sr.subject_id FROM student_results sr
            WHERE sr.student_id = p_student_id
            GROUP BY sr.subject_id
            HAVING bool_and(sr.status = 'FAIL')
        ) t),
        updated_at = NOW()
    WHERE id = p_student_id;
END;
$$ LANGUAGE plpgsql;
