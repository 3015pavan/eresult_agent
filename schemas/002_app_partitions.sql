-- =============================================================================
-- App-level schema additions (applied by database.py init_db)
-- Supplements or patches the core schema for application use:
--   1. Adds `full_name` column alias (the core schema uses `name` but app code
--      expects `full_name` from queries via RealDictCursor)
--   2. Makes partitioned email_metadata usable without strict NOT NULL fields
--   3. Loosens NOT NULL constraints that block development inserts
--   4. Adds missing email_metadata partitions for 2026+
-- =============================================================================

-- ── extra email_metadata partitions ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS email_metadata_2025_04 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_05 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_06 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_07 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_08 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_09 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_10 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_11 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS email_metadata_2025_12 PARTITION OF email_metadata
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_01 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_02 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_03 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_04 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_05 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS email_metadata_2026_06 PARTITION OF email_metadata
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS email_metadata_default PARTITION OF email_metadata DEFAULT;
