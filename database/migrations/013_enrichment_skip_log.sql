-- Migration 013 — Skip-filter audit log for enrichment queue.
--
-- New table records why each entity was skipped at queue-insertion time,
-- with a human-readable evidence string for the admin review UI.
-- Adds 'skipped' as a legal value of entities.enrichment_status (TEXT column,
-- no CHECK constraint — no ALTER needed).
--
-- Idempotent: CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS.

BEGIN;

CREATE TABLE IF NOT EXISTS enrichment_skip_log (
    entity_id UUID PRIMARY KEY REFERENCES entities(id) ON DELETE CASCADE,
    reason TEXT NOT NULL,           -- 'lawyer_name' | 'govt_entity' | 'bank_or_lender' | 'deceased' | 'low_value_score'
    score NUMERIC(4,3) NOT NULL,    -- always populated, 0.000–1.000
    evidence TEXT,                  -- human-readable string for admin UI
    skipped_at TIMESTAMPTZ DEFAULT NOW(),
    requeued_at TIMESTAMPTZ,        -- NULL = currently skipped; non-NULL = was requeued, kept for audit
    suppress_future_skip BOOLEAN DEFAULT FALSE  -- set TRUE on manual requeue so filter respects user intent
);

CREATE INDEX IF NOT EXISTS idx_skip_log_reason ON enrichment_skip_log(reason);
CREATE INDEX IF NOT EXISTS idx_skip_log_score  ON enrichment_skip_log(score);
CREATE INDEX IF NOT EXISTS idx_skip_log_active ON enrichment_skip_log(requeued_at) WHERE requeued_at IS NULL;

COMMIT;
