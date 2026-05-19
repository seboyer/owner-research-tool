-- Migration 007 — Cross-process manual pipeline triggers.
--
-- The web service can't run the pipeline itself (sync DB calls block the
-- asyncio event loop and trip Render's health check; see render.yaml
-- worker split). Instead the admin UI inserts into this table and the
-- worker service polls it every 30s.

BEGIN;

CREATE TABLE IF NOT EXISTS pipeline_triggers (
    id              BIGSERIAL PRIMARY KEY,
    pipeline        TEXT NOT NULL CHECK (pipeline IN ('daily', 'weekly')),
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'done', 'failed')),
    error_message   TEXT,
    requested_by    TEXT
);

-- Index that supports the worker's "give me the oldest pending" query.
CREATE INDEX IF NOT EXISTS idx_pipeline_triggers_pending
    ON pipeline_triggers (requested_at)
    WHERE status = 'pending';

COMMIT;
