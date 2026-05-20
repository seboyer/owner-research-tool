-- Migration 009 — Worker heartbeat / status row read by the admin dashboard.
--
-- The web service and worker service run as separate Render services with
-- independent env vars. The admin UI lives on the web service but the
-- scheduler lives on the worker, so reading config.AUTO_SEARCH_ENABLED in
-- the admin returned the WEB's value, not the worker's. This table is the
-- worker's heartbeat: it writes its own state on startup and every 30s
-- during the trigger-poll tick. The admin UI reads from here and shows the
-- worker's actual state (or "offline" if the row is stale).
--
-- Single-row table — the CHECK constraint enforces id=1 so we never have
-- more than one heartbeat row to reason about.

BEGIN;

CREATE TABLE IF NOT EXISTS scheduler_status (
    id                    INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    auto_search_enabled   BOOLEAN NOT NULL DEFAULT FALSE,
    weekly_pipeline_day   TEXT,
    last_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO scheduler_status (id, auto_search_enabled)
VALUES (1, FALSE)
ON CONFLICT (id) DO NOTHING;

COMMIT;
