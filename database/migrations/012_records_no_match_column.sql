-- Migration 012 — Split records_skipped into "skipped" + "no_match".
--
-- The enrichment stages were conflating two very different things into
-- records_skipped:
--   1. Skipped: not processed at all (allowlist filter, batch-cap, etc.).
--      Free — no API calls happened.
--   2. No match: processed end-to-end, all sources tried, nothing found.
--      Real cost — API calls happened, just didn't return anything useful.
--
-- Reading "78 fetched / 78 skipped / $23.40 cost" feels like waste, when in
-- reality the cost was for legitimate failed lookups. This column makes the
-- distinction explicit on the dashboard.

BEGIN;

ALTER TABLE ingestion_log
    ADD COLUMN IF NOT EXISTS records_no_match INTEGER DEFAULT 0;

COMMIT;
