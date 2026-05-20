-- Migration 011 — Per-run cost cap accounting for enrichment stages.
--
-- Adds two columns to ingestion_log so the admin dashboard can show
-- estimated API spend per enrichment run and flag runs that were
-- truncated by hitting the env-configured cap.
--
-- cost_estimated_usd is intentionally approximate — it's
-- (entities_processed × ENRICHMENT_COST_PER_ENTITY[stage]), not actual
-- billed cost. Used as a budget guardrail, not for billing.

BEGIN;

ALTER TABLE ingestion_log
    ADD COLUMN IF NOT EXISTS cost_estimated_usd  NUMERIC(10, 2);

ALTER TABLE ingestion_log
    ADD COLUMN IF NOT EXISTS stopped_by_cost_cap BOOLEAN DEFAULT FALSE;

COMMIT;
