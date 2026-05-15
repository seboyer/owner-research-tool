-- Migration 005 — Per-zipcode allowlist for scheduled enrichment.
-- Only the four enrichment queue-drainer stages consult this table.
-- NYC-wide ingestion (ACRIS delta, HPD full, WoW) is not gated.

BEGIN;

CREATE TABLE IF NOT EXISTS zipcode_allowlist (
    zip_code   TEXT PRIMARY KEY,
    enabled    BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_zipcode_allowlist_enabled
    ON zipcode_allowlist(enabled);

-- Seed with every zip currently present in properties; default to enabled
-- so rollout matches today's "process everything" behavior.
INSERT INTO zipcode_allowlist (zip_code, enabled)
SELECT DISTINCT zip_code, TRUE
FROM properties
WHERE zip_code IS NOT NULL AND zip_code <> ''
ON CONFLICT (zip_code) DO NOTHING;

COMMIT;
