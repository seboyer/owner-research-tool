-- ============================================================
-- Migration 004 — Drop entities.opencorporates_url
--
-- OpenCorporates was fully removed from the pipeline (see commit
-- c7ede58). The opencorporates_url column on entities is now dead.
-- schema.sql no longer declares it; this migration drops it from
-- the live DB to keep schema and database in sync.
--
-- Any opencorporates_url values currently stored will be lost.
-- This is intentional — the field is no longer used.
-- ============================================================

BEGIN;

ALTER TABLE entities DROP COLUMN IF EXISTS opencorporates_url;

COMMIT;
