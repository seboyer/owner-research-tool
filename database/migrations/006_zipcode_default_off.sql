-- Migration 006 — Change zipcode_allowlist column default to disabled.
-- New zips discovered by the admin UI now require explicit opt-in.
-- Existing enabled rows are unchanged.

BEGIN;

ALTER TABLE zipcode_allowlist
    ALTER COLUMN enabled SET DEFAULT FALSE;

COMMIT;
