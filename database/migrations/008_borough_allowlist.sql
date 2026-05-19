-- Migration 008 — Borough-level fallback gate for properties with NULL zip.
--
-- The zipcode allowlist (migration 005/006) only gates properties whose
-- zip_code we know. Some sources (ACRIS Legals) don't include a zip code,
-- so the corresponding entities slip through the gate via the "unknown zip
-- — default allow" branch. This table adds a per-borough switch that only
-- fires when a property's zip is NULL/empty — properties with known zips
-- continue to be gated solely by zipcode_allowlist.
--
-- Borough code is the first digit of a NYC BBL:
--   1 = Manhattan, 2 = Bronx, 3 = Brooklyn, 4 = Queens, 5 = Staten Island

BEGIN;

CREATE TABLE IF NOT EXISTS borough_allowlist (
    borough_code  CHAR(1) PRIMARY KEY CHECK (borough_code IN ('1','2','3','4','5')),
    borough_name  TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by    TEXT
);

INSERT INTO borough_allowlist (borough_code, borough_name) VALUES
    ('1', 'Manhattan'),
    ('2', 'Bronx'),
    ('3', 'Brooklyn'),
    ('4', 'Queens'),
    ('5', 'Staten Island')
ON CONFLICT (borough_code) DO NOTHING;

COMMIT;
