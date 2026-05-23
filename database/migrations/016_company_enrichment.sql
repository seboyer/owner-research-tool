-- Migration 016 — Company enrichment cascade
--
-- 1. Create company_enrichment_runs table (cache for multi-source company enrichment).
-- 2. Rename existing 'zoominfo' queue rows to 'company_enrich'.
-- 3. Backfill: enqueue corp/mgmt-company entities that were skipped under
--    the old portfolio_size >= 3 gate.
-- 4. Update allowed_enrichment_queue view to recognise 'company_enrich' type.

BEGIN;

-- ============================================================
-- Table: company_enrichment_runs
-- One row per (entity, run).  Keeps results cached for 90 days
-- so the cascade doesn't re-spend on the same company.
-- ============================================================
CREATE TABLE IF NOT EXISTS company_enrichment_runs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id           UUID REFERENCES entities(id) ON DELETE CASCADE,
    cost_tier           TEXT,           -- 'free'|'budget'|'standard'|'premium'
    sources_attempted   JSONB,
    sources_succeeded   JSONB,
    contacts_found      INTEGER DEFAULT 0,
    cost_cents          INTEGER DEFAULT 0,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              TEXT,           -- 'running'|'success'|'failed'
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_cer_company_entity
    ON company_enrichment_runs(entity_id);

-- ============================================================
-- Queue rename: 'zoominfo' → 'company_enrich'
-- ============================================================
UPDATE enrichment_queue
SET enrichment_type = 'company_enrich'
WHERE enrichment_type = 'zoominfo';

-- ============================================================
-- Backfill: enqueue LLC/corp/mgmt entities skipped by old gate
-- ============================================================
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority)
SELECT e.id, 'company_enrich', 5
FROM entities e
WHERE e.entity_type IN ('llc', 'corporation', 'management_company')
  AND (e.enrichment_status IN ('pending') OR e.enrichment_status IS NULL)
  AND NOT EXISTS (
      SELECT 1 FROM enrichment_queue q
      WHERE q.entity_id = e.id AND q.enrichment_type = 'company_enrich'
  )
ON CONFLICT (entity_id, enrichment_type) DO NOTHING;

-- ============================================================
-- Update allowed_enrichment_queue view to recognise 'company_enrich'
-- as equivalent to the old 'zoominfo' type (requires real property
-- association to pass zip/borough gate).
-- ============================================================
CREATE OR REPLACE VIEW allowed_enrichment_queue AS
SELECT q.*
FROM enrichment_queue q
WHERE
    -- LLC-pierce bypass: an LLC may have no property_roles yet because it
    -- hasn't been linked. Other queue types require a real property association.
    (q.enrichment_type = 'llc_pierce' AND NOT EXISTS (
        SELECT 1 FROM property_roles pr
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE
    ))
    OR EXISTS (
        SELECT 1 FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN zipcode_allowlist za ON za.zip_code = p.zip_code
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE AND za.enabled = TRUE
    )
    OR EXISTS (
        SELECT 1 FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN borough_allowlist ba ON ba.borough_code = LEFT(p.bbl, 1)
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE
          AND (p.zip_code IS NULL OR p.zip_code = '') AND ba.enabled = TRUE
    );

COMMIT;
