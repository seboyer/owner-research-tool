-- Migration 014 — Tighten allowed_enrichment_queue bypass arm.
--
-- The original view (migration 010) had an unconditional "no property_roles yet"
-- bypass that let ANY queue type through when an entity had no linked properties.
-- In practice this allowed ~604 orphan pierced-individuals (confirmed humans with
-- no property_role yet) to escape the zip/borough gate entirely, which was the
-- root cause of unbounded enrichment spend.
--
-- Fix: the bypass now only fires when enrichment_type = 'llc_pierce'. An LLC may
-- legitimately have no property_roles yet (it hasn't been pierced / linked).
-- For all other types (multi_source, zoominfo) the entity must have at least one
-- property_role that passes the zip or borough check — otherwise it stays hidden
-- from the batch fetcher until a property is linked.

BEGIN;

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
