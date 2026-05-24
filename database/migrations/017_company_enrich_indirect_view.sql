-- Migration 017 — allowed_enrichment_queue: recognise indirect property links
--
-- Problem: prong1 creates owner_operating / management company entities and
-- writes an entity_relationship (parent=mgmt_co, child=building_llc) — but
-- the management company itself never gets a direct property_role. Migration
-- 016's view only checked direct property_roles, so those entities pile up
-- in the raw `enrichment_queue` (3 today, more in the future as prong1 runs)
-- but never become visible to the orchestrator.
--
-- Fix: add an OR clause to the view that allows `company_enrich` rows when
-- the entity has a child via entity_relationships ('managed_by', 'operates_as',
-- 'owned_by') whose property_role passes the zip / borough allowlist. This
-- mirrors what enrichment/company/orchestrator.py:_load_company already does
-- when resolving BBLs for the cascade.
--
-- Out of scope: bucket-E orphans (~585 entities with no property_roles and
-- no relationships) — those come from a separate HPD ingest path that
-- creates entities without linking properties. Tracked as a follow-up.

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
    -- Direct property_role in an allowed zip
    OR EXISTS (
        SELECT 1 FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN zipcode_allowlist za ON za.zip_code = p.zip_code
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE AND za.enabled = TRUE
    )
    -- Direct property_role in an allowed borough (when zip is missing)
    OR EXISTS (
        SELECT 1 FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN borough_allowlist ba ON ba.borough_code = LEFT(p.bbl, 1)
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE
          AND (p.zip_code IS NULL OR p.zip_code = '') AND ba.enabled = TRUE
    )
    -- NEW: indirect property_role for company_enrich — entity has a child via
    -- entity_relationships whose property_role is in the allowlist. This is
    -- how prong1-created owner_operating / management cos surface their
    -- buildings (the building LLC is the child; the mgmt co is the parent).
    OR (
        q.enrichment_type = 'company_enrich' AND EXISTS (
            SELECT 1
            FROM entity_relationships er
            JOIN property_roles pr ON pr.entity_id = er.child_entity_id
            JOIN properties p ON p.id = pr.property_id
            LEFT JOIN zipcode_allowlist za ON za.zip_code = p.zip_code
            LEFT JOIN borough_allowlist ba ON ba.borough_code = LEFT(p.bbl, 1)
            WHERE er.parent_entity_id = q.entity_id
              AND er.relationship_type IN ('managed_by', 'operates_as', 'owned_by')
              AND pr.is_current = TRUE
              AND (
                  (za.enabled = TRUE)
                  OR (
                      (p.zip_code IS NULL OR p.zip_code = '')
                      AND ba.enabled = TRUE
                  )
              )
        )
    );

COMMIT;
