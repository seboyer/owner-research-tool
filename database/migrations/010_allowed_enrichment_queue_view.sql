-- Migration 010 — View-level allowlist filter for the enrichment queue.
--
-- Enrichment stages used to pull batches from enrichment_queue ordered by
-- (priority, created_at) and apply is_entity_allowed_by_zip() to each entity
-- in a Python loop. With most zips disabled, batches of 50 routinely had 49
-- entities filtered out — and because skipped entities were NOT removed
-- from the queue (mark_enrichment_done was only called on processed ones),
-- every subsequent batch pulled the same entities and re-skipped them. The
-- queue effectively jammed on a sliver of disabled-zip entities at the top.
--
-- This view applies the allowlist rule at the database layer so the queue
-- fetcher only sees entities that actually pass the gate. The view re-
-- evaluates on each query, so toggling a zip or borough takes effect on
-- the next get_enrichment_batch call.

BEGIN;

CREATE OR REPLACE VIEW allowed_enrichment_queue AS
SELECT q.*
FROM enrichment_queue q
WHERE
    -- Bypass: entity has no property_roles yet (LLC awaiting pierce — we
    -- can't filter by location until we discover what they own).
    NOT EXISTS (
        SELECT 1 FROM property_roles pr
        WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE
    )
    OR
    -- Any linked property has a known zip enabled in zipcode_allowlist.
    EXISTS (
        SELECT 1
        FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN zipcode_allowlist za ON za.zip_code = p.zip_code
        WHERE pr.entity_id = q.entity_id
          AND pr.is_current = TRUE
          AND za.enabled = TRUE
    )
    OR
    -- Any linked property has NULL/empty zip and a parseable BBL whose
    -- borough (first digit, 1-5) is enabled in borough_allowlist.
    EXISTS (
        SELECT 1
        FROM property_roles pr
        JOIN properties p ON p.id = pr.property_id
        JOIN borough_allowlist ba ON ba.borough_code = LEFT(p.bbl, 1)
        WHERE pr.entity_id = q.entity_id
          AND pr.is_current = TRUE
          AND (p.zip_code IS NULL OR p.zip_code = '')
          AND ba.enabled = TRUE
    );

COMMIT;
