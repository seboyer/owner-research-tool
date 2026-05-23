-- scripts/cleanup_test_artifacts.sql
--
-- One-off cleanup of testing-batch artifacts (2026-05-19 → 2026-05-22 ~13:00 UTC)
-- left behind because HANDOFF's rollback used cutoff '2026-05-20 04:00 UTC',
-- which missed the entire May 19 testing day.
--
-- KEEPS:
--   - All properties (real ingested data)
--   - All entities with a current property_role (real owners from acris_deeds)
--   - All 1,014 HPD-sourced contacts (real public data, no email/phone so they
--     won't trip the multi_source short-circuit at multi_source.py:775)
--   - All 17 May 22 13:33 single-address-test pierces + their relationships
--
-- WIPES:
--   - Enrichment-output contacts (non-HPD) from the testing window
--   - Dangling entity_relationships from the May 19-20 bulk batch (NOT today's pierces)
--   - The 72 orphan building-LLCs (no property_role, no relationships)
--   - The 11 corporate-shaped "individual" entities (vision extraction errors)
--   - PDF caches (so re-pierces can run fresh after backfill)
--   - enrichment_skip_log rows from the testing window
--   - Re-queues YO YO DEVELOP CORP (escaped HANDOFF rollback)
--   - Sets STUY HILL HOLDINGS LLC + JONATHAN TARABOKIJA back to 'done' (pierced)
--
-- Run each step in order. The SELECT before each DELETE/UPDATE is the count
-- preview — verify before applying.
--
-- The cutoffs '2026-05-19 00:00:00+00' and '2026-05-22 13:00:00+00' isolate the
-- bulk-testing window from (a) earlier April single-address tests and
-- (b) today's May 22 13:33 single-address pierces.

BEGIN;

-- ============================================================
-- Step 1: Delete enrichment-output contacts (non-HPD) since May 19
-- Expected: ~13 rows (5 ai_web_search from May 19 + 8 from May 22 daily runs)
-- ============================================================

-- Preview
SELECT source, COUNT(*) FROM contacts
WHERE source != 'hpd'
  AND created_at >= '2026-05-19 00:00:00+00'
GROUP BY source ORDER BY source;

DELETE FROM contacts
WHERE source != 'hpd'
  AND created_at >= '2026-05-19 00:00:00+00';


-- ============================================================
-- Step 2: Delete dangling testing-batch entity_relationships
-- (Created May 19-20 by the bulk testing pipeline. NOT today's
-- May 22 13:33 single-address-test pierces, which are kept.)
-- Expected: 12 rows
-- ============================================================

-- Preview
SELECT source, relationship_type, COUNT(*) FROM entity_relationships
WHERE created_at >= '2026-05-19 00:00:00+00'
  AND created_at <  '2026-05-22 13:00:00+00'
GROUP BY source, relationship_type;

DELETE FROM entity_relationships
WHERE created_at >= '2026-05-19 00:00:00+00'
  AND created_at <  '2026-05-22 13:00:00+00';


-- ============================================================
-- Step 3: Delete the 72 orphan building-LLCs from May 19
-- (Created by some prior code path; have no property_role and no relationships.
-- Currently invisible to allowed_enrichment_queue post-migration 015, but
-- still sitting in the entities table as noise.)
-- Expected: ~72 rows
-- ============================================================

-- Preview
SELECT COUNT(*) FROM entities e
WHERE e.is_building_llc = TRUE
  AND e.created_at::date BETWEEN '2026-05-19' AND '2026-05-20'
  AND NOT EXISTS (SELECT 1 FROM property_roles pr WHERE pr.entity_id = e.id)
  AND NOT EXISTS (SELECT 1 FROM entity_relationships r
                  WHERE r.child_entity_id = e.id OR r.parent_entity_id = e.id);

DELETE FROM entities
WHERE is_building_llc = TRUE
  AND created_at::date BETWEEN '2026-05-19' AND '2026-05-20'
  AND NOT EXISTS (SELECT 1 FROM property_roles pr WHERE pr.entity_id = entities.id)
  AND NOT EXISTS (SELECT 1 FROM entity_relationships r
                  WHERE r.child_entity_id = entities.id OR r.parent_entity_id = entities.id);


-- ============================================================
-- Step 4: Delete the corporate-shaped "individual" entities
-- (Vision extraction errors: Claude returned LLC names in the signers
-- array, got stored as entity_type='individual'. PR #33's is_human_name
-- filter prevents new ones, but old artifacts remain.)
-- Match by regex on name + entity_type=individual + no relationships and no contacts.
-- Expected: 11 rows
-- ============================================================

-- Preview
SELECT id, name, created_at FROM entities
WHERE entity_type = 'individual'
  AND name ~* '\m(LLC|L\.L\.C\.?|INC\.?|CORP\.?|CORPORATION|LTD\.?|LP|L\.P\.?|LLP|L\.L\.P\.?|COMPANY)\M'
  AND created_at >= '2026-05-19 00:00:00+00'
  AND NOT EXISTS (SELECT 1 FROM property_roles pr WHERE pr.entity_id = entities.id)
  AND NOT EXISTS (SELECT 1 FROM entity_relationships r
                  WHERE r.child_entity_id = entities.id OR r.parent_entity_id = entities.id)
  AND NOT EXISTS (SELECT 1 FROM contacts c WHERE c.entity_id = entities.id);

DELETE FROM entities
WHERE entity_type = 'individual'
  AND name ~* '\m(LLC|L\.L\.C\.?|INC\.?|CORP\.?|CORPORATION|LTD\.?|LP|L\.P\.?|LLP|L\.L\.P\.?|COMPANY)\M'
  AND created_at >= '2026-05-19 00:00:00+00'
  AND NOT EXISTS (SELECT 1 FROM property_roles pr WHERE pr.entity_id = entities.id)
  AND NOT EXISTS (SELECT 1 FROM entity_relationships r
                  WHERE r.child_entity_id = entities.id OR r.parent_entity_id = entities.id)
  AND NOT EXISTS (SELECT 1 FROM contacts c WHERE c.entity_id = entities.id);


-- ============================================================
-- Step 5: Clear PDF caches so re-pierces work after backfill
-- ============================================================

DELETE FROM seen_records WHERE source IN ('acris_pdf_pierce', 'acris_pdf_doc');


-- ============================================================
-- Step 6: Clear testing-window skip log
-- (Skip decisions from testing-window code paths; regenerate cleanly on next ingest.)
-- ============================================================

DELETE FROM enrichment_skip_log
WHERE created_at >= '2026-05-19 00:00:00+00';


-- ============================================================
-- Step 7: Fix the 3 pending oddballs from earlier diagnostics
-- ============================================================

-- STUY HILL HOLDINGS LLC + JONATHAN TARABOKIJA: already pierced via April
-- single-address tests, status was erroneously reset to 'pending' by the
-- HANDOFF rollback. Restore to 'done'.
UPDATE entities
SET enrichment_status = 'done'
WHERE name IN ('STUY HILL HOLDINGS LLC', 'JONATHAN TARABOKIJA')
  AND is_pierced = TRUE;

-- YO YO DEVELOP CORP: escaped the HANDOFF rollback's re-queue. Insert into queue.
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority, attempts)
SELECT id, 'llc_pierce', 5, 0
FROM entities
WHERE name = 'YO YO DEVELOP CORP.'
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq
    WHERE eq.entity_id = entities.id AND eq.enrichment_type = 'llc_pierce'
  );


COMMIT;


-- ============================================================
-- Verify final state
-- ============================================================

SELECT enrichment_status, COUNT(*) FROM entities GROUP BY enrichment_status ORDER BY enrichment_status;
SELECT enrichment_type, COUNT(*) FROM enrichment_queue GROUP BY enrichment_type ORDER BY enrichment_type;
SELECT source, COUNT(*) FROM contacts GROUP BY source ORDER BY COUNT(*) DESC;
