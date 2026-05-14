-- ============================================================
-- Migration 003 — Schema reconciliation, queue wire-up, view cleanup
--
-- Brings schema.sql back into agreement with the live DB, refactors
-- the enrichment_queue from a per-entity table into a per-task queue,
-- and drops three views that are no longer used.
--
-- Safe to re-run: every step is idempotent (IF NOT EXISTS / NOT IN
-- guards). Wrap in BEGIN/COMMIT so a failure rolls back cleanly.
-- ============================================================

BEGIN;

-- ============================================================
-- Part 1 — contacts.enrichment_complete_at
-- (Column was added to live DB outside the migration system; this
-- reconciles schema.sql + migration history.)
-- ============================================================
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS enrichment_complete_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_contacts_enrichment_complete_at
    ON contacts(enrichment_complete_at);

-- ============================================================
-- Part 2 — Wire the enrichment queue
--
-- Old design: UNIQUE(entity_id), one row per entity, enrichment_type
-- ∈ {'zoominfo', 'llc_pierce', 'both'}.  Could not track per-type
-- retry / failure state.
--
-- New design: UNIQUE(entity_id, enrichment_type), one row per
-- (entity, work-item), enrichment_type ∈ {'llc_pierce', 'zoominfo',
-- 'multi_source'}.  'both' is replaced by explicit type-per-row.
-- ============================================================

-- 2a. Drop the legacy entity_id UNIQUE constraint (auto-generated name).
DO $$
DECLARE con_name TEXT;
BEGIN
    SELECT conname INTO con_name
    FROM pg_constraint
    WHERE conrelid = 'enrichment_queue'::regclass
      AND contype = 'u'
      AND array_length(conkey, 1) = 1
      AND conkey[1] = (SELECT attnum FROM pg_attribute
                       WHERE attrelid = 'enrichment_queue'::regclass
                         AND attname = 'entity_id');
    IF con_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE enrichment_queue DROP CONSTRAINT %I', con_name);
    END IF;
END$$;

-- 2b. Delete queue rows for entities that are already 'done'.
--     (Pre-wire-up cruft — the queue was never drained, so 'done'
--      entities still had stale rows.)
DELETE FROM enrichment_queue
WHERE entity_id IN (
    SELECT id FROM entities WHERE enrichment_status = 'done'
);

-- 2c. Split 'both' rows for LLC/corp/mgmt entities into 'llc_pierce'
--     plus optionally 'zoominfo' (only if portfolio_size >= 3, which
--     is the default ZOOMINFO_MIN_PORTFOLIO_SIZE).
INSERT INTO enrichment_queue (entity_id, priority, enrichment_type, attempts, created_at)
SELECT q.entity_id, q.priority, 'zoominfo', 0, q.created_at
FROM enrichment_queue q
JOIN entities e ON e.id = q.entity_id
WHERE q.enrichment_type = 'both'
  AND e.entity_type IN ('llc', 'corporation', 'management_company')
  AND COALESCE(e.portfolio_size, 0) >= 3;

UPDATE enrichment_queue q
SET enrichment_type = 'llc_pierce'
FROM entities e
WHERE q.entity_id = e.id
  AND q.enrichment_type = 'both'
  AND e.entity_type IN ('llc', 'corporation', 'management_company');

-- 2d. For everything else that was tagged 'both' (individuals,
--     unknowns, partnerships, NULL types), retarget to 'multi_source'.
UPDATE enrichment_queue q
SET enrichment_type = 'multi_source', attempts = 0
FROM entities e
WHERE q.entity_id = e.id
  AND q.enrichment_type = 'both'
  AND COALESCE(e.entity_type, 'unknown') NOT IN ('llc', 'corporation', 'management_company');

-- 2e. Safety net: drop any 'both' rows that survived (orphaned, no entity).
DELETE FROM enrichment_queue WHERE enrichment_type = 'both';

-- 2f. New composite UNIQUE constraint.
ALTER TABLE enrichment_queue
    ADD CONSTRAINT enrichment_queue_entity_type_unique UNIQUE(entity_id, enrichment_type);

-- 2g. Index tailored to the new batch-fetch pattern
--     (get_enrichment_batch filters by enrichment_type + attempts < 3,
--     orders by priority then created_at).
CREATE INDEX IF NOT EXISTS idx_enrich_queue_type_priority
    ON enrichment_queue(enrichment_type, priority, created_at)
    WHERE attempts < 3;

-- ============================================================
-- Part 3 — Drop unused views
-- (User confirmed broker_pitch_list is still used; the others are not.)
-- ============================================================
DROP VIEW IF EXISTS landlord_profiles;
DROP VIEW IF EXISTS unpierced_llcs;
DROP VIEW IF EXISTS crm_export;

COMMIT;
