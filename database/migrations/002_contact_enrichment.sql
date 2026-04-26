-- ============================================================
-- Migration 002 — Contact Enrichment
--
-- Adds fields needed by the 3-prong contact enrichment system:
--   Prong 1: Signer → phone/email + owner operating company + mgmt company
--   Prong 2: Signer's network (coworkers, co-owners, co-officers)
--   Prong 3: Other contacts for the building (owners + managers)
-- ============================================================

-- ------------------------------------------------------------
-- ENTITIES: distinguish the *public-facing operating company*
-- from the single-purpose LLC that holds title.
-- ------------------------------------------------------------
ALTER TABLE entities
    ADD COLUMN IF NOT EXISTS role_category TEXT,          -- 'owner_operating', 'management', 'title_holding_llc', 'agent', 'other'
    ADD COLUMN IF NOT EXISTS website         TEXT,
    ADD COLUMN IF NOT EXISTS domain          TEXT,        -- bare domain, e.g. 'acme-realty.com'
    ADD COLUMN IF NOT EXISTS hq_phone        TEXT,
    ADD COLUMN IF NOT EXISTS hq_email        TEXT,
    ADD COLUMN IF NOT EXISTS linkedin_url    TEXT;

CREATE INDEX IF NOT EXISTS idx_entities_role_category ON entities(role_category);
CREATE INDEX IF NOT EXISTS idx_entities_domain        ON entities(domain);

-- ------------------------------------------------------------
-- CONTACTS: track WHICH prong found each contact, their role
-- relative to the seed signer, and which building/signer they
-- trace back to so we can present them to the broker with context.
-- ------------------------------------------------------------
ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS prong             INTEGER,   -- 1, 2, or 3
    ADD COLUMN IF NOT EXISTS role_category     TEXT,      -- 'owner', 'management', 'both', 'unknown'
    ADD COLUMN IF NOT EXISTS network_role      TEXT,      -- 'signer', 'coworker', 'co_owner', 'co_officer', 'building_contact', 'head_officer', 'registered_agent'
    ADD COLUMN IF NOT EXISTS seed_signer_id    UUID REFERENCES contacts(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS seed_property_id  UUID REFERENCES properties(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS seed_building_llc_id UUID REFERENCES entities(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS evidence          TEXT,      -- human-readable "why we think this is a valid contact"
    ADD COLUMN IF NOT EXISTS sources           JSONB,     -- array of {source, url, found_at} objects
    ADD COLUMN IF NOT EXISTS cost_cents        INTEGER DEFAULT 0; -- cumulative $ spent to find this contact

CREATE INDEX IF NOT EXISTS idx_contacts_prong           ON contacts(prong);
CREATE INDEX IF NOT EXISTS idx_contacts_seed_signer     ON contacts(seed_signer_id);
CREATE INDEX IF NOT EXISTS idx_contacts_seed_property   ON contacts(seed_property_id);
CREATE INDEX IF NOT EXISTS idx_contacts_role_category   ON contacts(role_category);
CREATE INDEX IF NOT EXISTS idx_contacts_network_role    ON contacts(network_role);

-- ------------------------------------------------------------
-- CONTACT ENRICHMENT RUNS
-- One row per (signer, prong) attempt. Lets us avoid redoing
-- expensive lookups and tracks cumulative spend.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contact_enrichment_runs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signer_contact_id UUID REFERENCES contacts(id) ON DELETE CASCADE,
    building_llc_id   UUID REFERENCES entities(id) ON DELETE CASCADE,
    property_id       UUID REFERENCES properties(id) ON DELETE CASCADE,
    prong             INTEGER NOT NULL,
    cost_tier         TEXT,                -- 'free', 'budget', 'standard', 'premium'
    sources_attempted JSONB,               -- array of source names tried
    sources_succeeded JSONB,
    contacts_found    INTEGER DEFAULT 0,
    companies_found   INTEGER DEFAULT 0,
    cost_cents        INTEGER DEFAULT 0,
    started_at        TIMESTAMPTZ DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    status            TEXT DEFAULT 'running',  -- 'running', 'success', 'failed', 'skipped_cached'
    error_message     TEXT,
    raw_data          JSONB
);

CREATE INDEX IF NOT EXISTS idx_cer_signer   ON contact_enrichment_runs(signer_contact_id);
CREATE INDEX IF NOT EXISTS idx_cer_building ON contact_enrichment_runs(building_llc_id);
CREATE INDEX IF NOT EXISTS idx_cer_prong    ON contact_enrichment_runs(prong);

-- Unique-ish guard so we don't re-run the same prong for the same signer within N days
-- (enforced at application layer using the finished_at timestamp)

-- ------------------------------------------------------------
-- ENTITY RELATIONSHIPS: add 'operates_as', 'manages', 'employs'
-- so the prong outputs map cleanly to graph edges.
-- (No schema change needed — just document the new rel_types.)
--
-- Allowed relationship_type values:
--   'owned_by'              (existing)
--   'managed_by'            (existing)
--   'registered_agent_of'   (existing)
--   'affiliated_with'       (existing)
--   'operates_as'           NEW — title LLC operates under this public brand
--   'manages'               NEW — mgmt co manages this title LLC / building
--   'employs'               NEW — operating company employs this contact's person entity
--   'co_officer_with'       NEW — two individuals are co-officers on an LLC
-- ------------------------------------------------------------

-- ------------------------------------------------------------
-- Helpful view: broker-ready pitch list
-- One row per (property, signer) with both the owner-side contact
-- and the management-side contact when we have them.
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW broker_pitch_list AS
SELECT
    p.address                AS property_address,
    p.borough,
    p.bbl,
    building_llc.name        AS title_llc,
    signer.full_name         AS mortgage_signer,
    signer.title             AS signer_title,
    signer.email             AS signer_email,
    signer.phone             AS signer_phone,
    owner_co.name            AS owner_operating_company,
    owner_co.hq_phone        AS owner_phone,
    owner_co.website         AS owner_website,
    mgmt_co.name             AS management_company,
    mgmt_co.hq_phone         AS mgmt_phone,
    mgmt_co.website          AS mgmt_website,
    (SELECT COUNT(*) FROM contacts c2
       WHERE c2.seed_signer_id = signer.id AND c2.prong = 2) AS network_contacts,
    (SELECT COUNT(*) FROM contacts c3
       WHERE c3.seed_property_id = p.id AND c3.prong = 3) AS other_building_contacts
FROM properties p
LEFT JOIN property_roles pr_own ON pr_own.property_id = p.id AND pr_own.role = 'owner' AND pr_own.is_current
LEFT JOIN entities building_llc ON building_llc.id = pr_own.entity_id
LEFT JOIN contacts signer ON signer.seed_building_llc_id = building_llc.id AND signer.network_role = 'signer'
LEFT JOIN entity_relationships er_op ON er_op.child_entity_id = building_llc.id AND er_op.relationship_type = 'operates_as'
LEFT JOIN entities owner_co ON owner_co.id = er_op.parent_entity_id
LEFT JOIN entity_relationships er_mg ON er_mg.child_entity_id = building_llc.id AND er_mg.relationship_type = 'managed_by'
LEFT JOIN entities mgmt_co ON mgmt_co.id = er_mg.parent_entity_id;
