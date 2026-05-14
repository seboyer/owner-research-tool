-- ============================================================
-- Owner Research Tool — Supabase Schema
-- Run this in your Supabase SQL editor to initialize the DB.
-- ============================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- for fuzzy text matching

-- ============================================================
-- PROPERTIES
-- Represents individual NYC tax lots (identified by BBL)
-- ============================================================
CREATE TABLE IF NOT EXISTS properties (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bbl             TEXT UNIQUE,           -- Borough-Block-Lot (e.g. "1000010001")
    borough         TEXT,
    block           TEXT,
    lot             TEXT,
    address         TEXT,
    house_number    TEXT,
    street_name     TEXT,
    zip_code        TEXT,
    unit_count      INTEGER,
    building_class  TEXT,
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    hpd_reg_id      TEXT,                  -- HPD Registration ID if applicable
    raw_data        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_properties_bbl     ON properties(bbl);
CREATE INDEX IF NOT EXISTS idx_properties_zip     ON properties(zip_code);
CREATE INDEX IF NOT EXISTS idx_properties_borough ON properties(borough);

-- ============================================================
-- ENTITIES
-- A landlord, management company, LLC, or individual owner.
-- ============================================================
CREATE TABLE IF NOT EXISTS entities (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name                TEXT NOT NULL,
    normalized_name     TEXT,              -- lowercased, stripped for dedup
    entity_type         TEXT,              -- 'llc', 'corporation', 'individual', 'management_company', 'partnership'
    is_building_llc     BOOLEAN DEFAULT FALSE,  -- TRUE = likely a single-building LLC
    is_pierced          BOOLEAN DEFAULT FALSE,  -- TRUE = we've attempted to find the real owner
    dos_id              TEXT,              -- NYS DOS entity ID
    opencorporates_url  TEXT,
    registered_agent    TEXT,
    registered_agent_address TEXT,
    formation_date      DATE,
    address             TEXT,
    city                TEXT,
    state               TEXT DEFAULT 'NY',
    zip_code            TEXT,
    portfolio_size      INTEGER DEFAULT 0, -- # of buildings owned/managed
    wow_portfolio_id    TEXT,              -- Who Owns What portfolio identifier
    enrichment_status   TEXT DEFAULT 'pending', -- 'pending', 'in_progress', 'done', 'failed'
    zoominfo_id         TEXT,
    raw_data            JSONB,
    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entities_name           ON entities USING gin(normalized_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_entities_type           ON entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_is_building_llc ON entities(is_building_llc);
CREATE INDEX IF NOT EXISTS idx_entities_enrichment     ON entities(enrichment_status);
CREATE INDEX IF NOT EXISTS idx_entities_dos_id         ON entities(dos_id);

-- ============================================================
-- ENTITY RELATIONSHIPS
-- Connects LLCs to parent companies, owners to managed buildings, etc.
-- ============================================================
CREATE TABLE IF NOT EXISTS entity_relationships (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    child_entity_id     UUID REFERENCES entities(id) ON DELETE CASCADE,
    parent_entity_id    UUID REFERENCES entities(id) ON DELETE CASCADE,
    relationship_type   TEXT,  -- 'owned_by', 'managed_by', 'registered_agent_of', 'affiliated_with'
    confidence          FLOAT DEFAULT 0.5,  -- 0.0 to 1.0
    source              TEXT,  -- 'opencorporates', 'ai_inference', 'hpd', 'acris', 'wow'
    evidence            TEXT,  -- human-readable explanation
    raw_data            JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(child_entity_id, parent_entity_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_entity_rel_child  ON entity_relationships(child_entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_rel_parent ON entity_relationships(parent_entity_id);

-- ============================================================
-- PROPERTY OWNERSHIP / MANAGEMENT
-- Links properties to entities (who owns or manages them)
-- ============================================================
CREATE TABLE IF NOT EXISTS property_roles (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID REFERENCES properties(id) ON DELETE CASCADE,
    entity_id   UUID REFERENCES entities(id) ON DELETE CASCADE,
    role        TEXT,  -- 'owner', 'manager', 'agent', 'corporate_owner', 'head_officer'
    source      TEXT,  -- 'hpd', 'acris', 'wow', 'manual'
    is_current  BOOLEAN DEFAULT TRUE,
    start_date  DATE,
    end_date    DATE,
    raw_data    JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(property_id, entity_id, role, source)
);

CREATE INDEX IF NOT EXISTS idx_property_roles_property ON property_roles(property_id);
CREATE INDEX IF NOT EXISTS idx_property_roles_entity   ON property_roles(entity_id);
CREATE INDEX IF NOT EXISTS idx_property_roles_current  ON property_roles(is_current);

-- ============================================================
-- CONTACTS
-- People associated with an entity (from HPD, Zoominfo, web)
-- ============================================================
CREATE TABLE IF NOT EXISTS contacts (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id               UUID REFERENCES entities(id) ON DELETE CASCADE,
    first_name              TEXT,
    last_name               TEXT,
    full_name               TEXT,
    title                   TEXT,
    email                   TEXT,
    email_verified          BOOLEAN DEFAULT FALSE,
    phone                   TEXT,
    phone_type              TEXT,  -- 'direct', 'mobile', 'office', 'hq'
    linkedin_url            TEXT,
    source                  TEXT,  -- 'hpd', 'zoominfo', 'web_scrape', 'acris_pdf'
    confidence              FLOAT DEFAULT 0.5,
    is_primary              BOOLEAN DEFAULT FALSE,
    zoominfo_id             TEXT,
    enrichment_complete_at  TIMESTAMPTZ,  -- stamped once all 3 prongs have succeeded
    raw_data                JSONB,
    created_at              TIMESTAMPTZ DEFAULT NOW(),
    updated_at              TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(entity_id, email)
);

CREATE INDEX IF NOT EXISTS idx_contacts_entity                  ON contacts(entity_id);
CREATE INDEX IF NOT EXISTS idx_contacts_email                   ON contacts(email);
CREATE INDEX IF NOT EXISTS idx_contacts_enrichment_complete_at  ON contacts(enrichment_complete_at);

-- ============================================================
-- DATA SOURCES SEEN
-- Track raw external records to avoid reprocessing
-- ============================================================
CREATE TABLE IF NOT EXISTS seen_records (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source      TEXT NOT NULL,   -- 'hpd_contact', 'acris_party', 'wow', 'opencorporates'
    external_id TEXT NOT NULL,   -- the source's unique ID for the record
    checksum    TEXT,            -- hash of key fields to detect changes
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_seen_records_source ON seen_records(source, external_id);

-- ============================================================
-- INGESTION LOG
-- One row per pipeline run per source for monitoring
-- ============================================================
CREATE TABLE IF NOT EXISTS ingestion_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source              TEXT NOT NULL,
    run_started_at      TIMESTAMPTZ DEFAULT NOW(),
    run_finished_at     TIMESTAMPTZ,
    records_fetched     INTEGER DEFAULT 0,
    records_created     INTEGER DEFAULT 0,
    records_updated     INTEGER DEFAULT 0,
    records_skipped     INTEGER DEFAULT 0,
    status              TEXT DEFAULT 'running',  -- 'running', 'success', 'failed'
    error_message       TEXT,
    metadata            JSONB
);

-- ============================================================
-- ENRICHMENT QUEUE
-- Work backlog for the enrichment stages. One row per (entity, type)
-- — an entity that needs multiple kinds of work has multiple rows.
-- Each batch processor (llc_piercer, zoominfo, multi_source) pulls
-- rows of its own enrichment_type, calls mark_enrichment_done /
-- mark_enrichment_failed when finished.
-- ============================================================
CREATE TABLE IF NOT EXISTS enrichment_queue (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id       UUID REFERENCES entities(id) ON DELETE CASCADE,
    priority        INTEGER DEFAULT 5,   -- 1 (highest) to 10 (lowest)
    enrichment_type TEXT NOT NULL,       -- 'llc_pierce', 'zoominfo', 'multi_source'
    attempts        INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT enrichment_queue_entity_type_unique UNIQUE(entity_id, enrichment_type)
);

CREATE INDEX IF NOT EXISTS idx_enrich_queue_priority      ON enrichment_queue(priority, created_at);
CREATE INDEX IF NOT EXISTS idx_enrich_queue_type_priority ON enrichment_queue(enrichment_type, priority, created_at) WHERE attempts < 3;

-- ============================================================
-- HELPFUL VIEWS
-- broker_pitch_list is defined in migrations/002_contact_enrichment.sql.
-- The earlier landlord_profiles / unpierced_llcs / crm_export views were
-- dropped in migration 003 (unused).
-- ============================================================

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_properties_updated_at  BEFORE UPDATE ON properties  FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_entities_updated_at    BEFORE UPDATE ON entities    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_contacts_updated_at    BEFORE UPDATE ON contacts    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_seen_records_updated_at BEFORE UPDATE ON seen_records FOR EACH ROW EXECUTE FUNCTION update_updated_at();
