-- Migration 018 — Zapier enriched contacts view
--
-- A flat, denormalised view used by the Zapier PostgreSQL trigger to push
-- newly-enriched contacts into the sales pipeline.  One row per enriched
-- record (person or company-level) with all relationship context: pierced
-- LLC, owner operating company, management company, and a representative
-- property address.
--
-- Recommended Zapier setup:
--   App:     PostgreSQL  (Supabase has no native Zapier trigger)
--   Trigger: New Row
--   Table:   zapier_enriched_contacts
--   ID col:  id           (UUID, for deduplication)
--   Order:   enriched_at  (timestamp, drives polling)
--
-- ── Address resolution ──────────────────────────────────────────────────────
-- Only the contact orchestrator (enrichment/contact/orchestrator.py) sets
-- seed_property_id explicitly.  The company cascade sets seed_building_llc_id
-- but not seed_property_id.  The 11 other paths (multi_source, acris_pdf,
-- llc_piercer, ingest/hpd, …) set neither.  Without fallbacks, most rows
-- would surface no address.
--
-- The view walks four tiers in priority order via COALESCE:
--   1. contacts.seed_property_id                 — contact orchestrator
--   2. seed_building_llc_id  → property_roles    — company cascade
--   3. entity_id             → property_roles    — contact attached directly
--                                                  to an LLC that has property
--   4. entity_id (parent)    → entity_relationships → child LLC
--                            → property_roles    — signer pattern (person
--                                                  entity is parent of the
--                                                  building LLC)
--
-- ── phone_type ──────────────────────────────────────────────────────────────
-- Most upstream code paths don't set contacts.phone_type.  We only infer a
-- type for sources whose phone output is unambiguous:
--   batchdata  → 'mobile'   (V3 skip trace returns mobile numbers)
--   hpd        → 'office'   (HPD registration phones are office lines)
-- ZoomInfo, Proxycurl, Apollo, Whitepages all return mixed phone types, so
-- we leave NULL rather than fabricate.  Fixing those upstream in
-- multi_source.py is tracked as a follow-up.

BEGIN;

DROP VIEW IF EXISTS zapier_enriched_contacts;

CREATE VIEW zapier_enriched_contacts AS

-- ── Part 1: Individual contacts with email or phone ─────────────────────────
SELECT
    'person'                          AS record_type,
    c.id                              AS id,
    c.updated_at                      AS enriched_at,
    c.full_name,
    c.first_name,
    c.last_name,
    c.title,
    c.network_role,
    c.email,
    c.phone,
    COALESCE(c.phone_type, CASE c.source
        WHEN 'batchdata' THEN 'mobile'
        WHEN 'hpd'       THEN 'office'
        ELSE NULL
    END)                              AS phone_type,
    c.source                          AS data_source,
    c.confidence,
    e.name                            AS contact_company,
    e.entity_type                     AS contact_company_type,
    e.website                         AS contact_company_website,
    bllc.name                         AS pierced_llc,
    owner_co.name                     AS owner_operating_company,
    owner_co.hq_phone                 AS owner_co_phone,
    owner_co.hq_email                 AS owner_co_email,
    owner_co.website                  AS owner_co_website,
    mgmt_co.name                      AS management_company,
    mgmt_co.hq_phone                  AS mgmt_co_phone,
    mgmt_co.hq_email                  AS mgmt_co_email,
    mgmt_co.website                   AS mgmt_co_website,
    p.address                         AS property_address,
    p.borough                         AS property_borough,
    p.bbl                             AS property_bbl,
    p.zip_code                        AS property_zip,
    p.unit_count                      AS property_units
FROM contacts c
LEFT JOIN entities e
    ON e.id = c.entity_id
LEFT JOIN entities bllc
    ON bllc.id = c.seed_building_llc_id
-- Owner operating company (building LLC operates_as this brand)
LEFT JOIN entity_relationships er_op
    ON er_op.child_entity_id   = c.seed_building_llc_id
   AND er_op.relationship_type = 'operates_as'
LEFT JOIN entities owner_co
    ON owner_co.id = er_op.parent_entity_id
-- Management company (building LLC managed_by this company)
LEFT JOIN entity_relationships er_mg
    ON er_mg.child_entity_id   = c.seed_building_llc_id
   AND er_mg.relationship_type = 'managed_by'
LEFT JOIN entities mgmt_co
    ON mgmt_co.id = er_mg.parent_entity_id
-- Address tier 2: representative property via the building LLC
LEFT JOIN LATERAL (
    SELECT pr.property_id AS pid
    FROM property_roles pr
    WHERE pr.entity_id  = c.seed_building_llc_id
      AND pr.is_current = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) bllc_prop ON TRUE
-- Address tier 3: contact's own entity_id directly has a property_role
LEFT JOIN LATERAL (
    SELECT pr.property_id AS pid
    FROM property_roles pr
    WHERE pr.entity_id  = c.entity_id
      AND pr.is_current = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) entity_prop ON TRUE
-- Address tier 4: signer pattern — entity_id is parent of a building LLC
LEFT JOIN LATERAL (
    SELECT pr.property_id AS pid
    FROM entity_relationships er
    JOIN property_roles pr ON pr.entity_id = er.child_entity_id
    WHERE er.parent_entity_id = c.entity_id
      AND pr.is_current       = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) child_prop ON TRUE
LEFT JOIN properties p
    ON p.id = COALESCE(
        c.seed_property_id,
        bllc_prop.pid,
        entity_prop.pid,
        child_prop.pid
    )
WHERE (c.email IS NOT NULL AND c.email <> '')
   OR (c.phone IS NOT NULL AND c.phone <> '')

UNION ALL

-- ── Part 2: Company-level contacts (entities with hq_email / hq_phone) ──────
SELECT
    'company'                         AS record_type,
    e.id                              AS id,
    e.updated_at                      AS enriched_at,
    e.name                            AS full_name,
    NULL::TEXT                        AS first_name,
    NULL::TEXT                        AS last_name,
    e.role_category                   AS title,
    e.role_category                   AS network_role,
    e.hq_email                        AS email,
    e.hq_phone                        AS phone,
    'hq'                              AS phone_type,
    'company_enrichment'              AS data_source,
    1.0::FLOAT                        AS confidence,
    e.name                            AS contact_company,
    e.entity_type                     AS contact_company_type,
    e.website                         AS contact_company_website,
    bllc.name                         AS pierced_llc,
    CASE WHEN e.role_category = 'owner_operating' THEN e.name     END AS owner_operating_company,
    CASE WHEN e.role_category = 'owner_operating' THEN e.hq_phone END AS owner_co_phone,
    CASE WHEN e.role_category = 'owner_operating' THEN e.hq_email END AS owner_co_email,
    CASE WHEN e.role_category = 'owner_operating' THEN e.website  END AS owner_co_website,
    CASE WHEN e.role_category = 'management'      THEN e.name     END AS management_company,
    CASE WHEN e.role_category = 'management'      THEN e.hq_phone END AS mgmt_co_phone,
    CASE WHEN e.role_category = 'management'      THEN e.hq_email END AS mgmt_co_email,
    CASE WHEN e.role_category = 'management'      THEN e.website  END AS mgmt_co_website,
    p.address                         AS property_address,
    p.borough                         AS property_borough,
    p.bbl                             AS property_bbl,
    p.zip_code                        AS property_zip,
    p.unit_count                      AS property_units
FROM entities e
LEFT JOIN LATERAL (
    SELECT er.child_entity_id AS llc_id
    FROM entity_relationships er
    WHERE er.parent_entity_id      = e.id
      AND er.relationship_type IN ('operates_as', 'managed_by', 'owned_by')
    ORDER BY er.created_at DESC
    LIMIT 1
) llc_rel ON TRUE
LEFT JOIN entities bllc
    ON bllc.id = llc_rel.llc_id
LEFT JOIN LATERAL (
    SELECT pr.property_id
    FROM property_roles pr
    WHERE pr.entity_id  = COALESCE(llc_rel.llc_id, e.id)
      AND pr.is_current = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) prop_rel ON TRUE
LEFT JOIN properties p
    ON p.id = prop_rel.property_id
WHERE e.entity_type IN ('corporation', 'management_company', 'llc', 'partnership')
  AND (
      (e.hq_email IS NOT NULL AND e.hq_email <> '')
   OR (e.hq_phone IS NOT NULL AND e.hq_phone <> '')
  );

COMMIT;
