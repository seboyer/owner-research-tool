-- Migration 021 — zapier_enriched_contacts: add owned-LLC name as a fallback
--
-- Most contact_company values still echo person names because very few LLCs
-- in the DB have an 'operates_as' edge (operating company enrichment hasn't
-- run widely).  In that case both employer.name and llc_chain.name are NULL
-- and the COALESCE falls all the way through to e.name (the person).
--
-- This migration adds one more LATERAL — `owned_llc` — which resolves the
-- LLC name itself when the chain to an operating company is broken.  For
-- small landlords whose LLC IS effectively the operating entity, that's
-- the correct answer; for larger operators it's at least a corporate-shaped
-- name a sales rep can recognise (versus "SMITH, JOHN").
--
-- Resolution order for contact_company:
--   1. Direct 'employs' edge
--   2. LLC chain: person owns LLC, LLC operates_as company
--   3. The LLC the person owns                          (NEW)
--   4. The entity itself (genuinely a person fallback)

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
    COALESCE(employer.name,        llc_chain.name,        owned_llc.name,        e.name)        AS contact_company,
    COALESCE(employer.entity_type, llc_chain.entity_type, owned_llc.entity_type, e.entity_type) AS contact_company_type,
    COALESCE(employer.website,     llc_chain.website,     owned_llc.website,     e.website)     AS contact_company_website,
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
-- Tier A: direct 'employs' edge
LEFT JOIN LATERAL (
    SELECT emp.id, emp.name, emp.entity_type, emp.website
    FROM entity_relationships er
    JOIN entities emp ON emp.id = er.parent_entity_id
    WHERE er.child_entity_id   = c.entity_id
      AND er.relationship_type = 'employs'
    ORDER BY er.created_at DESC
    LIMIT 1
) employer ON TRUE
-- Tier B: LLC chain — person owns LLC, LLC operates_as company
LEFT JOIN LATERAL (
    SELECT op.id, op.name, op.entity_type, op.website
    FROM entity_relationships er_own
    JOIN entity_relationships er_op
      ON er_op.child_entity_id   = er_own.child_entity_id
     AND er_op.relationship_type = 'operates_as'
    JOIN entities op ON op.id = er_op.parent_entity_id
    WHERE er_own.parent_entity_id = c.entity_id
      AND er_own.relationship_type = 'owned_by'
    ORDER BY er_op.created_at DESC
    LIMIT 1
) llc_chain ON TRUE
-- Tier C: the LLC itself (when the chain to an operating company is broken)
LEFT JOIN LATERAL (
    SELECT llc.id, llc.name, llc.entity_type, llc.website
    FROM entity_relationships er_own
    JOIN entities llc ON llc.id = er_own.child_entity_id
    WHERE er_own.parent_entity_id  = c.entity_id
      AND er_own.relationship_type = 'owned_by'
      AND llc.entity_type IN ('llc', 'corporation', 'management_company', 'partnership')
    ORDER BY er_own.created_at DESC
    LIMIT 1
) owned_llc ON TRUE
LEFT JOIN entities bllc
    ON bllc.id = c.seed_building_llc_id
LEFT JOIN entity_relationships er_op
    ON er_op.child_entity_id   = c.seed_building_llc_id
   AND er_op.relationship_type = 'operates_as'
LEFT JOIN entities owner_co
    ON owner_co.id = er_op.parent_entity_id
LEFT JOIN entity_relationships er_mg
    ON er_mg.child_entity_id   = c.seed_building_llc_id
   AND er_mg.relationship_type = 'managed_by'
LEFT JOIN entities mgmt_co
    ON mgmt_co.id = er_mg.parent_entity_id
LEFT JOIN LATERAL (
    SELECT pr.property_id AS pid
    FROM property_roles pr
    WHERE pr.entity_id  = c.seed_building_llc_id
      AND pr.is_current = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) bllc_prop ON TRUE
LEFT JOIN LATERAL (
    SELECT pr.property_id AS pid
    FROM property_roles pr
    WHERE pr.entity_id  = c.entity_id
      AND pr.is_current = TRUE
    ORDER BY pr.created_at DESC
    LIMIT 1
) entity_prop ON TRUE
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
-- Unchanged from migration 020.
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
