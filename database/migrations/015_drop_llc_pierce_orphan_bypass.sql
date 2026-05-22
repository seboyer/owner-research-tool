-- Migration 015 — Remove the llc_pierce "no property_roles" bypass arm.
--
-- Migration 014 kept a bypass arm for enrichment_type='llc_pierce' on the
-- premise that an LLC may legitimately have no property_roles yet because
-- it "hasn't been pierced / linked." That premise was wrong: piercing
-- discovers the human signer, it does not link properties. Property links
-- come from the ACRIS deed / HPD ingest path, which writes the
-- property_role atomically with the entity row.
--
-- Effect of the bypass: 72 orphan building-LLC entities (no property_role,
-- no dos_id, no registered_agent, no address — created 2026-05-19) sat at
-- the head of the FIFO queue and consumed the daily piercing batch with
-- a 0/72 success rate. strategy_acris_pdf and strategy_wow_portfolio
-- both require a BBL via property_roles; without it they short-circuit to
-- False, leaving only the agentic web-search fallback, which is too weak
-- for an unfurnished LLC name.
--
-- Fix: require a current property_role for every queue type. Orphan LLCs
-- now stay invisible to get_enrichment_batch until a property is linked.
-- New ACRIS-deed-ingested LLCs still flow through normally because their
-- property_role is written in the same ingest pass.

BEGIN;

CREATE OR REPLACE VIEW allowed_enrichment_queue AS
SELECT q.*
FROM enrichment_queue q
WHERE
    EXISTS (
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
