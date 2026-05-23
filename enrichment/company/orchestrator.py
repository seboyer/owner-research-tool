"""
enrichment/company/orchestrator.py — Corporate enrichment orchestrator.

Public API:
    enrich_company(entity_id, force=False)   # run cascade for one company
    run_batch(batch_size=None)               # drain 'company_enrich' queue

Persistence:
    - CompanyHits → entity website/domain/hq_phone/hq_email/linkedin_url/address
    - ContactHits → contacts table (seed_building_llc_id=entity_id, prong=NULL)
    - Run log   → company_enrichment_runs table (90-day cache)
"""

import asyncio
from datetime import UTC, datetime

import structlog

from database.client import (
    db,
    finish_ingestion_log,
    get_enrichment_batch,
    mark_enrichment_done,
    mark_enrichment_failed,
    start_ingestion_log,
    update_entity,
    upsert_contact,
)
from enrichment.contact.cost_tier import CostTier, tier_for_portfolio_size

from . import cascade
from .models import CompanyEnrichmentResult, CompanyEntity

log = structlog.get_logger(__name__)

STALE_DAYS = 90


# ============================================================
# Public API
# ============================================================

def enrich_company(entity_id: str, force: bool = False) -> dict:
    """Run the cascade for one company entity. Returns a stats dict."""
    company = _load_company(entity_id)
    if company is None:
        log.warning("enrich_company.not_found", entity_id=entity_id)
        return {"error": "entity not found"}

    if not force and not _needs_rerun(entity_id):
        log.info("enrich_company.cached", entity=company.name)
        return {"cached": True, "entity": company.name}

    tier = tier_for_portfolio_size(company.portfolio_size)
    log.info("enrich_company.start",
             entity=company.name, tier=tier.name, portfolio=company.portfolio_size)

    result = asyncio.run(cascade.run(company, tier))
    _persist_result(company, result, tier)

    stats = {
        "entity":             company.name,
        "tier":               tier.name,
        "contacts_found":     len(result.contacts),
        "companies_found":    len(result.companies),
        "cost_cents":         result.cost_cents,
        "sources_attempted":  result.sources_attempted,
        "sources_succeeded":  result.sources_succeeded,
    }
    log.info("enrich_company.done", **stats)
    return stats


async def run_batch(batch_size: int | None = None):
    """Drain the 'company_enrich' queue, respecting the per-run cost cap."""
    from config import config
    from pipeline.orchestrator import COST_PER_ENTITY, get_cost_tracker
    batch_size = batch_size or config.ENRICHMENT_BATCH_SIZE
    log_id = start_ingestion_log("company_enrichment")
    stats = {
        "records_fetched":      0,
        "records_created":      0,
        "records_skipped":      0,
        "records_no_match":     0,
        "cost_estimated_usd":   0.0,
        "stopped_by_cost_cap":  False,
    }
    tracker = get_cost_tracker()
    per_entity_cost = COST_PER_ENTITY.get("company_enrich", 2.00)

    try:
        batch_num = 0
        while True:
            if tracker.cap_hit:
                stats["stopped_by_cost_cap"] = True
                log.info("company_enrich.cost_cap_hit",
                         spent=tracker.total_spent, cap=tracker.cap_usd)
                break

            batch_num += 1
            queue_rows = get_enrichment_batch(
                enrichment_type="company_enrich", limit=batch_size
            )
            if not queue_rows:
                break

            stats["records_fetched"] += len(queue_rows)
            log.info("company_enrich.batch_iter", batch=batch_num, count=len(queue_rows))

            for row in queue_rows:
                entity = row.get("entities") or {}
                if not entity or not entity.get("id"):
                    continue
                entity_id = entity["id"]
                entity_name = entity.get("name", "")

                try:
                    if entity.get("enrichment_status") == "pending":
                        update_entity(entity_id, {"enrichment_status": "in_progress"})

                    company = _load_company(entity_id)
                    if company is None:
                        log.warning("company_enrich.load_failed", entity_id=entity_id)
                        mark_enrichment_failed(entity_id, "company_enrich", "entity not found")
                        stats["records_skipped"] += 1
                        continue

                    if _needs_rerun(entity_id):
                        tier = tier_for_portfolio_size(company.portfolio_size)
                        result = await cascade.run(company, tier)
                        _persist_result(company, result, tier)
                        mark_enrichment_done(entity_id, "company_enrich")
                        tracker.add("company_enrich", per_entity_cost)
                        if result.contacts or result.companies:
                            stats["records_created"] += 1
                        else:
                            stats["records_no_match"] += 1
                    else:
                        log.info("company_enrich.cached", entity=entity_name)
                        mark_enrichment_done(entity_id, "company_enrich")
                        stats["records_skipped"] += 1

                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                    log.error("company_enrich.entity_error", entity=entity_name, error=err)
                    mark_enrichment_failed(entity_id, "company_enrich", err)

                if tracker.cap_hit:
                    stats["stopped_by_cost_cap"] = True
                    log.info("company_enrich.cost_cap_hit_mid_batch",
                             spent=tracker.total_spent, cap=tracker.cap_usd)
                    break

                await asyncio.sleep(1.5)

            if tracker.cap_hit:
                break

        stats["cost_estimated_usd"] = round(tracker.stage_spent("company_enrich"), 2)
        finish_ingestion_log(log_id, stats)
        log.info("company_enrich.batch_complete", **stats, batches=batch_num)

    except Exception as e:
        stats["cost_estimated_usd"] = round(tracker.stage_spent("company_enrich"), 2)
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        log.error("company_enrich.batch_error", error=str(e))
        raise


# ============================================================
# Internal: loading
# ============================================================

def _load_company(entity_id: str) -> CompanyEntity | None:
    """Build a CompanyEntity by walking direct and indirect property links."""
    res = db().table("entities").select("*").eq("id", entity_id).single().execute()
    if not res.data:
        return None
    e = res.data
    bbls: set[str] = set()

    # Direct: this entity owns properties (title-holding LLCs)
    direct = (
        db().table("property_roles")
        .select("properties(bbl)")
        .eq("entity_id", entity_id)
        .execute()
        .data
    )
    for row in direct or []:
        bbl = (row.get("properties") or {}).get("bbl")
        if bbl:
            bbls.add(bbl)

    # Indirect: child LLCs of this company (managed_by, operates_as, owned_by)
    rels = (
        db().table("entity_relationships")
        .select("child_entity_id")
        .eq("parent_entity_id", entity_id)
        .in_("relationship_type", ["managed_by", "operates_as", "owned_by"])
        .execute()
        .data
    )
    child_ids = [r["child_entity_id"] for r in rels or []]
    if child_ids:
        # Supabase .in_ works on lists; chunk if needed (>100 ids is unusual)
        prs = (
            db().table("property_roles")
            .select("properties(bbl)")
            .in_("entity_id", child_ids[:100])
            .execute()
            .data
        )
        for row in prs or []:
            bbl = (row.get("properties") or {}).get("bbl")
            if bbl:
                bbls.add(bbl)

    # Persist corrected portfolio_size if we discovered more BBLs than recorded
    discovered = len(bbls)
    recorded = e.get("portfolio_size") or 0
    if discovered > recorded:
        update_entity(entity_id, {"portfolio_size": discovered})

    return CompanyEntity(
        entity_id=entity_id,
        name=e.get("name") or "",
        entity_type=e.get("entity_type"),
        role_category=e.get("role_category"),
        portfolio_size=max(recorded, discovered),
        bbls=list(bbls)[:100],
    )


def _needs_rerun(entity_id: str) -> bool:
    """True if this entity has never had a successful run OR it's > STALE_DAYS old."""
    res = (
        db().table("company_enrichment_runs")
        .select("finished_at, status")
        .eq("entity_id", entity_id)
        .eq("status", "success")
        .order("finished_at", desc=True)
        .limit(1)
        .execute()
    )
    if not res.data:
        return True
    finished = res.data[0].get("finished_at")
    if not finished:
        return True
    last = datetime.fromisoformat(finished.replace("Z", "+00:00"))
    age_days = (datetime.now(UTC) - last).days
    return age_days >= STALE_DAYS


# ============================================================
# Internal: persistence
# ============================================================

def _persist_result(
    company: CompanyEntity,
    result: CompanyEnrichmentResult,
    tier: CostTier,
):
    """Write company + contact hits and the run log row."""
    run_id = _insert_run(company, result, tier, status="running")

    # ── Update entity with any company-level data ──────────────────────
    # Pick the best CompanyHit (highest confidence) for entity fields
    if result.companies:
        best = max(result.companies, key=lambda c: c.confidence)
        entity_update: dict = {}
        for src_field, col in (
            ("website",      "website"),
            ("domain",       "domain"),
            ("phone",        "hq_phone"),
            ("email",        "hq_email"),
            ("linkedin_url", "linkedin_url"),
            ("address",      "address"),
        ):
            val = getattr(best, src_field, None)
            if val:
                entity_update[col] = val
        if entity_update:
            update_entity(company.entity_id, entity_update)

    # ── Upsert contacts ────────────────────────────────────────────────
    for ct in result.contacts:
        try:
            upsert_contact(company.entity_id, {
                "full_name":           ct.full_name,
                "first_name":          ct.first_name,
                "last_name":           ct.last_name,
                "title":               ct.title,
                "email":               ct.email,
                "phone":               ct.phone,
                "phone_type":          ct.phone_type,
                "linkedin_url":        ct.linkedin_url,
                "source":              ct.source,
                "confidence":          ct.confidence,
                # prong=NULL — these are company cascade contacts, not signer prong contacts
                "prong":               None,
                "network_role":        ct.network_role,
                "role_category":       ct.role_category,
                "seed_signer_id":      None,
                "seed_building_llc_id": company.entity_id,
                "evidence":            ct.evidence,
                "sources":             [{"source": ct.source, "url": ct.source_url}] if ct.source_url else None,
                "cost_cents":          ct.cost_cents,
            })
        except Exception as e:
            log.warning("persist.contact_failed",
                        entity=company.name, contact=ct.full_name, error=str(e))

    _finish_run(run_id, result, status=("failed" if result.error else "success"))


def _insert_run(
    company: CompanyEntity,
    result: CompanyEnrichmentResult,
    tier: CostTier,
    status: str,
) -> str:
    res = db().table("company_enrichment_runs").insert({
        "entity_id":          company.entity_id,
        "cost_tier":          tier.name,
        "sources_attempted":  result.sources_attempted,
        "status":             status,
    }).execute()
    return res.data[0]["id"]


def _finish_run(run_id: str, result: CompanyEnrichmentResult, status: str):
    db().table("company_enrichment_runs").update({
        "finished_at":       "now()",
        "sources_succeeded": result.sources_succeeded,
        "contacts_found":    len(result.contacts),
        "cost_cents":        result.cost_cents,
        "status":            status,
        "error_message":     result.error,
    }).eq("id", run_id).execute()
