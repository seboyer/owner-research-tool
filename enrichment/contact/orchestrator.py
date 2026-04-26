"""
enrichment/contact/orchestrator.py — Runs the 3 prongs for a signer.

Public API:
    enrich_signer(signer_contact_id)   # run all 3 prongs for one signer
    enrich_pending_signers(limit)      # batch: find signers without enrichment runs

Persistence:
    - CompanyHits  → entities table (upsert by normalized name, with role_category)
                     + entity_relationships edges (operates_as, managed_by, manages)
    - ContactHits  → contacts table (with prong, network_role, seed_signer_id, etc.)
    - Each prong run → contact_enrichment_runs row (cost, sources, stats)
"""

import asyncio
import traceback
from datetime import datetime, timezone
from typing import Optional

import structlog

from database.client import (
    db, upsert_entity, upsert_contact, upsert_relationship, update_entity,
    normalize_name,
)
from .cost_tier import tier_for_portfolio_size, CostTier
from .models import Signer, ProngResult, CompanyHit, ContactHit
from . import prong1_signer, prong2_network, prong3_building

log = structlog.get_logger(__name__)

# How long a prong result stays fresh before we re-run it (days)
STALE_DAYS = 90


# ============================================================
# Public API
# ============================================================

def enrich_signer(signer_contact_id: str, force: bool = False) -> dict:
    """Run all 3 prongs for one signer. Returns a stats dict."""
    signer = _load_signer(signer_contact_id)
    if signer is None:
        log.warn("enrich_signer.signer_not_found", contact_id=signer_contact_id)
        return {"error": "signer not found"}

    tier = tier_for_portfolio_size(signer.portfolio_size)
    log.info("enrich_signer.start",
             signer=signer.full_name, llc=signer.building_llc_name,
             tier=tier.name, portfolio=signer.portfolio_size)

    results = asyncio.run(_run_all_prongs(signer, tier, force=force))

    stats = {"signer": signer.full_name, "tier": tier.name}
    for r in results:
        stats[f"prong{r.prong}_companies"] = len(r.companies)
        stats[f"prong{r.prong}_contacts"] = len(r.contacts)
        stats[f"prong{r.prong}_cost_cents"] = r.cost_cents
        _persist_prong_result(signer, r)

    # Stamp enrichment_complete_at once all 3 prongs have a successful run.
    # The null guard makes this idempotent — safe to call on re-runs.
    _stamp_enrichment_complete(signer)

    log.info("enrich_signer.done", **stats)
    return stats


def enrich_pending_signers(limit: int = 20) -> dict:
    """Find signers without recent enrichment runs and process them."""
    signers = _find_pending_signers(limit=limit)
    log.info("enrich_pending_signers.start", count=len(signers))
    totals = {"processed": 0, "contacts_added": 0, "companies_added": 0}
    for i, s in enumerate(signers):
        stats = enrich_signer(s["id"])
        totals["processed"] += 1
        for k, v in stats.items():
            if k.endswith("_contacts"):
                totals["contacts_added"] += v or 0
            if k.endswith("_companies"):
                totals["companies_added"] += v or 0
        # Sleep between signers: Claude Sonnet has a 30k token/min rate limit.
        # Each web-search research call can burn 30-40k tokens across its 8 searches,
        # spanning well over a minute. A 90s gap ensures the window has fully reset
        # before the next signer's call starts.
        if i < len(signers) - 1:
            import time
            time.sleep(90)
    return totals


# ============================================================
# Internal: run prongs concurrently
# ============================================================

async def _run_all_prongs(signer: Signer, tier: CostTier, force: bool) -> list[ProngResult]:
    tasks = []
    if force or _needs_rerun(signer.contact_id, 1):
        tasks.append(prong1_signer.run(signer, tier))
    if force or _needs_rerun(signer.contact_id, 2):
        tasks.append(prong2_network.run(signer, tier))
    if force or _needs_rerun(signer.contact_id, 3):
        tasks.append(prong3_building.run(signer, tier))

    if not tasks:
        log.info("enrich_signer.all_prongs_cached", signer=signer.full_name)
        return []

    results = []
    for r in await asyncio.gather(*tasks, return_exceptions=True):
        if isinstance(r, Exception):
            log.error("prong.exception",
                      error=str(r),
                      traceback="".join(traceback.format_exception(type(r), r, r.__traceback__)))
            continue
        results.append(r)
    return results


# ============================================================
# Internal: loading + caching
# ============================================================

def _load_signer(contact_id: str) -> Optional[Signer]:
    """Build a Signer from a contacts row + associated entity & property.

    For ACRIS PDF signers the contact's entity is the *individual*, not the LLC.
    The property is linked to the LLC via entity_relationships (individual →
    owned_by → LLC → property_roles). We walk that chain to resolve the BBL.
    """
    res = db().table("contacts").select("*, entities!contacts_entity_id_fkey(*)").eq("id", contact_id).limit(1).execute()
    if not res.data:
        return None
    row = res.data[0]
    entity = row.get("entities") or {}
    entity_id = entity.get("id")

    prop_row = None
    llc_entity = entity  # default: entity IS the LLC

    if entity_id:
        # First try: direct property link (works when entity IS the LLC/owner)
        pr = db().table("property_roles")\
            .select("property_id, properties(*)")\
            .eq("entity_id", entity_id)\
            .eq("role", "owner")\
            .limit(1)\
            .execute()
        if pr.data:
            prop_row = pr.data[0].get("properties") or {}

        # Second try: walk entity_relationships — signer is an individual whose
        # LLC is the child in an "owned_by" relationship.
        # entity_relationships: child_entity_id=LLC, parent_entity_id=individual
        # So we need the reverse: find LLCs where THIS individual is the parent.
        if not prop_row:
            rels = db().table("entity_relationships")\
                .select("child_entity_id, entities!entity_relationships_child_entity_id_fkey(id, name, portfolio_size)")\
                .eq("parent_entity_id", entity_id)\
                .eq("relationship_type", "owned_by")\
                .limit(1)\
                .execute()
            if rels.data:
                llc_entity = rels.data[0].get("entities") or {}
                llc_id = llc_entity.get("id")
                if llc_id:
                    pr2 = db().table("property_roles")\
                        .select("property_id, properties(*)")\
                        .eq("entity_id", llc_id)\
                        .eq("role", "owner")\
                        .limit(1)\
                        .execute()
                    if pr2.data:
                        prop_row = pr2.data[0].get("properties") or {}

    return Signer(
        contact_id=row["id"],
        full_name=row.get("full_name") or "",
        title=row.get("title"),
        building_llc_id=llc_entity.get("id") or entity_id,
        building_llc_name=llc_entity.get("name") or entity.get("name") or "",
        property_id=(prop_row or {}).get("id"),
        bbl=(prop_row or {}).get("bbl"),
        portfolio_size=llc_entity.get("portfolio_size") or entity.get("portfolio_size") or 1,
        email=row.get("email"),
        phone=row.get("phone"),
    )


def _needs_rerun(signer_contact_id: str, prong: int) -> bool:
    """True if this signer+prong has never run OR was last run > STALE_DAYS ago."""
    res = db().table("contact_enrichment_runs")\
        .select("finished_at, status")\
        .eq("signer_contact_id", signer_contact_id)\
        .eq("prong", prong)\
        .eq("status", "success")\
        .order("finished_at", desc=True)\
        .limit(1)\
        .execute()
    if not res.data:
        return True
    finished = res.data[0].get("finished_at")
    if not finished:
        return True
    last = datetime.fromisoformat(finished.replace("Z", "+00:00"))
    age_days = (datetime.now(timezone.utc) - last).days
    return age_days >= STALE_DAYS


def _find_pending_signers(limit: int) -> list[dict]:
    """Contacts with network_role='signer' that need at least one prong run."""
    # Simple approach: pull recent signers, filter application-side.
    res = db().table("contacts")\
        .select("id, full_name")\
        .eq("network_role", "signer")\
        .order("created_at", desc=True)\
        .limit(limit * 3)\
        .execute()
    pending = []
    for row in res.data or []:
        if any(_needs_rerun(row["id"], p) for p in (1, 2, 3)):
            pending.append(row)
        if len(pending) >= limit:
            break
    return pending


# ============================================================
# Internal: persistence
# ============================================================

def _persist_prong_result(signer: Signer, r: ProngResult):
    """Write CompanyHits, ContactHits, and the run row to Supabase."""
    run_id = _insert_run(signer, r, status="running")

    # --- Companies → entities + relationships -----------------
    for co in r.companies:
        entity_type = _company_type_for(co.role_category)
        extra = {
            "role_category": co.role_category,
            "website": co.website,
            "domain": co.domain,
            "hq_phone": co.phone,
            "hq_email": co.email,
            "linkedin_url": co.linkedin_url,
        }
        extra = {k: v for k, v in extra.items() if v is not None}
        if co.address:
            extra["address"] = co.address
        entity_id = upsert_entity(co.name, entity_type, extra=extra)

        rel_type = _relationship_for(co.role_category)
        if rel_type and signer.building_llc_id and entity_id != signer.building_llc_id:
            upsert_relationship(
                child_entity_id=signer.building_llc_id,
                parent_entity_id=entity_id,
                rel_type=rel_type,
                source=co.source,
                confidence=co.confidence,
                evidence=co.evidence,
            )

    # --- Contacts → contacts table ----------------------------
    for ct in r.contacts:
        # Resolve the entity this contact belongs to (company they work for)
        target_entity_id = None
        if ct.company_name:
            target_entity_id = upsert_entity(
                ct.company_name,
                _company_type_for(ct.role_category) or "corporation",
                extra={"role_category": _company_role_for_contact(ct)},
            )
        # Fallback: attach to signer's building LLC
        if not target_entity_id:
            target_entity_id = signer.building_llc_id
        if not target_entity_id:
            continue

        upsert_contact(target_entity_id, {
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
            "prong":               r.prong,
            "network_role":        ct.network_role,
            "role_category":       ct.role_category,
            "seed_signer_id":      signer.contact_id,
            "seed_property_id":    signer.property_id,
            "seed_building_llc_id": signer.building_llc_id,
            "evidence":            ct.evidence,
            "sources":             [{"source": ct.source, "url": ct.source_url}] if ct.source_url else None,
            "cost_cents":          ct.cost_cents,
        })

    _finish_run(run_id, r, status=("failed" if r.error else "success"))


def _stamp_enrichment_complete(signer: Signer):
    """Set enrichment_complete_at on the signer contact once all 3 prongs have succeeded.

    Checks _needs_rerun for all three prongs — if any still need a run (never
    succeeded or cache expired) we skip. The .is_("enrichment_complete_at", "null")
    filter makes the update a no-op if the column was already stamped on a prior run.
    """
    all_done = all(not _needs_rerun(signer.contact_id, p) for p in (1, 2, 3))
    if not all_done:
        log.info("enrich_signer.enrichment_incomplete", signer=signer.full_name)
        return
    db().table("contacts").update({
        "enrichment_complete_at": datetime.now(timezone.utc).isoformat(),
    }).eq("id", signer.contact_id).is_("enrichment_complete_at", "null").execute()
    log.info("enrich_signer.enrichment_complete", signer=signer.full_name)


def _insert_run(signer: Signer, r: ProngResult, status: str) -> str:
    res = db().table("contact_enrichment_runs").insert({
        "signer_contact_id": signer.contact_id,
        "building_llc_id":   signer.building_llc_id,
        "property_id":       signer.property_id,
        "prong":             r.prong,
        "sources_attempted": r.sources_attempted,
        "status":            status,
    }).execute()
    return res.data[0]["id"]


def _finish_run(run_id: str, r: ProngResult, status: str):
    db().table("contact_enrichment_runs").update({
        "finished_at":       "now()",
        "sources_succeeded": r.sources_succeeded,
        "contacts_found":    len(r.contacts),
        "companies_found":   len(r.companies),
        "cost_cents":        r.cost_cents,
        "status":            status,
        "error_message":     r.error,
    }).eq("id", run_id).execute()


def _company_type_for(role_category: str) -> str:
    return {
        "owner_operating":    "corporation",
        "management":         "management_company",
        "title_holding_llc":  "llc",
        "agent":              "individual",
    }.get(role_category, "corporation")


def _relationship_for(role_category: str) -> Optional[str]:
    return {
        "owner_operating": "operates_as",
        "management":      "managed_by",
    }.get(role_category)


def _company_role_for_contact(ct: ContactHit) -> str:
    if ct.role_category == "owner":      return "owner_operating"
    if ct.role_category == "management": return "management"
    return "other"
