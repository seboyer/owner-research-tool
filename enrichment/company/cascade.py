"""
enrichment/company/cascade.py — Multi-source waterfall for corporate entities.

Input:  CompanyEntity (entity_id, name, entity_type, role_category,
                       portfolio_size, bbls)
Output: CompanyEnrichmentResult

Tier waterfall (mirrors prong1_signer.py):

FREE:
  1. HPD cross-portfolio aggregation (hpd_portfolio.head_officers_for_company)
  2. HPD per-BBL contacts (hpd_building_contacts.contacts_for_bbl)
  3. Claude company web search (claude_company_research.research_company)

BUDGET (tier >= BUDGET):
  4. Google Places — fill website + phone + address
  5. Hunter — email domain search once we know the domain

STANDARD (tier >= STANDARD):
  6. Apollo org → people (apollo_org.apollo_org_search + apollo_org_people)

PREMIUM (tier >= PREMIUM):
  7. Zoominfo — existing enrich_entity_with_zoominfo
  8. Proxycurl — LinkedIn for top contacts missing a linkedin_url

Short-circuit guards:
  - Skip Apollo / Zoominfo if HPD + Hunter already produced >= 2 contacts
    with both phone and email.
  - Skip Proxycurl if all top contacts already have linkedin_url.
"""

import re

import structlog

from enrichment.contact.cost_tier import (
    BUDGET,
    PREMIUM,
    STANDARD,
    CostTier,
    tier_allows,
)
from enrichment.contact.filters import is_govt_entity
from enrichment.contact.sources import apollo_org as _apollo_org_src
from enrichment.contact.sources import claude_company_research as _claude_src
from enrichment.contact.sources import google_places as _google_places_src
from enrichment.contact.sources import hpd_building_contacts
from enrichment.contact.sources import hpd_portfolio as _hpd_portfolio_src
from enrichment.contact.sources import hunter as _hunter_src
from enrichment.contact.sources import proxycurl as _proxycurl_src

from .models import CompanyEnrichmentResult, CompanyEntity, CompanyHit, ContactHit

log = structlog.get_logger(__name__)


async def run(company: CompanyEntity, tier: CostTier) -> CompanyEnrichmentResult:
    """Run the full cascade for a company entity at the given cost tier."""
    r = CompanyEnrichmentResult(entity_id=company.entity_id)
    attempted, succeeded = r.sources_attempted, r.sources_succeeded

    # ── 1. HPD cross-portfolio aggregation (FREE) ─────────────────────────
    attempted.append("hpd_portfolio")
    hpd_portfolio_hits = await _hpd_portfolio_src.head_officers_for_company(company.name)
    if hpd_portfolio_hits:
        succeeded.append("hpd_portfolio")
        for h in hpd_portfolio_hits:
            _merge_contact(r, h)

    # ── 2. HPD per-BBL contacts (FREE) ────────────────────────────────────
    # Cap to first 10 BBLs to avoid excessive Socrata fanout
    bbl_sample = company.bbls[:10]
    if bbl_sample:
        attempted.append("hpd_bbl_contacts")
        bbl_hits: list[ContactHit] = []
        for bbl in bbl_sample:
            try:
                hits = await hpd_building_contacts.contacts_for_bbl(bbl)
                bbl_hits.extend(hits)
            except Exception as e:
                log.warning("cascade.hpd_bbl_failed", entity=company.name, bbl=bbl, error=str(e))
        # Keep only HeadOfficer/Officer/ManagingAgent rows that aren't govt
        relevant = [
            h for h in bbl_hits
            if h.title in ("HeadOfficer", "Officer", "ManagingAgent", "IndividualOwner")
            and not is_govt_entity(h.full_name)
        ]
        if relevant:
            succeeded.append("hpd_bbl_contacts")
            for h in relevant:
                _merge_contact(r, h)

    # ── 3. Claude company web search (FREE) ───────────────────────────────
    attempted.append("claude_company_research")
    # Derive human-readable addresses from BBLs (best-effort; cascade doesn't
    # have property rows, but passing the BBLs as context still helps Claude)
    addr_context = [f"BBL {b}" for b in company.bbls[:5]]
    web_data = await _claude_src.research_company(company.name, addresses=addr_context)
    if web_data:
        succeeded.append("claude_company_research")
        cos, cts = _claude_src.parse_to_hits(web_data, company.name)
        for c in cos:
            _merge_company(r, c)
        for ct in cts:
            _merge_contact(r, ct)

    # ── 4. Google Places — website + phone (BUDGET) ───────────────────────
    if tier_allows(tier, BUDGET):
        attempted.append("google_places")
        gp_hits = await _google_places_src.google_places_find_company(company.name)
        if gp_hits:
            succeeded.append("google_places")
            for gp in gp_hits:
                _merge_company(r, gp)

    # ── 5. Hunter — domain email search (BUDGET) ──────────────────────────
    if tier_allows(tier, BUDGET):
        domain = _best_domain(r)
        if domain:
            attempted.append("hunter")
            hunter_hits = await _hunter_src.hunter_domain_search(domain)
            if hunter_hits:
                succeeded.append("hunter")
                for h in hunter_hits:
                    h.company_name = company.name
                    h.role_category = "owner"
                    _merge_contact(r, h)
                    r.cost_cents += h.cost_cents

    # ── Short-circuit: skip Apollo/Zoominfo if we already have enough ─────
    rich_contacts = _contacts_with_both(r)

    # ── 6. Apollo org → people (STANDARD) ────────────────────────────────
    #   Two-step: api_search (discovery, masked names) → enrich top 3 with
    #   has_email=true via people/match by id (full name + verified email).
    if tier_allows(tier, STANDARD) and len(rich_contacts) < 2:
        attempted.append("apollo_org")
        org_id = await _apollo_org_src.apollo_org_search(company.name)
        if org_id:
            discovery = await _apollo_org_src.apollo_org_people(org_id)
            if discovery:
                succeeded.append("apollo_org")

                # Rank candidates by data availability — email > phone, prefer both
                def _avail_score(h: ContactHit) -> int:
                    raw = h.raw or {}
                    e = 1 if raw.get("has_email") else 0
                    p_flag = str(raw.get("has_direct_phone") or "").lower() == "yes"
                    return (e * 2) + (1 if p_flag else 0)

                ranked = sorted(discovery, key=_avail_score, reverse=True)

                # Enrich top N candidates that have has_email=true
                MAX_ENRICH = 3
                enriched_ids: set[str] = set()
                any_enriched = False
                for h in ranked[:MAX_ENRICH]:
                    raw = h.raw or {}
                    if not raw.get("has_email"):
                        continue
                    pid = raw.get("id")
                    if not pid:
                        continue
                    if "apollo_person_enrich" not in attempted:
                        attempted.append("apollo_person_enrich")
                    enriched = await _apollo_org_src.apollo_person_enrich_by_id(pid)
                    if enriched:
                        enriched.company_name = company.name
                        _merge_contact(r, enriched)
                        r.cost_cents += enriched.cost_cents
                        enriched_ids.add(pid)
                        any_enriched = True
                if any_enriched:
                    succeeded.append("apollo_person_enrich")

                # Append remaining discovery rows (titles + first names are
                # still useful as a bench of candidates a broker can research)
                for h in discovery:
                    pid = (h.raw or {}).get("id")
                    if pid and pid in enriched_ids:
                        continue  # already represented by enriched hit
                    h.company_name = company.name
                    _merge_contact(r, h)
                    r.cost_cents += h.cost_cents

    # Refresh rich_contacts count after Apollo
    rich_contacts = _contacts_with_both(r)

    # ── 7. Zoominfo (PREMIUM) ─────────────────────────────────────────────
    if tier_allows(tier, PREMIUM) and len(rich_contacts) < 2:
        from config import config as _cfg
        if _cfg.ZOOMINFO_CLIENT_ID:
            attempted.append("zoominfo")
            try:
                from enrichment.zoominfo import enrich_entity_with_zoominfo
                success = await enrich_entity_with_zoominfo(
                    company.entity_id, company.name
                )
                if success:
                    succeeded.append("zoominfo")
                    # Zoominfo writes directly to DB; no ContactHits returned here.
                    # Cost is tracked by the orchestrator's CostTracker at entity level.
            except Exception as e:
                log.warning("cascade.zoominfo_failed", entity=company.name, error=str(e))

    # ── 8. Proxycurl — LinkedIn for contacts missing it (PREMIUM) ─────────
    if tier_allows(tier, PREMIUM):
        top_contacts = [c for c in r.contacts if not c.linkedin_url][:3]
        if top_contacts:
            attempted.append("proxycurl")
            any_found = False
            for ct in top_contacts:
                parts = ct.full_name.split()
                if len(parts) < 2:
                    continue
                op_co = _first_company(r, "owner_operating")
                try:
                    pc_hits = await _proxycurl_src.proxycurl_person_lookup(
                        parts[0], parts[-1],
                        company_name=(op_co.domain or op_co.name) if op_co else company.name,
                    )
                    if pc_hits:
                        any_found = True
                        for h in pc_hits:
                            _merge_contact(r, h)
                            r.cost_cents += h.cost_cents
                except Exception as e:
                    log.warning("cascade.proxycurl_failed",
                                entity=company.name, error=str(e))
            if any_found:
                succeeded.append("proxycurl")

    log.info(
        "cascade.complete",
        entity=company.name,
        tier=tier.name,
        contacts=len(r.contacts),
        companies=len(r.companies),
        cost_cents=r.cost_cents,
        sources_attempted=attempted,
        sources_succeeded=succeeded,
    )
    return r


# ============================================================
# Merge helpers (copied from prong1_signer.py — kept local to
# avoid cross-package coupling)
# ============================================================

def _merge_company(r: CompanyEnrichmentResult, new: CompanyHit):
    for existing in r.companies:
        if _same_company(existing, new):
            _enrich_company(existing, new)
            return
    if new.website and not new.domain:
        new.domain = _domain_from_url(new.website)
    r.companies.append(new)


def _merge_contact(r: CompanyEnrichmentResult, new: ContactHit):
    for existing in r.contacts:
        if _same_contact(existing, new):
            _enrich_contact(existing, new)
            return
    r.contacts.append(new)


def _enrich_company(a: CompanyHit, b: CompanyHit):
    for f in ("website", "domain", "phone", "email", "address", "linkedin_url"):
        if not getattr(a, f) and getattr(b, f):
            setattr(a, f, getattr(b, f))
    if b.confidence > a.confidence:
        a.confidence = b.confidence
    if b.evidence and b.evidence not in (a.evidence or ""):
        a.evidence = f"{a.evidence or ''} | {b.evidence}".strip(" |")


def _enrich_contact(a: ContactHit, b: ContactHit):
    for f in ("email", "phone", "linkedin_url", "title", "company_name"):
        if not getattr(a, f) and getattr(b, f):
            setattr(a, f, getattr(b, f))
    if b.confidence > a.confidence:
        a.confidence = b.confidence
    if b.network_role and b.network_role != "unknown" and a.network_role == "unknown":
        a.network_role = b.network_role


def _same_company(a: CompanyHit, b: CompanyHit) -> bool:
    def norm(s): return re.sub(r"[^a-z0-9]", "", (s or "").lower())
    return norm(a.name) == norm(b.name) and a.role_category == b.role_category


def _same_contact(a: ContactHit, b: ContactHit) -> bool:
    if a.email and b.email and a.email.lower() == b.email.lower():
        return True
    return _name_match(a.full_name, b.full_name)


def _name_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    def tokens(s): return set(re.findall(r"[a-z]+", s.lower())) - {"mr", "mrs", "ms", "jr", "sr", "iii"}
    ta, tb = tokens(a), tokens(b)
    if not ta or not tb:
        return False
    return len(ta & tb) >= 2


def _first_company(r: CompanyEnrichmentResult, role_cat: str):
    for c in r.companies:
        if c.role_category == role_cat:
            return c
    return None


def _best_domain(r: CompanyEnrichmentResult) -> str:
    """Return first domain we know about across all company hits."""
    for c in r.companies:
        if c.domain:
            return c.domain
        if c.website:
            d = _domain_from_url(c.website)
            if d:
                return d
    return ""


def _contacts_with_both(r: CompanyEnrichmentResult) -> list[ContactHit]:
    """Contacts that have both a phone and an email."""
    return [c for c in r.contacts if c.phone and c.email]


def _domain_from_url(url: str) -> str:
    m = re.match(r"https?://(?:www\.)?([^/]+)/?", url)
    if not m:
        return ""
    return m.group(1).lower()
