"""
Prong 1 — Signer lookup.

Input:  Signer (name, title, title-holding LLC, BBL)
Output:
    - The signer's phone + email + LinkedIn (as ContactHit, network_role='signer')
    - The OWNER OPERATING COMPANY (the public-facing brand the signer runs
      buildings under — e.g. "Doe Realty Group") as a CompanyHit
    - The MANAGEMENT COMPANY (if distinct from the owner) as a CompanyHit

Waterfall (stops expanding tiers once we have phone+email+both companies):
    FREE:
      1. ACRIS party history — what address does ACRIS have on file?
      2. HPD registration — is there a head officer / managing agent already?
      3. Claude web_search — resolve owner-operating-co vs mgmt-co from web
         (OpenCorporates removed — price prohibitive)

    BUDGET:  + BatchData V3 skip trace (direct phone+email from property address)
             + Google Places (phone + website for the candidate company)
             + Hunter (domain email finder once we know the website)

    STANDARD: + Apollo person match (direct email + dial)
              + Whitepages (50-query trial — last resort, evaluate efficacy)

    PREMIUM:  + Zoominfo, Proxycurl
"""

import asyncio
import re
import structlog

from .cost_tier import CostTier, BUDGET, STANDARD, PREMIUM, tier_allows
from .models import Signer, ProngResult, CompanyHit, ContactHit
from .sources import (
    acris_party_history, hpd_building_contacts, opencorporates_graph,
    claude_web_search, paid_stubs, batchdata,
)

log = structlog.get_logger(__name__)


async def run(signer: Signer, tier: CostTier) -> ProngResult:
    r = ProngResult(prong=1, signer_contact_id=signer.contact_id)
    attempted, succeeded = r.sources_attempted, r.sources_succeeded

    # ---------- 1. ACRIS address for the signer ----------
    attempted.append("acris_party_history")
    acris_addr = await acris_party_history.address_for_party(signer.full_name)
    if acris_addr:
        succeeded.append("acris_party_history")
        log.info("prong1.acris_addr", signer=signer.full_name, address=acris_addr)

    # ---------- 2. HPD registration (may reveal mgmt co already) ----------
    if signer.bbl:
        attempted.append("hpd_registration")
        hpd_contacts = await hpd_building_contacts.contacts_for_bbl(signer.bbl)
        if hpd_contacts:
            succeeded.append("hpd_registration")
            _ingest_hpd_into_prong1(r, signer, hpd_contacts)

    # ---------- 3. OpenCorporates — disabled (price prohibitive) ----------
    # Intentionally not added to sources_attempted — disabled globally.
    # oc_companies = await opencorporates_graph.companies_for_person(signer.full_name)

    # ---------- 4. Claude web_search — authoritative resolution ----------
    attempted.append("claude_web_search")
    property_address = _property_address(signer)
    web = await claude_web_search.research_signer(
        signer_name=signer.full_name,
        title_llc=signer.building_llc_name,
        property_address=property_address or "",
        signer_title=signer.title,
    )
    if web:
        succeeded.append("claude_web_search")
        cos, cts = claude_web_search.parse_to_hits(web, signer.full_name)
        for c in cos:
            _merge_company(r, c)
        for ct in cts:
            _merge_contact(r, ct)

    # ---------- 5. Budget tier — BatchData skip trace + Google Places + Hunter ----------
    if tier_allows(tier, BUDGET):
        # BatchData V3: direct phone + email from the property address.
        # Cheapest way to get the signer's personal contact info when we have a BBL.
        if signer.bbl and not _have_signer_email_phone(r):
            street, city, state, zip_code = _address_parts_from_bbl(signer)
            if street:
                attempted.append("batchdata_skip_trace")
                bd_hits = await batchdata.skip_trace_property(
                    street=street, city=city, state=state, zip_code=zip_code,
                    network_role="signer", role_category="owner",
                )
                if bd_hits:
                    succeeded.append("batchdata_skip_trace")
                    for h in bd_hits:
                        # The first person returned is the primary property owner —
                        # promote them to signer; any additional persons go in as co_owner
                        if bd_hits.index(h) == 0 and _name_match(h.full_name, signer.full_name):
                            h.network_role = "signer"
                        _merge_contact(r, h)
                        r.cost_cents += h.cost_cents

    if tier_allows(tier, BUDGET):
        for co in list(r.companies):
            if co.role_category not in ("owner_operating", "management"):
                continue
            if co.phone and co.website:
                continue
            attempted.append("google_places")
            gp = await paid_stubs.google_places_find_company(co.name)
            if gp:
                succeeded.append("google_places")
                _enrich_company(co, gp[0])

        # Pull emails off the owner company domain
        for co in list(r.companies):
            if co.role_category != "owner_operating" or not co.domain:
                continue
            attempted.append("hunter")
            hunter_hits = await paid_stubs.hunter_domain_search(co.domain)
            if hunter_hits:
                succeeded.append("hunter")
                # Promote signer-name matches straight to signer contact
                for h in hunter_hits:
                    if _name_match(h.full_name, signer.full_name):
                        h.network_role = "signer"
                    h.company_name = co.name
                    h.role_category = "owner"
                    _merge_contact(r, h)

    # ---------- 6. Standard tier — Apollo ----------
    if tier_allows(tier, STANDARD) and not _have_signer_email_phone(r):
        op_co = _first_company(r, "owner_operating")
        attempted.append("apollo")
        hits = await paid_stubs.apollo_person_match(
            signer.full_name, company=(op_co.name if op_co else None),
        )
        if hits:
            succeeded.append("apollo")
            for h in hits:
                h.network_role = "signer"
                h.role_category = "owner"
                _merge_contact(r, h)
                r.cost_cents += h.cost_cents

    # ---------- 6b. Standard tier — Whitepages (last resort, evaluate efficacy) ----------
    # Whitepages is expensive ($220/mo after the 50-query trial). Only call it
    # when Apollo has already run and we still have no phone. Monitor hit rate
    # via contact_enrichment_runs.sources_succeeded before committing to a plan.
    if tier_allows(tier, STANDARD) and not _have_signer_email_phone(r):
        attempted.append("whitepages")
        wp_hits = await paid_stubs.whitepages_person_search(
            signer.full_name, city="New York"
        )
        if wp_hits:
            succeeded.append("whitepages")
            for h in wp_hits:
                h.network_role = "signer"
                h.role_category = "owner"
                _merge_contact(r, h)
                r.cost_cents += h.cost_cents

    # ---------- 7. Premium tier — Proxycurl + Zoominfo ----------
    if tier_allows(tier, PREMIUM) and not _have_signer_linkedin(r):
        parts = signer.full_name.split()
        if len(parts) >= 2:
            attempted.append("proxycurl")
            op_co = _first_company(r, "owner_operating")
            pc = await paid_stubs.proxycurl_person_lookup(
                parts[0], parts[-1],
                company_name=(op_co.domain or op_co.name) if op_co else None,
            )
            if pc:
                succeeded.append("proxycurl")
                for h in pc:
                    h.network_role = "signer"
                    _merge_contact(r, h)
                    r.cost_cents += h.cost_cents

    # Guarantee a signer contact row even if only the name is known
    if not any(c.network_role == "signer" for c in r.contacts):
        r.contacts.append(ContactHit(
            full_name=signer.full_name,
            title=signer.title,
            network_role="signer",
            role_category="owner",
            confidence=0.4,
            source="acris_mortgage_pdf",
            evidence=f"Mortgage signer for {signer.building_llc_name}",
        ))

    return r


# ============================================================
# helpers
# ============================================================

def _ingest_hpd_into_prong1(r: ProngResult, signer: Signer, hpd_contacts: list[ContactHit]):
    """HPD rows at the same BBL can surface the management company directly."""
    for h in hpd_contacts:
        # "ManagingAgent" row with a corporationname → management company
        if h.title in ("ManagingAgent", "Agent") and h.company_name:
            _merge_company(r, CompanyHit(
                name=h.company_name,
                role_category="management",
                confidence=0.85,
                source="hpd_registration",
                evidence=f"HPD lists {h.company_name} as managing agent for BBL {signer.bbl}",
                raw=h.raw,
            ))
        # HeadOfficer rows with a corporationname → owner operating company
        if h.title == "HeadOfficer" and h.company_name:
            _merge_company(r, CompanyHit(
                name=h.company_name,
                role_category="owner_operating",
                confidence=0.6,
                source="hpd_registration",
                evidence=f"HPD head-officer corp for BBL {signer.bbl}",
                raw=h.raw,
            ))
        # Signer name match → fill signer contact phone
        if _name_match(h.full_name, signer.full_name):
            _merge_contact(r, ContactHit(
                full_name=signer.full_name,
                title=h.title,
                phone=(h.raw or {}).get("businessphone") or h.phone,
                network_role="signer",
                role_category="owner",
                confidence=0.7,
                source="hpd_registration",
                evidence=f"Signer matches HPD {h.title} for BBL {signer.bbl}",
            ))


def _merge_company(r: ProngResult, new: CompanyHit):
    for existing in r.companies:
        if _same_company(existing, new):
            _enrich_company(existing, new)
            return
    # Attempt to derive domain from website
    if new.website and not new.domain:
        new.domain = _domain_from_url(new.website)
    r.companies.append(new)


def _merge_contact(r: ProngResult, new: ContactHit):
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


def _first_company(r: ProngResult, role_cat: str):
    for c in r.companies:
        if c.role_category == role_cat:
            return c
    return None


def _have_signer_email_phone(r: ProngResult) -> bool:
    for c in r.contacts:
        if c.network_role == "signer" and c.email and c.phone:
            return True
    return False


def _have_signer_linkedin(r: ProngResult) -> bool:
    return any(c.network_role == "signer" and c.linkedin_url for c in r.contacts)


def _property_address(signer: Signer) -> str:
    """Return a human-readable property address string for use in web search context."""
    street, city, state, zip_code = _address_parts_from_bbl(signer)
    if street:
        return f"{street}, {city}, {state} {zip_code}".strip(", ")
    return ""


_BOROUGH_TO_CITY = {
    "MANHATTAN":     "New York",
    "BROOKLYN":      "Brooklyn",
    "QUEENS":        "Queens",
    "BRONX":         "Bronx",
    "STATEN ISLAND": "Staten Island",
}


def _address_parts_from_bbl(signer: Signer) -> tuple[str, str, str, str]:
    """
    Return (street, city, state, zip) for the property tied to this signer.
    Derives from the properties table columns: house_number, street_name,
    zip_code, borough.
    """
    if not signer.property_id:
        return "", "", "", ""
    try:
        from database.client import db
        res = (
            db()
            .table("properties")
            .select("house_number, street_name, address, zip_code, borough")
            .eq("id", signer.property_id)
            .limit(1)
            .execute()
        )
        if not res.data:
            return "", "", "", ""
        row = res.data[0]
        street = f"{row.get('house_number') or ''} {row.get('street_name') or ''}".strip()
        # Fall back to parsing the combined address field (e.g. "1100 BEDFORD AVENUE")
        if not street:
            street = (row.get("address") or "").split(",")[0].strip()
        borough = (row.get("borough") or "").upper()
        city = _BOROUGH_TO_CITY.get(borough, "New York")
        return street, city, "NY", row.get("zip_code") or ""
    except Exception as e:
        log.warn("prong1.address_parts_failed", error=str(e))
        return "", "", "", ""


def _domain_from_url(url: str) -> str:
    m = re.match(r"https?://([^/]+)/?", url)
    if not m:
        return ""
    return m.group(1).lower().lstrip("www.")
