"""
Prong 3 — Other building contacts.

Input:  the property (BBL) tied to the seed signer
Output: every OTHER person tied to the building's ownership or management,
        filtered OUT any pure-tenant or non-relevant contacts.

Valid network_roles produced:
    'head_officer'       — HPD HeadOfficer who isn't the signer
    'registered_agent'   — HPD Managing Agent / Agent
    'co_owner'           — HPD Individual / Corporate / Joint owner rows
    'co_officer'         — HPD Officer / Shareholder rows
    'building_contact'   — DOB permit filers, site managers

Filters applied:
    - role_category must be 'owner' or 'management' (per user's answer)
    - exclude the seed signer themselves
    - exclude known tenants / lessees unless they're flagged as management
"""

import structlog

from .cost_tier import CostTier, BUDGET, tier_allows
from .models import Signer, ProngResult, ContactHit
from .sources import hpd_building_contacts, dob_permits, acris_party_history, batchdata
from .filters import is_govt_entity

log = structlog.get_logger(__name__)


async def run(signer: Signer, tier: CostTier) -> ProngResult:  # noqa: C901
    r = ProngResult(prong=3, signer_contact_id=signer.contact_id)
    attempted, succeeded = r.sources_attempted, r.sources_succeeded

    if not signer.bbl:
        r.error = "no bbl on seed signer; prong 3 skipped"
        return r

    # ---------- HPD building contacts ----------
    attempted.append("hpd_registration")
    hpd = await hpd_building_contacts.contacts_for_bbl(signer.bbl)
    if hpd:
        succeeded.append("hpd_registration")
        for c in hpd:
            if _is_self(c, signer):
                continue
            if c.role_category not in ("owner", "management"):
                continue
            if is_govt_entity(c.full_name):
                continue
            r.contacts.append(c)

    # ---------- DOB permit filings ----------
    attempted.append("dob_permits")
    dob = await dob_permits.permits_for_bbl(signer.bbl)
    if dob:
        succeeded.append("dob_permits")
        for c in dob:
            if _is_self(c, signer):
                continue
            if c.role_category not in ("owner", "management"):
                continue
            if is_govt_entity(c.full_name):
                continue
            r.contacts.append(c)

    # ---------- ACRIS historical parties on the SAME BBL ----------
    attempted.append("acris_bbl_history")
    acris_people = await _acris_parties_for_bbl(signer.bbl, signer.full_name)
    if acris_people:
        succeeded.append("acris_bbl_history")
        for c in acris_people:
            if _is_self(c, signer):
                continue
            r.contacts.append(c)

    # ---------- BatchData skip trace (Budget+) ----------
    # Enriches existing name-only contacts with real phones and emails,
    # and may surface additional owners not in HPD/DOB records.
    if tier_allows(tier, BUDGET) and signer.property_id:
        street, city, state, zip_code = await _property_address(signer)
        if street:
            attempted.append("batchdata_skip_trace")
            bd_hits = await batchdata.skip_trace_property(
                street=street, city=city, state=state, zip_code=zip_code,
                network_role="building_contact", role_category="owner",
            )
            if bd_hits:
                succeeded.append("batchdata_skip_trace")
                for h in bd_hits:
                    if _is_self(h, signer):
                        continue
                    # Merge into existing name-only contact if name matches,
                    # otherwise add as a new building_contact
                    merged = False
                    for existing in r.contacts:
                        if _names_overlap(existing.full_name, h.full_name):
                            if h.phone and not existing.phone:
                                existing.phone = h.phone
                                existing.phone_type = h.phone_type
                            if h.email and not existing.email:
                                existing.email = h.email
                            if h.confidence > existing.confidence:
                                existing.confidence = h.confidence
                            existing.source = f"{existing.source}+batchdata"
                            merged = True
                            break
                    if not merged:
                        r.contacts.append(h)
                    r.cost_cents += h.cost_cents

    # Dedup by (name, role_category) keeping the highest confidence
    dedup: dict[tuple[str, str], ContactHit] = {}
    for c in r.contacts:
        key = (c.full_name.upper(), c.role_category)
        if key not in dedup or c.confidence > dedup[key].confidence:
            dedup[key] = c
    r.contacts = list(dedup.values())
    return r


_BOROUGH_TO_CITY = {
    "MANHATTAN":     "New York",
    "BROOKLYN":      "Brooklyn",
    "QUEENS":        "Queens",
    "BRONX":         "Bronx",
    "STATEN ISLAND": "Staten Island",
}


async def _property_address(signer: Signer) -> tuple[str, str, str, str]:
    """
    Return (street, city, state, zip) from the properties table.
    Derives from house_number, street_name, zip_code, borough columns.
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
        # Fall back to parsing the combined address field
        if not street:
            street = (row.get("address") or "").split(",")[0].strip()
        borough = (row.get("borough") or "").upper()
        city = _BOROUGH_TO_CITY.get(borough, "New York")
        return street, city, "NY", row.get("zip_code") or ""
    except Exception as e:
        log.warn("prong3.address_lookup_failed", error=str(e))
        return "", "", "", ""


def _names_overlap(a: str, b: str) -> bool:
    """True if two name strings share ≥2 word tokens (first+last match)."""
    import re
    if not a or not b:
        return False
    stopwords = {"mr", "mrs", "ms", "jr", "sr", "iii", "ii"}
    def tokens(s):
        return set(re.findall(r"[a-z]+", s.lower())) - stopwords
    ta, tb = tokens(a), tokens(b)
    return len(ta & tb) >= 2


def _is_self(c: ContactHit, signer: Signer) -> bool:
    if not c.full_name or not signer.full_name:
        return False
    a = set(c.full_name.upper().split())
    b = set(signer.full_name.upper().split())
    return len(a & b) >= 2


async def _acris_parties_for_bbl(bbl: str, exclude_name: str) -> list[ContactHit]:
    """Look up every ACRIS party ever tied to this BBL (via Legals dataset)."""
    import httpx
    from config import config
    url_legals = config.ACRIS_LEGALS_URL
    url_parties = config.ACRIS_PARTIES_URL
    headers = {"Accept": "application/json"}
    if config.NYC_OPENDATA_APP_TOKEN:
        headers["X-App-Token"] = config.NYC_OPENDATA_APP_TOKEN

    if not bbl or len(bbl) < 10:
        return []
    boro, block, lot = bbl[0], str(int(bbl[1:6])), str(int(bbl[6:10]))
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            legals = await client.get(url_legals, headers=headers, params={
                "borough": boro, "block": block, "lot": lot, "$limit": 200,
            })
            legals.raise_for_status()
            docs = legals.json()
        except Exception as e:
            log.warn("acris_bbl_history.legals_failed", error=str(e))
            return []
        if not docs:
            return []
        doc_ids = list({d["document_id"] for d in docs if d.get("document_id")})[:100]
        if not doc_ids:
            return []
        try:
            r = await client.get(url_parties, headers=headers, params={
                "$where": "document_id in ('" + "','".join(doc_ids) + "')",
                "$limit": 500,
            })
            r.raise_for_status()
            parties = r.json()
        except Exception as e:
            log.warn("acris_bbl_history.parties_failed", error=str(e))
            return []

    out: list[ContactHit] = []
    seen = set()
    self_upper = exclude_name.upper()
    for p in parties:
        nm = (p.get("name") or "").strip()
        if not nm or nm.upper() in seen:
            continue
        if self_upper and self_upper in nm.upper():
            continue
        # Entity name? Skip — we want people.
        if any(tag in nm.upper() for tag in (" LLC", " CORP", " INC", " LP", " LTD", " BANK", " TRUST", " N.A.")):
            continue
        if is_govt_entity(nm):
            continue
        seen.add(nm.upper())
        out.append(ContactHit(
            full_name=nm,
            title=p.get("party_type"),
            network_role="co_owner",
            role_category="owner",
            confidence=0.5,
            source="acris_bbl_history",
            source_url=f"{url_parties}?document_id={p.get('document_id')}",
            evidence=f"Historical party on ACRIS doc {p.get('document_id')} for BBL {bbl}",
            raw=p,
        ))
    return out
