"""
ACRIS Party History (via Socrata, no scraping needed)

Dataset: Real Property Parties (636b-3b5g)
https://data.cityofnewyork.us/resource/636b-3b5g.json

Given a person's name, returns every ACRIS document where they appear as a
party, with the corresponding BBL, role, and document type. From that we can:
  1. Surface OTHER deals they've been a party to → potential co-owners
  2. See the address ACRIS has on file for them → hints at operating company
  3. Find aliases (e.g. "John P. Smith" vs "John Smith")
"""

from typing import Optional
import httpx
import structlog

from config import config
from ..models import ContactHit, CompanyHit
from ..filters import is_govt_entity

log = structlog.get_logger(__name__)

ACRIS_PARTIES_URL = config.ACRIS_PARTIES_URL   # 636b-3b5g
ACRIS_MASTER_URL  = config.ACRIS_MASTER_URL    # bnx9-e6tj (doc types, dates)
ACRIS_LEGALS_URL  = config.ACRIS_LEGALS_URL    # 8h5j-fqxa (BBL lookup)


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if config.NYC_OPENDATA_APP_TOKEN:
        h["X-App-Token"] = config.NYC_OPENDATA_APP_TOKEN
    return h


async def find_deals_by_party(full_name: str, limit: int = 50) -> list[dict]:
    """
    Search ACRIS Parties for this name. Returns raw rows with document_id,
    party_type, address fields. Caller joins to Master/Legals for BBL.
    """
    if not full_name:
        return []
    # ACRIS stores names upper-cased; match case-insensitively.
    where = f"upper(name) like '%{full_name.upper().replace(chr(39), '')}%'"
    params = {"$where": where, "$limit": limit}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(ACRIS_PARTIES_URL, headers=_headers(), params=params)
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.warn("acris_party_history.query_failed", name=full_name, error=str(e))
            return []
    return rows


async def co_owners_on_other_deals(
    full_name: str, exclude_bbl: Optional[str] = None, max_deals: int = 25,
) -> list[ContactHit]:
    """
    Find OTHER people who were parties on the same ACRIS documents as this person.
    These are Prong-2 co-owners / co-borrowers.
    """
    deals = await find_deals_by_party(full_name, limit=max_deals)
    if not deals:
        return []
    doc_ids = list({d["document_id"] for d in deals if d.get("document_id")})
    if not doc_ids:
        return []

    where = "document_id in ('" + "','".join(doc_ids) + "')"
    params = {"$where": where, "$limit": 500}
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(ACRIS_PARTIES_URL, headers=_headers(), params=params)
            r.raise_for_status()
            all_parties = r.json()
        except Exception as e:
            log.warn("acris_party_history.co_query_failed", error=str(e))
            return []

    self_upper = full_name.upper()
    out: list[ContactHit] = []
    seen = set()
    for p in all_parties:
        nm = (p.get("name") or "").strip()
        if not nm or self_upper in nm.upper() or nm.upper() in self_upper:
            continue
        # Skip entity-looking names (LLC/CORP/INC) — those belong to CompanyHit, not ContactHit.
        if any(tag in nm.upper() for tag in (" LLC", " L.L.C", " CORP", " INC", " LP", " LTD", " TRUST", " BANK")):
            continue
        if is_govt_entity(nm):
            continue
        key = nm.upper()
        if key in seen:
            continue
        seen.add(key)
        out.append(ContactHit(
            full_name=nm,
            network_role="co_owner",
            role_category="owner",
            confidence=0.6,
            source="acris_party_history",
            source_url=f"{ACRIS_PARTIES_URL}?document_id={p.get('document_id')}",
            evidence=f"Co-party on ACRIS doc {p.get('document_id')} ({p.get('party_type')})",
            raw=p,
        ))
    return out


async def address_for_party(full_name: str) -> Optional[str]:
    """Return the most common address ACRIS has recorded for this party name."""
    rows = await find_deals_by_party(full_name, limit=20)
    if not rows:
        return None
    # Pick most recent-looking row with an address
    for r in rows:
        parts = [r.get(k, "") for k in ("address_1", "address_2", "city", "state", "zip")]
        addr = ", ".join(p for p in parts if p).strip(", ")
        if addr:
            return addr
    return None
