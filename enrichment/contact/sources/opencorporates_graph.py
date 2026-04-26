"""
OpenCorporates — person-as-officer graph.

DISABLED: OpenCorporates has been removed from the active pipeline — price
prohibitive for commercial use. Code is retained in case it is re-enabled.

To re-enable: set _DISABLED = False and restore OPENCORPORATES_API_KEY in
config.py and .env.
"""

import httpx
import re
import structlog

from ..models import ContactHit, CompanyHit

log = structlog.get_logger(__name__)

_DISABLED = True  # set False to re-enable

OFFICERS_SEARCH = "https://api.opencorporates.com/v0.4/officers/search"
COMPANY_URL     = "https://api.opencorporates.com/v0.4/companies"


def _params(**extra) -> dict:
    return {"per_page": 30, "jurisdiction_code": "us_ny", **extra}


async def companies_for_person(full_name: str) -> list[CompanyHit]:
    """NY companies where this person is listed as an officer."""
    if _DISABLED:
        return []
    if not full_name:
        return []
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(OFFICERS_SEARCH, params=_params(q=full_name))
            r.raise_for_status()
            officers = r.json().get("results", {}).get("officers", [])
        except Exception as e:
            log.warn("oc.officers_search_failed", name=full_name, error=str(e))
            return []

    hits: list[CompanyHit] = []
    seen = set()
    for entry in officers:
        o = entry.get("officer", {})
        co = o.get("company", {})
        name = co.get("name") or ""
        if not name or name.upper() in seen:
            continue
        seen.add(name.upper())
        # Skip probable single-building LLCs
        role_cat = "title_holding_llc" if is_building_llc(name) else "owner_operating"
        hits.append(CompanyHit(
            name=name,
            role_category=role_cat,
            confidence=0.55 if role_cat == "owner_operating" else 0.3,
            source="opencorporates",
            source_url=co.get("opencorporates_url"),
            evidence=f"{full_name} is listed as {o.get('position')} of {name}",
            raw=entry,
        ))
    return hits


async def co_officers_for_company(opencorporates_url: str, exclude_name: str) -> list[ContactHit]:
    """Pull the officer list for a company and return everyone except `exclude_name`."""
    if _DISABLED:
        return []
    if not opencorporates_url:
        return []
    # OC REST needs jurisdiction/company_number extracted from the URL
    m = re.search(r"companies/([^/]+)/([^/?#]+)", opencorporates_url)
    if not m:
        return []
    juris, co_num = m.group(1), m.group(2)
    url = f"{COMPANY_URL}/{juris}/{co_num}"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(url, params=_params())
            r.raise_for_status()
            co = r.json().get("results", {}).get("company", {})
        except Exception as e:
            log.warn("oc.company_fetch_failed", url=url, error=str(e))
            return []
    officers = co.get("officers", []) or []
    out: list[ContactHit] = []
    for o in officers:
        o_inner = o.get("officer", o)
        nm = (o_inner.get("name") or "").strip()
        if not nm or nm.upper() == (exclude_name or "").upper():
            continue
        out.append(ContactHit(
            full_name=nm,
            title=o_inner.get("position"),
            company_name=co.get("name"),
            network_role="co_officer",
            role_category="owner",
            confidence=0.7,
            source="opencorporates",
            source_url=o_inner.get("opencorporates_url") or opencorporates_url,
            evidence=f"Co-officer of {co.get('name')} with {exclude_name}",
            raw=o,
        ))
    return out
