"""
NYC DOB Permit Filings (via Socrata)

Dataset: DOB Permit Issuance (ipu4-2q9a)
https://data.cityofnewyork.us/resource/ipu4-2q9a.json

Returns the owner name, owner business name, owner phone, and filing
applicant on recent permits for a BBL. These are high-signal contacts —
DOB permit contact info is kept more up-to-date than HPD.
"""

import httpx
import structlog

from config import config
from database.client import parse_bbl
from database.retry import retry_external
from ..models import ContactHit

log = structlog.get_logger(__name__)

DOB_PERMITS_URL = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if config.NYC_OPENDATA_APP_TOKEN:
        h["X-App-Token"] = config.NYC_OPENDATA_APP_TOKEN
    return h


@retry_external(max_attempts=5)
async def _fetch_dob_permits(client: httpx.AsyncClient, params: dict) -> list[dict]:
    """Inner HTTP helper for DOB permits query — retried on transient errors."""
    r = await client.get(DOB_PERMITS_URL, headers=_headers(), params=params)
    r.raise_for_status()
    return r.json()


async def permits_for_bbl(bbl: str, limit: int = 25) -> list[ContactHit]:
    parsed = parse_bbl(bbl)
    if parsed is None:
        return []
    boro, block, lot = parsed
    boro_names = {"1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN", "4": "QUEENS", "5": "STATEN ISLAND"}
    where = f"borough='{boro_names.get(boro, '')}' AND block='{block}' AND lot='{lot}'"
    params = {"$where": where, "$limit": limit, "$order": "filing_date DESC"}

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            rows = await _fetch_dob_permits(client, params)
        except Exception as e:
            log.warning("dob_permits.query_failed", bbl=bbl, error=str(e))
            return []

    out: list[ContactHit] = []
    seen = set()
    for row in rows:
        owner_name = (row.get("owner_s_first_name") or "") + " " + (row.get("owner_s_last_name") or "")
        owner_name = owner_name.strip()
        owner_biz = (row.get("owner_s_business_name") or "").strip()
        phone = row.get("owner_s_phone__") or row.get("owner_s_phone_")
        if not owner_name and not owner_biz:
            continue
        key = (owner_name.upper(), owner_biz.upper())
        if key in seen:
            continue
        seen.add(key)
        out.append(ContactHit(
            full_name=owner_name or owner_biz,
            title=row.get("owner_type") or "Owner on DOB permit",
            phone=phone,
            company_name=owner_biz or None,
            network_role="building_contact",
            role_category="owner",
            confidence=0.75,
            source="dob_permits",
            source_url=f"https://a810-bisweb.nyc.gov/bisweb/JobsQueryByLocationServlet?borough={boro}&block={block}&lot={lot}",
            evidence=f"Owner on DOB permit #{row.get('job__')} filed {row.get('filing_date', '')[:10]}",
            raw=row,
        ))

        # Filing applicant (architect / expeditor) — usually a managing agent contact
        applicant = (row.get("applicant_s_first_name") or "") + " " + (row.get("applicant_s_last_name") or "")
        applicant = applicant.strip()
        if applicant and applicant.upper() != owner_name.upper():
            akey = (applicant.upper(), "APPLICANT")
            if akey not in seen:
                seen.add(akey)
                out.append(ContactHit(
                    full_name=applicant,
                    title=row.get("applicant_professional_title") or "Permit applicant",
                    company_name=row.get("applicant_business_name") or None,
                    network_role="building_contact",
                    role_category="management",
                    confidence=0.4,
                    source="dob_permits",
                    evidence=f"Filed permit #{row.get('job__')} on behalf of {owner_biz or owner_name}",
                    raw=row,
                ))
    return out
