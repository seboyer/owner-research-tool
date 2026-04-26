"""
HPD Building Contacts (via Socrata)

Datasets:
    HPD Registrations (tesw-yqqr)  — one row per registered building
    HPD Contacts (feu5-w2e2)       — contact rows linked by registrationid

For a given BBL we can pull every Head Officer, Owner, Managing Agent,
Site Manager, and Corporate Officer that HPD has on file — along with
business phone/address. This is Prong 3's most reliable source.
"""

from typing import Optional
import httpx
import structlog

from config import config
from ..models import ContactHit

log = structlog.get_logger(__name__)

HPD_REG_URL = config.HPD_REGISTRATIONS_URL
HPD_CONTACTS_URL = config.HPD_CONTACTS_URL


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if config.NYC_OPENDATA_APP_TOKEN:
        h["X-App-Token"] = config.NYC_OPENDATA_APP_TOKEN
    return h


# HPD "Type" codes → our network_role / role_category mapping
_ROLE_MAP = {
    "HeadOfficer":       ("head_officer",     "owner"),
    "IndividualOwner":   ("co_owner",         "owner"),
    "CorporateOwner":    ("co_owner",         "owner"),
    "JointOwner":        ("co_owner",         "owner"),
    "Officer":           ("co_officer",       "owner"),
    "Shareholder":       ("co_officer",       "owner"),
    "Agent":             ("registered_agent", "management"),
    "ManagingAgent":     ("registered_agent", "management"),
    "SiteManager":       ("building_contact", "management"),
    "Lessee":            ("building_contact", "management"),
}


async def contacts_for_bbl(bbl: str) -> list[ContactHit]:
    """Return every HPD contact row for this BBL."""
    if not bbl or len(bbl) < 10:
        return []
    boro = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:10]))
    where = f"boroid='{boro}' AND block='{block}' AND lot='{lot}'"

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            reg = await client.get(HPD_REG_URL, headers=_headers(),
                                   params={"$where": where, "$limit": 50})
            reg.raise_for_status()
            regs = reg.json()
        except Exception as e:
            log.warn("hpd.reg_query_failed", bbl=bbl, error=str(e))
            return []

        if not regs:
            return []
        reg_ids = [r["registrationid"] for r in regs if r.get("registrationid")]
        if not reg_ids:
            return []

        try:
            cts = await client.get(HPD_CONTACTS_URL, headers=_headers(), params={
                "$where": "registrationid in (" + ",".join(f"'{r}'" for r in reg_ids) + ")",
                "$limit": 500,
            })
            cts.raise_for_status()
            rows = cts.json()
        except Exception as e:
            log.warn("hpd.contacts_query_failed", bbl=bbl, error=str(e))
            return []

    out: list[ContactHit] = []
    seen = set()
    for row in rows:
        type_code = row.get("type") or ""
        network_role, role_cat = _ROLE_MAP.get(type_code, ("building_contact", "unknown"))
        fn = (row.get("firstname") or "").strip()
        ln = (row.get("lastname") or "").strip()
        corp = (row.get("corporationname") or "").strip()
        full = f"{fn} {ln}".strip() or corp
        if not full:
            continue
        key = (full.upper(), type_code)
        if key in seen:
            continue
        seen.add(key)
        address = ", ".join(filter(None, [
            row.get("businesshousenumber"), row.get("businessstreetname"),
            row.get("businessapartment"),
            row.get("businesscity"), row.get("businessstate"), row.get("businesszip"),
        ]))
        out.append(ContactHit(
            full_name=full,
            first_name=fn or None,
            last_name=ln or None,
            title=type_code,
            company_name=corp or None,
            network_role=network_role,
            role_category=role_cat,
            confidence=0.85,
            source="hpd_registration",
            source_url=HPD_CONTACTS_URL,
            evidence=f"HPD {type_code} on file for BBL {bbl}; address {address}".strip(),
            raw=row,
        ))
    return out
