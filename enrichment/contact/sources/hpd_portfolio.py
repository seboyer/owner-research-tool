"""
HPD cross-portfolio aggregation — find head officers for a company.

For a given company name, queries the HPD Contacts dataset across all
registered buildings where that company appears as the registered owner/officer.
Returns the most-frequently-occurring HeadOfficer / Officer names ranked by
building count, suitable as seed contacts for corporate enrichment.

Dataset: HPD Contacts (feu5-w2e2).
Cost: $0 (public NYC OpenData endpoint).
"""

import re
from collections import defaultdict

import httpx
import structlog

from config import config
from database.retry import retry_external
from enrichment.contact.filters import is_govt_entity

from ..models import ContactHit

log = structlog.get_logger(__name__)

HPD_CONTACTS_URL = config.HPD_CONTACTS_URL

# Suffixes to strip when normalising a company name for HPD lookup.
# The intent is to produce a distinctive core token set — e.g.
# "STONEHENGE PARTNERS LLC" → "STONEHENGE PARTNERS" so a LIKE query
# on HPD's corporationname column still catches it.
_CORP_SUFFIX_RE = re.compile(
    r"\b(LLC|L\.?L\.?C\.?|INC|CORP|CORPORATION|LP|L\.?P\.?|LTD|LLP"
    r"|REALTY|GROUP|HOLDINGS|MANAGEMENT|PROPERTIES|ASSOCIATES|PARTNERS"
    r"|CO|COMPANY)\b\.?",
    re.I,
)

# HPD contact types we care about for corporate enrichment
_TARGET_TYPES = ("HeadOfficer", "Officer", "IndividualOwner", "CorporateOwner")


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if config.NYC_OPENDATA_APP_TOKEN:
        h["X-App-Token"] = config.NYC_OPENDATA_APP_TOKEN
    return h


def _normalize_company(name: str) -> str:
    """Strip common legal suffixes, uppercase, collapse whitespace.

    "STONEHENGE PARTNERS LLC" → "STONEHENGE PARTNERS"
    """
    n = _CORP_SUFFIX_RE.sub("", name)
    n = re.sub(r"[^A-Z0-9 ]", " ", n.upper())
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _is_distinctive(normalized: str) -> bool:
    """Guard against matching everything: require >= 4 chars and >= 1 token."""
    tokens = [t for t in normalized.split() if len(t) >= 2]
    return len(normalized) >= 4 and len(tokens) >= 1


@retry_external(max_attempts=5)
async def _fetch_hpd_contacts(client: httpx.AsyncClient, where: str) -> list[dict]:
    """Inner HTTP helper — retried on transient errors."""
    r = await client.get(
        HPD_CONTACTS_URL,
        headers=_headers(),
        params={"$where": where, "$limit": 1000},
    )
    r.raise_for_status()
    return r.json()


async def head_officers_for_company(company_name: str) -> list[ContactHit]:
    """Return the most-frequent HeadOfficer/Officer contacts across all HPD
    buildings where *company_name* is the registered corporation.

    Steps:
      1. Normalise name (strip LLC/CORP/etc, uppercase).
      2. Guard: require >= 4 chars + >= 1 distinctive token after normalisation.
      3. Socrata LIKE query on corporationname.
      4. Group by (firstname, lastname); count distinct registrationids.
      5. Filter through is_govt_entity.
      6. Return top 5 by building_count as ContactHit.
    """
    norm = _normalize_company(company_name)
    if not _is_distinctive(norm):
        log.debug("hpd_portfolio.name_not_distinctive", entity=company_name, normalized=norm)
        return []

    where = (
        f"upper(corporationname) like '%{norm}%' "
        f"AND type in ({', '.join(repr(t) for t in _TARGET_TYPES)})"
    )

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            rows = await _fetch_hpd_contacts(client, where)
        except Exception as e:
            log.warning("hpd_portfolio.query_failed", entity=company_name, error=str(e))
            return []

    if not rows:
        return []

    # Group by person name → count distinct registration IDs
    person_regs: dict[tuple[str, str], set[str]] = defaultdict(set)
    person_type: dict[tuple[str, str], str] = {}

    for row in rows:
        fn = (row.get("firstname") or "").strip()
        ln = (row.get("lastname") or "").strip()
        if not fn and not ln:
            continue
        key = (fn.upper(), ln.upper())
        # Skip government entities
        full = f"{fn} {ln}".strip()
        if is_govt_entity(full):
            continue
        reg_id = row.get("registrationid") or ""
        if reg_id:
            person_regs[key].add(reg_id)
        # Track the most common type for this person
        row_type = row.get("type") or "HeadOfficer"
        if key not in person_type:
            person_type[key] = row_type

    if not person_regs:
        return []

    # Rank by building count, take top 5
    ranked = sorted(person_regs.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]

    out: list[ContactHit] = []
    for (fn, ln), reg_ids in ranked:
        count = len(reg_ids)
        confidence = min(0.95, 0.5 + 0.1 * count)
        type_code = person_type.get((fn, ln), "HeadOfficer")
        full_name = f"{fn.title()} {ln.title()}".strip()
        out.append(ContactHit(
            full_name=full_name,
            first_name=fn.title() or None,
            last_name=ln.title() or None,
            title=type_code,
            company_name=company_name,
            network_role="head_officer",
            role_category="owner",
            confidence=confidence,
            source="hpd_portfolio",
            source_url=HPD_CONTACTS_URL,
            evidence=f"{type_code} on {count} HPD-registered building{'s' if count != 1 else ''}",
            cost_cents=0,
        ))

    log.info("hpd_portfolio.found",
             entity=company_name, normalized=norm, count=len(out))
    return out
