"""
Hunter.io — domain email finder.

API key: HUNTER_API_KEY in .env / config.
Returns ContactHit list from Hunter.io domain-search endpoint.
"""

import httpx
import structlog

from config import config
from database.retry import retry_external
from ..models import ContactHit

log = structlog.get_logger(__name__)


@retry_external(max_attempts=3)
async def _fetch_domain_search(domain: str, api_key: str) -> list[dict]:
    url = "https://api.hunter.io/v2/domain-search"
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url, params={"domain": domain, "api_key": api_key, "limit": 25})
        r.raise_for_status()
        return r.json().get("data", {}).get("emails", [])


async def hunter_domain_search(domain: str) -> list[ContactHit]:
    k = config.HUNTER_API_KEY
    if not k or not domain:
        log.debug("hunter.skipped", domain=domain, has_key=bool(k))
        return []
    try:
        emails = await _fetch_domain_search(domain, k)
    except Exception as e:
        log.warning("hunter.failed", error=str(e))
        return []
    out = []
    for e in emails:
        full = f"{e.get('first_name','')} {e.get('last_name','')}".strip()
        out.append(ContactHit(
            full_name=full or e.get("value", ""),
            first_name=e.get("first_name"),
            last_name=e.get("last_name"),
            title=e.get("position"),
            email=e.get("value"),
            phone=e.get("phone_number"),
            linkedin_url=e.get("linkedin"),
            network_role="coworker",
            role_category="unknown",
            confidence=float(e.get("confidence", 50)) / 100.0,
            source="hunter",
            evidence=f"Hunter.io email on domain {domain}",
            cost_cents=1,
        ))
    return out
