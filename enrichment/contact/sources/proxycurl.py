"""
Proxycurl — LinkedIn profile resolver.

API key: PROXYCURL_API_KEY in .env / config.
Resolves a person's first + last name (optionally their company domain) to a
LinkedIn profile URL and headline via the Proxycurl profile-resolve endpoint.
"""

import httpx
import structlog

from config import config
from ..models import ContactHit

log = structlog.get_logger(__name__)


async def proxycurl_person_lookup(
    first_name: str, last_name: str, company_name: str = None,
) -> list[ContactHit]:
    k = config.PROXYCURL_API_KEY
    if not k:
        log.debug("proxycurl.skipped", name=f"{first_name} {last_name}")
        return []
    url = "https://nubela.co/proxycurl/api/linkedin/profile/resolve"
    params = {"first_name": first_name, "last_name": last_name}
    if company_name:
        params["company_domain"] = company_name
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(url, params=params,
                                 headers={"Authorization": f"Bearer {k}"})
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warn("proxycurl.failed", error=str(e))
            return []
    if not data.get("url"):
        return []
    return [ContactHit(
        full_name=f"{first_name} {last_name}",
        first_name=first_name,
        last_name=last_name,
        title=data.get("headline"),
        linkedin_url=data.get("url"),
        company_name=company_name,
        network_role="signer",
        role_category="owner",
        confidence=0.75,
        source="proxycurl",
        evidence="Proxycurl LinkedIn match",
        cost_cents=10,
        raw=data,
    )]
