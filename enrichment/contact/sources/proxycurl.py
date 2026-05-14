"""
Proxycurl — LinkedIn profile resolver.

API key: PROXYCURL_API_KEY in .env / config.
Resolves a person's first + last name (optionally their company domain) to a
LinkedIn profile URL and headline via the Proxycurl profile-resolve endpoint.
"""

import httpx
import structlog

from config import config
from database.retry import retry_external
from ..models import ContactHit

log = structlog.get_logger(__name__)


@retry_external(max_attempts=3)
async def _resolve_profile(params: dict, api_key: str) -> dict:
    url = "https://nubela.co/proxycurl/api/linkedin/profile/resolve"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, params=params,
                             headers={"Authorization": f"Bearer {api_key}"})
        r.raise_for_status()
        return r.json()


async def proxycurl_person_lookup(
    first_name: str, last_name: str, company_name: str = None,
) -> list[ContactHit]:
    k = config.PROXYCURL_API_KEY
    if not k:
        log.debug("proxycurl.skipped", name=f"{first_name} {last_name}")
        return []
    params = {"first_name": first_name, "last_name": last_name}
    if company_name:
        params["company_domain"] = company_name
    try:
        data = await _resolve_profile(params, k)
    except Exception as e:
        log.warning("proxycurl.failed", error=str(e))
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
