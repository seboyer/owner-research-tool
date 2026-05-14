"""
Apollo.io — person + email + phone match.

API key: APOLLO_API_KEY in .env / config.
Uses Apollo's /v1/people/match endpoint to resolve a person by name
(and optionally company) into a full contact record with email and phone.
"""

import httpx
import structlog

from config import config
from ..models import ContactHit

log = structlog.get_logger(__name__)


async def apollo_person_match(full_name: str, company: str = None) -> list[ContactHit]:
    k = config.APOLLO_API_KEY
    if not k:
        log.debug("apollo.skipped", name=full_name)
        return []
    url = "https://api.apollo.io/v1/people/match"
    payload = {"name": full_name}
    if company:
        payload["organization_name"] = company
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(
                url,
                json=payload,
                headers={"X-Api-Key": k, "Content-Type": "application/json"},
            )
            r.raise_for_status()
            p = r.json().get("person", {})
        except Exception as e:
            log.warn("apollo.failed", error=str(e))
            return []
    if not p:
        return []
    return [ContactHit(
        full_name=p.get("name") or full_name,
        first_name=p.get("first_name"),
        last_name=p.get("last_name"),
        title=p.get("title"),
        email=p.get("email"),
        phone=(p.get("phone_numbers") or [{}])[0].get("sanitized_number"),
        linkedin_url=p.get("linkedin_url"),
        company_name=(p.get("organization") or {}).get("name"),
        network_role="signer",
        role_category="owner",
        confidence=0.85,
        source="apollo",
        evidence="Apollo.io person match",
        cost_cents=5,
        raw=p,
    )]
