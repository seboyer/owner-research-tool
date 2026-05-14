"""
Apollo.io — person + email + phone match.

API key: APOLLO_API_KEY in .env / config.
Uses Apollo's /v1/people/match endpoint to resolve a person by name
(and optionally company) into a full contact record with email and phone.
"""

import httpx
import structlog

from config import config
from database.retry import retry_external
from ..models import ContactHit

log = structlog.get_logger(__name__)


@retry_external(max_attempts=3)
async def _post_people_match(payload: dict, api_key: str) -> dict:
    url = "https://api.apollo.io/v1/people/match"
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            url, json=payload,
            headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
        )
        r.raise_for_status()
        return r.json().get("person", {})


async def apollo_person_match(full_name: str, company: str = None) -> list[ContactHit]:
    k = config.APOLLO_API_KEY
    if not k:
        log.debug("apollo.skipped", name=full_name)
        return []
    payload = {"name": full_name}
    if company:
        payload["organization_name"] = company
    try:
        p = await _post_people_match(payload, k)
    except Exception as e:
        log.warning("apollo.failed", error=str(e))
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
