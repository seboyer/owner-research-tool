"""
Google Places — company phone/website by name.

API key: GOOGLE_PLACES_API_KEY in .env / config.
Uses the Places "findplacefromtext" + details endpoints to surface a
company's phone number, website, and address from its business name.
"""

import httpx
import structlog

from config import config
from ..models import CompanyHit

log = structlog.get_logger(__name__)


async def google_places_find_company(name: str, city: str = "New York, NY") -> list[CompanyHit]:
    k = config.GOOGLE_PLACES_API_KEY
    if not k:
        log.debug("google_places.skipped", name=name)
        return []
    # Places "findplacefromtext" -> details
    find_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get(find_url, params={
                "input": f"{name} {city}",
                "inputtype": "textquery",
                "fields": "place_id,name,formatted_address",
                "key": k,
            })
            r.raise_for_status()
            cands = r.json().get("candidates", [])
            if not cands:
                return []
            pid = cands[0]["place_id"]
            r2 = await client.get(details_url, params={
                "place_id": pid,
                "fields": "name,formatted_phone_number,website,formatted_address",
                "key": k,
            })
            r2.raise_for_status()
            res = r2.json().get("result", {})
        except Exception as e:
            log.warn("google_places.failed", error=str(e))
            return []
    return [CompanyHit(
        name=res.get("name") or name,
        role_category="owner_operating",
        website=res.get("website"),
        phone=res.get("formatted_phone_number"),
        address=res.get("formatted_address"),
        confidence=0.65,
        source="google_places",
        evidence="Google Places business listing",
        raw=res,
    )]
