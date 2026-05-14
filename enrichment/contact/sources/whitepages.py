"""
Whitepages Pro — individual phone lookup.

API key: WHITEPAGES_API_KEY in .env / config.
Billing: every 200 response is 1 query (even if empty results).
         Only called after Apollo has already failed, to conserve the trial.

Returns at most 1 ContactHit (highest-score record with a usable phone).
"""

import httpx
import structlog

from config import config
from ..models import ContactHit

log = structlog.get_logger(__name__)


async def whitepages_person_search(full_name: str, city: str = "New York") -> list[ContactHit]:
    """
    Whitepages Pro Person Search — GET /v2/person
    Auth: X-Api-Key header.
    Billing: every 200 response is 1 query (even if empty results).
             Only called after Apollo has already failed, to conserve the trial.

    Returns at most 1 ContactHit (highest-score record with a usable phone).
    """
    k = config.WHITEPAGES_API_KEY
    if not k:
        log.debug("whitepages.skipped", name=full_name, has_key=False)
        return []
    if not full_name:
        return []

    params = {
        "name": full_name,
        "city": city,
        "state_code": "NY",
        "include_fuzzy_matching": "true",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(
                "https://api.whitepages.com/v2/person",
                params=params,
                headers={"X-Api-Key": k},
            )
            r.raise_for_status()
            records = r.json() or []
        except httpx.HTTPStatusError as e:
            log.warn("whitepages.http_error",
                     status=e.response.status_code, body=e.response.text[:200])
            return []
        except Exception as e:
            log.warn("whitepages.failed", error=str(e))
            return []

    if not records:
        return []

    # Whitepages should return a list; guard against dict error responses
    if not isinstance(records, list):
        log.warn("whitepages.unexpected_response_type",
                 type=type(records).__name__, body=str(records)[:300])
        return []

    # Take the highest-score record that has at least a phone or email
    best = None
    best_score = -1
    for rec in records:
        if rec.get("is_dead"):
            continue
        score = rec.get("score", 0)
        has_contact = bool(rec.get("phones") or rec.get("emails"))
        if has_contact and score > best_score:
            best = rec
            best_score = score

    if not best:
        return []

    # Best phone — prefer mobile, then highest score
    phones = sorted(
        best.get("phones") or [],
        key=lambda p: (0 if p.get("type") == "mobile" else 1, -(p.get("score") or 0)),
    )
    best_phone = phones[0] if phones else None
    phone_num = best_phone.get("number") if best_phone else None
    phone_type = (best_phone.get("type") or "").capitalize() if best_phone else None

    # Best email — highest score
    emails = sorted(best.get("emails") or [], key=lambda e: -(e.get("score") or 0))
    best_email = emails[0].get("address") if emails else None

    evidence_parts = [f"Whitepages person search (match score={best_score})"]
    if best_phone:
        evidence_parts.append(f"phone score={best_phone.get('score', '?')}")
    if best_email:
        evidence_parts.append(f"email score={emails[0].get('score', '?')}")

    return [ContactHit(
        full_name=best.get("name") or full_name,
        phone=phone_num,
        phone_type=phone_type,
        email=best_email,
        linkedin_url=best.get("linkedin_url"),
        company_name=best.get("company_name"),
        title=best.get("job_title"),
        network_role="signer",
        role_category="owner",
        confidence=round(best_score / 100, 2),
        source="whitepages",
        evidence=" | ".join(evidence_parts),
        cost_cents=15,  # rough estimate; actual cost depends on subscription tier
        raw=best,
    )]
