"""
Paid source stubs — interfaces are wired but no implementation runs without
the matching API key in .env. Each function logs a one-liner + returns [] so
the orchestrator's waterfall is unaffected when keys are missing.

Add keys to .env as you acquire them:
    APOLLO_API_KEY
    PROXYCURL_API_KEY
    WHITEPAGES_API_KEY
    HUNTER_API_KEY
    GOOGLE_PLACES_API_KEY
    PROPERTY_RADAR_API_KEY
    REONOMY_API_KEY
"""

import os
import httpx
import structlog

from ..models import ContactHit, CompanyHit

log = structlog.get_logger(__name__)


def _key(name: str) -> str:
    return os.getenv(name, "")


# ============================================================
# Hunter.io — domain email finder
# ============================================================

async def hunter_domain_search(domain: str) -> list[ContactHit]:
    k = _key("HUNTER_API_KEY")
    if not k or not domain:
        log.debug("hunter.skipped", domain=domain, has_key=bool(k))
        return []
    url = "https://api.hunter.io/v2/domain-search"
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(url, params={"domain": domain, "api_key": k, "limit": 25})
            r.raise_for_status()
            emails = r.json().get("data", {}).get("emails", [])
        except Exception as e:
            log.warn("hunter.failed", error=str(e))
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


# ============================================================
# Google Places — company phone/website by name
# ============================================================

async def google_places_find_company(name: str, city: str = "New York, NY") -> list[CompanyHit]:
    k = _key("GOOGLE_PLACES_API_KEY")
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


# ============================================================
# Apollo.io — person + email + phone
# ============================================================

async def apollo_person_match(full_name: str, company: str = None) -> list[ContactHit]:
    k = _key("APOLLO_API_KEY")
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


# ============================================================
# Whitepages Pro — phone lookup
# ============================================================

async def whitepages_person_search(full_name: str, city: str = "New York") -> list[ContactHit]:
    """
    Whitepages Pro Person Search — GET /v2/person
    Auth: X-Api-Key header.
    Billing: every 200 response is 1 query (even if empty results).
             Only called after Apollo has already failed, to conserve the trial.

    Returns at most 1 ContactHit (highest-score record with a usable phone).
    """
    k = _key("WHITEPAGES_API_KEY")
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


# ============================================================
# Proxycurl — LinkedIn profile
# ============================================================

async def proxycurl_person_lookup(
    first_name: str, last_name: str, company_name: str = None,
) -> list[ContactHit]:
    k = _key("PROXYCURL_API_KEY")
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


# ============================================================
# Zoominfo — person search (corporate contacts)
# ============================================================

async def zoominfo_person_at_company(full_name: str, company: str) -> list[ContactHit]:
    # Delegates to the existing enrichment/zoominfo.py module if wired.
    try:
        from enrichment.zoominfo import search_contacts_at_company
        return await search_contacts_at_company(full_name, company)
    except ImportError:
        return []
    except Exception as e:
        log.warn("zoominfo.failed", error=str(e))
        return []
