"""
Apollo.io — organisation search + people at org.

Two entry points:
  apollo_org_search(name)               → Apollo org_id (str) or None
  apollo_org_people(org_id, ...)        → list[ContactHit]

Auth: X-Api-Key header (NOT in JSON body — Apollo returns 422 otherwise).
API key: APOLLO_API_KEY in .env / config.

Endpoints used:
  POST /v1/mixed_companies/search       — find best org by name
  POST /v1/mixed_people/api_search      — find senior people at that org
                                          (the older /search endpoint is now
                                          deprecated for API callers — returns 422)
  POST /v1/people/match (id=...)        — enrich one person by Apollo id to
                                          retrieve unmasked last_name + email

Two-step workflow for actual contact data:
  1. api_search returns DISCOVERY rows — masked last_name (e.g. "Mo***s"),
     no email/phone, only has_email / has_direct_phone flags.
  2. people/match with id=<apollo_id> returns the FULL record — full last_name,
     verified email, linkedin_url. Costs ~1 credit per person.
  Phone reveal requires a webhook callback (async) and is not implemented
  here — we rely on Hunter / BatchData / HPD for phone numbers.
"""


import httpx
import structlog

from config import config
from database.retry import retry_external

from ..models import ContactHit

log = structlog.get_logger(__name__)

APOLLO_BASE = "https://api.apollo.io"

_DEFAULT_SENIORITIES = ("owner", "founder", "c_suite", "vp", "director")


def _api_key() -> str | None:
    return config.APOLLO_API_KEY or None


@retry_external(max_attempts=3)
async def _post(client: httpx.AsyncClient, path: str, payload: dict, api_key: str) -> dict:
    """POST to Apollo API with X-Api-Key header."""
    r = await client.post(
        f"{APOLLO_BASE}{path}",
        json=payload,
        headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
    )
    r.raise_for_status()
    return r.json()


async def apollo_org_search(name: str) -> str | None:
    """POST /v1/mixed_companies/search with the company name.

    Returns the Apollo organization_id of the best match, or None.
    """
    k = _api_key()
    if not k:
        log.debug("apollo_org.search.skipped", entity=name)
        return None

    payload = {
        "q_organization_name": name,
        "per_page": 1,
        "page": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            data = await _post(client, "/v1/mixed_companies/search", payload, k)
    except Exception as e:
        log.warning("apollo_org.search.failed", entity=name, error=str(e))
        return None

    orgs = data.get("organizations") or []
    if not orgs:
        log.debug("apollo_org.search.no_match", entity=name)
        return None

    org = orgs[0]
    org_id = org.get("id")
    log.info("apollo_org.search.matched",
             entity=name, apollo_name=org.get("name"), org_id=org_id)
    return org_id


async def apollo_org_people(
    org_id: str,
    seniorities: tuple[str, ...] = _DEFAULT_SENIORITIES,
    per_page: int = 25,
) -> list[ContactHit]:
    """POST /v1/mixed_people/api_search for senior people at this Apollo org.

    Returns ContactHit per person with:
      network_role='officer', role_category='owner',
      confidence=0.65 (discovery-only, no email/phone),
      cost_cents=1 (search is much cheaper than enrichment).

    NOTE: api_search returns DISCOVERY data only. last_name is obfuscated,
    email/phone are not returned — only boolean flags (has_email,
    has_direct_phone). Callers that need real email/phone must layer a
    second enrichment call (e.g. apollo_person_match) on top of these hits.
    The evidence field records what Apollo claims is available so downstream
    code can decide whether to spend a credit on enrichment.
    """
    k = _api_key()
    if not k:
        log.debug("apollo_org.people.skipped", org_id=org_id)
        return []

    payload = {
        "organization_ids": [org_id],
        "person_seniorities": list(seniorities),
        "per_page": per_page,
        "page": 1,
    }
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            data = await _post(client, "/v1/mixed_people/api_search", payload, k)
    except Exception as e:
        log.warning("apollo_org.people.failed", org_id=org_id, error=str(e))
        return []

    people = data.get("people") or []
    out: list[ContactHit] = []
    for p in people:
        first = (p.get("first_name") or "").strip()
        # api_search obfuscates surnames; fall back to the masked form when
        # full last_name isn't present (the masked form is still useful for
        # display + dedup).
        last = (p.get("last_name") or p.get("last_name_obfuscated") or "").strip()
        full = (p.get("name") or f"{first} {last}").strip()
        if not full or not first:
            continue

        has_email = bool(p.get("has_email"))
        # has_direct_phone is the string "Yes"/"No" in api_search
        has_phone = str(p.get("has_direct_phone") or "").lower() == "yes"

        availability_bits = []
        if has_email:
            availability_bits.append("email")
        if has_phone:
            availability_bits.append("direct phone")
        availability = ", ".join(availability_bits) or "no contact data"

        out.append(ContactHit(
            full_name=full,
            first_name=first or None,
            last_name=last or None,
            title=p.get("title"),
            # api_search does not return these — leave None, surface availability
            # in evidence so the cascade can decide whether to spend on enrichment.
            email=None,
            phone=None,
            linkedin_url=None,
            company_name=(p.get("organization") or {}).get("name"),
            network_role="officer",
            role_category="owner",
            confidence=0.65,
            source="apollo_org",
            evidence=f"Apollo.io org people search (has: {availability})",
            cost_cents=1,
            raw=p,
        ))

    log.info("apollo_org.people.found", org_id=org_id, count=len(out))
    return out


async def apollo_person_enrich_by_id(person_id: str) -> ContactHit | None:
    """Enrich a single person by their Apollo id.

    Calls POST /v1/people/match with {"id": person_id}, which returns the
    full unmasked person record including verified email + linkedin_url.

    Cost: ~1 Apollo credit per call (≈ $1 depending on plan). We record
    cost_cents=100 conservatively. Phone retrieval requires a webhook
    callback (asynchronous) and is intentionally not requested.

    Returns None if the api_key is missing, the person isn't found, or
    Apollo returns no useful fields.
    """
    k = _api_key()
    if not k:
        log.debug("apollo_org.enrich.skipped", person_id=person_id)
        return None

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            data = await _post(client, "/v1/people/match", {"id": person_id}, k)
    except Exception as e:
        log.warning("apollo_org.enrich.failed", person_id=person_id, error=str(e))
        return None

    p = data.get("person") or {}
    if not p:
        return None

    first = (p.get("first_name") or "").strip() or None
    last = (p.get("last_name") or "").strip() or None
    full = (p.get("name") or f"{first or ''} {last or ''}").strip()
    if not full:
        return None

    email = p.get("email") or None
    # Apollo flags certain placeholder emails as "email_status": "unavailable"
    if (p.get("email_status") or "").lower() in ("unavailable", "bounced"):
        email = None

    return ContactHit(
        full_name=full,
        first_name=first,
        last_name=last,
        title=p.get("title"),
        email=email,
        phone=None,  # phone reveal needs async webhook — skipped
        linkedin_url=p.get("linkedin_url"),
        company_name=(p.get("organization") or {}).get("name"),
        network_role="officer",
        role_category="owner",
        confidence=0.85,
        source="apollo_person_enrich",
        evidence=f"Apollo people/match id={person_id} (email_status={p.get('email_status')})",
        cost_cents=100,
        raw=p,
    )
