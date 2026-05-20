"""
enrichment/multi_source.py — Multi-Source Enrichment for Small Landlords

Zoominfo works well for corporate entities with public profiles.
For smaller/individual landlords, we need different approaches:

  1. AI Web Search (Claude)     — searches Google/Bing for the person/company,
                                  scrapes LinkedIn, business websites, etc.
  2. Whitepages Pro             — phone + address for individuals (US residential)
  3. PropertyRadar              — property owner contact info (direct RE intelligence)
  4. Google Places API          — phone/website for businesses (mgmt companies)
  5. Hunter.io                  — email discovery by domain
  6. Proxycurl / LinkedIn       — LinkedIn contact info

These are layered: we try cheaper/faster sources first, only escalate if needed.

Configuration:
  All API keys are optional — the system degrades gracefully if a key is missing.
  Add keys to .env as you acquire them.
"""

import asyncio
import json
import re
from typing import Optional

import httpx
import structlog
from anthropic import Anthropic

from config import config
from database.client import (
    db, upsert_contact, update_entity,
    start_ingestion_log, finish_ingestion_log,
    get_enrichment_batch, mark_enrichment_done, mark_enrichment_failed,
)
from database.retry import retry_external

log = structlog.get_logger(__name__)
anthropic = Anthropic(api_key=config.ANTHROPIC_API_KEY)


# ============================================================
# Source 1: AI Web Search (Claude + OpenAI)
# Works for: any entity — uses public web information
# Cost: per-token Claude/OpenAI costs only
# ============================================================

CONTACT_EXTRACTION_PROMPT = """You are researching a NYC real estate landlord/property owner to find their contact information.

Entity: {name}
Type: {entity_type}
Address: {address}
Context: This entity owns/manages real estate in New York City.

Search for and extract:
1. Email address(es) — prefer business/professional emails
2. Phone number(s) — direct line or office preferred
3. Website or LinkedIn URL
4. Any individual's name associated with this entity (if it's a company)

Sources to check mentally: company website, LinkedIn, NYC property records, news articles,
court filings, building permit applications, real estate listings, BBB, Yelp, Google Maps.

Return ONLY a JSON object:
{{
  "contacts": [
    {{
      "full_name": "Name if individual",
      "email": "email@example.com or null",
      "phone": "212-555-1234 or null",
      "website": "https://... or null",
      "source_url": "where you found this",
      "confidence": 0.0-1.0
    }}
  ],
  "notes": "any useful context about this entity"
}}

If no contact info found, return {{"contacts": [], "notes": "not found"}}
"""


async def enrich_via_ai_web_search(
    entity_id: str,
    entity_name: str,
    entity_type: str,
    address: str = "",
) -> bool:
    """
    Use Claude with web search to find contact info for any entity.
    This is a general-purpose fallback that works for any entity type.
    """
    try:
        # Try using OpenAI with web search (gpt-4o-search-preview) first
        # as it has native web search
        if config.OPENAI_API_KEY:
            from openai import OpenAI
            oai = OpenAI(api_key=config.OPENAI_API_KEY)

            prompt = CONTACT_EXTRACTION_PROMPT.format(
                name=entity_name,
                entity_type=entity_type,
                address=address or "NYC",
            )

            response = oai.chat.completions.create(
                model="gpt-4o-search-preview",
                messages=[
                    {"role": "system", "content": "You research NYC landlord contact information. Return only valid JSON."},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=800,
            )
            text = response.choices[0].message.content or ""
        else:
            # Fallback to Claude (which has knowledge but no real-time search by default)
            response = anthropic.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=800,
                messages=[{
                    "role": "user",
                    "content": CONTACT_EXTRACTION_PROMPT.format(
                        name=entity_name,
                        entity_type=entity_type,
                        address=address or "NYC",
                    ),
                }],
            )
            text = response.content[0].text

        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if not json_match:
            return False

        result = json.loads(json_match.group())
        contacts = result.get("contacts", [])

        # Track ACTUAL writes — not just "AI returned a JSON list". Previously
        # we returned bool(contacts) which counted entries with no email/phone
        # as a success, inflating records_created on the dashboard while no
        # contact actually landed in the contacts table.
        written = 0
        for contact in contacts:
            if not (contact.get("email") or contact.get("phone")):
                continue

            full_name = contact.get("full_name", entity_name)
            name_parts = full_name.rsplit(" ", 1)

            upsert_contact(entity_id, {
                "first_name": name_parts[0] if len(name_parts) > 1 else full_name,
                "last_name": name_parts[1] if len(name_parts) > 1 else "",
                "full_name": full_name,
                "email": contact.get("email"),
                "phone": contact.get("phone"),
                "source": "ai_web_search",
                "confidence": contact.get("confidence", 0.6),
                "raw_data": {"source_url": contact.get("source_url"), "notes": result.get("notes")},
            })
            written += 1

        if written:
            log.info("multi_source.ai_found", entity=entity_name, count=written)
        return written > 0

    except Exception as e:
        log.warning("multi_source.ai_error", entity=entity_name, error=str(e))
        return False


# ============================================================
# Source 2: Whitepages Pro
# Works for: individuals with US phone numbers
# Best for: small residential landlords, individual owners
# Docs: https://api.whitepages.com/docs/documentation/getting-started
#
# Migrated from the legacy proapi.whitepages.com/3.0/person endpoint
# (NXDOMAIN as of 2026) to api.whitepages.com/v2/person. Auth moved from
# api_key query param to X-Api-Key header. Response shape simplified:
# the v2 endpoint returns a bare JSON array, not {"results": [...]}, and
# phones use {number, type} instead of {line_type_name, phone_number}.
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_whitepages(
    client: httpx.AsyncClient, full_name: str, city: str, state: str, api_key: str,
) -> httpx.Response:
    """Inner HTTP helper for Whitepages v2 person lookup — retried on transient errors."""
    resp = await client.get(
        "https://api.whitepages.com/v2/person",
        params={"name": full_name, "city": city, "state_code": state},
        headers={"X-Api-Key": api_key},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_whitepages(
    entity_id: str,
    full_name: str,
    address: str = "",
    city: str = "New York",
    state: str = "NY",
) -> bool:
    """
    Look up an individual via Whitepages v2 person endpoint and upsert phone/email.
    WHITEPAGES_API_KEY must be set on the worker service.
    """
    api_key = config.__dict__.get("WHITEPAGES_API_KEY") or \
               __import__("os").getenv("WHITEPAGES_API_KEY", "")
    if not api_key:
        return False

    async with httpx.AsyncClient() as client:
        try:
            resp = await _fetch_whitepages(client, full_name, city, state, api_key)
            # v2 returns a bare JSON array. A 404 means "no match found" — the
            # docs note this is normal; treat it as no-result rather than error.
            people = resp.json() or []
            if not people:
                return False

            person = people[0]
            phones = person.get("phones") or []
            emails = person.get("emails") or []

            phone = None
            if phones:
                p0 = phones[0]
                num = p0.get("number") or ""
                ptype = p0.get("type") or ""
                phone = f"{ptype} {num}".strip() if ptype else num
            email = None
            if emails:
                e0 = emails[0]
                email = e0.get("email") or e0.get("email_address")

            if not (phone or email):
                return False

            upsert_contact(entity_id, {
                "full_name": person.get("name") or full_name,
                "phone": phone,
                "email": email,
                "source": "whitepages",
                "confidence": 0.80,
                "raw_data": person,
            })
            log.info("multi_source.whitepages_found", entity=full_name,
                     has_phone=bool(phone), has_email=bool(email))
            return True

        except httpx.HTTPStatusError as e:
            # 404 = no records matched the query — normal, not an error.
            if e.response.status_code == 404:
                return False
            log.warning("multi_source.whitepages_http_error",
                        entity=full_name, status=e.response.status_code,
                        body=e.response.text[:200])
        except Exception as e:
            log.warning("multi_source.whitepages_error", entity=full_name, error=str(e))

    return False


# ============================================================
# Source 3a: Apollo.io
# Works for: individuals (person match by name)
# Best for: any individual landlord with a professional web presence
# Cost: ~$0.05/match. Auth via X-Api-Key header (NOT in JSON body).
# Docs: https://api-docs.apollo.io/reference/match-person
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_apollo_person(
    client: httpx.AsyncClient, full_name: str, api_key: str, company: str | None = None,
) -> httpx.Response:
    """Inner HTTP helper for Apollo people/match — retried on transient errors."""
    payload: dict = {"name": full_name}
    if company:
        payload["organization_name"] = company
    resp = await client.post(
        "https://api.apollo.io/v1/people/match",
        json=payload,
        headers={"X-Api-Key": api_key, "Content-Type": "application/json"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_apollo(
    entity_id: str,
    full_name: str,
    company: str | None = None,
) -> bool:
    """
    Look up a person's email + phone via Apollo.io /v1/people/match.
    APOLLO_API_KEY must be set (on the worker service in production).
    """
    api_key = config.APOLLO_API_KEY
    if not api_key:
        return False

    async with httpx.AsyncClient() as client:
        try:
            resp = await _fetch_apollo_person(client, full_name, api_key, company)
            data = resp.json() or {}
            person = data.get("person") or {}
            if not person:
                return False

            email = person.get("email")
            phones = person.get("phone_numbers") or []
            phone = phones[0].get("sanitized_number") if phones else None

            if not (email or phone):
                return False

            name_parts = (person.get("name") or full_name).rsplit(" ", 1)
            upsert_contact(entity_id, {
                "first_name": person.get("first_name") or (name_parts[0] if len(name_parts) > 1 else full_name),
                "last_name": person.get("last_name") or (name_parts[1] if len(name_parts) > 1 else ""),
                "full_name": person.get("name") or full_name,
                "email": email,
                "phone": phone,
                "source": "apollo",
                "confidence": 0.85,
                "raw_data": person,
            })
            log.info("multi_source.apollo_found", entity=full_name,
                     has_email=bool(email), has_phone=bool(phone))
            return True
        except Exception as e:
            log.warning("multi_source.apollo_error", entity=full_name, error=str(e))
            return False


# ============================================================
# Source 3b: BatchData V3 Skip Trace
# Works for: any property — returns up to 3 persons at the address.
# Best for: individual landlords (the most reliable source per testing).
# Cost: ~$0.40 per matched property.
# Auth: Bearer token. Docs: https://app.batchdata.com/docs/api/v3
# ============================================================

async def enrich_via_batchdata(entity_id: str, entity_name: str) -> bool:
    """
    Skip-trace the property linked to this entity via BatchData V3 and write
    the matched person's contact info. Reuses the existing
    enrichment.contact.sources.batchdata.skip_trace_property helper.
    """
    if not config.BATCHDATA_API_KEY:
        return False

    # Find a property address for this entity.
    roles_res = db().table("property_roles")\
        .select("properties(house_number, street_name, zip_code, address)")\
        .eq("entity_id", entity_id)\
        .eq("is_current", True)\
        .limit(1)\
        .execute()
    if not roles_res.data:
        return False

    prop = roles_res.data[0].get("properties") or {}
    house = (prop.get("house_number") or "").strip()
    street_name = (prop.get("street_name") or "").strip()
    street = f"{house} {street_name}".strip() if (house or street_name) else (prop.get("address") or "").strip()
    zip_code = (prop.get("zip_code") or "").strip()
    if not street or not zip_code:
        # BatchData needs at least street + zip (or street + city).
        return False

    try:
        from enrichment.contact.sources.batchdata import skip_trace_property
        hits = await skip_trace_property(
            street=street,
            city="New York",
            state="NY",
            zip_code=zip_code,
        )
    except Exception as e:
        log.warning("multi_source.batchdata_error", entity=entity_name, error=str(e))
        return False

    if not hits:
        return False

    # Take the first returned person (treated as primary property owner by
    # prong1_signer.py, same convention here). Match Whitepages/Apollo
    # behavior: only upsert if we got a real email or phone.
    hit = hits[0]
    if not (hit.email or hit.phone):
        return False

    upsert_contact(entity_id, {
        "first_name": hit.first_name,
        "last_name": hit.last_name,
        "full_name": hit.full_name or entity_name,
        "email": hit.email,
        "phone": hit.phone,
        "source": "batchdata_skip_trace",
        "confidence": hit.confidence,
        "raw_data": hit.raw,
    })
    log.info("multi_source.batchdata_found", entity=entity_name,
             matched=hit.full_name, has_email=bool(hit.email), has_phone=bool(hit.phone))
    return True


# ============================================================
# Source 3: PropertyRadar
# Works for: any property owner — has contact info tied to BBL
# Best for: individual landlords who don't have a web presence
# Docs: https://www.propertyradar.com/api
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_propertyradar(client: httpx.AsyncClient, entity_name: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for PropertyRadar owner lookup — retried on transient errors."""
    resp = await client.get(
        "https://api.propertyradar.com/v1/properties",
        params={"ownerName": entity_name, "state": "NY", "county": "New York"},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_propertyradar(
    entity_id: str,
    entity_name: str,
    bbl: str = "",
) -> bool:
    """
    Look up property owner contact info via PropertyRadar.
    Add PROPERTYRADAR_API_KEY to .env.

    PropertyRadar has phone, email, and mailing address for property owners.
    Works especially well for individual landlords.
    """
    api_key = __import__("os").getenv("PROPERTYRADAR_API_KEY", "")
    if not api_key:
        return False

    # PropertyRadar uses APN (Assessor's Parcel Number) or owner name search
    async with httpx.AsyncClient() as client:
        try:
            # Search by owner name
            resp = await _fetch_propertyradar(client, entity_name, api_key)
            data = resp.json()
            properties = data.get("results", [])

            if not properties:
                return False

            # Get contact info for the first match
            prop = properties[0]
            owner = prop.get("owner", {})

            phone = owner.get("phone") or owner.get("mobilePhone")
            email = owner.get("email")
            full_name = owner.get("fullName") or entity_name

            if phone or email:
                name_parts = full_name.rsplit(" ", 1)
                upsert_contact(entity_id, {
                    "first_name": name_parts[0] if len(name_parts) > 1 else full_name,
                    "last_name": name_parts[1] if len(name_parts) > 1 else "",
                    "full_name": full_name,
                    "phone": phone,
                    "email": email,
                    "source": "propertyradar",
                    "confidence": 0.85,
                    "raw_data": prop,
                })
                log.info("multi_source.propertyradar_found", entity=entity_name)
                return True

        except Exception as e:
            log.warning("multi_source.propertyradar_error", entity=entity_name, error=str(e))

    return False


# ============================================================
# Source 4: Google Places API
# Works for: management companies with a business listing
# Best for: professional property managers, real estate firms
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_google_places_search(client: httpx.AsyncClient, entity_name: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for Google Places text search — retried on transient errors."""
    resp = await client.get(
        "https://maps.googleapis.com/maps/api/place/textsearch/json",
        params={
            "query": f"{entity_name} NYC real estate",
            "location": "40.7128,-74.0060",
            "radius": 50000,
            "key": api_key,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


@retry_external(max_attempts=3)
async def _fetch_google_places_details(client: httpx.AsyncClient, place_id: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for Google Places details — retried on transient errors."""
    resp = await client.get(
        "https://maps.googleapis.com/maps/api/place/details/json",
        params={
            "place_id": place_id,
            "fields": "name,formatted_phone_number,website,formatted_address",
            "key": api_key,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_google_places(
    entity_id: str,
    entity_name: str,
    address: str = "",
) -> bool:
    """
    Look up a business in Google Places to get phone + website.
    Works well for management companies that have a public Google listing.
    Add GOOGLE_PLACES_API_KEY to .env.
    """
    api_key = __import__("os").getenv("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        return False

    async with httpx.AsyncClient() as client:
        try:
            # Text search
            resp = await _fetch_google_places_search(client, entity_name, api_key)
            data = resp.json()
            places = data.get("results", [])

            if not places:
                return False

            place = places[0]
            place_id = place.get("place_id")

            # Get details
            details_resp = await _fetch_google_places_details(client, place_id, api_key)
            details = details_resp.json().get("result", {})

            phone = details.get("formatted_phone_number")
            website = details.get("website")

            if phone or website:
                upsert_contact(entity_id, {
                    "full_name": entity_name,
                    "phone": phone,
                    "source": "google_places",
                    "confidence": 0.75,
                    "raw_data": {"website": website, "google_place_id": place_id},
                })
                update_entity(entity_id, {
                    "raw_data": {"website": website, "google_place_id": place_id},
                })
                log.info("multi_source.google_places_found", entity=entity_name)
                return True

        except Exception as e:
            log.warning("multi_source.google_places_error", entity=entity_name, error=str(e))

    return False


# ============================================================
# Source 5: Hunter.io Email Finder
# Works for: any entity with a known website domain
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_hunter_domain_search(client: httpx.AsyncClient, domain: str, entity_name: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for Hunter.io domain search — retried on transient errors."""
    resp = await client.get(
        "https://api.hunter.io/v2/domain-search",
        params={"domain": domain, "company": entity_name, "limit": 5, "api_key": api_key},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_hunter(
    entity_id: str,
    entity_name: str,
    domain: str = "",
) -> bool:
    """
    Use Hunter.io to find email addresses for a domain/company.
    Add HUNTER_API_KEY to .env.
    """
    api_key = __import__("os").getenv("HUNTER_API_KEY", "")
    if not api_key or not domain:
        return False

    async with httpx.AsyncClient() as client:
        try:
            resp = await _fetch_hunter_domain_search(client, domain, entity_name, api_key)
            data = resp.json().get("data", {})
            emails = data.get("emails", [])

            found = False
            for email_entry in emails[:3]:
                email = email_entry.get("value")
                if not email:
                    continue
                first = email_entry.get("first_name", "")
                last = email_entry.get("last_name", "")
                confidence_score = email_entry.get("confidence", 0) / 100.0

                upsert_contact(entity_id, {
                    "first_name": first,
                    "last_name": last,
                    "full_name": f"{first} {last}".strip() or entity_name,
                    "title": email_entry.get("position", ""),
                    "email": email,
                    "email_verified": email_entry.get("verification", {}).get("status") == "valid",
                    "source": "hunter",
                    "confidence": confidence_score,
                })
                found = True

            if found:
                log.info("multi_source.hunter_found", entity=entity_name, count=len(emails))
            return found

        except Exception as e:
            log.warning("multi_source.hunter_error", entity=entity_name, error=str(e))

    return False


# ============================================================
# Source 6: Proxycurl (LinkedIn)
# Works for: individuals with a LinkedIn presence
# ============================================================

@retry_external(max_attempts=3)
async def _fetch_proxycurl_resolve(client: httpx.AsyncClient, first_name: str, last_name: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for Proxycurl LinkedIn profile resolve — retried on transient errors."""
    resp = await client.get(
        "https://nubela.co/proxycurl/api/linkedin/profile/resolve",
        params={
            "first_name": first_name,
            "last_name": last_name,
            "company_domain": "",
            "location": "New York, New York, United States",
            "title": "real estate",
            "similarity_checks": "include",
        },
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


@retry_external(max_attempts=3)
async def _fetch_proxycurl_profile(client: httpx.AsyncClient, linkedin_url: str, api_key: str) -> httpx.Response:
    """Inner HTTP helper for Proxycurl LinkedIn profile details — retried on transient errors."""
    resp = await client.get(
        "https://nubela.co/proxycurl/api/v2/linkedin",
        params={"url": linkedin_url, "personal_email": "include", "personal_contact_number": "include"},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp


async def enrich_via_proxycurl(
    entity_id: str,
    full_name: str,
    company_name: str = "",
) -> bool:
    """
    Use Proxycurl to find a person's LinkedIn profile and contact info.
    Add PROXYCURL_API_KEY to .env.
    Good for finding email + phone for individual property owners.
    """
    api_key = __import__("os").getenv("PROXYCURL_API_KEY", "")
    if not api_key:
        return False

    async with httpx.AsyncClient() as client:
        try:
            first_name = full_name.split(" ")[0]
            last_name = full_name.split(" ")[-1] if " " in full_name else ""
            # Person search
            resp = await _fetch_proxycurl_resolve(client, first_name, last_name, api_key)
            data = resp.json()
            linkedin_url = data.get("url")

            if not linkedin_url:
                return False

            # Now get the profile details
            profile_resp = await _fetch_proxycurl_profile(client, linkedin_url, api_key)
            profile = profile_resp.json()

            emails = profile.get("personal_emails", [])
            phones = profile.get("personal_numbers", [])
            email = emails[0] if emails else None
            phone = phones[0] if phones else None

            if email or phone or linkedin_url:
                name_parts = full_name.rsplit(" ", 1)
                upsert_contact(entity_id, {
                    "first_name": name_parts[0] if len(name_parts) > 1 else full_name,
                    "last_name": name_parts[1] if len(name_parts) > 1 else "",
                    "full_name": full_name,
                    "email": email,
                    "phone": phone,
                    "linkedin_url": linkedin_url,
                    "source": "proxycurl",
                    "confidence": 0.85,
                })
                log.info("multi_source.proxycurl_found", entity=full_name)
                return True

        except Exception as e:
            log.warning("multi_source.proxycurl_error", entity=full_name, error=str(e))

    return False


# ============================================================
# Orchestrated Multi-Source Enrichment
# ============================================================

async def enrich_entity(entity: dict) -> bool:
    """
    Run all available enrichment sources for an entity.
    Chooses the right sources based on entity type and what info we already have.
    Returns True if we found any contact info.
    """
    entity_id = entity["id"]
    entity_name = entity["name"]
    entity_type = entity.get("entity_type", "unknown")
    address = entity.get("address", "")

    found_any = False

    # Check if we already have good contacts — short-circuit to avoid paying
    # for re-enrichment of entities we already have data for. Returning False
    # here makes the run accounting honest (records_created counts only
    # entities where NEW contacts were written; this entity goes to
    # records_skipped instead).
    existing = db().table("contacts")\
        .select("id")\
        .eq("entity_id", entity_id)\
        .not_.is_("email", "null")\
        .limit(1)\
        .execute()

    if existing.data:
        log.info("multi_source.already_has_contacts", entity=entity_name)
        return False

    # Management companies + larger firms → Google Places first
    if entity_type in ("management_company", "corporation") or \
       "management" in entity_name.lower() or "realty" in entity_name.lower():
        found_any |= await enrich_via_google_places(entity_id, entity_name, address)
        await asyncio.sleep(0.5)

    # AI web search — works for any entity type
    found_any |= await enrich_via_ai_web_search(entity_id, entity_name, entity_type, address)
    await asyncio.sleep(0.5)

    # Individuals → BatchData + Apollo + PropertyRadar + Whitepages + Proxycurl
    if entity_type == "individual" or (
        entity_type == "unknown" and " " in entity_name and "LLC" not in entity_name.upper()
    ):
        # BatchData V3 skip trace — highest-yield source per testing.
        # ~$0.40 per matched property; goes first so the best source wins.
        found_any |= await enrich_via_batchdata(entity_id, entity_name)
        await asyncio.sleep(0.5)

        # Apollo.io — cheap (~$0.05) person match, high quality when it hits.
        found_any |= await enrich_via_apollo(entity_id, entity_name)
        await asyncio.sleep(0.5)

        # PropertyRadar — best for property owners
        bbl = None
        roles = db().table("property_roles")\
            .select("properties(bbl)")\
            .eq("entity_id", entity_id)\
            .limit(1)\
            .execute()
        if roles.data:
            bbl = roles.data[0].get("properties", {}).get("bbl", "")

        if bbl:
            found_any |= await enrich_via_propertyradar(entity_id, entity_name, bbl)
            await asyncio.sleep(0.5)

        # Whitepages for residential landlords
        found_any |= await enrich_via_whitepages(entity_id, entity_name)
        await asyncio.sleep(0.5)

        # LinkedIn via Proxycurl
        found_any |= await enrich_via_proxycurl(entity_id, entity_name)
        await asyncio.sleep(0.5)

    return found_any


async def run_batch(batch_size: int = 100):
    """
    Drain the multi_source enrichment queue, respecting the per-run
    cost cap (config.DAILY_ENRICHMENT_COST_CAP_USD). When the cap is hit
    the loop exits and unprocessed entities stay in the queue for the
    next run.
    """
    from pipeline.orchestrator import get_cost_tracker, COST_PER_ENTITY

    log_id = start_ingestion_log("multi_source_enrichment")
    stats = {
        "records_fetched": 0,
        "records_created": 0,
        "records_skipped": 0,
        "records_no_match": 0,
        "cost_estimated_usd": 0.0,
        "stopped_by_cost_cap": False,
    }
    tracker = get_cost_tracker()
    per_entity_cost = COST_PER_ENTITY["multi_source_enrich"]

    try:
        batch_num = 0
        while True:
            if tracker.cap_hit:
                stats["stopped_by_cost_cap"] = True
                log.info("multi_source.cost_cap_hit",
                         spent=tracker.total_spent, cap=tracker.cap_usd)
                break

            batch_num += 1
            queue_rows = get_enrichment_batch(enrichment_type="multi_source", limit=batch_size)
            if not queue_rows:
                break
            stats["records_fetched"] += len(queue_rows)
            log.info("multi_source.batch_iter", batch=batch_num, count=len(queue_rows))

            for row in queue_rows:
                entity = row.get("entities") or {}
                if not entity or not entity.get("id"):
                    continue
                entity_id = entity["id"]
                try:
                    # First job to pick up this entity flips 'pending' -> 'in_progress'.
                    if entity.get("enrichment_status") == "pending":
                        update_entity(entity_id, {"enrichment_status": "in_progress"})
                    found = await enrich_entity(entity)
                    mark_enrichment_done(entity_id, "multi_source")
                    tracker.add("multi_source_enrich", per_entity_cost)
                    if found:
                        stats["records_created"] += 1
                    else:
                        # Sources ran, no new contact written. Distinct from
                        # records_skipped which is reserved for entities we
                        # didn't process (allowlist/cap filtering).
                        stats["records_no_match"] += 1
                except Exception as e:
                    err = f"{type(e).__name__}: {e}"
                    log.error("multi_source.entity_error", entity=entity.get("name"), error=err)
                    mark_enrichment_failed(entity_id, "multi_source", err)

                if tracker.cap_hit:
                    stats["stopped_by_cost_cap"] = True
                    log.info("multi_source.cost_cap_hit_mid_batch",
                             spent=tracker.total_spent, cap=tracker.cap_usd)
                    break

                await asyncio.sleep(1)

            if tracker.cap_hit:
                break

        stats["cost_estimated_usd"] = round(tracker.stage_spent("multi_source_enrich"), 2)
        finish_ingestion_log(log_id, stats)
        log.info("multi_source.batch_complete", **stats, batches=batch_num)

    except Exception as e:
        stats["cost_estimated_usd"] = round(tracker.stage_spent("multi_source_enrich"), 2)
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        raise
