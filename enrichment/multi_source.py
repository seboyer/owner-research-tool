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
)

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

        if contacts:
            log.info("multi_source.ai_found", entity=entity_name, count=len(contacts))
        return bool(contacts)

    except Exception as e:
        log.warning("multi_source.ai_error", entity=entity_name, error=str(e))
        return False


# ============================================================
# Source 2: Whitepages Pro
# Works for: individuals with US phone numbers
# Best for: small residential landlords, individual owners
# Docs: https://pro.whitepages.com/developer/documentation/
# ============================================================

async def enrich_via_whitepages(
    entity_id: str,
    full_name: str,
    address: str = "",
    city: str = "New York",
    state: str = "NY",
) -> bool:
    """
    Look up an individual's phone number via Whitepages Pro.
    Add WHITEPAGES_API_KEY to .env when you have it.
    """
    api_key = config.__dict__.get("WHITEPAGES_API_KEY") or \
               __import__("os").getenv("WHITEPAGES_API_KEY", "")
    if not api_key:
        return False

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(
                "https://proapi.whitepages.com/3.0/person",
                params={
                    "name": full_name,
                    "city": city,
                    "state_code": state,
                    "api_key": api_key,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            people = data.get("results", [])
            if not people:
                return False

            person = people[0]
            phones = person.get("phones", [])
            emails = person.get("emails", [])

            phone = phones[0].get("line_type_name", "") + " " + phones[0].get("phone_number", "") if phones else None
            email = emails[0].get("email_address") if emails else None

            if phone or email:
                upsert_contact(entity_id, {
                    "full_name": full_name,
                    "phone": phone,
                    "email": email,
                    "source": "whitepages",
                    "confidence": 0.80,
                })
                log.info("multi_source.whitepages_found", name=full_name)
                return True

        except Exception as e:
            log.warning("multi_source.whitepages_error", name=full_name, error=str(e))

    return False


# ============================================================
# Source 3: PropertyRadar
# Works for: any property owner — has contact info tied to BBL
# Best for: individual landlords who don't have a web presence
# Docs: https://www.propertyradar.com/api
# ============================================================

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
            resp = await client.get(
                "https://api.propertyradar.com/v1/properties",
                params={
                    "ownerName": entity_name,
                    "state": "NY",
                    "county": "New York",
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
            )
            resp.raise_for_status()
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
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params={
                    "query": f"{entity_name} NYC real estate",
                    "location": "40.7128,-74.0060",  # NYC lat/lng
                    "radius": 50000,
                    "key": api_key,
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            places = data.get("results", [])

            if not places:
                return False

            place = places[0]
            place_id = place.get("place_id")

            # Get details
            details_resp = await client.get(
                "https://maps.googleapis.com/maps/api/place/details/json",
                params={
                    "place_id": place_id,
                    "fields": "name,formatted_phone_number,website,formatted_address",
                    "key": api_key,
                },
                timeout=10,
            )
            details_resp.raise_for_status()
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
            resp = await client.get(
                "https://api.hunter.io/v2/domain-search",
                params={
                    "domain": domain,
                    "company": entity_name,
                    "limit": 5,
                    "api_key": api_key,
                },
                timeout=10,
            )
            resp.raise_for_status()
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
            # Person search
            resp = await client.get(
                "https://nubela.co/proxycurl/api/linkedin/profile/resolve",
                params={
                    "first_name": full_name.split(" ")[0],
                    "last_name": full_name.split(" ")[-1] if " " in full_name else "",
                    "company_domain": "",
                    "location": "New York, New York, United States",
                    "title": "real estate",
                    "similarity_checks": "include",
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            linkedin_url = data.get("url")

            if not linkedin_url:
                return False

            # Now get the profile details
            profile_resp = await client.get(
                "https://nubela.co/proxycurl/api/v2/linkedin",
                params={
                    "url": linkedin_url,
                    "personal_email": "include",
                    "personal_contact_number": "include",
                },
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
            )
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
                log.info("multi_source.proxycurl_found", name=full_name)
                return True

        except Exception as e:
            log.warning("multi_source.proxycurl_error", name=full_name, error=str(e))

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

    # Check if we already have good contacts
    existing = db().table("contacts")\
        .select("id")\
        .eq("entity_id", entity_id)\
        .not_.is_("email", "null")\
        .limit(1)\
        .execute()

    if existing.data:
        log.info("multi_source.already_has_contacts", entity=entity_name)
        update_entity(entity_id, {"enrichment_status": "done"})
        return True

    # Management companies + larger firms → Google Places first
    if entity_type in ("management_company", "corporation") or \
       "management" in entity_name.lower() or "realty" in entity_name.lower():
        found_any |= await enrich_via_google_places(entity_id, entity_name, address)
        await asyncio.sleep(0.5)

    # AI web search — works for any entity type
    found_any |= await enrich_via_ai_web_search(entity_id, entity_name, entity_type, address)
    await asyncio.sleep(0.5)

    # Individuals → PropertyRadar + Whitepages + Proxycurl
    if entity_type == "individual" or (
        entity_type == "unknown" and " " in entity_name and "LLC" not in entity_name.upper()
    ):
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

    if found_any:
        update_entity(entity_id, {"enrichment_status": "done"})

    return found_any


async def run_batch(batch_size: int = 100):
    """
    Enrich a batch of entities that:
    - Are either individuals, small LLCs, or entities Zoominfo missed
    - Don't yet have contact info
    """
    log_id = start_ingestion_log("multi_source_enrichment")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        # Individual landlords + small LLCs (below Zoominfo threshold)
        res = db().table("entities")\
            .select("*")\
            .or_("entity_type.eq.individual,entity_type.eq.unknown")\
            .neq("enrichment_status", "done")\
            .order("portfolio_size", desc=True)\
            .limit(batch_size)\
            .execute()

        entities = res.data
        stats["records_fetched"] = len(entities)
        log.info("multi_source.batch_start", count=len(entities))

        for entity in entities:
            try:
                found = await enrich_entity(entity)
                if found:
                    stats["records_created"] += 1
                else:
                    stats["records_skipped"] += 1
            except Exception as e:
                log.error("multi_source.entity_error",
                          entity=entity.get("name"), error=str(e))

            await asyncio.sleep(1)

        finish_ingestion_log(log_id, stats)
        log.info("multi_source.batch_complete", **stats)

    except Exception as e:
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        raise
