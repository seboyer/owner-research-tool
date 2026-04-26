"""
enrichment/zoominfo.py — Zoominfo Contact Enrichment

Zoominfo is best for:
  - Management companies (corporate entities, public-facing)
  - Large private landlord companies (Blackstone, Related, etc.)
  - Any entity with 3+ buildings (portfolio_size >= threshold)

NOT good for:
  - Individual small landlords (John Smith who owns 2 brownstones)
  - Shell LLCs (no profile in Zoominfo)

Zoominfo API auth:
  JWT-based: sign a JWT with your private key, exchange for access token.
  Docs: https://api-docs.zoominfo.com/#authentication

We use credits sparingly:
  - Only enrich entities with portfolio_size >= ZOOMINFO_MIN_PORTFOLIO_SIZE
  - Cache results in Supabase (don't re-query for 30 days)
"""

import json
import time
import asyncio
from typing import Optional

import httpx
import jwt  # PyJWT
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import config
from database.client import (
    db, upsert_contact, update_entity,
    start_ingestion_log, finish_ingestion_log,
)

log = structlog.get_logger(__name__)


# ============================================================
# Authentication
# ============================================================

_access_token: Optional[str] = None
_token_expires_at: float = 0


def _generate_jwt() -> str:
    """Generate a JWT for Zoominfo authentication."""
    payload = {
        "iss": config.ZOOMINFO_CLIENT_ID,
        "sub": config.ZOOMINFO_USERNAME,
        "iat": int(time.time()),
        "exp": int(time.time()) + 3600,
    }
    private_key = config.ZOOMINFO_PRIVATE_KEY.replace("\\n", "\n")
    return jwt.encode(payload, private_key, algorithm="RS256")


async def get_access_token() -> str:
    """Get a valid Zoominfo access token, refreshing if expired."""
    global _access_token, _token_expires_at

    if _access_token and time.time() < _token_expires_at - 60:
        return _access_token

    jwt_token = _generate_jwt()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            config.ZOOMINFO_AUTH_URL,
            json={"jwt": jwt_token},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        _access_token = data["jwt"]
        _token_expires_at = time.time() + data.get("expiresIn", 3600)

    return _access_token


# ============================================================
# Zoominfo API Client
# ============================================================

class ZoominfoClient:
    def __init__(self):
        self._token: Optional[str] = None

    async def _headers(self) -> dict:
        token = await get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    async def search_company(self, company_name: str, city: str = "New York") -> list[dict]:
        """Search for a company by name. Returns list of matches."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                config.ZOOMINFO_COMPANY_SEARCH_URL,
                headers=await self._headers(),
                json={
                    "rpp": 5,
                    "page": 1,
                    "companyName": company_name,
                    "cityName": city,
                    "stateCode": "NY",
                    "outputFields": [
                        "id", "name", "website", "phone", "street",
                        "city", "state", "zipCode", "employeeCount",
                        "linkedinUrl",
                    ],
                },
                timeout=20,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("outputFields", [])

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    async def search_contacts(
        self,
        company_id: str,
        titles: list[str] = None,
    ) -> list[dict]:
        """Get contacts at a specific Zoominfo company."""
        title_filter = titles or [
            "CEO", "President", "Owner", "Principal", "Managing Director",
            "Managing Member", "Partner", "VP", "Vice President",
            "Property Manager", "Asset Manager", "Director",
        ]

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                config.ZOOMINFO_CONTACT_SEARCH_URL,
                headers=await self._headers(),
                json={
                    "rpp": 20,
                    "page": 1,
                    "companyId": company_id,
                    "jobTitle": title_filter,
                    "outputFields": [
                        "id", "firstName", "lastName", "email",
                        "phone", "directPhone", "mobilePhone",
                        "jobTitle", "jobFunction", "linkedinUrl",
                        "companyId", "companyName",
                    ],
                },
                timeout=20,
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", {}).get("outputFields", [])

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=15))
    async def enrich_company(self, company_id: str) -> dict | None:
        """Get full enrichment data for a Zoominfo company ID."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{config.ZOOMINFO_COMPANY_ENRICH_URL}/{company_id}",
                headers=await self._headers(),
                timeout=20,
            )
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.json().get("data", {})


# ============================================================
# Enrichment Logic
# ============================================================

async def enrich_entity_with_zoominfo(entity_id: str, entity_name: str) -> bool:
    """
    Enrich a single entity using Zoominfo.
    Returns True if we found useful data.
    """
    zi = ZoominfoClient()

    # Step 1: Find the company in Zoominfo
    companies = await zi.search_company(entity_name)
    if not companies:
        log.info("zoominfo.company_not_found", entity_name=entity_name)
        return False

    # Take best match
    company = companies[0]
    zi_company_id = str(company.get("id", ""))

    if not zi_company_id:
        return False

    log.info("zoominfo.company_found",
             entity_name=entity_name,
             zi_name=company.get("name"),
             zi_id=zi_company_id)

    # Step 2: Update entity with Zoominfo company data
    update_entity(entity_id, {
        "zoominfo_id": zi_company_id,
        "address": company.get("street") or None,
        "zip_code": company.get("zipCode") or None,
        "raw_data": company,
    })

    # Step 3: Fetch contacts at this company
    contacts = await zi.search_contacts(zi_company_id)

    if not contacts:
        log.info("zoominfo.no_contacts", entity_name=entity_name, zi_id=zi_company_id)
        # Still count as success — we found the company
        return True

    # Step 4: Store contacts
    for contact in contacts:
        email = contact.get("email", "")
        phone = (
            contact.get("directPhone")
            or contact.get("mobilePhone")
            or contact.get("phone")
            or ""
        )

        if not (email or phone):
            continue  # Skip contacts with no useful data

        upsert_contact(entity_id, {
            "first_name": contact.get("firstName", ""),
            "last_name": contact.get("lastName", ""),
            "full_name": f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip(),
            "title": contact.get("jobTitle", ""),
            "email": email or None,
            "phone": phone or None,
            "phone_type": "direct" if contact.get("directPhone") else "mobile" if contact.get("mobilePhone") else "office",
            "linkedin_url": contact.get("linkedinUrl"),
            "source": "zoominfo",
            "zoominfo_id": str(contact.get("id", "")),
            "confidence": 0.95,
            "is_primary": contacts.index(contact) == 0,
            "raw_data": contact,
        })

    log.info("zoominfo.enriched",
             entity_name=entity_name,
             contacts_found=len(contacts))
    return True


# ============================================================
# search_contacts_at_company — used by paid_stubs.zoominfo_person_at_company
# ============================================================

async def search_contacts_at_company(full_name: str, company: str) -> list:
    """
    Search Zoominfo for a specific person at a named company.
    Returns a list of ContactHit objects (imported lazily to avoid circular deps).
    """
    from enrichment.contact.models import ContactHit

    zi = ZoominfoClient()

    try:
        companies = await zi.search_company(company)
    except Exception as e:
        log.warn("zoominfo.search_contacts.company_lookup_failed", company=company, error=str(e))
        return []

    if not companies:
        log.debug("zoominfo.search_contacts.company_not_found", company=company)
        return []

    zi_company_id = str(companies[0].get("id", ""))
    if not zi_company_id:
        return []

    try:
        contacts = await zi.search_contacts(zi_company_id)
    except Exception as e:
        log.warn("zoominfo.search_contacts.contacts_failed", company=company, error=str(e))
        return []

    # Filter to name-match if a specific person was requested
    name_parts = set(full_name.lower().split()) - {"mr", "mrs", "ms", "jr", "sr"}
    out = []
    for contact in contacts:
        first = (contact.get("firstName") or "").lower()
        last = (contact.get("lastName") or "").lower()
        contact_tokens = {t for t in (first + " " + last).split() if t}
        if name_parts and len(name_parts & contact_tokens) < 1:
            continue  # doesn't match the requested person

        phone = (
            contact.get("directPhone")
            or contact.get("mobilePhone")
            or contact.get("phone")
            or ""
        )
        email = contact.get("email") or ""
        if not (email or phone):
            continue

        out.append(ContactHit(
            full_name=f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip(),
            first_name=contact.get("firstName"),
            last_name=contact.get("lastName"),
            title=contact.get("jobTitle"),
            email=email or None,
            phone=phone or None,
            phone_type=(
                "direct" if contact.get("directPhone")
                else "mobile" if contact.get("mobilePhone")
                else "office"
            ),
            linkedin_url=contact.get("linkedinUrl"),
            company_name=companies[0].get("name") or company,
            network_role="signer",
            role_category="owner",
            confidence=0.90,
            source="zoominfo",
            evidence=f"Zoominfo person match at {company}",
            cost_cents=20,
            raw=contact,
        ))

    return out


# ============================================================
# Batch Runner
# ============================================================

async def run_batch(batch_size: int = None):
    """
    Enrich a batch of entities using Zoominfo.
    Respects the ZOOMINFO_MIN_PORTFOLIO_SIZE threshold to conserve credits.
    """
    batch_size = batch_size or config.ENRICHMENT_BATCH_SIZE
    log_id = start_ingestion_log("zoominfo_enrichment")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        # Only enrich larger portfolios to conserve credits
        # (Small individual landlords get enriched via other sources)
        res = db().table("entities")\
            .select("id, name, entity_type, portfolio_size")\
            .in_("entity_type", ["llc", "corporation", "management_company", "unknown"])\
            .gte("portfolio_size", config.ZOOMINFO_MIN_PORTFOLIO_SIZE)\
            .is_("zoominfo_id", "null")\
            .neq("enrichment_status", "done")\
            .order("portfolio_size", desc=True)\
            .limit(batch_size)\
            .execute()

        entities = res.data
        stats["records_fetched"] = len(entities)
        log.info("zoominfo.batch_start", count=len(entities))

        for entity in entities:
            entity_id = entity["id"]
            entity_name = entity["name"]

            try:
                success = await enrich_entity_with_zoominfo(entity_id, entity_name)
                if success:
                    stats["records_created"] += 1
                    update_entity(entity_id, {"enrichment_status": "done"})
                else:
                    stats["records_skipped"] += 1
            except Exception as e:
                log.error("zoominfo.entity_error", entity=entity_name, error=str(e))
                stats["records_skipped"] += 1

            # Zoominfo rate limit: be conservative
            await asyncio.sleep(1.5)

        finish_ingestion_log(log_id, stats)
        log.info("zoominfo.batch_complete", **stats)

    except Exception as e:
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        log.error("zoominfo.batch_error", error=str(e))
        raise
