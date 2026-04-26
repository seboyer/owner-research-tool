"""
ingest/opencorporates.py — OpenCorporates LLC Lookup

OpenCorporates aggregates NYS DOS (Department of State) corporate filings.
We use it to:
  1. Look up an LLC by name and get its registered agent + formation date
  2. Find other companies at the same registered agent address (key for piercing)
  3. Identify the "ultimate" corporate parent if OpenCorporates has that data

Free tier: 50 req/day.  Paid tier: much more.
API docs: https://api.opencorporates.com/documentation/API-Reference

NYS DOS also has a public lookup at https://apps.dos.ny.gov/publicInquiry/
We include a fallback scraper for that.
"""

import asyncio
import re
from typing import Optional

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import config
from database.client import db, upsert_entity, upsert_relationship, update_entity

log = structlog.get_logger(__name__)

NY_JURISDICTION = "us_ny"


class OpenCorporatesClient:
    BASE_URL = "https://api.opencorporates.com/v0.4"

    def __init__(self):
        self.api_key = config.OPENCORPORATES_API_KEY
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            headers={"User-Agent": "NYC-Landlord-Finder/1.0"},
            timeout=20,
        )
        return self

    async def __aexit__(self, *args):
        if self._client:
            await self._client.aclose()

    def _params(self, extra: dict = None) -> dict:
        p = {}
        if self.api_key:
            p["api_token"] = self.api_key
        if extra:
            p.update(extra)
        return p

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def search_company(self, name: str, jurisdiction: str = NY_JURISDICTION) -> list[dict]:
        """Search for a company by name in a given jurisdiction."""
        params = self._params({
            "q": name,
            "jurisdiction_code": jurisdiction,
            "per_page": 10,
            "order": "score",
        })
        resp = await self._client.get(f"{self.BASE_URL}/companies/search", params=params)
        resp.raise_for_status()
        data = resp.json()
        companies = data.get("results", {}).get("companies", [])
        return [c["company"] for c in companies]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def get_company(self, jurisdiction: str, company_number: str) -> dict | None:
        """Fetch full details for a specific company."""
        params = self._params()
        resp = await self._client.get(
            f"{self.BASE_URL}/companies/{jurisdiction}/{company_number}",
            params=params,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json().get("results", {}).get("company")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def search_by_registered_agent(self, agent_name: str, jurisdiction: str = NY_JURISDICTION) -> list[dict]:
        """
        Find all companies that share a registered agent.
        This is a key LLC piercing technique: if 20 LLCs share an agent named
        "John Smith at 123 Main St", they're likely all owned by the same person.
        """
        params = self._params({
            "registered_address": agent_name,
            "jurisdiction_code": jurisdiction,
            "per_page": 50,
        })
        resp = await self._client.get(f"{self.BASE_URL}/companies/search", params=params)
        resp.raise_for_status()
        companies = resp.json().get("results", {}).get("companies", [])
        return [c["company"] for c in companies]


# ============================================================
# Enrichment functions
# ============================================================

def _extract_registered_agent(company: dict) -> tuple[str, str]:
    """Extract registered agent name and address from OpenCorporates company data."""
    agent_name = ""
    agent_address = ""

    officers = company.get("officers", [])
    for officer in officers:
        o = officer.get("officer", {})
        if "agent" in o.get("position", "").lower():
            agent_name = o.get("name", "")
            addr = o.get("address", "")
            if addr:
                agent_address = addr
            break

    # Also check registered_address field
    reg_addr = company.get("registered_address", {})
    if isinstance(reg_addr, dict):
        agent_address = agent_address or ", ".join(
            filter(None, [
                reg_addr.get("street_address"),
                reg_addr.get("locality"),
                reg_addr.get("region"),
                reg_addr.get("postal_code"),
            ])
        )

    return agent_name, agent_address


def _classify_agent(agent_name: str) -> str:
    """
    Classify a registered agent.
    Law firms / CT Corp / etc. are professional agents — not the real owner.
    """
    PROFESSIONAL_AGENTS = [
        "CT CORPORATION", "CORPORATION SERVICE COMPANY", "CSC",
        "NATIONAL REGISTERED AGENTS", "NRAI", "INCORP SERVICES",
        "REGISTERED AGENTS INC", "NORTHWEST REGISTERED AGENT",
        "LEGALZOOM", "HARBOR COMPLIANCE", "COGENCY GLOBAL",
    ]
    agent_upper = agent_name.upper() if agent_name else ""
    for agent in PROFESSIONAL_AGENTS:
        if agent in agent_upper:
            return "professional_agent"
    return "individual_or_company"


async def lookup_llc(entity_id: str, entity_name: str) -> dict | None:
    """
    Look up an LLC in OpenCorporates and return enriched data.
    Updates the entity record with registered agent info.
    Returns the company data dict or None.
    """
    async with OpenCorporatesClient() as oc:
        results = await oc.search_company(entity_name)
        if not results:
            log.info("opencorporates.not_found", entity_name=entity_name)
            return None

        # Take the first result (highest relevance score)
        company = results[0]
        company_number = company.get("company_number")
        jurisdiction = company.get("jurisdiction_code", NY_JURISDICTION)

        # Fetch full details
        if company_number:
            full = await oc.get_company(jurisdiction, company_number)
            if full:
                company = full

        agent_name, agent_address = _extract_registered_agent(company)
        agent_type = _classify_agent(agent_name)

        update_entity(entity_id, {
            "dos_id": company.get("company_number"),
            "opencorporates_url": company.get("opencorporates_url"),
            "registered_agent": agent_name or None,
            "registered_agent_address": agent_address or None,
            "formation_date": company.get("incorporation_date"),
            "raw_data": company,
        })

        log.info("opencorporates.enriched",
                 entity_name=entity_name,
                 agent_name=agent_name,
                 agent_type=agent_type)

        return {
            "company": company,
            "agent_name": agent_name,
            "agent_address": agent_address,
            "agent_type": agent_type,
        }


async def find_sibling_llcs(registered_agent_address: str, exclude_entity_id: str) -> list[dict]:
    """
    Find other LLCs that share the same registered agent address.
    These are likely owned by the same person — key piercing signal.

    Returns list of company dicts from OpenCorporates.
    """
    if not registered_agent_address:
        return []

    async with OpenCorporatesClient() as oc:
        siblings = await oc.search_by_registered_agent(registered_agent_address)

    log.info("opencorporates.siblings_found",
             address=registered_agent_address,
             count=len(siblings))
    return siblings


# ============================================================
# NYS DOS Fallback Scraper
# ============================================================

async def scrape_nys_dos(entity_name: str) -> dict | None:
    """
    Fallback: scrape the NYS DOS public entity search.
    Use only when OpenCorporates doesn't have the entity.

    Note: NYS DOS doesn't have a formal API, so this is fragile.
    Consider using a scraping service like Browserless/ScraperAPI for reliability.
    """
    DOS_SEARCH_URL = "https://apps.dos.ny.gov/publicInquiry/EntitySearch"

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; NYC-Landlord-Finder/1.0)"},
        follow_redirects=True,
        timeout=20,
    ) as client:
        try:
            # First GET to get the form/session
            resp = await client.get("https://apps.dos.ny.gov/publicInquiry/")
            resp.raise_for_status()

            # POST the search form
            resp = await client.post(DOS_SEARCH_URL, data={
                "entityName": entity_name,
                "entityType": "DOM_LLC",  # Domestic LLC
                "county": "",
                "searchType": "EntityName",
            })
            resp.raise_for_status()

            # Parse results
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "lxml")

            # Look for result rows in the table
            rows = soup.select("table.results tr")
            if len(rows) < 2:
                return None

            # First result row (skip header)
            cells = rows[1].select("td")
            if len(cells) < 4:
                return None

            return {
                "name": cells[0].get_text(strip=True),
                "dos_id": cells[1].get_text(strip=True),
                "status": cells[2].get_text(strip=True),
                "formation_date": cells[3].get_text(strip=True),
                "source": "nys_dos_scrape",
            }

        except Exception as e:
            log.warning("nys_dos.scrape_error", entity_name=entity_name, error=str(e))
            return None


async def run_batch(batch_size: int = 20):
    """
    Process a batch of entities that need OpenCorporates lookup.
    Only processes LLCs and corporations (not individuals).
    """
    res = db().table("entities")\
        .select("id, name, entity_type, dos_id")\
        .in_("entity_type", ["llc", "corporation", "unknown"])\
        .is_("dos_id", "null")\
        .is_("is_pierced", "false")\
        .limit(batch_size)\
        .execute()

    entities = res.data
    log.info("opencorporates.batch_start", count=len(entities))

    for entity in entities:
        entity_id = entity["id"]
        entity_name = entity["name"]

        try:
            result = await lookup_llc(entity_id, entity_name)
            if result:
                log.info("opencorporates.enriched_entity", name=entity_name)
            else:
                # Fallback to DOS scraper
                dos_data = await scrape_nys_dos(entity_name)
                if dos_data:
                    update_entity(entity_id, {
                        "dos_id": dos_data.get("dos_id"),
                        "formation_date": dos_data.get("formation_date"),
                    })
        except Exception as e:
            log.error("opencorporates.entity_error", name=entity_name, error=str(e))

        await asyncio.sleep(1.2)  # Respect rate limits (free tier: ~1 req/sec)
