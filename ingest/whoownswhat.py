"""
ingest/whoownswhat.py — Who Owns What (WoW) by JustFix

WoW is a free NYC tool that maps LLC portfolios — it groups buildings
owned by the same person/company even when they use different LLC names.
This is our single best source for LLC-to-portfolio mapping.

API docs: https://whoownswhat.justfix.org/en/about

Usage:
  1. Search by address to get a portfolio
  2. Each portfolio has a list of buildings linked to the same "landlord"
  3. We store the portfolio ID on entities and use it to group LLCs

We use this in two ways:
  A. Proactively: for every address in our properties table, fetch its WoW portfolio
  B. Reactively: during LLC piercing, look up any LLC's known address in WoW
"""

import asyncio

import httpx
import structlog

from config import config
from database.client import (
    db, already_seen, mark_seen,
    upsert_entity, upsert_property, upsert_property_role,
    upsert_relationship, update_entity,
    start_ingestion_log, finish_ingestion_log,
)

log = structlog.get_logger(__name__)


async def search_wow(house_number: str, street: str, borough: str, zip_code: str = "") -> dict | None:
    """
    Search Who Owns What by address.
    Returns the portfolio data or None.

    NOTE: The WoW public API (whoownswhat.justfix.org) changed to a SPA and now
    returns HTML for all endpoints. This function will return None until an
    alternative endpoint or data source is wired up.

    Borough can be: MANHATTAN, BRONX, BROOKLYN, QUEENS, STATEN ISLAND
    """
    async with httpx.AsyncClient() as client:
        params = {
            "houseNumber": house_number.strip(),
            "street": street.strip(),
            "borough": borough.strip().upper(),
            "apt": "",
            "zip": zip_code or "",
        }
        try:
            resp = await client.get(config.WOW_SEARCH_URL, params=params, timeout=15)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "text/html" in content_type:
                # WoW API is returning HTML (SPA shell) — the JSON API is gone.
                log.warning("wow.api_dead", msg="WoW returned HTML instead of JSON — API endpoint has changed")
                return None
            data = resp.json()
            return data if data.get("result") else None
        except (httpx.HTTPError, Exception) as e:
            log.warning("wow.search_error", error=str(e), address=f"{house_number} {street}")
            return None


async def get_wow_portfolio(bbl: str) -> dict | None:
    """
    Get the full portfolio for a BBL from WoW.
    A portfolio = all buildings linked to the same apparent owner.
    """
    async with httpx.AsyncClient() as client:
        try:
            # WoW portfolio endpoint accepts BBL
            resp = await client.get(
                f"{config.WOW_PORTFOLIO_URL}/{bbl}",
                timeout=15
            )
            if resp.status_code in (404, 422):
                return None
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "text/html" in content_type:
                log.warning("wow.api_dead", bbl=bbl, msg="WoW returned HTML — API endpoint has changed")
                return None
            return resp.json()
        except (httpx.HTTPError, Exception) as e:
            log.warning("wow.portfolio_error", error=str(e), bbl=bbl)
            return None


async def process_wow_portfolio(portfolio: dict) -> str | None:
    """
    Parse a WoW portfolio response and create/update entities + properties.

    WoW portfolio structure:
    {
      "result": {
        "landlord_names": ["JOHN SMITH", "JS PROPERTIES LLC"],
        "portfolio": [
          {
            "bbl": "1000010001",
            "address": "123 MAIN ST",
            "zip": "10001",
            "borough": "MANHATTAN",
            "units": 24,
            "open_violations": 5,
            ...
          }
        ]
      }
    }
    """
    result = portfolio.get("result", {})
    if not result:
        return None

    landlord_names: list[str] = result.get("landlord_names", [])
    buildings: list[dict] = result.get("portfolio", [])

    if not landlord_names or not buildings:
        return None

    portfolio_size = len(buildings)
    portfolio_id = f"wow_{buildings[0].get('bbl', 'unknown')}"

    # Create/update the primary entity (largest portfolio landlord)
    primary_name = landlord_names[0] if landlord_names else "Unknown"

    # Determine entity type from name
    entity_type = "llc" if "LLC" in primary_name.upper() else \
                  "management_company" if any(k in primary_name.upper() for k in ["MGMT", "MANAGEMENT", "REALTY", "PROPERTIES"]) else \
                  "individual"

    primary_entity_id = upsert_entity(primary_name, entity_type, extra={
        "portfolio_size": portfolio_size,
        "wow_portfolio_id": portfolio_id,
        "enrichment_status": "pending",
    })

    # If WoW identified multiple names (LLC aliases), create them and link
    for alias_name in landlord_names[1:]:
        if not alias_name or alias_name == primary_name:
            continue
        alias_type = "llc" if "LLC" in alias_name.upper() else entity_type
        alias_entity_id = upsert_entity(alias_name, alias_type, extra={
            "wow_portfolio_id": portfolio_id,
        })
        # Mark as affiliated (same portfolio)
        upsert_relationship(
            child_entity_id=alias_entity_id,
            parent_entity_id=primary_entity_id,
            rel_type="affiliated_with",
            source="wow",
            confidence=0.85,
            evidence=f"WoW groups these entities as the same landlord (portfolio_id={portfolio_id})",
        )

    # Process each building in the portfolio
    for building in buildings:
        bbl = building.get("bbl", "")
        if not bbl:
            continue

        property_id = upsert_property(bbl, {
            "borough": building.get("borough", ""),
            "address": building.get("address", ""),
            "zip_code": building.get("zip", ""),
            "unit_count": building.get("units", None),
            "raw_data": building,
        })

        upsert_property_role(property_id, primary_entity_id, "owner", "wow")

    log.info("wow.portfolio_processed", primary_name=primary_name, buildings=portfolio_size)
    return primary_entity_id


async def enrich_properties_with_wow(batch_size: int = 100):
    """
    For all properties in our DB that haven't been WoW-enriched,
    look them up in WoW and pull their full portfolio.

    This is a batch enrichment job — run weekly.
    """
    log_id = start_ingestion_log("wow_enrichment")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        # Fetch properties we haven't WoW'd yet
        res = db().table("properties")\
            .select("id, bbl, house_number, street_name, borough, zip_code")\
            .not_.is_("bbl", "null")\
            .limit(batch_size)\
            .execute()

        properties = res.data
        log.info("wow.batch_start", count=len(properties))

        for prop in properties:
            bbl = prop.get("bbl", "")

            if already_seen("wow_bbl", bbl):
                stats["records_skipped"] += 1
                continue

            stats["records_fetched"] += 1

            # Try WoW portfolio lookup by BBL first
            portfolio = await get_wow_portfolio(bbl)

            if not portfolio and prop.get("house_number") and prop.get("street_name"):
                # Fallback: search by address
                await asyncio.sleep(0.3)  # Rate limit
                portfolio = await search_wow(
                    house_number=prop["house_number"],
                    street=prop["street_name"],
                    borough=prop.get("borough", ""),
                    zip_code=prop.get("zip_code", ""),
                )

            if portfolio:
                await process_wow_portfolio(portfolio)
                stats["records_created"] += 1

            mark_seen("wow_bbl", bbl)
            await asyncio.sleep(0.2)  # Polite rate limiting — WoW is a free service

        finish_ingestion_log(log_id, stats)
        log.info("wow.complete", **stats)

    except Exception as e:
        log.error("wow.error", error=str(e))
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        raise


async def lookup_entity_in_wow(entity_address: str, borough: str) -> dict | None:
    """
    Helper used by the LLC piercer: given an entity's registered address,
    look it up in WoW to find what portfolio it belongs to.
    """
    parts = entity_address.split(" ", 1)
    if len(parts) < 2:
        return None
    house_number, street = parts[0], parts[1]
    return await search_wow(house_number, street, borough)


async def run():
    await enrich_properties_with_wow()
