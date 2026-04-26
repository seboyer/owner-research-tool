"""
enrichment/llc_piercer.py — AI-Powered LLC Piercing Engine

Orchestrates multiple strategies to find the real person/company behind a
building LLC. Strategies are tried in order of reliability:

  1. ACRIS Mortgage PDF Signer  — best: finds the actual human signature
  2. Who Owns What Portfolio    — groups LLCs by apparent owner
  3. OpenCorporates Registered Agent — shared agent = shared owner
  4. Claude Agentic Reasoning   — AI synthesizes all signals and searches
  5. Web Search (Claude)        — last resort: Google the LLC name

Each strategy writes results to entity_relationships with a confidence score.
The entity is marked is_pierced=True when any strategy succeeds.
"""

import asyncio
import json
import re
from typing import Any

import structlog
from anthropic import Anthropic

from config import config
from database.client import (
    db, upsert_entity, upsert_contact, upsert_relationship,
    update_entity, already_seen, mark_seen,
    start_ingestion_log, finish_ingestion_log,
)
from enrichment.acris_pdf import pierce_llc_via_mortgage
from ingest.opencorporates import lookup_llc, find_sibling_llcs
from ingest.whoownswhat import lookup_entity_in_wow

log = structlog.get_logger(__name__)
anthropic = Anthropic(api_key=config.ANTHROPIC_API_KEY)


# ============================================================
# Strategy 1: ACRIS PDF Signer (delegated to acris_pdf.py)
# ============================================================

async def strategy_acris_pdf(entity: dict) -> bool:
    """Try to find signers via ACRIS mortgage PDFs."""
    entity_id = entity["id"]
    entity_name = entity["name"]

    # Get a real BBL for this entity
    roles_res = db().table("property_roles")\
        .select("property_id, properties(bbl)")\
        .eq("entity_id", entity_id)\
        .eq("is_current", True)\
        .limit(1)\
        .execute()

    if not roles_res.data:
        return False

    bbl = roles_res.data[0].get("properties", {}).get("bbl", "")
    if not bbl or bbl.startswith("hpd_bldg_"):
        return False

    return await pierce_llc_via_mortgage(entity_id, entity_name, bbl)


# ============================================================
# Strategy 2: HPD Portfolio — find the head officer behind the LLC
# ============================================================
# NOTE: Who Owns What (WoW) API is dead as of 2025 — the site became an SPA
# and no longer serves JSON from its API endpoints. Replaced with direct HPD
# portfolio lookup, which provides the same HeadOfficer / CorporateOwner data.

async def strategy_wow_portfolio(entity: dict) -> bool:
    """
    Look up the entity's registered address in HPD to find the real person
    (HeadOfficer) behind the LLC. If we find a human name registered as
    HeadOfficer for this building, they are almost certainly the real owner.
    """
    entity_id = entity["id"]
    entity_name = entity["name"]

    # Get a real BBL for this entity
    roles_res = db().table("property_roles")\
        .select("property_id, properties(bbl)")\
        .eq("entity_id", entity_id)\
        .eq("is_current", True)\
        .limit(1)\
        .execute()

    if not roles_res.data:
        return False

    bbl = (roles_res.data[0].get("properties") or {}).get("bbl", "")
    if not bbl or bbl.startswith("hpd_bldg_"):
        return False

    # Parse BBL → boro / block / lot
    if len(bbl) < 10:
        return False
    boro = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:10]))

    import httpx
    from config import config as _cfg

    async with httpx.AsyncClient() as client:
        # Query HPD registrations for this BBL
        params = {
            "$where": f"boroid='{boro}' AND block='{block}' AND lot='{lot}'",
            "$limit": 5,
            "$order": "lastregistrationdate DESC",
        }
        if _cfg.NYC_OPENDATA_APP_TOKEN:
            params["$$app_token"] = _cfg.NYC_OPENDATA_APP_TOKEN

        try:
            resp = await client.get(_cfg.HPD_REGISTRATIONS_URL, params=params, timeout=15)
            regs = resp.json() if resp.status_code == 200 else []
        except Exception as e:
            log.warning("llc_piercer.hpd_portfolio_error", error=str(e))
            return False

        if not regs:
            return False

        reg_ids = [r["registrationid"] for r in regs if r.get("registrationid")]
        ids_clause = " OR ".join(f"registrationid='{rid}'" for rid in reg_ids)

        try:
            cresp = await client.get(_cfg.HPD_CONTACTS_URL, params={
                "$where": ids_clause,
                "$limit": 20,
                **({"$$app_token": _cfg.NYC_OPENDATA_APP_TOKEN} if _cfg.NYC_OPENDATA_APP_TOKEN else {}),
            }, timeout=15)
            contacts = cresp.json() if cresp.status_code == 200 else []
        except Exception as e:
            log.warning("llc_piercer.hpd_contacts_error", error=str(e))
            return False

    found = False
    for c in contacts:
        ctype = c.get("type", "")
        if ctype not in ("HeadOfficer", "IndividualOwner", "Owner"):
            continue

        first = c.get("firstname", "").strip()
        last = c.get("lastname", "").strip()
        name = f"{first} {last}".strip()
        if not name or name.upper() == entity_name.upper():
            continue

        owner_entity_id = upsert_entity(name, "individual", extra={
            "address": " ".join(filter(None, [
                c.get("businesshousenumber", ""),
                c.get("businessstreetname", ""),
            ])) or None,
        })

        upsert_contact(owner_entity_id, {
            "first_name": first,
            "last_name": last,
            "full_name": name,
            "source": "hpd",
            "confidence": 0.80,
        })

        upsert_relationship(
            child_entity_id=entity_id,
            parent_entity_id=owner_entity_id,
            rel_type="owned_by",
            source="hpd_registration",
            confidence=0.80,
            evidence=f"HPD lists {name} as {ctype} for BBL {bbl}",
        )
        log.info("llc_piercer.hpd_portfolio_match", entity=entity_name, owner=name, role=ctype)
        found = True

    return found


# ============================================================
# Strategy 3: Registered Agent Analysis
# ============================================================

async def strategy_registered_agent(entity: dict) -> bool:
    """
    Look up the LLC in OpenCorporates and analyze its registered agent.
    If many LLCs share the same non-professional agent, they likely have a common owner.
    """
    entity_id = entity["id"]
    entity_name = entity["name"]

    # Get or fetch OpenCorporates data
    oc_data = await lookup_llc(entity_id, entity_name)
    if not oc_data:
        return False

    agent_name = oc_data.get("agent_name", "")
    agent_address = oc_data.get("agent_address", "")
    agent_type = oc_data.get("agent_type", "")

    if agent_type == "professional_agent":
        # CT Corp / CSC etc. — useless for piercing
        log.info("llc_piercer.professional_agent_skip",
                 entity=entity_name, agent=agent_name)
        return False

    if not agent_name:
        return False

    # The individual registered agent IS often the real owner
    # Especially if their name is a person's name (not a company)
    is_person = bool(re.match(r"^[A-Z][a-z]+ [A-Z][a-z]+", agent_name))
    if is_person:
        owner_entity_id = upsert_entity(agent_name, "individual", extra={
            "address": agent_address or None,
        })
        name_parts = agent_name.split(" ", 1)
        upsert_contact(owner_entity_id, {
            "first_name": name_parts[0],
            "last_name": name_parts[1] if len(name_parts) > 1 else "",
            "full_name": agent_name,
            "source": "opencorporates",
            "confidence": 0.75,
        })
        upsert_relationship(
            child_entity_id=entity_id,
            parent_entity_id=owner_entity_id,
            rel_type="owned_by",
            source="opencorporates",
            confidence=0.75,
            evidence=f"Registered agent '{agent_name}' is likely the real owner",
        )
        log.info("llc_piercer.agent_is_person", entity=entity_name, agent=agent_name)
        return True

    # Check if multiple LLCs share this agent (cluster analysis)
    if agent_address:
        siblings = await find_sibling_llcs(agent_address, entity_id)
        if len(siblings) >= 3:
            # Significant cluster — flag for AI analysis
            log.info("llc_piercer.sibling_cluster",
                     entity=entity_name,
                     sibling_count=len(siblings),
                     agent_address=agent_address)
            # Store siblings as affiliated entities
            for sibling in siblings[:10]:
                sibling_name = sibling.get("name", "")
                if sibling_name and sibling_name != entity_name:
                    sibling_id = upsert_entity(sibling_name, "llc")
                    upsert_relationship(
                        child_entity_id=sibling_id,
                        parent_entity_id=entity_id,
                        rel_type="affiliated_with",
                        source="opencorporates",
                        confidence=0.65,
                        evidence=f"Shares registered agent address with {entity_name}",
                    )

    return False


# ============================================================
# Strategy 4: Claude Agentic Reasoning
# ============================================================

PIERCE_SYSTEM_PROMPT = """You are a real estate investigator specializing in identifying the
real people and companies behind NYC building LLCs. You have access to search tools.

Your goal: given an LLC name, find the real human owner(s) behind it.

Key facts:
- Building LLCs in NYC almost always have 1-3 real people behind them
- Signature blocks in mortgage documents always name a real person (look for "Managing Member", "Member/Manager")
- NYS Secretary of State filings list registered agents
- Real estate professionals often have a public web presence
- Cross-reference multiple signals: same address, same attorney, same lender, same formation date

Return a structured JSON result with the real owners you find."""

PIERCE_TOOLS = [
    {
        "name": "web_search",
        "description": "Search the web for information about a person or company",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_opencorporates",
        "description": "Look up a company on OpenCorporates to get registered agent and filing info",
        "input_schema": {
            "type": "object",
            "properties": {
                "company_name": {"type": "string"},
                "state": {"type": "string", "default": "NY"},
            },
            "required": ["company_name"],
        },
    },
    {
        "name": "search_nyc_properties",
        "description": "Find properties in NYC owned by a given entity name or person",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Owner name to search"},
            },
            "required": ["name"],
        },
    },
]


async def _execute_tool(tool_name: str, tool_input: dict) -> str:
    """Execute a tool call from Claude's agentic loop."""
    import httpx

    if tool_name == "web_search":
        # Use OpenAI's search or Claude's web search capability
        # For now, return a placeholder — integrate with your preferred search API
        query = tool_input.get("query", "")
        try:
            # Use OpenAI with web search if available
            from openai import OpenAI
            oai = OpenAI(api_key=config.OPENAI_API_KEY)
            resp = oai.chat.completions.create(
                model="gpt-4o-search-preview",
                messages=[{"role": "user", "content": f"Search the web: {query}"}],
                max_tokens=500,
            )
            return resp.choices[0].message.content or "No results found"
        except Exception as e:
            return f"Search error: {e}"

    elif tool_name == "get_opencorporates":
        company_name = tool_input.get("company_name", "")
        async with httpx.AsyncClient() as client:
            params = {
                "q": company_name,
                "jurisdiction_code": "us_ny",
                "per_page": 3,
            }
            if config.OPENCORPORATES_API_KEY:
                params["api_token"] = config.OPENCORPORATES_API_KEY
            try:
                resp = await client.get(
                    "https://api.opencorporates.com/v0.4/companies/search",
                    params=params, timeout=10
                )
                data = resp.json()
                companies = data.get("results", {}).get("companies", [])
                if companies:
                    c = companies[0]["company"]
                    return json.dumps({
                        "name": c.get("name"),
                        "company_number": c.get("company_number"),
                        "incorporation_date": c.get("incorporation_date"),
                        "registered_address": c.get("registered_address"),
                        "officers": c.get("officers", [])[:3],
                    })
                return "Company not found"
            except Exception as e:
                return f"OpenCorporates error: {e}"

    elif tool_name == "search_nyc_properties":
        name = tool_input.get("name", "")
        # Search our own DB first
        res = db().table("property_roles")\
            .select("*, entities(name), properties(bbl, address)")\
            .like("entities.name", f"%{name}%")\
            .limit(5)\
            .execute()
        if res.data:
            return json.dumps([{
                "address": r.get("properties", {}).get("address", ""),
                "bbl": r.get("properties", {}).get("bbl", ""),
                "entity": r.get("entities", {}).get("name", ""),
            } for r in res.data])
        return "No properties found in our database"

    return "Tool not found"


async def strategy_claude_agentic(entity: dict) -> bool:
    """
    Use Claude in an agentic loop with search tools to reason about
    who owns this LLC. This is the most powerful but also most expensive strategy.
    """
    entity_id = entity["id"]
    entity_name = entity["name"]
    entity_address = entity.get("address", "Unknown address")
    registered_agent = entity.get("registered_agent", "Unknown")
    dos_id = entity.get("dos_id", "")

    # Build context from what we already know
    known_info = f"""
LLC Name: {entity_name}
Registered Address: {entity_address}
NYS DOS ID: {dos_id or 'Unknown'}
Registered Agent: {registered_agent or 'Unknown'}
"""

    # Check if we have sibling info from the DB
    rels = db().table("entity_relationships")\
        .select("*, entities!entity_relationships_child_entity_id_fkey(name)")\
        .eq("parent_entity_id", entity_id)\
        .eq("relationship_type", "affiliated_with")\
        .limit(10)\
        .execute()

    if rels.data:
        sibling_names = [r.get("entities", {}).get("name", "") for r in rels.data]
        known_info += f"\nAffiliated LLCs (share same agent/address): {', '.join(sibling_names[:5])}"

    messages = [
        {
            "role": "user",
            "content": f"""Please investigate this NYC real estate LLC and identify the real person(s) or company behind it.

{known_info}

Steps:
1. Search the web for "{entity_name} NYC owner" and related queries
2. Check OpenCorporates for registered agent details
3. Look for any news, court filings, or public records mentioning this LLC
4. If you find affiliated LLCs, research those too

Return a JSON object with:
{{
  "real_owners": [
    {{
      "name": "Full Name or Company Name",
      "type": "individual" or "company",
      "confidence": 0.0-1.0,
      "evidence": "How you found this",
      "email": "if found",
      "phone": "if found",
      "title": "their role, e.g. Managing Member"
    }}
  ],
  "summary": "brief explanation of ownership structure"
}}

If you cannot determine the owner, return {{"real_owners": [], "summary": "Unable to determine"}}
""",
        }
    ]

    # Agentic loop
    max_iterations = 5
    for i in range(max_iterations):
        response = anthropic.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=2000,
            system=PIERCE_SYSTEM_PROMPT,
            tools=PIERCE_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "end_turn":
            # Extract JSON result from final response
            for block in response.content:
                if hasattr(block, "text"):
                    json_match = re.search(r"\{.*\}", block.text, re.DOTALL)
                    if json_match:
                        try:
                            result = json.loads(json_match.group())
                            return await _process_agentic_result(result, entity_id, entity_name)
                        except json.JSONDecodeError:
                            pass
            break

        if response.stop_reason == "tool_use":
            # Process tool calls
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_output = await _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": tool_output,
                    })

            messages.append({"role": "assistant", "content": response.content})
            messages.append({"role": "user", "content": tool_results})

    return False


async def _process_agentic_result(result: dict, entity_id: str, entity_name: str) -> bool:
    """Store the results from Claude's agentic investigation."""
    real_owners = result.get("real_owners", [])
    if not real_owners:
        return False

    found = False
    for owner in real_owners:
        owner_name = owner.get("name", "").strip()
        if not owner_name:
            continue

        confidence = owner.get("confidence", 0.6)
        owner_type = owner.get("type", "individual")

        owner_entity_id = upsert_entity(owner_name, owner_type)

        # Store contact info if found
        if owner.get("email") or owner.get("phone") or owner_type == "individual":
            name_parts = owner_name.rsplit(" ", 1)
            upsert_contact(owner_entity_id, {
                "first_name": name_parts[0] if len(name_parts) > 1 else owner_name,
                "last_name": name_parts[1] if len(name_parts) > 1 else "",
                "full_name": owner_name,
                "email": owner.get("email"),
                "phone": owner.get("phone"),
                "title": owner.get("title", ""),
                "source": "ai_research",
                "confidence": confidence,
            })

        upsert_relationship(
            child_entity_id=entity_id,
            parent_entity_id=owner_entity_id,
            rel_type="owned_by",
            source="ai_inference",
            confidence=confidence,
            evidence=owner.get("evidence", "Identified by AI investigation"),
        )

        log.info("llc_piercer.agentic_result",
                 entity=entity_name,
                 owner=owner_name,
                 confidence=confidence)
        found = True

    return found


# ============================================================
# Main LLC Piercing Orchestrator
# ============================================================

STRATEGIES = [
    ("acris_pdf",       strategy_acris_pdf,       0.95),
    ("wow_portfolio",   strategy_wow_portfolio,   0.80),
    # OpenCorporates registered-agent strategy disabled — price prohibitive.
    # ("registered_agent", strategy_registered_agent, 0.75),
    ("claude_agentic",  strategy_claude_agentic,  0.70),
]


async def pierce_entity(entity: dict) -> bool:
    """
    Run all piercing strategies for a single entity.
    Returns True if we found the real owner through any strategy.
    Stops after the first successful high-confidence pierce.
    """
    entity_id = entity["id"]
    entity_name = entity["name"]

    log.info("llc_piercer.starting", entity=entity_name)

    for strategy_name, strategy_fn, min_confidence in STRATEGIES:
        try:
            log.info("llc_piercer.trying_strategy",
                     entity=entity_name, strategy=strategy_name)
            success = await strategy_fn(entity)
            if success:
                log.info("llc_piercer.strategy_succeeded",
                         entity=entity_name, strategy=strategy_name)
                update_entity(entity_id, {"is_pierced": True})
                return True
        except Exception as e:
            log.error("llc_piercer.strategy_error",
                      entity=entity_name,
                      strategy=strategy_name,
                      error=str(e))

        await asyncio.sleep(0.5)

    log.info("llc_piercer.all_strategies_failed", entity=entity_name)
    return False


async def run_batch(batch_size: int = 20):
    """
    Process a batch of building LLCs that haven't been pierced yet.
    Prioritize LLCs with larger portfolios (more buildings = bigger landlord).
    """
    log_id = start_ingestion_log("llc_piercing")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        res = db().table("entities")\
            .select("*")\
            .eq("is_building_llc", True)\
            .eq("is_pierced", False)\
            .order("portfolio_size", desc=True)\
            .limit(batch_size)\
            .execute()

        entities = res.data
        stats["records_fetched"] = len(entities)
        log.info("llc_piercer.batch_start", count=len(entities))

        for entity in entities:
            try:
                pierced = await pierce_entity(entity)
                if pierced:
                    stats["records_created"] += 1
                else:
                    stats["records_skipped"] += 1
            except Exception as e:
                log.error("llc_piercer.entity_error",
                          entity=entity.get("name"),
                          error=str(e))

            await asyncio.sleep(1)

        finish_ingestion_log(log_id, stats)
        log.info("llc_piercer.batch_complete", **stats)

    except Exception as e:
        finish_ingestion_log(log_id, stats, status="failed", error=str(e))
        raise
