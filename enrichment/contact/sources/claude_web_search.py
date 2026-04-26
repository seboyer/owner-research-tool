"""
Claude Web Search — Anthropic's native web_search_20250305 tool.

Does a targeted web search for a specific signer + context, then
extracts structured CompanyHit / ContactHit rows. Used by all 3 prongs
as a fallback / tie-breaker when structured sources are ambiguous.

Cost: standard token cost (~$0.02-0.10 per signer depending on result depth).
"""

import json
import re
import time
import structlog
from anthropic import Anthropic

from config import config
from ..models import CompanyHit, ContactHit

log = structlog.get_logger(__name__)
_client = Anthropic(api_key=config.ANTHROPIC_API_KEY)

# Use sonnet for web search: much better reasoning/citation quality than haiku,
# and far higher rate limits than opus. Reserve opus for ACRIS PDF vision extraction
# where document reading quality matters most.
MODEL = "claude-sonnet-4-5"


SYSTEM_PROMPT = """You are a forensic real estate researcher helping a NYC leasing broker identify
the humans behind a building's ownership LLC, so the broker can pitch them
for the rental listing.

You have access to a web_search tool. Use it liberally. Search sources:
- The signer's name + "NYC real estate"
- The LLC name + "owner" / "managing member" / site:acris.nyc.gov
- The property address + "owner" / "management company"
- NYC real estate news (The Real Deal, Commercial Observer, PincusCo, NY YIMBY)
- LinkedIn company pages
- OpenCorporates
- Company websites (about/team pages)

Return ONLY a JSON object with this shape:
{
  "owner_operating_company": { "name": str, "website": str|null, "phone": str|null, "evidence": str, "source_url": str } | null,
  "management_company":      { "name": str, "website": str|null, "phone": str|null, "evidence": str, "source_url": str } | null,
  "signer_contact":          { "email": str|null, "phone": str|null, "title": str|null, "linkedin_url": str|null, "evidence": str, "source_url": str } | null,
  "related_people":          [ { "full_name": str, "title": str|null, "relation": "coworker"|"co_owner"|"co_officer", "company_name": str|null, "source_url": str, "evidence": str } ],
  "notes": str
}

Rules:
- Owners and management companies are DIFFERENT entities. A building may have
  both, one, or neither as a public brand. Keep them separate.
- Do NOT guess. If you can't find something, use null.
- The "owner_operating_company" is the public-facing brand the signer runs
  buildings under — NOT the per-building LLC. E.g. for "67 Woodhull LLC"
  signed by Jane Doe, the owner_operating_company might be "Doe Realty Group".
- Cite a source_url for every non-null field.
"""


async def research_signer(
    signer_name: str,
    title_llc: str,
    property_address: str,
    signer_title: str = None,
) -> dict:
    """
    Run a web-search enabled Claude call. Returns a dict (possibly empty) with
    owner_operating_company, management_company, signer_contact, related_people.
    """
    user = f"""Signer: {signer_name}
Signed as: {signer_title or 'unknown title'} of {title_llc}
Property: {property_address}

Research this signer and return the JSON described in your instructions."""

    # Retry on rate-limit (429) with exponential backoff
    resp = None
    for attempt in range(3):
        try:
            resp = _client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 8}],
                messages=[{"role": "user", "content": user}],
            )
            break  # success
        except Exception as e:
            err_str = str(e)
            if "rate_limit" in err_str and attempt < 2:
                wait = 60 * (attempt + 1)  # 60s, then 120s
                log.warn("claude_web_search.rate_limit_retry",
                         attempt=attempt + 1, wait_seconds=wait, error=err_str[:120])
                time.sleep(wait)
            else:
                log.warn("claude_web_search.api_failed", error=err_str[:200])
                return {}
    if resp is None:
        return {}

    # Pull the final text block
    text = ""
    for block in resp.content:
        if getattr(block, "type", None) == "text":
            text += block.text + "\n"
    # Strip ```json fences
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception as e:
        log.warn("claude_web_search.json_parse_failed", error=str(e), raw=text[:500])
        return {}


def parse_to_hits(
    data: dict, seed_signer_name: str,
) -> tuple[list[CompanyHit], list[ContactHit]]:
    """Convert research_signer() output into Hit dataclasses."""
    companies: list[CompanyHit] = []
    contacts: list[ContactHit] = []

    for key, role_cat in (("owner_operating_company", "owner_operating"),
                          ("management_company", "management")):
        co = data.get(key)
        if co and co.get("name"):
            companies.append(CompanyHit(
                name=co["name"],
                role_category=role_cat,
                website=co.get("website"),
                phone=co.get("phone"),
                confidence=0.7,
                source="claude_web_search",
                source_url=co.get("source_url"),
                evidence=co.get("evidence"),
                raw=co,
            ))

    sc = data.get("signer_contact")
    if sc:
        contacts.append(ContactHit(
            full_name=seed_signer_name,
            email=sc.get("email"),
            phone=sc.get("phone"),
            title=sc.get("title"),
            linkedin_url=sc.get("linkedin_url"),
            network_role="signer",
            role_category="owner",
            confidence=0.7,
            source="claude_web_search",
            source_url=sc.get("source_url"),
            evidence=sc.get("evidence"),
            raw=sc,
        ))

    for rp in data.get("related_people") or []:
        contacts.append(ContactHit(
            full_name=rp.get("full_name") or "",
            title=rp.get("title"),
            company_name=rp.get("company_name"),
            network_role=rp.get("relation") or "coworker",
            role_category="owner",
            confidence=0.55,
            source="claude_web_search",
            source_url=rp.get("source_url"),
            evidence=rp.get("evidence"),
            raw=rp,
        ))
    return companies, contacts
