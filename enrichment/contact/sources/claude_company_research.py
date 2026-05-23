"""
Claude company research — Anthropic's native web_search tool.

Researches a company by name (and optionally known addresses) and returns:
  - Structured company data (website, phone, address, linkedin)
  - Named officers / principals with contact hints

Parallel to claude_web_search.research_signer but keyed on a *company*
rather than an individual signer.

Cost: standard token cost (~$0.02-0.10 per company call).
"""

import json
import re
import time

import structlog
from anthropic import Anthropic

from config import config
from enrichment.contact.models import CompanyHit, ContactHit

log = structlog.get_logger(__name__)
_client = Anthropic(api_key=config.ANTHROPIC_API_KEY)

MODEL = "claude-sonnet-4-5"

SYSTEM_PROMPT = """You are a forensic real estate researcher helping a NYC leasing broker
identify the key people behind a property management or ownership company.

You have access to a web_search tool. Use it liberally. Search for:
- The company name + "NYC real estate" / "New York"
- The company name + "owner" / "principal" / "managing member"
- The company website's team/about page
- LinkedIn company page
- NYC real estate news (The Real Deal, Commercial Observer, PincusCo)
- HPD registration records (data.cityofnewyork.us)

Return ONLY a JSON object with this shape:
{
  "website": "https://..." | null,
  "phone": "212-555-1234" | null,
  "address": "123 Main St, New York, NY 10001" | null,
  "linkedin_url": "https://linkedin.com/company/..." | null,
  "officers": [
    {
      "full_name": "Jane Doe",
      "title": "CEO" | null,
      "email": "jane@example.com" | null,
      "phone": "212-555-5678" | null,
      "evidence": "Source of this information",
      "source_url": "https://..." | null
    }
  ],
  "notes": "Any useful context"
}

Rules:
- Do NOT guess. If you can't find something, use null.
- Officers list should contain the actual humans running the company,
  not legal registered agents or government contacts.
- Cite a source_url for each officer where possible.
- Return at most 10 officers (the most senior / most likely to own the buildings).
"""


async def research_company(
    name: str,
    addresses: list[str] | None = None,
) -> dict:
    """Run a web-search enabled Claude call for a company.

    Returns a dict with keys: website, phone, address, linkedin_url, officers, notes.
    Returns {} on failure.
    """
    addr_context = ""
    if addresses:
        sample = addresses[:5]  # cap to keep prompt tight
        addr_context = f"\nKnown property addresses: {'; '.join(sample)}"

    user = (
        f"Company: {name}{addr_context}\n\n"
        "Research this company and return the JSON described in your instructions."
    )

    resp = None
    for attempt in range(3):
        try:
            resp = _client.messages.create(
                model=MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 6}],
                messages=[{"role": "user", "content": user}],
            )
            break
        except Exception as e:
            err_str = str(e)
            if "rate_limit" in err_str and attempt < 2:
                wait = 60 * (attempt + 1)  # 60s, then 120s
                log.warning(
                    "claude_company_research.rate_limit_retry",
                    attempt=attempt + 1,
                    wait_seconds=wait,
                    error=err_str[:120],
                )
                time.sleep(wait)
            else:
                log.warning("claude_company_research.api_failed", error=err_str[:200])
                return {}
    if resp is None:
        return {}

    text = ""
    for block in resp.content:
        if getattr(block, "type", None) == "text":
            text += block.text + "\n"

    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception as e:
        log.warning("claude_company_research.json_parse_failed",
                    error=str(e), raw=text[:500])
        return {}


def parse_to_hits(
    data: dict,
    company_name: str,
) -> tuple[list[CompanyHit], list[ContactHit]]:
    """Convert research_company() output into Hit dataclasses."""
    companies: list[CompanyHit] = []
    contacts: list[ContactHit] = []

    # Company-level data → one CompanyHit
    if data.get("website") or data.get("phone") or data.get("address"):
        website = data.get("website")
        domain = None
        if website:
            m = re.match(r"https?://(?:www\.)?([^/]+)/?", website)
            if m:
                domain = m.group(1).lower()
        companies.append(CompanyHit(
            name=company_name,
            role_category="owner_operating",
            website=website,
            domain=domain,
            phone=data.get("phone"),
            address=data.get("address"),
            linkedin_url=data.get("linkedin_url"),
            confidence=0.70,
            source="claude_company_research",
            evidence="Claude web research",
            raw=data,
        ))

    # Officers → ContactHits
    for officer in data.get("officers") or []:
        full = officer.get("full_name") or ""
        if not full:
            continue
        contacts.append(ContactHit(
            full_name=full,
            title=officer.get("title"),
            email=officer.get("email"),
            phone=officer.get("phone"),
            company_name=company_name,
            network_role="officer",
            role_category="owner",
            confidence=0.65,
            source="claude_company_research",
            source_url=officer.get("source_url"),
            evidence=officer.get("evidence"),
            cost_cents=0,
            raw=officer,
        ))

    return companies, contacts
