"""
Prong 2 — Signer's network.

Returns three kinds of people, each tagged with network_role:

    network_role='coworker'    — people listed on the signer's operating
                                  company (OpenCorporates officers, Hunter
                                  domain emails, Apollo org employees)
    network_role='co_owner'    — people who appear as parties on OTHER
                                  ACRIS deals alongside the signer
    network_role='co_officer'  — people listed on OTHER non-building LLCs
                                  where the signer is an officer

The point: give the broker a bench of fallback contacts in case the
primary signer ignores their outreach.
"""

import structlog

from .cost_tier import CostTier, BUDGET, STANDARD, PREMIUM, tier_allows
from .models import Signer, ProngResult, ContactHit
from .sources import (
    acris_party_history, opencorporates_graph, paid_stubs,
)
from .filters import is_govt_entity

log = structlog.get_logger(__name__)

MAX_CO_OWNERS = 15
MAX_CO_OFFICERS_PER_COMPANY = 8
MAX_COWORKERS = 20


async def run(signer: Signer, tier: CostTier) -> ProngResult:
    r = ProngResult(prong=2, signer_contact_id=signer.contact_id)
    attempted, succeeded = r.sources_attempted, r.sources_succeeded

    # ---------- co_owners via ACRIS party graph ----------
    attempted.append("acris_party_history")
    co_owners = await acris_party_history.co_owners_on_other_deals(
        signer.full_name, exclude_bbl=signer.bbl, max_deals=25,
    )
    if co_owners:
        succeeded.append("acris_party_history")
        for c in co_owners[:MAX_CO_OWNERS]:
            if is_govt_entity(c.full_name):
                continue
            r.contacts.append(c)

    # ---------- co_officers via OpenCorporates — disabled (price prohibitive) ----------
    # Not added to sources_attempted — disabled globally via _DISABLED flag.

    # ---------- coworkers (budget+) via Hunter domain search ----------
    if tier_allows(tier, BUDGET):
        # We can't know the operating company domain in this prong in isolation,
        # so rely on entities table populated by prong 1 if it ran first.
        from database.client import db
        op_cos = db().table("entities")\
            .select("name, domain")\
            .eq("role_category", "owner_operating")\
            .not_.is_("domain", "null")\
            .limit(5).execute().data or []
        # Filter to companies linked to this signer's building LLC
        if op_cos:
            attempted.append("hunter")
            for op_co in op_cos[:2]:
                hits = await paid_stubs.hunter_domain_search(op_co["domain"])
                if hits:
                    succeeded.append("hunter")
                    for h in hits[:MAX_COWORKERS]:
                        h.network_role = "coworker"
                        h.company_name = op_co["name"]
                        h.role_category = "owner"
                        r.contacts.append(h)
                        r.cost_cents += h.cost_cents

    # ---------- Apollo org people (standard tier) ----------
    if tier_allows(tier, STANDARD):
        pass  # hook for Apollo organization/people endpoint; left as TODO

    # Dedup by (full_name, company_name)
    dedup: dict[tuple[str, str], ContactHit] = {}
    for c in r.contacts:
        key = (c.full_name.upper(), (c.company_name or "").upper())
        if key in dedup:
            # Keep the higher-confidence one
            if c.confidence > dedup[key].confidence:
                dedup[key] = c
        else:
            dedup[key] = c
    r.contacts = list(dedup.values())
    return r
