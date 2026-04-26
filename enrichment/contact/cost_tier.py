"""
enrichment/contact/cost_tier.py — Cost tier selection.

Matches a portfolio_size to a budget tier that determines which sources the
orchestrator will call for each prong. The tier names are used as keys into
the SOURCE_MATRIX in orchestrator.py.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CostTier:
    name: str
    max_spend_cents_per_signer: int
    description: str


FREE = CostTier(
    "free", 0,
    "Only no-cost public data (ACRIS, HPD, DOB, OpenCorporates search, NYS DOS, Claude web_search).",
)
BUDGET = CostTier(
    "budget", 50,
    "Free sources + Hunter.io domain search + Google Places (~$0.10-$0.50 per signer).",
)
STANDARD = CostTier(
    "standard", 300,
    "Adds Apollo.io person match + Whitepages + PropertyRadar (~$1-$3 per signer).",
)
PREMIUM = CostTier(
    "premium", 2000,
    "Adds Zoominfo + Proxycurl + Reonomy/PropertyShark (~$5-$20 per signer).",
)


def tier_for_portfolio_size(portfolio_size: int) -> CostTier:
    if portfolio_size is None or portfolio_size <= 1:
        return FREE
    if portfolio_size < 10:
        return BUDGET
    if portfolio_size < 50:
        return STANDARD
    return PREMIUM


def tier_allows(tier: CostTier, required_tier: CostTier) -> bool:
    """True if `tier` is at or above `required_tier`."""
    order = [FREE, BUDGET, STANDARD, PREMIUM]
    return order.index(tier) >= order.index(required_tier)
