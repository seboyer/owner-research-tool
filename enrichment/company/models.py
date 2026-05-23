"""
enrichment/company/models.py — Dataclasses for company enrichment.

Reuses CompanyHit and ContactHit from enrichment.contact.models — those
are the universal hit types throughout the pipeline.
"""

from dataclasses import dataclass, field

# Re-export so cascade/orchestrator can import from one place
from enrichment.contact.models import CompanyHit, ContactHit


@dataclass
class CompanyEntity:
    """The subject of a company enrichment run.

    Built by _load_company() from an entities row + its linked property BBLs.
    """
    entity_id: str
    name: str
    entity_type: str | None         # 'llc' | 'corporation' | 'management_company'
    role_category: str | None       # 'owner_operating' | 'management' | etc.
    portfolio_size: int                # max(entities.portfolio_size, len(bbls))
    bbls: list[str] = field(default_factory=list)


@dataclass
class CompanyEnrichmentResult:
    """Output of cascade.run() for one company entity."""
    entity_id: str
    companies: list[CompanyHit] = field(default_factory=list)
    contacts: list[ContactHit] = field(default_factory=list)
    sources_attempted: list[str] = field(default_factory=list)
    sources_succeeded: list[str] = field(default_factory=list)
    cost_cents: int = 0
    error: str | None = None
