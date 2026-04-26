"""
enrichment/contact/models.py — Pydantic-ish dataclasses shared between prongs
and source adapters. Plain dataclasses keep the dependency footprint small.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Signer:
    """The seed input: a person who signed an ACRIS mortgage for a building LLC."""
    contact_id: str                  # contacts.id row in Supabase
    full_name: str
    title: Optional[str]             # e.g. "Managing Member"
    building_llc_id: str             # entities.id of the title-holding LLC
    building_llc_name: str
    property_id: Optional[str]
    bbl: Optional[str]
    portfolio_size: int = 1          # of the building LLC (usually 1) — for tier selection
    email: Optional[str] = None      # known so far (may be filled in by prong 1)
    phone: Optional[str] = None


@dataclass
class CompanyHit:
    """A candidate operating company / management company for a signer or building."""
    name: str
    role_category: str               # 'owner_operating' | 'management' | 'title_holding_llc' | 'agent' | 'other'
    website: Optional[str] = None
    domain: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    linkedin_url: Optional[str] = None
    confidence: float = 0.5
    source: str = ""                 # which adapter produced this
    source_url: Optional[str] = None
    evidence: Optional[str] = None
    raw: dict = field(default_factory=dict)


@dataclass
class ContactHit:
    """A candidate person-level contact."""
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    title: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    phone_type: Optional[str] = None
    linkedin_url: Optional[str] = None
    company_name: Optional[str] = None         # company this person is linked to
    network_role: str = "unknown"              # 'signer'|'coworker'|'co_owner'|'co_officer'|'building_contact'|'head_officer'|'registered_agent'
    role_category: str = "unknown"             # 'owner'|'management'|'both'|'unknown'
    confidence: float = 0.5
    source: str = ""
    source_url: Optional[str] = None
    evidence: Optional[str] = None
    cost_cents: int = 0
    raw: dict = field(default_factory=dict)


@dataclass
class ProngResult:
    prong: int
    signer_contact_id: str
    companies: list[CompanyHit] = field(default_factory=list)
    contacts: list[ContactHit] = field(default_factory=list)
    sources_attempted: list[str] = field(default_factory=list)
    sources_succeeded: list[str] = field(default_factory=list)
    cost_cents: int = 0
    error: Optional[str] = None
