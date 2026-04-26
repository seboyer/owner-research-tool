"""
enrichment/contact/ — 3-Prong Contact Enrichment System

Given a mortgage signer (name + title LLC + property), find the people and
companies a broker would actually call to pitch for the rental listing.

Three prongs, run in parallel:

  Prong 1 (SIGNER):
      Given: signer full name + title-holding LLC + property BBL
      Produce: phone, email, the owner's public-facing operating company
               ("Acme Realty Corp"), AND the management company if distinct.

  Prong 2 (NETWORK):
      Given: signer + their operating company
      Produce: coworkers at the operating company, co-owners on other NYC deals,
               co-officers on other LLCs registered to the same person.

  Prong 3 (BUILDING):
      Given: property BBL
      Produce: other contacts tied to the same building's ownership OR
               management — HPD head officer, managing agent, site manager,
               DOB permit filers, ACRIS historical parties on the same BBL.

Cost tiers (driven by portfolio_size):
  Portfolio  1 building      → 'free'      only free public sources
  Portfolio  2–9             → 'budget'    + Hunter domain search, Google Places
  Portfolio  10–49           → 'standard'  + Apollo.io, Whitepages
  Portfolio  50+             → 'premium'   + Zoominfo, Proxycurl, Reonomy

Every contact row stores which prong found it, which signer/property seeded it,
and a JSONB `sources` array with citation URLs — so the broker can verify.
"""

from .orchestrator import enrich_signer, enrich_pending_signers

__all__ = ["enrich_signer", "enrich_pending_signers"]
