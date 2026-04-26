"""
enrichment/contact/sources/ — Per-provider adapters.

Every adapter exposes async functions that return lists of CompanyHit or
ContactHit. Adapters should never raise for missing API keys — they should
log and return [] so the orchestrator's waterfall keeps flowing.

Free / public sources (always on):
    acris_party_history     ACRIS Parties dataset via NYC OpenData
    hpd_building_contacts   HPD Registration Contacts via NYC OpenData
    dob_permits             NYC DOB permit issuance
    opencorporates_graph    OpenCorporates officer/company lookups
    nys_dos                 NY State DOS corporate search (registered agents)
    claude_web_search       Claude with native web_search tool

Budget-tier (paid, per-query):
    batchdata               BatchData V3 skip trace — phones + emails by property address
    google_places           Google Maps API — company phone/website by name
    hunter                  Hunter.io — email finder by domain

Standard-tier:
    apollo                  Apollo.io — person + email + phone match
    whitepages              Whitepages Pro — individual phones
    property_radar          PropertyRadar — real estate owner contacts

Premium-tier:
    zoominfo_person         Zoominfo contact search
    proxycurl               LinkedIn profile scraper
    reonomy                 Reonomy / PropertyShark
"""
