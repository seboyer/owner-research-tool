"""
enrichment/contact/sources/ — Per-provider adapters.

Every adapter exposes async functions that return lists of CompanyHit or
ContactHit. Adapters should never raise for missing API keys — they should
log and return [] so the orchestrator's waterfall keeps flowing.

Free / public sources (always on):
    acris_party_history     ACRIS Parties dataset via NYC OpenData
    hpd_building_contacts   HPD Registration Contacts via NYC OpenData
    dob_permits             NYC DOB permit issuance
    nys_dos                 NY State DOS corporate search (registered agents)
    claude_web_search       Claude with native web_search tool

Budget-tier (paid, per-query):
    batchdata               BatchData V3 skip trace — phones + emails by property address
    google_places           Google Maps API — company phone/website by name (GOOGLE_PLACES_API_KEY)
    hunter                  Hunter.io — email finder by domain (HUNTER_API_KEY)

Standard-tier:
    apollo                  Apollo.io — person + email + phone match (APOLLO_API_KEY)
    whitepages              Whitepages Pro — individual phones (WHITEPAGES_API_KEY)
    property_radar          PropertyRadar — real estate owner contacts

Premium-tier:
    proxycurl               LinkedIn profile scraper (PROXYCURL_API_KEY)
    zoominfo                Zoominfo contact search (delegates to enrichment/zoominfo.py)
    reonomy                 Reonomy / PropertyShark

SourceResult / SourceStatus
---------------------------
Used by the multi_source aggregator (enrichment/multi_source.py) to
distinguish between a source that ran cleanly, one that returned no data,
one that was skipped (key not configured), and one that errored. The
aggregator uses these counts to decide whether to call mark_enrichment_done
or mark_enrichment_failed.

    OK_FOUND        — source ran and returned usable data
    OK_NO_DATA      — source ran cleanly, legitimate miss
    SKIPPED_NO_KEY  — API key not configured; source not attempted
    ERRORED         — non-2xx or unexpected exception
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SourceStatus(str, Enum):
    OK_FOUND = "ok_found"            # cleanly tried, returned data
    OK_NO_DATA = "ok_no_data"        # cleanly tried, no data (legitimate miss)
    SKIPPED_NO_KEY = "skipped_no_key"  # API key not set; source not attempted
    ERRORED = "errored"              # raised or got a non-2xx the handler couldn't recover


@dataclass
class SourceResult:
    status: SourceStatus
    data: list[dict[str, Any]] | dict[str, Any] | None = None
    error: str | None = None
    cost_usd: float = 0.0

    def __repr__(self) -> str:
        parts = [f"status={self.status.value!r}"]
        if self.data is not None:
            n = len(self.data) if isinstance(self.data, list) else 1
            parts.append(f"data({n} items)")
        if self.error:
            parts.append(f"error={self.error!r}")
        if self.cost_usd:
            parts.append(f"cost_usd={self.cost_usd}")
        return f"SourceResult({', '.join(parts)})"


__all__ = ["SourceResult", "SourceStatus"]
