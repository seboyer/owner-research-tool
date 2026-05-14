"""
Zoominfo — person search (corporate contacts).

Delegates to the enrichment/zoominfo.py module which handles JWT auth.
No direct API key check here — the zoominfo module manages its own auth.
"""

import structlog

from ..models import ContactHit

log = structlog.get_logger(__name__)


async def zoominfo_person_at_company(full_name: str, company: str) -> list[ContactHit]:
    # Delegates to the existing enrichment/zoominfo.py module if wired.
    try:
        from enrichment.zoominfo import search_contacts_at_company
        return await search_contacts_at_company(full_name, company)
    except ImportError:
        return []
    except Exception as e:
        log.warn("zoominfo.failed", error=str(e))
        return []
