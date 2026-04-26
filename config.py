"""
config.py — Central configuration loaded from environment variables.
All modules import from here rather than calling os.environ directly.

Supabase note: newer projects use SUPABASE_KEY (anon key + RLS) rather than
a service role key. Both SUPABASE_KEY and the legacy SUPABASE_SERVICE_KEY are
accepted — whichever is set will be used.

OpenCorporates: removed — price prohibitive for commercial use.

Source waterfall (free → paid, activated by API key presence):
  Free:     ACRIS, HPD, DOB, WoW, Claude web_search
  Budget:   BatchData, Google Places, Hunter
  Standard: Apollo, then Whitepages (trial — evaluate efficacy)
  Premium:  Proxycurl, Zoominfo
"""

import os
from dotenv import load_dotenv

load_dotenv(override=True)


class Config:
    # ------------------------------------------------------------------
    # Supabase
    # Accepts SUPABASE_KEY (preferred) or legacy SUPABASE_SERVICE_KEY.
    # Both are optional at load time; will raise at first DB call if absent.
    # ------------------------------------------------------------------
    SUPABASE_URL: str = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY: str = (
        os.getenv("SUPABASE_KEY")
        or os.getenv("SUPABASE_SERVICE_KEY")
        or ""
    )

    # ------------------------------------------------------------------
    # AI APIs
    # ------------------------------------------------------------------
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

    # ------------------------------------------------------------------
    # NYC OpenData (Socrata)
    # ------------------------------------------------------------------
    NYC_OPENDATA_APP_TOKEN: str = os.getenv("NYC_OPENDATA_APP_TOKEN", "")

    # ------------------------------------------------------------------
    # Contact enrichment — Budget tier
    # ------------------------------------------------------------------
    BATCHDATA_API_KEY: str = os.getenv("BATCHDATA_API_KEY", "")
    GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
    HUNTER_API_KEY: str = os.getenv("HUNTER_API_KEY", "")

    # ------------------------------------------------------------------
    # Contact enrichment — Standard tier
    # ------------------------------------------------------------------
    APOLLO_API_KEY: str = os.getenv("APOLLO_API_KEY", "")
    # Whitepages: 50-query free trial, $220/mo thereafter.
    # Positioned last in the standard waterfall — monitor efficacy before
    # committing to a paid subscription.
    WHITEPAGES_API_KEY: str = os.getenv("WHITEPAGES_API_KEY", "")

    # ------------------------------------------------------------------
    # Contact enrichment — Premium tier
    # ------------------------------------------------------------------
    PROXYCURL_API_KEY: str = os.getenv("PROXYCURL_API_KEY", "")
    PROPERTYRADAR_API_KEY: str = os.getenv("PROPERTYRADAR_API_KEY", "")  # code kept, not yet active

    # ------------------------------------------------------------------
    # Zoominfo — Premium tier (JWT auth)
    # ------------------------------------------------------------------
    ZOOMINFO_CLIENT_ID: str = os.getenv("ZOOMINFO_CLIENT_ID", "")
    ZOOMINFO_PRIVATE_KEY: str = os.getenv("ZOOMINFO_PRIVATE_KEY", "")
    ZOOMINFO_USERNAME: str = os.getenv("ZOOMINFO_USERNAME", "")

    # ------------------------------------------------------------------
    # ACRIS PDF download — Playwright → Browserless → ScraperAPI fallback
    # ------------------------------------------------------------------
    BROWSERLESS_API_KEY: str = os.getenv("BROWSERLESS_API_KEY", "")
    SCRAPERAPI_KEY: str = os.getenv("SCRAPERAPI_KEY", "")

    # ------------------------------------------------------------------
    # Pipeline tunables
    # ------------------------------------------------------------------
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    HPD_BATCH_SIZE: int = int(os.getenv("HPD_BATCH_SIZE", "1000"))
    ACRIS_BATCH_SIZE: int = int(os.getenv("ACRIS_BATCH_SIZE", "500"))
    ENRICHMENT_BATCH_SIZE: int = int(os.getenv("ENRICHMENT_BATCH_SIZE", "50"))
    ACRIS_LOOKBACK_DAYS: int = int(os.getenv("ACRIS_LOOKBACK_DAYS", "30"))
    ZOOMINFO_MIN_PORTFOLIO_SIZE: int = int(os.getenv("ZOOMINFO_MIN_PORTFOLIO_SIZE", "3"))

    # ------------------------------------------------------------------
    # Auto-search worker toggle
    # When false (default), the cron-based daily/weekly pipelines are
    # disabled. The webhook listener still accepts manually-fed addresses
    # via the Airtable integration. Flip to "true" in the Render dashboard
    # once the pipeline has been validated against live data.
    # ------------------------------------------------------------------
    AUTO_SEARCH_ENABLED: bool = os.getenv("AUTO_SEARCH_ENABLED", "false").lower() in ("true", "1", "yes")

    # ------------------------------------------------------------------
    # Webhook + Airtable integration (manual address feed)
    # ------------------------------------------------------------------
    WEBHOOK_PORT: int = int(os.getenv("PORT", "8000"))  # Render injects PORT
    AIRTABLE_WEBHOOK_SECRET: str = os.getenv("AIRTABLE_WEBHOOK_SECRET", "")
    AIRTABLE_API_KEY: str = os.getenv("AIRTABLE_API_KEY", "")
    AIRTABLE_BASE_ID: str = os.getenv("AIRTABLE_BASE_ID", "appstQVl7JeMfr7d0")  # LL Pipeline
    AIRTABLE_ADDRESS_TABLE_ID: str = os.getenv("AIRTABLE_ADDRESS_TABLE_ID", "tblVOwshwfY0F3gSS")
    AIRTABLE_BBL_FIELD_ID: str = os.getenv("AIRTABLE_BBL_FIELD_ID", "fldYiP7RhhoYx6QpS")
    AIRTABLE_HPD_FIELD_ID: str = os.getenv("AIRTABLE_HPD_FIELD_ID", "fldqyrQmuJlglgXqu")

    # ------------------------------------------------------------------
    # NYC OpenData Socrata endpoints
    # ------------------------------------------------------------------
    HPD_REGISTRATIONS_URL = "https://data.cityofnewyork.us/resource/tesw-yqqr.json"
    HPD_CONTACTS_URL = "https://data.cityofnewyork.us/resource/feu5-w2e2.json"
    ACRIS_MASTER_URL = "https://data.cityofnewyork.us/resource/bnx9-e6tj.json"
    ACRIS_PARTIES_URL = "https://data.cityofnewyork.us/resource/636b-3b5g.json"
    ACRIS_LEGALS_URL = "https://data.cityofnewyork.us/resource/8h5j-fqxa.json"
    PLUTO_URL = "https://data.cityofnewyork.us/resource/64uk-42ks.json"

    # ------------------------------------------------------------------
    # Who Owns What (JustFix) — free, no auth
    # ------------------------------------------------------------------
    WOW_SEARCH_URL = "https://whoownswhat.justfix.org/api/search"
    WOW_PORTFOLIO_URL = "https://whoownswhat.justfix.org/api/portfolio"

    # ------------------------------------------------------------------
    # Zoominfo endpoints
    # ------------------------------------------------------------------
    ZOOMINFO_AUTH_URL = "https://api.zoominfo.com/authenticate"
    ZOOMINFO_COMPANY_SEARCH_URL = "https://api.zoominfo.com/search/company"
    ZOOMINFO_CONTACT_SEARCH_URL = "https://api.zoominfo.com/search/contact"
    ZOOMINFO_COMPANY_ENRICH_URL = "https://api.zoominfo.com/enrich/company"

    # ------------------------------------------------------------------
    # AI models
    # ------------------------------------------------------------------
    CLAUDE_MODEL = "claude-opus-4-6"
    OPENAI_MODEL = "gpt-4o"


config = Config()


def validate_required_config() -> list[str]:
    """
    Validate that the keys we cannot run without are set. Returns a list of
    missing keys. Callers (CLI commands, webhook startup, scheduler) decide
    whether to abort or just warn. We intentionally do NOT raise at import
    time — the `pierce` and `enrich` commands work without ANTHROPIC_API_KEY,
    and unit-test scenarios may set keys after import.
    """
    missing = []
    if not config.SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not config.SUPABASE_KEY:
        missing.append("SUPABASE_KEY (or legacy SUPABASE_SERVICE_KEY)")
    if not config.ANTHROPIC_API_KEY:
        missing.append("ANTHROPIC_API_KEY")
    return missing
