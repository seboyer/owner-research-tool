"""
config.py — Central configuration loaded from environment variables.
All modules import from here rather than calling os.environ directly.

Supabase note: newer projects use SUPABASE_KEY (anon key + RLS) rather than
a service role key. Both SUPABASE_KEY and the legacy SUPABASE_SERVICE_KEY are
accepted — whichever is set will be used.

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
    # ZOOMINFO_MIN_PORTFOLIO_SIZE removed — the company_enrich cascade
    # queues all LLC/corp/mgmt entities regardless of portfolio size.
    # Spend is governed by cost tier (FREE for 1-building entities).

    # ------------------------------------------------------------------
    # Auto-search worker toggle
    # When false (default), the cron-based daily/weekly pipelines are
    # disabled. The webhook listener still accepts manually-fed addresses
    # via the Airtable integration. Flip to "true" in the Render dashboard
    # once the pipeline has been validated against live data.
    # ------------------------------------------------------------------
    AUTO_SEARCH_ENABLED: bool = os.getenv("AUTO_SEARCH_ENABLED", "false").lower() in ("true", "1", "yes")
    WEEKLY_PIPELINE_DAY: str = os.getenv("WEEKLY_PIPELINE_DAY", "tue")
    ADMIN_PASSWORD: str = os.getenv("ADMIN_PASSWORD", "")

    # ------------------------------------------------------------------
    # Enrichment cost cap (per pipeline run)
    # Set DAILY_ENRICHMENT_COST_CAP_USD to a positive float to cap
    # estimated spend per Run Daily / Run Weekly. When the cap is hit,
    # remaining queue items roll over to the next run (they have older
    # created_at, so the natural ordering serves them first). Default
    # 0 = no cap.
    #
    # Per-entity estimates are conservative (overestimate rather than
    # under). Tunable per-stage so you can calibrate from observed spend.
    # ------------------------------------------------------------------
    SKIP_LOW_VALUE_THRESHOLD: float = float(os.getenv("SKIP_LOW_VALUE_THRESHOLD", "0.30"))
    DAILY_ENRICHMENT_COST_CAP_USD: float = float(os.getenv("DAILY_ENRICHMENT_COST_CAP_USD", "0"))
    COST_PER_ENTITY_LLC_PIERCE: float = float(os.getenv("COST_PER_ENTITY_LLC_PIERCE", "0.30"))
    COST_PER_ENTITY_ACRIS_PDF: float = float(os.getenv("COST_PER_ENTITY_ACRIS_PDF", "0.30"))
    # Company cascade can burn up to ~$5 at PREMIUM tier (50+ buildings):
    # Apollo org_search ($0.01) + 3× Apollo people enrich ($3.00) + Zoominfo ($1.50)
    # + Hunter ($0.25) + Google Places ($0.10) + 3× Proxycurl ($0.30).
    COST_PER_ENTITY_COMPANY_ENRICH: float = float(os.getenv("COST_PER_ENTITY_COMPANY_ENRICH", "5.00"))
    COST_PER_ENTITY_MULTI_SOURCE: float = float(os.getenv("COST_PER_ENTITY_MULTI_SOURCE", "1.00"))

    # ------------------------------------------------------------------
    # Building-size gate (PLUTO)
    #
    # ACRIS records every deed transfer regardless of building size, so
    # without a gate the pipeline stores 1-2 family homes and pays to
    # enrich owners who are not landlords. PLUTO supplies the residential
    # unit count HPD does not publish.
    #
    # A lot is admitted when it looks like a landlord property — see
    # ingest.pluto.is_landlord_lot() — or when PLUTO has no record of the
    # BBL (the gate fails open). Set PLUTO_GATE_ENABLED=false to store
    # everything and rely on downstream filtering instead.
    # ------------------------------------------------------------------
    PLUTO_MIN_RESIDENTIAL_UNITS: int = int(os.getenv("PLUTO_MIN_RESIDENTIAL_UNITS", "3"))
    PLUTO_GATE_ENABLED: bool = os.getenv("PLUTO_GATE_ENABLED", "true").lower() != "false"

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
    # Airtable CRM sync (pipeline/airtable_sync.py)
    # Pushes researched Managements / Contacts / Addresses into the same
    # LL Pipeline base the address webhook above writes back to.
    # ------------------------------------------------------------------
    AIRTABLE_MANAGEMENT_TABLE_ID: str = os.getenv(
        "AIRTABLE_MANAGEMENT_TABLE_ID", "tblXSLY5l2ON0sChK"
    )
    AIRTABLE_CONTACTS_TABLE_ID: str = os.getenv(
        "AIRTABLE_CONTACTS_TABLE_ID", "tblid0IpZKpI6O14q"
    )
    # Record in the Types table that tags a Management as ORT-sourced.
    AIRTABLE_ORT_TYPE_RECORD_ID: str = os.getenv(
        "AIRTABLE_ORT_TYPE_RECORD_ID", "recpeksnQCHm0qlRd"
    )
    # Pipeline stage stamped on Managements this tool creates.
    AIRTABLE_NEW_PIPELINE_STAGE: str = os.getenv(
        "AIRTABLE_NEW_PIPELINE_STAGE", "New/Unsorted"
    )

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
