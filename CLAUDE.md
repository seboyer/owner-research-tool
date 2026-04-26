# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Owner Research Tool is a data pipeline for researching NYC real estate property owners. It ingests property data from NYC government sources, identifies LLC/corporate entities, pierces shell LLCs to find real owners, and enriches contact data — all stored in Supabase (PostgreSQL).

## Commands

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env  # then fill in API keys

# Run schema migrations in order (Supabase SQL editor or psql)
# 1. database/schema.sql
# 2. database/migrations/002_contact_enrichment.sql

# CLI (main entry point)
python main.py full-load          # one-time initial load (hours)
python main.py daily              # ACRIS delta + enrichment
python main.py weekly             # HPD refresh + WoW sync + daily pipeline
python main.py enrich             # enrichment only (Zoominfo + multi-source + LLC piercing)
python main.py enrich-contacts    # 3-prong contact enrichment for all pending signers
python main.py enrich-contacts --signer-id <uuid>  # single signer
python main.py enrich-contacts --force              # ignore 90-day cache
python main.py stats              # print DB statistics
python main.py pierce --entity "123 BROADWAY LLC"  # pierce a specific LLC
python main.py ingest hpd|acris|wow           # individual ingestors (opencorporates removed)
python main.py schedule           # start persistent scheduler (production)

# Targeted address test (runs full pipeline on specific addresses)
python test_addresses.py

# Deployment
docker build -t owner-research-tool .
docker run --env-file .env owner-research-tool
```

There is no test suite; behavior is validated by running pipeline stages against live APIs/DB.

## Architecture

### Data Flow

```
NYC Gov't Sources (HPD, ACRIS, WoW)
        ↓
[ingest/: parse, normalize, deduplicate]
        ↓
Supabase (entities, properties, property_roles)
        ↓
[enrichment_queue] → [enrichment/: LLC piercing, contacts]
        ↓
entity_relationships + contacts → CRM export views
```

### Pipeline Stages (`pipeline/orchestrator.py`)

7 composable stages run in three modes:

| Mode | Stages | Schedule |
|------|--------|----------|
| `full-load` | All stages, large batch sizes | One-time |
| `daily` | ACRIS delta, LLC piercing, ACRIS PDFs, Zoominfo, multi-source | Daily 3 AM ET |
| `weekly` | HPD full sync + WoW portfolio + daily stages | Sunday 2 AM ET |

### LLC Piercing — 3-Strategy Cascade (`enrichment/llc_piercer.py`)

Tries strategies in order, stops on first success:
1. **ACRIS PDF signer** — real human who signed the mortgage document (Claude Vision extracts name/title from PDF)
2. **HPD portfolio lookup** — queries HPD Registrations + Contacts for HeadOfficer/IndividualOwner at the same BBL (replaced the defunct Who Owns What API)
3. **Claude agentic reasoning** — AI synthesizes all signals + web search as last resort

*(Who Owns What API is dead — returns an HTML SPA shell for all endpoints. Graceful detection added in `ingest/whoownswhat.py`. OpenCorporates removed — price prohibitive.)*

Each successful pierce writes to `entity_relationships` with a confidence score (0–1).

### LLC Piercing → Contact Enrichment flow

Once a mortgage signer is identified (via ACRIS PDF + Claude Vision in `enrichment/acris_pdf.py`), a `contacts` row is created with `network_role='signer'`. This kicks off the 3-prong contact enrichment system.

**3-Prong Contact Enrichment** (`enrichment/contact/`) — see `CONTACT_ENRICHMENT.md` for full design:

| Prong | Input | Output |
|-------|-------|--------|
| 1 — Signer | name, title LLC, BBL | signer phone/email + owner operating co + mgmt co |
| 2 — Network | signer name + operating co | coworkers, co-owners (ACRIS), co-officers |
| 3 — Building | BBL | all owner/mgmt contacts on file at HPD, DOB, ACRIS |

Cost tiers scale with `portfolio_size`: free (1 building) → budget → standard → premium (50+).

Source waterfall (free first, paid only at higher tiers):
- Free: ACRIS party history, HPD registration, DOB permits, Claude `web_search`
- Budget: BatchData V3 skip trace, Google Places, Hunter.io
- Standard: Apollo.io — then Whitepages last (50-query trial, $220/mo — evaluate before committing)
- Premium: Proxycurl, Zoominfo

BatchData skip traces the property address and returns up to 3 persons with ranked phones (DNC-filtered, TCPA-flagged) and tested emails. Used in Prong 1 and Prong 3. See `enrichment/contact/sources/batchdata.py`.

**Address lookup for BatchData/prong3**: `_address_parts_from_bbl()` reads `house_number` + `street_name` from the `properties` table, falling back to parsing the combined `address` column if the structured fields are null. Always store both when upserting a property.

OpenCorporates removed from all prongs and LLC piercing — price prohibitive for commercial use. Code retained but gated behind `_DISABLED = True` in `enrichment/contact/sources/opencorporates_graph.py`.

Whitepages returns a dict (not a list) on some responses — the handler guards with `isinstance(records, list)` before iterating. Whitepages is positioned last in Standard tier, only firing after Apollo has already run with no result.

**Apollo auth**: API key must be passed in the `X-Api-Key` request header — NOT in the JSON body. The API returns 422 if the key is in the body.

**Claude Sonnet rate limits**: The web search model (`claude-sonnet-4-5`) has a 30k input token/minute org-wide limit. Each signer research call can consume 25–40k tokens across its 8 web searches. The orchestrator sleeps 90 seconds between signers to avoid rate limit collisions. The `claude_web_search` source retries on 429 with 60s then 120s backoff before giving up.

**Government contact filter** (`enrichment/contact/filters.py`): All ACRIS party history results and HPD/DOB contacts are passed through `is_govt_entity()` before being stored. This removes City of New York, Commissioner of Finance (and its many ACRIS misspellings), NYC HPD, NYCHA, HUD, etc. The filter uses flexible regex patterns — not exact strings — to handle ACRIS's inconsistent spelling.

Paid sources activate automatically when the matching key is added to `.env`:
`BATCHDATA_API_KEY`, `APOLLO_API_KEY`, `HUNTER_API_KEY`, `GOOGLE_PLACES_API_KEY`, `PROXYCURL_API_KEY`, `WHITEPAGES_API_KEY`

Key broker-facing DB objects:
- `entities.role_category` — `'owner_operating'` vs `'management'` vs `'title_holding_llc'`
- `contacts.network_role` — `'signer'` / `'coworker'` / `'co_owner'` / `'co_officer'` / `'building_contact'`
- `contacts.seed_signer_id` — traces every contact back to its seed
- `broker_pitch_list` view — one row per property with owner co, mgmt co, and signer contact in one query
- `contact_enrichment_runs` — caches per-prong results for 90 days to avoid repeat spend

### Signer Loading (`enrichment/contact/orchestrator.py`)

ACRIS PDF signers are stored as individual-type entities, but property ownership is recorded on the LLC entity via `property_roles`. `_load_signer()` walks the chain:
1. Direct: `property_roles` where `entity_id = signer_entity_id` (works when entity IS the LLC)
2. Indirect: `entity_relationships` where `parent_entity_id = signer_entity_id` → find the child LLC → then `property_roles` on that LLC

FK ambiguity: the `contacts` table has two foreign keys to `entities`. Always use the explicit hint `entities!contacts_entity_id_fkey(*)` in Supabase queries.

### Database Client (`database/client.py`)

Wraps Supabase with project-specific helpers:
- **Normalized name matching**: lowercase, strip punctuation, expand abbreviations
- **Checksum-based change detection**: only updates records when content changed
- **`seen_records` table**: idempotent ingestor guard — prevents reprocessing identical source records
- All writes use upsert-on-conflict for idempotency
- `load_dotenv(override=True)` in `config.py` — required so `.env` values override any shell environment variables (e.g. an empty `ANTHROPIC_API_KEY` in the shell)

### Key Database Objects (`database/schema.sql`)

Core tables: `properties` (BBL-keyed, includes `house_number` + `street_name` as structured fields), `entities` (unified LLC/corp/individual), `entity_relationships` (child→parent with confidence), `property_roles` (property↔entity with role+source), `contacts`, `seen_records`, `enrichment_queue`, `ingestion_log`, `contact_enrichment_runs`

Key views: `landlord_profiles` (full entity profile), `unpierced_llcs` (prioritized work queue), `crm_export` (contacts ready for CRM import), `broker_pitch_list` (property + owner co + mgmt co + signer in one row)

### External API Tiers

- **Free/public**: NYC OpenData (HPD, ACRIS, PLUTO, DOB permits via Socrata)
- **Budget (~$0.40–$1/signer)**: BatchData V3 skip trace (~$0.40/match), Hunter.io, Google Places
- **Standard (~$1–$3/signer)**: Apollo.io (person match, key in `X-Api-Key` header), Whitepages (50-query trial, $220/mo — last-resort, efficacy TBD)
- **Premium (~$5–$20/signer)**: Proxycurl (LinkedIn), Zoominfo (JWT auth, `search_contacts_at_company` in `enrichment/zoominfo.py`)
- **Not active**: Who Owns What (API dead — returns HTML), PropertyRadar (code kept, key not set), OpenCorporates (removed — too expensive)
- **AI**: Anthropic Claude Sonnet (`claude-sonnet-4-5`) for web search enrichment; Claude Opus for ACRIS PDF vision extraction. 90s inter-signer sleep required to avoid Sonnet rate limits.
- **ACRIS PDFs**: Playwright local → Browserless (cloud) → ScraperAPI (proxy) fallback chain

### Approximate Run Cost

End-to-end cost per address at STANDARD tier (BatchData + Apollo + Claude Sonnet):
- **~$0.76/address** with paid APIs active
- **$0.00** at FREE tier (1-building landlords — BatchData/Apollo gated behind BUDGET+)
- Validated run across 5 addresses: $3.82 total ($3.20 BatchData, $0.52 Claude est., $0.10 Apollo)

### Configuration (`config.py`)

All settings loaded from environment variables. Key tunables:
- `HPD_BATCH_SIZE`, `ACRIS_BATCH_SIZE`, `ENRICHMENT_BATCH_SIZE`
- `ACRIS_LOOKBACK_DAYS` (default: 30)
- `ZOOMINFO_MIN_PORTFOLIO_SIZE` (skip small portfolios)
- `ENVIRONMENT` (development/production)

All enrichment API keys are optional — the system degrades gracefully when keys are missing.

### Scheduler (`scheduler.py`)

APScheduler-based persistent service. Runs as the production entry point in Docker/Railway. Coalesces missed runs (1-hour grace for daily, 2-hour for weekly).

## Deployment

Render.com is the primary deployment target. The Docker container runs `python scheduler.py` as the default command (see `Dockerfile`). Auto-deploys on push to `main`.

**Dependency constraint**: `supabase==2.4.0` requires `httpx<0.26,>=0.24`. Keep `httpx` pinned to `0.25.x` — do not upgrade past 0.25.x without also upgrading `supabase` to a version that supports newer httpx. `anthropic` and `openai` both accept any httpx>=0.23 so they are not the constraint.
