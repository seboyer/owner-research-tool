# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Owner Research Tool is a data pipeline for researching NYC real estate property owners. It ingests property data from NYC government sources, identifies LLC/corporate entities, pierces shell LLCs to find real owners, and enriches contact data — all stored in Supabase (PostgreSQL).

## Conventions

- **Logging**: `log.warning(...)`, never `log.warn(...)`. Use `entity=<name>` as the keyword for entity names (not `entity_name=` or `name=`).
- **Config**: all environment-variable reads go through `config.py`. Never call `os.getenv(...)` directly in feature code.
- **Schema changes**: add a numbered migration file under `database/migrations/`. Never edit live tables via the Supabase UI without a matching migration; `schema.sql` and the migration set together should always describe the running DB.
- **Disabled code**: if a code path is no longer used, delete it. No `_DISABLED` flags or `_DISABLED = True` patterns.
- **Linting**: `ruff.toml` + `.pre-commit-config.yaml` are checked in. Run `pre-commit install` once to enable hooks. CI is not yet wired but the config exists.

## Commands

```bash
# Setup
pip install -r requirements.txt
cp .env.example .env  # then fill in API keys

# Run schema migrations in order (Supabase SQL editor or psql)
# 1. database/schema.sql
# 2. database/migrations/002_contact_enrichment.sql
# 3. database/migrations/003_reconcile_and_queue.sql
# 4. database/migrations/004_drop_opencorporates_url.sql
# 5. database/migrations/005_zipcode_allowlist.sql
# ... (006–015 in order)
# 16. database/migrations/016_company_enrichment.sql

# CLI (main entry point)
python main.py full-load          # one-time initial load (hours)
python main.py daily              # ACRIS delta + enrichment
python main.py weekly             # HPD refresh + WoW sync + daily pipeline
python main.py enrich             # enrichment only (company cascade + multi-source + LLC piercing)
python main.py enrich-contacts    # 3-prong contact enrichment for all pending signers
python main.py enrich-contacts --signer-id <uuid>  # single signer
python main.py enrich-contacts --force              # ignore 90-day cache
python main.py stats              # print DB statistics
python main.py pierce --entity "123 BROADWAY LLC"  # pierce a specific LLC
python main.py pierce --address "123 BROADWAY, MANHATTAN"  # research a full address
python main.py ingest hpd|acris|wow|pluto     # individual ingestors
python main.py backfill-pluto     # fill properties.unit_count/building_class from PLUTO
python main.py backfill-pluto --all  # re-check every property, not just NULL ones
python main.py schedule           # start persistent scheduler (production)
python main.py sync-airtable --dry-run  # preview the Airtable CRM sync
python main.py sync-airtable      # push Managements/Contacts/Addresses to Airtable

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
| `daily` | ACRIS delta, LLC piercing, ACRIS PDFs, company cascade, multi-source | Daily 3 AM ET |
| `weekly` | HPD full sync + WoW portfolio + daily stages | Sunday 2 AM ET |

### LLC Piercing — 3-Strategy Cascade (`enrichment/llc_piercer.py`)

Tries strategies in order, stops on first success:
1. **ACRIS PDF signer** — real human who signed the mortgage document (Claude Vision extracts name/title from PDF)
2. **HPD portfolio lookup** — queries HPD Registrations + Contacts for HeadOfficer/IndividualOwner at the same BBL (replaced the defunct Who Owns What API)
3. **Claude agentic reasoning** — AI synthesizes all signals + web search as last resort

*(Who Owns What API is dead — returns an HTML SPA shell for all endpoints. Graceful detection added in `ingest/whoownswhat.py`.)*

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

OpenCorporates fully removed — price prohibitive for commercial use. All source files, imports, and dead code paths have been deleted.

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
- `company_enrichment_runs` — caches company cascade results for 90 days

### Company Enrichment Cascade (`enrichment/company/`)

Replaces the Zoominfo-only corporate path with a FREE→BUDGET→STANDARD→PREMIUM waterfall keyed on LLC/corporation/management_company entities. Queue type: `'company_enrich'` (renamed from `'zoominfo'` in migration 016). No portfolio_size gate — the cost tier governs spend (FREE for 1-building entities, PREMIUM for 50+).

Cascade steps: (1) HPD cross-portfolio aggregation, (2) HPD per-BBL contacts, (3) Claude company web search, (4) Google Places [BUDGET], (5) Hunter domain email [BUDGET], (6) Apollo org→people [STANDARD], (7) Zoominfo [PREMIUM, requires `ZOOMINFO_CLIENT_ID`], (8) Proxycurl LinkedIn [PREMIUM].

New sources in `enrichment/contact/sources/`: `hpd_portfolio.py`, `apollo_org.py`, `claude_company_research.py`.

**Apollo two-step flow**: Apollo deprecated `/v1/mixed_people/search` for API callers in 2026 (returns 422). The replacement `/v1/mixed_people/api_search` returns discovery rows only — masked surnames (`"Mo***s"`), no email/phone, only `has_email` / `has_direct_phone` boolean flags. To get actual contact data, `apollo_org.py:apollo_person_enrich_by_id` calls `/v1/people/match` with `{"id": <apollo_id>}` per person (~$1/credit). The cascade enriches the top 3 candidates with `has_email=true` per company. Phone reveal requires an async webhook callback (not implemented) — phones come from Hunter / BatchData / HPD instead.

Run `scripts/eval_apollo_org.py` (free, discovery-only) to validate Apollo org-match quality; add `--enrich=N` to spend ~$N per company verifying real email retrieval. `enrichment/zoominfo.py` retains `enrich_entity_with_zoominfo` and `search_contacts_at_company` — `run_batch` was deleted (moved to the cascade orchestrator).

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
- `parse_bbl(bbl)` — decomposes a 10-digit BBL into `(boro, block, lot)` with leading zeros stripped (Socrata API expects unpadded values). Use this everywhere instead of inline `bbl[1:6]` / `str(int(...))` patterns.
- `_determine_enrichment_types(name, entity_type, extra)` — decides which work items to queue at `upsert_entity` time. Returns a subset of `{'llc_pierce', 'company_enrich', 'multi_source'}`.
- `queue_for_enrichment(entity_id, enrichment_type)` / `get_enrichment_batch(enrichment_type, limit)` / `mark_enrichment_done(entity_id, type)` / `mark_enrichment_failed(entity_id, type, error)` — the per-type queue API. Each enrichment stage drains its own type.
- `load_dotenv(override=True)` in `config.py` — required so `.env` values override any shell environment variables (e.g. an empty `ANTHROPIC_API_KEY` in the shell)

### Enrichment Queue (`enrichment_queue` table)

One row per `(entity_id, enrichment_type)`. Each batch processor (`llc_piercer`, `company_enrich`, `multi_source`) pulls its own type, flips `entities.enrichment_status` to `'in_progress'` on first pickup, then calls `mark_enrichment_done` / `mark_enrichment_failed` per work item. Entity status is only promoted to `'done'` when no queue rows remain. After 3 failed attempts on a queue row, the row is dropped and (if no other rows remain) the entity flips to `'failed'` with the error appended to `notes`.

### Retry Policy (`database/retry.py`)

External HTTP calls are wrapped with `@retry_external(max_attempts=N)` from `database/retry.py`. Retries trigger only on transient errors (timeouts, network errors, HTTP 408/425/429/500-504). NYC Socrata endpoints retry 5 times; all other external APIs retry 3 times. **Anthropic and OpenAI SDK calls are NOT wrapped** — those SDKs have built-in retries. The decorated function must call `response.raise_for_status()` so HTTPStatusError surfaces to the decorator.

### Key Database Objects (`database/schema.sql`)

Core tables: `properties` (BBL-keyed, includes `house_number` + `street_name` as structured fields), `entities` (unified LLC/corp/individual), `entity_relationships` (child→parent with confidence), `property_roles` (property↔entity with role+source), `contacts`, `seen_records`, `enrichment_queue`, `ingestion_log`, `contact_enrichment_runs`, `company_enrichment_runs`

Key views: `broker_pitch_list` (property + owner co + mgmt co + signer in one row). Earlier views (`landlord_profiles`, `unpierced_llcs`, `crm_export`) were dropped in migration 003 — they were unused.

### Building-size gate (`ingest/pluto.py`)

**`is_landlord_lot(units_res, bldg_class)` is the single definition of what qualifies**, used by both the ingest gate and the CRM sync so they cannot drift. The target is landlords; the thing being excluded is the owner-occupied home:

- **`A`/`B` class → out, always.** One- and two-family dwellings. `B` goes even though it has a tenant — it is the classic owner-upstairs house.
- **`S`/`K` class → in, on any residential unit at all**, and *not* subject to the unit threshold. Commercial space under the apartments means the owner runs a building rather than living in a house. A store with zero apartments is still out; this is a residential-landlord pipeline.
- Everything else → in at `unitsres >= PLUTO_MIN_RESIDENTIAL_UNITS` (default 3).

- **The gate fails open.** A BBL PLUTO does not know, or a failed PLUTO request, is *admitted*. Silently dropping real ownership data is worse than carrying a few small buildings. 4,341 of 135,388 properties are not in PLUTO.
- **`lookup()` is batched and cached**; `cached(bbl)` never issues a request, so it is safe inside a per-row loop.
- **PLUTO is the only source for these columns.** HPD Registrations (`tesw-yqqr`) has no `unitcount` and no `buildingclassid` column; reading them off a registration row returns `None` for every row, which is why `unit_count` and `building_class` were NULL across the whole table. That also silently degraded `skip_filter`'s `low_value_score` — its `unit_count >= 3` bonus could never fire and its `max_units <= 2` penalty always did.
- `properties.unit_count` stores **`unitsres` only**, not `unitstotal`.

Backfilled 2026-07-31: 131,047 of 135,388 properties populated. Distribution — 1,747 at 0 residential units, 6,097 at 1, 17,552 at 2, 105,651 at 3+.

### Paging Supabase reads

Always `.order("id")` (or another stable column) on a paged `.range()` read. Postgres does not guarantee row order across requests for an unordered select, so paging one silently **skips and duplicates** rows. This produced a real 35% under-read during the PLUTO backfill (87,845 of 135,388 rows seen) before it was caught.

### External API Tiers

- **Free/public**: NYC OpenData (HPD, ACRIS, PLUTO, DOB permits via Socrata)
- **Budget (~$0.40–$1/signer)**: BatchData V3 skip trace (~$0.40/match), Hunter.io, Google Places
- **Standard (~$1–$3/signer)**: Apollo.io (person match, key in `X-Api-Key` header), Whitepages (50-query trial, $220/mo — last-resort, efficacy TBD)
- **Premium (~$5–$20/signer)**: Proxycurl (LinkedIn), Zoominfo (JWT auth, `enrich_entity_with_zoominfo` / `search_contacts_at_company` in `enrichment/zoominfo.py` — called from the company cascade at PREMIUM tier and from prong1 respectively)
- **Not active**: Who Owns What (API dead — returns HTML), PropertyRadar (code kept, key not set)
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
- `COST_PER_ENTITY_COMPANY_ENRICH` (budget cap signal, default $5.00 — covers PREMIUM-tier worst case of Apollo enrich × 3 + Zoominfo + Hunter + Proxycurl × 3; actual cost tracked per-run in `company_enrichment_runs.cost_cents`)
- `ENVIRONMENT` (development/production)
- `ADMIN_PASSWORD` — HTTP Basic Auth password for `/admin`. Required to access admin dashboard; returns 500 if unset.

All enrichment API keys are optional — the system degrades gracefully when keys are missing.

### Scheduler (`scheduler.py`)

APScheduler-based persistent service. Runs as the production entry point in Docker/Railway. Coalesces missed runs (1-hour grace for daily, 2-hour for weekly).

### Airtable CRM sync (`pipeline/airtable_sync.py`)

Outbound counterpart to the `webhook.py` address feed — pushes researched entities into the LL Pipeline base as Management (primary) + Contacts + Addresses. Full design in `docs/AIRTABLE_SYNC.md`. Re-runnable: a second run over unchanged data writes zero records. Always `--dry-run` first after changing matching rules.

Three rules exist because breaking them corrupts the CRM, and each is easy to reintroduce:

- **A phone match is not a personal identity.** Everyone at a firm shares the switchboard, so matching a contact on phone alone merges distinct people into one record and loses the rest. Email is identity on its own; a phone match needs the names to agree too. A phone match is still fine for identifying the *Management*. For the same reason the email/phone indexes are **multi-valued** — a single-entry index hides everyone but the first person at a shared number, so the rest get re-created on every run.
- **Two source entities never merge on a weak signal.** Matching a record already in the base is the point of the sync, but when the names disagree the match rests on a shared mobile number or consumer-ISP domain, which fuses unrelated landlords. Every surviving merge is printed in the run report.
- **A weak cross-entity signal never outranks an entity's own Management record.** This is what makes the sync stable across runs: records created by run 1 stop looking "in-run" to run 2, so a guard written only against same-run merges silently lapses and every merge it prevented happens the second time.

After any change to the matching rules, run the sync then `--dry-run` again — it must report zero creates, zero updates and no new merges, or the rules don't converge.

Writes are **additive only**. Records the tool did not create never get a field overwritten, never have Pipeline touched, and never receive the ORT note / checkbox / Types link — they did not originate here.

`_attach_addresses()` applies the **read-side building-size gate** — the counterpart to the ingest gate, covering sub-threshold lots ingested before that gate existed. It calls the same `ingest.pluto.is_landlord_lot()`. An entity is dropped when *every* property it holds fails; failing addresses are withheld even from entities that survive on a larger building. It fails open twice over: a property with a NULL `unit_count` qualifies, and an entity with **no** `property_roles` at all is kept — management companies reached via a contact's employer own nothing directly and are the most valuable records here. As of 2026-07-31 this drops 56 of 532 units (52 individual, 4 unknown; all `A`/`B` class).

**`scripts/purge_small_buildings.py`** is the one-off retraction of what earlier runs already pushed — the sync itself never deletes, which is why this is a separate script and not a sync mode. Four rules: only ORT-flagged Managements; never a Management a surviving unit still resolves to; Contacts only when ORT-flagged *and* every Management link is doomed; Addresses only when every link is doomed (that table has no ORT flag, so sole-linkage is the only evidence of origin). Run 2026-07-31: 55 Managements, 72 Contacts, 55 Addresses deleted; 1 Management protected by the shared-record rule.

`_EXTRA_GOVT_RE` / `_EXTRA_BANK_RE` in that module patch two verified gaps in the shared filters. Both are narrowly scoped so enrichment behavior is unchanged, and both should be fixed upstream rather than widened here:

- `filters.is_govt_entity()` **is now fixed** — it builds the Commissioner patterns from `_COMMISSIONER = COMM(?:IS+(?:IO|O|OI)N(?:ER)?)?`, varying the S count, the vowels and the trailing `ER` independently, so `COMMISIONER OF FINANCE` (the single-S form ACRIS actually records) matches along with `COMMISSONER`, `COMMISSOINER` and `COMMISSION`. `_EXTRA_GOVT_RE` is therefore redundant on the Python path and is retained only because **the SQL `is_govt_name()` from migration 022 still has the gap**. Delete both together or neither.
- `skip_filter._BANK_RE` uses `\bSAVINGS\b` / `\bBANK\b`, which miss `GREEN POINT SAVINGSBANK`, `AMERICAN BROKERS CONDUIT` and `CARVER FEDL SAVS & LOAN ASSN` — all real "owners" on ACRIS foreclosure deeds.

A Zapier zap writes these same three tables from the `zapier_enriched_contacts` view (migrations 018–022), i.e. two implementations of one job. It uses a New Row trigger, so it does not re-create records this sync has already written; the overlap applies to newly enriched contacts going forward, and choosing one path is still open. `docs/CRM_DEDUPLICATION.md` compares them. **`docs/RECONCILIATION_PLAN.md` is the handoff document** — read it before merging, renumbering or applying any migration; it covers the unmerged contact-boundary line and the conflicting migration numbers.

## Deployment

Render.com is the primary deployment target. The Docker container runs `uvicorn webhook:app --host 0.0.0.0 --port ${PORT:-8000}` as the default command (see `Dockerfile`). The FastAPI app starts the APScheduler in its lifespan handler — `webhook.py` and `scheduler.py` run inside the same process. Auto-deploys on push to `main`.

**Dependency constraint**: `supabase==2.29.0` requires `httpx>=0.26,<0.29`. Keep `httpx` pinned to `0.28.x`. `anthropic` and `openai` both accept any httpx>=0.23 so they are not the constraint. Upgraded from supabase==2.4.0/httpx==0.25.2 to support the new `sb_secret_` Supabase key format (old JWT service role keys are deprecated).

**Anthropic SDK**: Upgraded from `anthropic==0.28.0` to `==0.97.0` in April 2026. This is a large jump — if pipeline runs fail with unexpected errors in AI calls, check for breaking changes in streaming response shapes, tool use syntax, or message construction between these versions. The codebase was written targeting Claude 4 models so the model names (`claude-opus-4-6`, `claude-sonnet-4-5`) are correct, but response-handling code may need updates if the SDK's object shapes changed.

## Troubleshooting

### Stuck pipeline trigger after worker restart

**Symptom**: The "Run Daily" / "Run Weekly" button in `/admin` stays disabled and shows the pipeline as `running` for much longer than a typical run (compare against recent `ingestion_log` durations — normally 10–50 min). Worker logs show the worker is alive (`scheduler.health` ticks) but no pipeline activity.

**Cause**: The worker received SIGTERM mid-pipeline (Render deploy, dyno cycle, OOM). The `pipeline_triggers` row and any in-flight `ingestion_log` rows are stuck at `status='running'` because the killed worker never closed them. The existing orphan-cleanup in `claim_next_pipeline_trigger` only fires for triggers >12h old, so anything more recent has to be cleared manually.

**Diagnose**:
```sql
SELECT id, pipeline, status, requested_at, started_at, finished_at
FROM pipeline_triggers
WHERE status = 'running'
ORDER BY requested_at DESC;

SELECT id, source, status, run_started_at
FROM ingestion_log
WHERE status = 'running'
ORDER BY run_started_at DESC;
```

**Fix** (manual unblock):
```sql
UPDATE pipeline_triggers
SET status = 'failed', finished_at = NOW(),
    error_message = 'orphaned by worker restart'
WHERE id = <stuck_trigger_id>;

UPDATE ingestion_log
SET status = 'failed', run_finished_at = NOW(),
    error_message = 'orphaned by worker restart'
WHERE id IN ('<stuck_uuid>', ...);
```

After running these, the button unlocks and a fresh "Run Daily" can be triggered.

**Better long-term fix (not yet implemented)**: on worker boot, mark any `pipeline_triggers` with `status='running' AND started_at < worker_boot_time` as failed. Robust to long-running pipelines — a heartbeat-staleness approach doesn't work because legitimate full-load / weekly runs can take hours. The boot-time signal is correct by construction: a "running" trigger started before the current worker booted must belong to a dead worker.
