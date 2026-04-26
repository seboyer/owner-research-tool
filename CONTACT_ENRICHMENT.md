# Contact Enrichment — 3-Prong Design

## Goal
Give a leasing broker the names, phone numbers, emails, and **both the owner operating company AND the management company** for every NYC building we've identified — so they can pitch to win the rental listing.

## Seed
The pipeline upstream has already:
1. Ingested ACRIS mortgages + parties.
2. Downloaded the mortgage PDF and run Claude Vision on the signature block.
3. Created a `contacts` row with `network_role='signer'` tied to a building-holding LLC.

That signer is the seed. From the seed we run three prongs in parallel.

---

## Prong 1 — Signer

**Question:** Who is this person publicly, and what companies does the broker call?

**Produces:**
- Signer's email, phone, LinkedIn (`ContactHit` with `network_role='signer'`)
- **Owner operating company** (`CompanyHit` with `role_category='owner_operating'`) — the public-facing real-estate brand (e.g. "Doe Realty Group"), distinct from the single-purpose title LLC.
- **Management company** (`CompanyHit` with `role_category='management'`) — if distinct from the owner.

**Sources (waterfall):**
| Tier | Source | What it answers |
|------|--------|-----------------|
| Free | ACRIS party history | What address does ACRIS have on file for the signer? |
| Free | HPD registration | Is there a managing agent / head officer already recorded? |
| Free | OpenCorporates | Non-building LLCs where signer is an officer → owner operating candidates |
| Free | Claude `web_search` | Resolve ambiguity by searching The Real Deal / PincusCo / LinkedIn |
| Budget | **BatchData V3 Skip Trace** | **Direct phone + email for the property owner by address** |
| Budget | Google Places | Phone + website for candidate company |
| Budget | Hunter.io | Emails on company domain (incl. signer's work email) |
| Standard | Apollo.io | Direct person match: email + mobile dial |
| Premium | Proxycurl / Zoominfo | LinkedIn + corporate contacts |

## Prong 2 — Network

**Question:** Who else can the broker call if the primary signer ghosts them?

**Produces three kinds of `ContactHit`:**
- `network_role='coworker'` — people at the signer's operating company (from Hunter, Apollo, LinkedIn)
- `network_role='co_owner'` — people listed on other ACRIS deals alongside the signer
- `network_role='co_officer'` — people listed on other NY LLCs where the signer is an officer

**Sources:**
| Tier | Source |
|------|--------|
| Free | ACRIS party history (join on document_id) |
| Free | OpenCorporates (officer graph) |
| Budget | Hunter.io domain search |
| Standard | Apollo.io org-people |

> Note: BatchData is not called in Prong 2 — it returns property owners, not co-workers or co-officers.

## Prong 3 — Building

**Question:** Who else is on record as an owner or manager of THIS building?

**Filter:** only returns contacts with `role_category ∈ {owner, management}`. Tenants, lessees, and unrelated parties are excluded.

**Produces `ContactHit` with network_role ∈ { `head_officer`, `registered_agent`, `co_owner`, `co_officer`, `building_contact` }.**

**Sources:**
| Tier | Source |
|------|--------|
| Free | HPD registration contacts |
| Free | DOB permit filings (owner + applicant) |
| Free | ACRIS historical parties on the same BBL |
| Budget | **BatchData V3 Skip Trace** — enriches name-only contacts with phones + emails, surfaces additional owners not in HPD/DOB |

---

## Cost tiers

Driven by the title LLC's `portfolio_size` (at upsert time it's usually 1; re-enrichment uses the current value after piercing completes).

| Portfolio | Tier | Budget/signer |
|-----------|------|---------------|
| 1         | free | $0.00 |
| 2–9       | budget | ~$0.50 |
| 10–49     | standard | ~$3.00 |
| 50+       | premium | ~$20.00 |

Implemented in `enrichment/contact/cost_tier.py`. The orchestrator only calls a source if the current tier includes it.

---

## Data written to Supabase

**New/updated rows on `entities`:**
- Upsert operating company & management company with `role_category`, `website`, `domain`, `hq_phone`.
- Add `entity_relationships` edges:
  - `building_llc --operates_as--> owner_operating_company`
  - `building_llc --managed_by--> management_company`

**New rows on `contacts`:** every ContactHit, carrying:
- `prong` (1, 2, or 3)
- `network_role`, `role_category`
- `seed_signer_id`, `seed_property_id`, `seed_building_llc_id`
- `sources` JSONB with citation URLs
- `evidence` (human-readable)
- `cost_cents`

**Audit trail:** one `contact_enrichment_runs` row per (signer, prong) attempt — caches success so we don't re-run within `STALE_DAYS` (90).

**Broker-ready view:** `broker_pitch_list` joins property → title LLC → signer → owner co → mgmt co in one row with contact counts.

---

## Running it

```bash
# All pending signers (batch)
python main.py enrich-contacts

# Single signer
python main.py enrich-contacts --signer-id <contacts.id>

# Force re-run (ignore cache)
python main.py enrich-contacts --signer-id <contacts.id> --force
```

---

## Additional sources to consider (not yet wired)

| Source | Use case | Cost |
|--------|----------|------|
| **NYS DOS Corp Search** | Registered agent address → real office | Free |
| **PACER** | Federal court filings mentioning the signer | Free (basic) |
| **SEC EDGAR** | If owner is an SEC filer / tied to a REIT | Free |
| **PincusCo / The Real Deal** scraping | NYC deal history narratives | Free (rate-limited) |
| **Reonomy / PropertyShark** | Full owner contact + LTV data | ~$500/mo subscription |
| **ATTOM Data** | Bulk property owner phones | ~$500/mo |
| **Spokeo / TruePeopleSearch** | Individual residential phones | Low $ per query |

The `enrichment/contact/sources/paid_stubs.py` module has placeholder interfaces — drop an API key in `.env` and they light up automatically.

---

## BatchData integration

BatchData is wired in `enrichment/contact/sources/batchdata.py`.

**Endpoint used:** `POST https://api.batchdata.com/api/v3/property/skip-trace`

**What it returns per property:**
- Up to 3 matched persons, each with:
  - Ranked phone numbers with `type` (Mobile/Landline), `dnc` flag, `tcpa` flag, `reachable` flag
  - Ranked emails with `tested` flag
  - `litigator` boolean (TCPA litigator status)
  - `deceased` boolean

**How it fits in the workflow:**

| Prong | Role | Behaviour |
|-------|------|-----------|
| Prong 1 | Signer phone + email | Called at Budget tier if signer has no phone+email yet. Person 1 matched by name → `network_role='signer'`. |
| Prong 3 | Building contacts | Called at Budget tier after HPD/DOB. Enriches name-only contacts with phones/emails; additional persons added as `network_role='building_contact'`. |

**Compliance handling:**
- DNC phones are silently skipped (brokers cannot call them).
- TCPA status is encoded into `phone_type` (e.g. `"Mobile (TCPA)"`) so brokers see it in the UI.
- `litigator=true` persons are flagged in the `evidence` field.

**Cost:** ~$0.40 per matched property record (one charge per property, not per person). Add `BATCHDATA_API_KEY` to `.env` to activate.
