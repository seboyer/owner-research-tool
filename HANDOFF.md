# Owner Research Tool — Handoff Document

Living doc for the next-phase architecture work. Updated as we debug and
stabilize. The architectural session picks up from here once enrichment
is running reliably.

**Last updated:** 2026-05-20 (Sam debugging session, mid-evening ET)

---

## TL;DR

The enrichment pipeline now works end-to-end and is correctly gated by
zipcode/borough allowlist + per-run cost cap. But the **economics don't
scale**: NYC has ~80k+ enrichable entities, hit rates across sources are
~10%, and per-entity cost is ~$1–2 estimated. Even one zipcode costs
real money; one borough is hundreds of dollars; full NYC is tens of
thousands.

The next-session question is **not "how do we make the pipeline work"**
(it does now). It's **"which subset of entities is worth $X to enrich,
and how do we prioritize so the budget hits them first?"**

---

## Current Stability State

| Component | Status |
|-----------|--------|
| Web service (Airtable webhook + admin UI) | ✅ Healthy |
| Worker service (scheduler + bulk enrichment) | ✅ Healthy (with caveats) |
| Zip/borough allowlist gating | ✅ Working |
| Cost cap (`DAILY_ENRICHMENT_COST_CAP_USD`) | ✅ Verified working 2026-05-20 ~22:45 ET. Log line `pipeline.cost_tracker_reset cap_usd=30.0` confirmed worker reads from env group correctly. |
| Heartbeat / WORKER OFFLINE badge | ⚠️ Known limitation: during a long pipeline run, the heartbeat doesn't refresh because `_job_poll_triggers` is busy with the pipeline. Badge goes gray. Not a real outage. Fix is to move heartbeat to a separate job. |
| Orphan-trigger cleanup | ✅ After PR #26 deploys — claim loop closes triggers >12h old |
| BatchData source | ✅ Wired into bulk enrichment (PR #24). Working — $0.07 of real spend in last run confirmed by their dashboard. |
| Apollo source | ⚠️ Wired (PR #24). Old key returned 401. New key in env group — **untested** post-fix. Curl-verify before trusting. |
| Whitepages source | ❌ Trial expired, 403 Forbidden on all calls. Recommendation: remove `WHITEPAGES_API_KEY` from env group so the code's early-return kicks in. URL migration to v2 endpoint (PR #25) is in place but moot until a working key exists. |
| OpenAI AI web search | ⚠️ Broken pre-PR #26 due to `proxies` httpx incompat. Fixed in PR #26 (bumps openai 1.30 → 1.55). Verify post-deploy. |
| Anthropic fallback for AI search | Never fires today — gated behind `OPENAI_API_KEY` being unset, but it IS set. Won't matter once OpenAI works. |

## What Got Built/Fixed This Session

In rough chronological order. All PRs except #20 (orphaned) are merged
or in flight.

| PR | What | Notes |
|----|------|-------|
| #1 | Initial zipcode allowlist gating | Per-zip toggle, defaults enabled |
| #2 | Default new zips to DISABLED | Safer default; required explicit opt-in |
| #3 | `WEEKLY_PIPELINE_DAY` env var | Configurable weekly schedule |
| #4 | Manual Run Daily/Weekly buttons | Initially in-process, later moved to worker |
| #5 | HPD `lifecyclestage` filter fix | Wrong column name → 400 from Socrata |
| #6 | Stage timeouts + orphan-row cleanup in `ingestion_log` | `start_ingestion_log` closes prior `running` rows |
| #7 | **Worker split** — scheduler moved to its own Render service | Critical fix: web's event loop was getting blocked by sync Supabase calls, tripping the /health timeout |
| #8 | Manual triggers via DB-backed queue (`pipeline_triggers`) | Web inserts → worker polls every 30s |
| #9 | (skipped) |  |
| #10 | HPD zip column was `zipcode` instead of `zip` | Every HPD row had NULL zip, defeating the allowlist gate |
| #11 | Resilient ingest loops — inner try/except for transport errors | Caught `RemoteProtocolError` (HTTP/2 GOAWAY) inside per-row loops |
| #12 | Broaden transient catch to `httpx.TransportError` | PR #11 missed `RemoteProtocolError` because of httpx inheritance |
| #13 | Borough fallback for NULL-zip properties + alphabetical zip sort |  |
| #14 | `@supabase_retry` decorator on all hot-path helpers | Supabase API gateway closes connections after ~20k streams; retries pull a fresh connection from the pool |
| #15 | Roll zip-gated entities into `records_skipped` |  |
| #16 | `scheduler_status` heartbeat table for admin badge | Replaces env-var-reading badge with DB-backed worker state |
| #17 | Paginate `/admin/api/zipcodes` | PostgREST 1000-row cap was silently truncating |
| #18 | Per-borough property counts in admin |  |
| #19 | View-level allowlist filter (`allowed_enrichment_queue`) + drain loop + `enrich_via_ai_web_search` "lying counter" fix | Key fix: queue jamming. Skipped entities were never leaving the queue, so every batch re-pulled them. View filters at SQL layer. |
| #20 | (orphaned — merged into wrong base branch) | Cost-cap PR; superseded by #21 |
| #21 | **Per-run cost cap** (`DAILY_ENRICHMENT_COST_CAP_USD`) + dashboard est. cost column | The cap mechanism we're hoping is still active |
| #22 | Clickable error cells |  |
| #23 | Counter accuracy: `records_skipped` split into `skipped` + `no_match`, lawyer filter (`is_lawyer_name`), AI-search lying-counter fix, defensive `api_status` | Lots of small accuracy bugs |
| #24 | **Apollo + BatchData wired into bulk enrichment** | They existed in the codebase but only the single-address (Airtable webhook) flow used them. Bulk enrichment ran without them for $100+ of real spend. |
| #25 | Whitepages URL migration (`proapi.whitepages.com` dead → `api.whitepages.com/v2/person`) | Now moot since trial expired |
| #26 | Orphan-trigger cleanup in `claim_next_pipeline_trigger` + bump `openai 1.30 → 1.55` | **Currently open / awaiting merge** |

## Known Open Issues

1. **`DAILY_ENRICHMENT_COST_CAP_USD` may have been lost** during env-group cleanup. The last multi_source run cost $426 estimated (real ≈ $0.07) without hitting the cap. Verify on worker's Environment tab — should be a positive number (e.g. 50 or 100).
2. **Apollo new key untested** — needs `curl -X POST https://api.apollo.io/v1/people/match -H "X-Api-Key: $KEY" -H "Content-Type: application/json" -d '{"name":"John Smith"}'` to confirm 200.
3. **Whitepages trial expired** — remove `WHITEPAGES_API_KEY` from env group; the code's `if not api_key: return False` handles it cleanly.
4. **Pre-fix `Est. cost` numbers are inflated** — sources were silently erroring so almost no real API spend happened. Cost estimates assumed every source was billing. Don't trust historical dashboard cost numbers from before today's PR #23/#24/#26.
5. **Heartbeat-during-long-run** — the badge shows WORKER OFFLINE during a long pipeline run because `_job_poll_triggers` is busy executing the pipeline (no time slot for the 30s heartbeat tick). Worker is alive; UI just doesn't know. Fix: separate heartbeat job, ~15 min change.
6. **`acris_pdf_pierce` not in cost cap** — that stage doesn't write `ingestion_log` rows so the cap mechanism can't track its spend. Bounded naturally by batch_size + ACRIS PDF availability but should be integrated properly.
7. **`mark_enrichment_done` fires on silent-source-failure** — when all sources error out (OpenAI/Apollo/Whitepages all simultaneously broken, as in pre-fix runs), `enrich_entity` returns False but `mark_enrichment_done` still deletes the queue row. The entity is permanently considered "enriched" despite zero real API work. **~450 entities in zip 11216 were lost to this in pre-fix runs** and need manual re-queue if we want them retried. SQL in the "Re-queue" section below. The deeper fix (architecture-session-worthy) is to distinguish "sources tried cleanly, no data" from "sources errored, no data" and only mark done in the first case.

## Rollback Plan — First Task for the New Session

Sam's decision (2026-05-21): treat all of this session's enrichment runs as testing. Reset the enrichment queue to a "0 attempted enrichments" state without touching contacts (existing real data) or schema (the new migrations stay).

**Execute these in order. Verify with the `-- Count` query between each step. STOP if any count looks wildly off.**

### Step 1 — Re-queue all multi_source-eligible entities not currently in queue

```sql
-- Count first
SELECT COUNT(*) AS would_insert
FROM entities e
WHERE (e.entity_type IN ('individual', 'unknown') OR e.entity_type IS NULL)
  AND e.name !~* '\m(esq\.?|attorney|atty\.?|law\s+(office|offices|firm|group))\M'
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq
    WHERE eq.entity_id = e.id AND eq.enrichment_type = 'multi_source'
  );

-- Then insert
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority, attempts)
SELECT DISTINCT id, 'multi_source', 5, 0
FROM entities e
WHERE (e.entity_type IN ('individual', 'unknown') OR e.entity_type IS NULL)
  AND e.name !~* '\m(esq\.?|attorney|atty\.?|law\s+(office|offices|firm|group))\M'
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq
    WHERE eq.entity_id = e.id AND eq.enrichment_type = 'multi_source'
  );
```

### Step 2 — Re-queue building-LLC-looking entities for llc_pierce

```sql
-- Count first
SELECT COUNT(*) AS would_insert
FROM entities e
WHERE e.entity_type IN ('llc', 'corporation')
  AND (
    e.name ~* '\m\d+\s+(west\s+|east\s+|north\s+|south\s+|w\.\s*|e\.\s*|n\.\s*|s\.\s*)?\w+\s+(st|ave|blvd|rd|dr|ln|ct|pl|way|street|avenue|boulevard|road|drive|lane|court|place)\M'
    OR e.name ~* '\m(one|two|three|four|five|six|seven|eight|nine|ten)\s+\w+\s+(st|ave|blvd|rd|street|avenue)\M'
    OR (e.name ~* '\m(owner|property|holdings|realty)\s+llc\M' AND e.name ~ '\d')
  )
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq
    WHERE eq.entity_id = e.id AND eq.enrichment_type = 'llc_pierce'
  );

-- Then insert
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority, attempts)
SELECT DISTINCT id, 'llc_pierce', 5, 0
FROM entities e
WHERE e.entity_type IN ('llc', 'corporation')
  AND (
    e.name ~* '\m\d+\s+(west\s+|east\s+|north\s+|south\s+|w\.\s*|e\.\s*|n\.\s*|s\.\s*)?\w+\s+(st|ave|blvd|rd|dr|ln|ct|pl|way|street|avenue|boulevard|road|drive|lane|court|place)\M'
    OR e.name ~* '\m(one|two|three|four|five|six|seven|eight|nine|ten)\s+\w+\s+(st|ave|blvd|rd|street|avenue)\M'
    OR (e.name ~* '\m(owner|property|holdings|realty)\s+llc\M' AND e.name ~ '\d')
  )
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq
    WHERE eq.entity_id = e.id AND eq.enrichment_type = 'llc_pierce'
  );
```

### Step 3 — Re-queue large-portfolio corporate entities for zoominfo

```sql
-- Count first
WITH eligible AS (
  SELECT pr.entity_id
  FROM property_roles pr
  JOIN entities e ON e.id = pr.entity_id
  WHERE pr.is_current = TRUE
    AND e.entity_type IN ('llc', 'corporation', 'management_company')
  GROUP BY pr.entity_id
  HAVING COUNT(DISTINCT pr.property_id) >= 3
)
SELECT COUNT(*) AS would_insert
FROM eligible
WHERE NOT EXISTS (
  SELECT 1 FROM enrichment_queue eq
  WHERE eq.entity_id = eligible.entity_id AND eq.enrichment_type = 'zoominfo'
);

-- Then insert
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority, attempts)
SELECT entity_id, 'zoominfo', 5, 0
FROM (
  SELECT pr.entity_id
  FROM property_roles pr
  JOIN entities e ON e.id = pr.entity_id
  WHERE pr.is_current = TRUE
    AND e.entity_type IN ('llc', 'corporation', 'management_company')
  GROUP BY pr.entity_id
  HAVING COUNT(DISTINCT pr.property_id) >= 3
) eligible
WHERE NOT EXISTS (
  SELECT 1 FROM enrichment_queue eq
  WHERE eq.entity_id = eligible.entity_id AND eq.enrichment_type = 'zoominfo'
);
```

### Step 4 — Zero out attempts on all queue rows

```sql
UPDATE enrichment_queue SET attempts = 0 WHERE attempts > 0;
```

### Step 5 — Reset entities.enrichment_status to 'pending'

```sql
UPDATE entities
SET enrichment_status = 'pending'
WHERE enrichment_status IN ('in_progress', 'done', 'failed');
```

### Step 6 — Clean stale pipeline_triggers (UI's stuck "Running…" button)

```sql
UPDATE pipeline_triggers
SET status = 'failed', finished_at = NOW(), error_message = 'reset before re-enrichment'
WHERE status IN ('pending', 'running');
```

### Step 7 — Delete contacts created this session

Removes today's noise (mix of real partial-info contacts from BatchData/AI and a few junk rows). Real BatchData hits will be re-discovered cheaply on the next run; the goal is a clean slate to validate post-fix enrichment quality. Pre-session contacts (Airtable single-address research, prior testing) are preserved.

```sql
-- Count first
SELECT COUNT(*) AS would_delete, MIN(created_at), MAX(created_at)
FROM contacts
WHERE created_at > '2026-05-20 04:00:00+00';

-- Then delete
DELETE FROM contacts
WHERE created_at > '2026-05-20 04:00:00+00';
```

### Verify final state

```sql
SELECT enrichment_type, COUNT(*) AS rows, MAX(attempts) AS max_attempts
FROM enrichment_queue GROUP BY enrichment_type ORDER BY enrichment_type;

SELECT enrichment_status, COUNT(*) FROM entities GROUP BY enrichment_status;
```

Expected: all `max_attempts=0`, all entities `enrichment_status='pending'`, multi_source row count in the low thousands.

### Caveats

This plan replicates `_determine_enrichment_types()` rules in SQL approximately. The Python heuristics for `is_building_llc` are more nuanced than the regex above; expect ~5-10% drift in llc_pierce counts vs. what fresh-ingest would produce. Good enough for this purpose.

After Step 7's contact delete, the next enrichment run will produce real new contacts (no `already_has_contacts` short-circuit since contacts are gone). Cost will be slightly higher on this first post-reset run — figure $30–80 for a borough's worth — but you'll have clean honest numbers.

## Re-queue SQL for lost-to-silent-failure entities (HISTORICAL — superseded by rollback plan above)

```sql
-- Re-queue every entity in an enabled zip that has no contacts yet.
-- Picks up the ~450 zip-11216 entities that were marked done despite
-- zero successful enrichment in pre-fix runs.
INSERT INTO enrichment_queue (entity_id, enrichment_type, priority)
SELECT DISTINCT e.id, 'multi_source', 5
FROM entities e
JOIN property_roles pr ON pr.entity_id = e.id AND pr.is_current
JOIN properties p ON p.id = pr.property_id
JOIN zipcode_allowlist za ON za.zip_code = p.zip_code AND za.enabled = TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM contacts c WHERE c.entity_id = e.id
)
  AND e.name !~* '\m(esq\.?|attorney|atty\.?|law\s+(office|offices|firm|group))\M'
  AND NOT EXISTS (
    SELECT 1 FROM enrichment_queue eq2
    WHERE eq2.entity_id = e.id AND eq2.enrichment_type = 'multi_source'
  );
```

**Set `DAILY_ENRICHMENT_COST_CAP_USD` first** before triggering Run Daily after this insert — gives you a safety net against another unbounded spend if a key turns out broken again.

## Critical Architectural Question for Next Session

The pipeline now works correctly but is **economically unscalable** at the current strategy of "queue every entity, fan out to all sources per entity, drain everything in enabled zips."

### Cost math from real test runs

- Zip 11216 alone: $50 cap hit after 27 entities, **3 useful contacts** (1 with email, 2 phone-only). **$16/useful contact** at the cap.
- Extrapolation: one borough = thousands of dollars. NYC = tens of thousands.
- Steady-state ongoing cost (after initial drain): ~$3–30/day per enabled zip from new ACRIS-deed entities.

### What's making it expensive

1. **Most queued entities aren't worth enriching.** Lots of one-off home buyers, shell LLCs, deceased people, lawyers (now filtered). The system pays the same per-entity cost regardless of value.
2. **All sources fan out per entity** with no short-circuit on success. `found_any |= ...` doesn't break the chain; we pay for sources we didn't need.
3. **No prioritization signal.** Queue is `(priority, created_at)` and priority is always 5. Portfolio size, building count, deed value — none of it influences order.
4. **Sources have wildly different quality and cost.** BatchData ($0.40, best yield), Apollo ($0.05, cheap), Whitepages ($1.50 when working). Some are still untested at scale (Apollo new key).

### Directions to explore

1. **Filter harder at queue-insertion time.** Beyond lawyers, what entities should never be queued? (Single-family owners? Sub-portfolio-size? Out-of-state? Old records?)
2. **Prioritize the queue.** Set `priority` based on portfolio_size, building count, deed value, borough activity. Process valuable entities first; let everything else age.
3. **Short-circuit source chain.** Stop after the first source finds a contact. Cheaper sources first (BatchData $0.40 → Apollo $0.05 — wait, Apollo's cheaper; reorder).
4. **Tiering.** High-value entities (portfolio_size > N) get all sources; low-value gets BatchData only.
5. **Targeted UX instead of drain.** Shift from "enable a zip and drain everything" to "give me the top 100 landlords in zone X by some scoring function."
6. **Fix the "no property_roles → allow" bypass leak.** See "Bypass leak" below.

The right answer is probably a mix. The point of the next session is to decide which direction(s) and design the implementation.

### Bypass leak (discovered 2026-05-21)

The `allowed_enrichment_queue` view (migration 010) has three OR'd conditions; the first is a bypass for entities with no current `property_roles`:

```sql
NOT EXISTS (SELECT 1 FROM property_roles pr WHERE pr.entity_id = q.entity_id AND pr.is_current = TRUE)
```

This was designed for **LLCs awaiting pierce** — we can't filter them by location until we discover what property they own, so we have to let them through the gate. But it applies to **every entity type**, including `individual` entities. Diagnostic confirmed: **604 individuals exist with no current property_roles** in the DB right now, and the failed $426 run pulled most of them via this bypass — not from zip 11216 as initially assumed.

These 604 are almost certainly pierced-LLC-owners — humans revealed by `llc_piercer` strategies. The piercer creates an `entity_relationships` row (owner → LLC) but **no property_role** linking the human directly to the building. So they exist in our DB orphaned from any property.

The bypass means:
- Pierced humans bypass the zip/borough gate entirely
- A $426 unbounded run can fire even with only one tiny zip enabled
- Future cost cap math underestimates risk because the gate is leaky

Tighter design options:
- Bypass only for `llc_pierce` queue type (so it only applies to LLCs)
- OR backfill property_roles for pierced humans from their LLC's properties
- OR add an explicit "needs_property" flag and only bypass for entities flagged as such

Either way, this is worth a real fix in the architecture pass.

## Key Files / Code Map

- `pipeline/orchestrator.py` — pipeline orchestration, `CostTracker`, per-stage timeouts
- `enrichment/multi_source.py` — main bulk contact enrichment (BatchData, Apollo, PropertyRadar, Whitepages, Proxycurl, AI search)
- `enrichment/llc_piercer.py` — LLC piercing (3 strategies)
- `enrichment/zoominfo.py` — corporate enrichment
- `enrichment/acris_pdf.py` — Claude vision on mortgage PDFs
- `admin/allowlist.py` — zip + borough gating + `is_entity_allowed_by_zip`
- `database/client.py` — all DB helpers, `_determine_enrichment_types` (queue-insertion logic), `is_lawyer_name`, all wrapped in `@supabase_retry`
- `database/migrations/*.sql` — 12 migrations
- `scheduler.py` — APScheduler config (cron + 30s poll loop)
- `render.yaml` — two-service split (web + worker)
- `CLAUDE.md` — project guide; worth reading first in any new session

## Cost Caveats

Anything from this session's dashboard `Est. cost` column dated **before 2026-05-20 evening** is an inflated worst-case figure. Most sources were silently erroring (OpenAI proxies bug, Apollo 401, Whitepages DNS/403). Real spend was much lower. **Use API provider dashboards (BatchData, Apollo, Anthropic, OpenAI) for ground-truth cost data when calibrating.**

After PR #26 deploys + Apollo new key verified working, the dashboard estimates will start converging with reality.

---

## Update Log

Append as we go:

- **2026-05-20 ~21:00 ET**: Initial handoff doc created. PR #26 open (orphan cleanup + openai bump). Whitepages trial confirmed expired; advice is to remove env var. Apollo new key in env group, untested.
- **2026-05-20 ~21:30 ET**: Discovered the `mark_enrichment_done`-on-silent-failure bug. ~450 zip-11216 entities were burned through pre-fix without real enrichment and are no longer in the queue. Added Re-queue SQL section above. Architecture session needs to address per-source-error tracking so this can't repeat.
- **2026-05-20 ~21:35 ET**: PR #26 merged (orphan-trigger cleanup + openai 1.55 bump). Sam re-queued ~450 lost entities via SQL. About to validate cost cap with a $50 setting before re-running.
- **2026-05-20 ~22:45 ET**: Cost cap validated. Worker logs `pipeline.cost_tracker_reset cap_usd=30.0`, confirming env group propagation + float parsing. Run #1 of post-fix multi_source enrichment in progress.
- **2026-05-20 ~23:00 ET**: Re-queue SQL only inserted 9 rows because most of zip 11216 has at least one contact (the lying counter wrote some, BatchData wrote some). The `NOT EXISTS contacts` filter excludes them. If we want to retry entities with only partial data, the filter needs to broaden.
- **2026-05-20 ~23:15 ET**: openai 1.55 (PR #26) was insufficient — proxies error still firing. PR #27 bumps to 1.60.0 + adds version logging at worker boot so we can verify what's actually installed.
- **2026-05-21 ~01:00 ET**: Diagnosed the actual source of the failed $426 run's 426 entities. **Not from zip 11216** as initially assumed — only 9 11216 entities are multi_source-eligible. Instead they came from the view's `NOT EXISTS property_roles` bypass, which catches 604 individual entities (almost certainly pierced-LLC-owners) regardless of zip/borough state. Updated handoff with this as architecture-session item. The bypass is structurally a gating leak.
- **2026-05-21 ~01:30 ET**: Sam decided to treat this whole session's enrichment runs as testing and reset. Added explicit Rollback Plan section to HANDOFF as the FIRST task for the new session. Targeted SQL (preserves schema), not a backup restore.
- **2026-05-21 ~01:45 ET**: Expanded rollback plan to also delete contacts created in this session (cutoff `2026-05-20 04:00:00+00`). Reasoning: today's contacts are mix of real partial-info + some noise; cleaner to start fresh and let post-fix enrichment regenerate the real ones cheaply.
