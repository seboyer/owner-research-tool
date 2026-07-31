# HANDOFF — Airtable CRM sync & repo reconciliation

**Status: paused, waiting on a git sync fix.** Written July 2026 to be
picked up by a fresh session once the repository situation is resolved.
Assume the reader has no prior context.

---

## 1. Read this first

Work was **deliberately stopped** here. The blocker is not technical debt
in the code — it is that this directory has no git repository, and the
owner has significant work-in-progress on a local machine that must not be
disturbed.

**Do not attempt to resolve the git situation by creating a repository
here.** See §6.

### What is already done

**PR #44 is open and complete:**
<https://github.com/seboyer/owner-research-tool/pull/44>
— *Add Airtable CRM sync (Management / Contacts / Addresses)*,
branch `airtable-crm-sync`, 1 commit, +2191/−1 across 8 files, cut from
`main` and verified against `main`'s code.

The Airtable base has already been reseeded using that code — 1850 records
written, verified convergent. **That work is finished; do not redo it.**

### The blocker

- This directory (`/home/sam/Dev/owner-research-tool`) is unison-synced
  from the owner's Mac. **Unison syncs files, not `.git`** — there is no
  git checkout anywhere on this machine.
- The owner has substantial WIP locally that has not been pushed.
- Until the local repo and GitHub `main` are unified, any further branching
  or merging risks clobbering that WIP.

**Nothing in §5 should start until the owner confirms git is unified.**

---

## 2. Step 0 for the next session — establish ground truth

Everything in this document was true when written. Verify before acting;
do not trust it blind.

```bash
# 1. Is PR #44 merged, closed, or still open?
#    (token: GITHUB_CLAUDE in /home/sam/Dev/1-Resources/master.env — works,
#     authenticates as seboyer with push+admin)
curl -s -H "Authorization: Bearer $TOKEN" \
  https://api.github.com/repos/seboyer/owner-research-tool/pulls/44 \
  | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["state"],d.get("merged"))'

# 2. Has the divergence been resolved? Compare main against this tree.
git clone https://github.com/seboyer/owner-research-tool.git /tmp/ort && \
  diff -rq --exclude=.git --exclude=venv --exclude=__pycache__ \
       --exclude=.ruff_cache --exclude=.claude --exclude='*.env' \
       /tmp/ort /home/sam/Dev/owner-research-tool

# 3. On the OWNER'S MAC, not here — is the WIP an unpushed branch?
#    git status ; git branch -a ; git log --oneline origin/main..HEAD
```

### Environment gotchas

- **`venv/` is a stale macOS virtualenv** — its symlinks point at
  `/opt/homebrew/...` and it does not run on this Linux box. `python3-venv`
  is not installed either. Install deps to a target dir instead:
  `pip install --target <dir> supabase==2.29.0 'httpx>=0.28,<0.29' structlog click tenacity python-dotenv anthropic openai`
  then run with `PYTHONPATH=<dir>`.
- **Secrets** live in `/home/sam/Dev/1-Resources/master.env` (outside this
  directory; Bash reaches it without `/add-dir`). `GITHUB_DIGITALOCEAN_DEPLOY_TOKEN`
  is **expired (401)** — use `GITHUB_CLAUDE`.
- `.env` here already has a working `AIRTABLE_API_KEY` (added this session
  from `render.env`). `.gitignore` covers `.env`, so it will not be
  committed.

---

## 3. Evidence already gathered — do not re-derive

### The live database is on `main`'s line

Probed directly against Supabase REST:

| Artifact | Line | In live DB? |
|---|---|---|
| `zapier_enriched_contacts` view | `main` 018 | **yes** |
| `company_enrichment_runs` table | `main` 016 | **yes** |
| `enrichment_queue` rows typed `company_enrich` | `main` 016 rename | **yes — 5521** |
| `enrichment_queue` rows typed `zoominfo` | pre-016 | **zero** |
| `contacts.status` column | this tree, 017 | no (HTTP 400) |
| `ranked_property_contacts` view | this tree, 018 | no (HTTP 404) |
| `contact_observations` / `contact_scores` | this tree, 017 | no (HTTP 404) |

**`main` is production.** The contact-boundary line in this working copy
(`016_enable_rls`, `017_contact_boundary`, `018_broker_surface_v2`, plus
`enrichment/contact/identity.py`, `scoring.py`, `store.py`, and the
`score-contacts` CLI command) was **never applied and never merged**.

`016_enable_rls` is the one item this probe cannot settle — RLS with no
policies is invisible to a service-key client. Check in Supabase directly.

### Conflicting migration numbers

| # | GitHub `main` | This working copy |
|---|---|---|
| 016 | `company_enrichment` | `enable_rls` |
| 017 | `company_enrich_indirect_view` | `contact_boundary` |
| 018 | `zapier_enriched_contacts_view` | `broker_surface_v2` |

`main` additionally has 019–022 (Zapier view refinements) that are absent
here. **Same-numbered files are not the same migrations.**

### Current Airtable state (LL Pipeline, `appstQVl7JeMfr7d0`)

| Table | Total | From the sync | Pre-existing, untouched |
|---|---|---|---|
| Management | 647 | 522 | 125 |
| Contacts | 843 | 723 | 120 |
| Addresses | 740 | 624 | 116 |

Post-run audit: no obfuscated emails, no orphaned links, **no contact
linked to more than one Management**. One duplicate contact
(`JOSEPH BRUNNER` / `Joseph Brunner`, from a since-fixed bug) was deleted;
`recmVieqFQE7hID44` is the survivor.

### The zap is not a duplication risk

Confirmed by the owner: *New Row* trigger on `zapier_enriched_contacts`,
runs from a daily cron when enabled. It has already passed every row the
reseed covered — those are the rows it originally pushed, which were later
polluted **by an unrelated tool** and cleared. The zap did not cause the
pollution. Overlap with the sync is forward-looking only.

### Two verified upstream filter gaps

Both tested against `main`'s code, both patched locally in
`pipeline/airtable_sync.py` so enrichment behavior is unchanged:

- `filters.is_govt_entity()` spells the misspellings
  `COMM(?:ISSIONER|ISSONER|ISSOINER)?` — every alternative **double-S** —
  so `is_govt_entity('COMMISIONER OF FINANCE')` returns `False`. That
  single-S form is what ACRIS actually records. **The SQL `is_govt_name()`
  in migration 022 has the identical gap, so the zap is affected too.**
- `skip_filter._BANK_RE` uses `\bSAVINGS\b` / `\bBANK\b`, missing
  `GREEN POINT SAVINGSBANK` (no word boundary in the run-together form),
  `AMERICAN BROKERS CONDUIT` and `CARVER FEDL SAVS & LOAN ASSN`.

---

## 4. Phase 1 — done, for reference

PR #44. Recorded here because *how* it was built matters for the phases
that follow.

**Branch cut from `main`, not from this working copy.** Four new files
(`pipeline/airtable_sync.py`, `docs/AIRTABLE_SYNC.md`,
`docs/CRM_DEDUPLICATION.md`, this file) landed clean. Four shared files —
`config.py`, `.env.example`, `main.py`, `CLAUDE.md` — had their edits
**hand-applied to `main`'s versions**.

That distinction is essential and applies to every future phase:

> This tree's `main.py` has `score-contacts` (undeployed contact-boundary
> work); `main` has `enrich-company` (production). Copying either file
> wholesale deletes the other side's feature.

Verified before push: `--dry-run` from the branch reported **0 creates, 0
updates, 0 writes** against the live base — identical to this tree, proving
convergence holds with `main`'s filters; the 12-case filter test passed;
`ruff` clean on the new file, with `config.py`/`main.py` errors confirmed
pre-existing by stashing and re-running on untouched `main`.

---

## 5. Phases 2–4 — blocked on the git fix

### Phase 2 — triage the contact-boundary line

Work involved: `016_enable_rls`, `017_contact_boundary`,
`018_broker_surface_v2`, `enrichment/contact/{identity,scoring,store}.py`,
edits to `orchestrator.py` / `models.py` / `filters.py`, and the
`score-contacts` command. Spec: `docs/SPEC_contact_boundary.md`.

It is the designed fix for exactly the data-quality problem the sync's
quality gate works around — a candidate→publish boundary that scores
contacts and publishes only what clears the bar. It is also stale and never
deployed.

Options: **(a)** land it; **(b)** shelve it deliberately, tagged and
documented; **(c)** cherry-pick `016_enable_rls` alone.

**Recommendation: (c) now, then decide (a) vs (b) separately.** RLS is a
security control and should not stay blocked behind an unrelated feature.

### Phase 3 — if the boundary line lands

Renumber past `main`'s 016–022:

| Current here | Becomes |
|---|---|
| `016_enable_rls.sql` | `023_enable_rls.sql` |
| `017_contact_boundary.sql` | `024_contact_boundary.sql` |
| `018_broker_surface_v2.sql` | `025_broker_surface_v2.sql` |

Then update the migration list in `CLAUDE.md` and the references in
`docs/SPEC_contact_boundary.md` §7 and the `018` header.

- **Take `main`'s `filters.py`**, not this tree's — `main`'s is strictly
  newer (government patterns *plus* `is_govt_email()`). Re-apply only
  genuinely boundary-specific changes on top.
- **Sequencing:** `018/025` must not be applied until
  `scripts/backfill_contact_boundary.py` has scored every existing contact
  row, or the broker surface empties out — nothing is `published` yet.
- `025` narrows `broker_pitch_list` to `status = 'published'`; anything
  downstream will see fewer rows the moment it applies.

### Phase 4 — de-duplicate concerns across the two paths

| Concern | On `main` | In the sync | Resolution |
|---|---|---|---|
| Government filter | `filters.py` **done in this tree** (`_COMMISSIONER`) + SQL `is_govt_name()` (022) **still open** | `_EXTRA_GOVT_RE` (redundant on the Python path, retained) | widen `is_govt_name()` to match, then delete the patch |
| Bank/lender filter | `skip_filter._BANK_RE` | `_EXTRA_BANK_RE` | fix at source, then delete the patch |
| Company name resolution | 4-tier COALESCE in the Zapier view (022) | `entities.name` | **port the cascade into the sync** |
| Contact eligibility | `WHERE email/phone NOT NULL` in the view | the sync's quality gate | converge on `status='published'` if Phase 3 lands |

The company-name cascade is the one worth porting deliberately: employer →
LLC chain → owned LLC → direct entity is better than what the sync does
today. It is the difference between a Management named `WOODS, REGINALD R`
and one named after the actual operating company.

---

## 5A. DONE — session of 2026-07-27/31

> **Status: executed 2026-07-31.** The §1 blocker is resolved — unison now
> syncs `.git`, so this directory is a real checkout. Both PRs below are
> **open on GitHub**:
>
> | PR | Branch | Base |
> |---|---|---|
> | [#45](https://github.com/seboyer/owner-research-tool/pull/45) | `size-gate-and-sync-fixes` | `airtable-crm-sync` |
> | [#46](https://github.com/seboyer/owner-research-tool/pull/46) | `ingest-size-gate` | `size-gate-and-sync-fixes` |
>
> Stacked 44 → 45 → 46; merge in that order. Built in a throwaway clone off
> `origin/airtable-crm-sync` so the synced `.git` and the unpushed
> `contact-boundary` WIP were never touched.
>
> **`docs/HANDOFF_2026-07-31_size_gate.md` is the handoff for the
> `contact-boundary` session** — read that one if you are working on the
> contact-boundary line. It covers the `ingest/hpd.py` conflict and two
> live-database changes.
>
> The rest of this section is the plan as written, kept for the reasoning.

### Ground truth, re-verified 2026-07-27

Ran §2 Step 0. Nothing has moved since this document was written:

| Check | Result |
|---|---|
| PR #44 | **open**, unmerged, branch `airtable-crm-sync` |
| Other open PRs | none |
| `main` HEAD | `6350706` (2026-05-24) — unchanged |

### The wholesale-copy trap, now measured

§4 warned that four shared files must be hand-applied. Diffing this tree
against `origin/airtable-crm-sync` shows **the list is longer than four**,
and two of the additions are ones a careless copy would silently regress:

| File | Verdict | What a wholesale copy would destroy |
|---|---|---|
| `pipeline/airtable_sync.py` | **take wholesale** | nothing — every difference is from this session |
| `ingest/pluto.py` | **new** | — |
| `scripts/purge_small_buildings.py` | **new** | — |
| `ingest/acris.py` | **take wholesale** | nothing — verified, every difference is from this session |
| `enrichment/contact/filters.py` | **hand-apply** | `is_govt_email()` **and 12 government patterns** (`SECY OF HOUSING`, `IRS`, `DOJ`, `FBI`, `INTERNAL REVENUE`, `SOCIAL SECURITY ADMIN`, …). `main`'s `(?:^|\s)HUD\b` is also better than this tree's `\bHUD\b`, which false-positives on HUDSON. Confirms Phase 3's "take `main`'s `filters.py`". |
| `ingest/hpd.py` | **hand-apply** | `upsert_contact()` — this tree uses the unmerged contact-boundary `record_contact`/`ContactHit` refactor |
| `admin/routes.py` | **hand-apply** | the `zoominfo` → `company_enrich` rename from `main` 016 |
| `config.py` | **hand-apply** | `COST_PER_ENTITY_COMPANY_ENRICH`; would resurrect the deleted `ZOOMINFO_MIN_PORTFOLIO_SIZE` |
| `main.py` | **hand-apply** | `enrich-company` (§4) |
| `CLAUDE.md` | **hand-apply** | §4 |

Only apply the named hunks to the hand-apply files. Everything else in
them belongs to `main`.

**No migrations were added or changed.** `database/migrations/` is
untouched at 17 files — `properties.unit_count` and `.building_class`
already existed in `schema.sql` and had simply never been populated. This
whole body of work is therefore independent of the 016/017/018 numbering
conflict and can land without waiting on Phases 2–3.

### PR A — onto the existing `airtable-crm-sync` branch (updates #44)

Self-contained and deployable on its own. `airtable_sync.py` imports
`is_landlord_lot` from `ingest/pluto.py`, so the PLUTO *module* ships here
even though ingest-time gating waits for PR B.

Files: `pipeline/airtable_sync.py`, `ingest/pluto.py` (new),
`scripts/purge_small_buildings.py` (new), `enrichment/contact/filters.py`†,
`config.py`†, `main.py`†, `admin/routes.py`†, `CLAUDE.md`†,
`docs/AIRTABLE_SYNC.md`, `docs/CRM_DEDUPLICATION.md`, this file.
(† hand-apply)

```markdown
## CRM sync correctness, personal-name handling, and the building-size gate

Builds on the sync introduced in this PR. Four independent defects, plus
the read side of a new size gate.

**1. Management names were being written surname-first.** 83% of units
(440/532) are `LAST, FIRST` — ACRIS deed parties, all `entity_type=
'individual'`, all holding an `owner` role. They are landlords who own
apartment buildings in their own name rather than through an LLC, so the
data is right; only the presentation was wrong. `format_person_name()`
reorders them, shape-gated so company names and ACRIS annotations
(`JAMES, DOROTHY M/ADMIN OF`) pass through untouched.

The rename needed a matching key or every renamed person would duplicate
against their own existing record, so `person_key()` (sorted tokens) and a
`mgmt_by_person_key` index came with it.

**2. `_acceptable()` could not find an entity's own Management** when the
stored spelling differed in word order, so the rule "a weak cross-entity
signal never outranks an entity's own record" silently lapsed. This was a
pre-existing bug the rename surfaced. **Merges dropped 17 → 7.** Ten
landlords had been fusing into unrelated people on a shared email —
`Edmee Fontaine` and `Willis Attico` both into `ANDERSON, BERRIS`.

**3. `company_domain()` treated consumer ISPs and third parties as company
domains.** Of 45 non-free domains on individual owners, 16 were legacy ISPs
(`onebox`, `citlink`, `qwest`, `uswest`, `angelfire`, `concentric.net`, …)
and 12 were brokerages or law firms — the listing agent or closing
attorney, never the landlord. Both classes fed `mgmt_by_domain`, so they
merged unrelated landlords who happened to share an ISP. Adds the ISPs to
`_FREE_EMAIL_DOMAINS` and a new `_THIRD_PARTY_DOMAIN_RE`.

Related and deliberately **not** done: deriving a company name from the
domain. 92% of these emails (384/419) have a local part sharing no name
token with the entity — BatchData skip-traces the *address* and returns
whoever it finds. A hard-gated rule yields 9 of 440.

**4. Paged reads without `ORDER BY` silently skip and duplicate rows.**
Postgres does not guarantee ordering across `.range()` requests. Found when
a backfill processed 87,845 of 135,388 rows — a 35% under-read. Fixed in
`airtable_sync.py` (the contact load, which would have started losing
people the moment contacts passed 1,000), `main.py` and `admin/routes.py`.
`scripts/backfill_*.py` already had it right.

**5. Read-side building-size gate.** `_attach_addresses()` drops entities
whose every property is a one- or two-family home, via
`ingest.pluto.is_landlord_lot()` — the same predicate the ingest gate uses,
so the two cannot drift. Fails open twice: unknown `unit_count` qualifies,
and an entity with no `property_roles` is kept (management companies
reached via a contact's employer own nothing directly).

**Government filter fixed upstream.** `filters.py` now builds the
Commissioner patterns from `_COMMISSIONER = COMM(?:IS+(?:IO|O|OI)N(?:ER)?)?`,
varying the S count, the vowels and the trailing `ER` independently. Note
the fix suggested in `CLAUDE.md` (`COMMIS+ION(?:ER)?`) would have **lost**
`COMMISSONER` and `COMMISSOINER`. `_EXTRA_GOVT_RE` is retained despite
being redundant: SQL `is_govt_name()` (022) still has the gap.

### Verification

- `sync-airtable` applied, then `--dry-run`: **0 creates, 0 updates, no new
  merges** — converges per CLAUDE.md.
- Merges 17 → 7 → 6 (one merging entity fell below the size threshold).
- Formatter, `person_key`, domain and `is_landlord_lot` cases: all pass.
- `ruff` clean on changed files; the repo's other 115 errors are
  pre-existing.

### Already executed against production

- `backfill-pluto`: 131,047 of 135,388 properties populated.
- `purge_small_buildings.py --apply`: **55 Managements, 72 Contacts, 55
  Addresses deleted**; 1 Management protected because a surviving unit
  still resolved to it. Idempotent — a re-run finds nothing.
```

### PR B — stacked on A, or cut after A merges

Changes what gets **ingested**, so it is worth reviewing separately.

Files: `ingest/acris.py`, `ingest/hpd.py`†, `CLAUDE.md`†.

```markdown
## Gate ingestion on building size

ACRIS records every deed transfer regardless of building size, so the
pipeline stored one- and two-family homes and paid to enrich private
homeowners. `pluto.admits()` runs once per ingest batch in ACRIS and HPD;
lots failing `is_landlord_lot()` are never stored.

**Mixed use is explicitly in.** `A`/`B` class (one- and two-family) is out
always; `S`/`K` (apartments over a store) is in on any residential unit and
exempt from the unit threshold — commercial space under the apartments
means the owner runs a building rather than living in a house. A store with
no apartments stays out.

**`unit_count` and `building_class` had never been populated — 100% NULL
across 135,388 rows.** `ingest/hpd.py` read `reg.get("unitcount")` and
`reg.get("buildingclassid")`, and the HPD Registrations dataset
(`tesw-yqqr`) **has neither column**; both calls returned `None` for every
row ever ingested. PLUTO is the only NYC source that publishes them.

That silently degraded `skip_filter._evaluate_low_value()`: its
`+0.30 unit_count >= 3` bonus could never fire and its `-0.50 max_units
<= 2` penalty always did, leaving it an `hpd_reg_id` check in disguise.

HPD's row stream needed a new `buffered()` helper — feeding a batched
lookup one row at a time would turn one request into one per building.

**The gate fails open.** A BBL PLUTO does not know, or a failed request, is
admitted; 4,341 of 135,388 properties are not in PLUTO. Silently dropping
real ownership data is worse than carrying a few small buildings.

Measured effect: ~25,400 lots (19% of those with a known size) would no
longer be stored, along with the enrichment spend behind them.
```

### Sequencing

1. Update #44 with PR A. It is self-contained; the ingest gate is not
   required for it to be correct.
2. Land PR B after, or stack it — `is_landlord_lot()` lives in PR A.
3. Neither blocks, nor is blocked by, Phases 2–4.

---

## 6. Do not do these

- **Do not `git init` in this working copy.** Unison would propagate `.git`
  to the Mac, where a real repository and active WIP already exist. This is
  the single most damaging thing available here.
- **Do not copy `main.py`, `config.py`, `CLAUDE.md` or `.env.example`
  wholesale** in either direction (§4).
- **Do not apply this tree's `016/017/018`** to the production database.
  `main`'s 016–022 are already applied.
- **Do not re-run the reseed** expecting changes. It is convergent; a run
  now writes zero records. `--dry-run` first, always.
- **Do not delete `_EXTRA_GOVT_RE` / `_EXTRA_BANK_RE`** from
  `pipeline/airtable_sync.py` on the assumption that `main` covers them. An
  earlier draft of this plan said exactly that and was wrong — see §3.
  `_EXTRA_GOVT_RE`'s Python-side gap is now fixed in this tree's
  `filters.py`, which is *not* the same as `main` covering it: taking
  `main`'s `filters.py` during reconciliation reopens the gap, and SQL
  `is_govt_name()` still has it. Delete only once both sides are widened.

---

## 7. Open decisions for the owner

1. **Resolve the git sync**, so the local WIP and `main` are one line.
   Everything below is blocked on this.
2. **Is the contact-boundary line an unpushed branch or abandoned work?**
   Answered by `git status` / `git branch -a` on the Mac.
3. **Phase 2:** land, shelve, or cherry-pick RLS only.
4. **Merge PR #44** — independent of 2 and 3; it adds no migration and
   depends only on modules already on `main`.
5. **Zapier or cron** for the CRM write path — tracked separately in
   `docs/CRM_DEDUPLICATION.md` §7. If cron, the audit log specced in §6 of
   that document is a prerequisite.

---

## 8. Reference

| Document | Covers |
|---|---|
| `docs/AIRTABLE_SYNC.md` | sync mechanics, field mapping, matching rules |
| `docs/CRM_DEDUPLICATION.md` | the 8 dedup rules, Zapier vs cron, audit-log spec |
| `docs/SPEC_contact_boundary.md` | the candidate→publish boundary (Phase 2) |
| `CLAUDE.md` | conventions; the sync section records the three matching rules |

**Note:** this file is also committed in PR #44 at an earlier revision.
The copy in the working tree is authoritative — if the PR is still open,
its copy should be refreshed from this one before merge.
