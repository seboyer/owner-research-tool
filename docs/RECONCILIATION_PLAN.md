# Reconciliation plan — this working copy vs GitHub `main`

Drafted July 2026, after the Airtable reseed. This working copy
(`/home/sam/Dev/owner-research-tool`, unison-synced from the Mac) and
`github.com/seboyer/owner-research-tool` `main` have diverged into two
lines with conflicting migration numbers.

Goal: get `pipeline/airtable_sync.py` onto `main` without dragging two
months of unrelated divergence with it, and decide what happens to the
rest.

---

## 1. Evidence — which line is production

The live Supabase database was probed directly for each line's artifacts.

| Artifact | Line | Present in live DB? |
|---|---|---|
| `zapier_enriched_contacts` view | `main` 018 | **yes** (HTTP 200) |
| `company_enrichment_runs` table | `main` 016 | **yes** (HTTP 200) |
| `enrichment_queue` rows typed `company_enrich` | `main` 016 rename | **yes — 5521 rows** |
| `enrichment_queue` rows typed `zoominfo` | pre-016 | **zero** (rename ran) |
| `contacts.status` column | here 017 | no (HTTP 400) |
| `ranked_property_contacts` view | here 018 | no (HTTP 404) |
| `contact_observations` table | here 017 | no (HTTP 404) |
| `contact_scores` table | here 017 | no (HTTP 404) |

**Verdict: `main` is production.** The contact-boundary line in this
working copy (`016_enable_rls`, `017_contact_boundary`,
`018_broker_surface_v2`, plus `identity.py`, `scoring.py`, `store.py`) has
never been applied to the database and never been merged.

`016_enable_rls` is the one item this probe cannot settle — RLS with no
policies is invisible to a service-key client. Check it directly in
Supabase before assuming either way.

### Likely cause

Unison syncs *files*, not `.git`. There is no git checkout on the Linux
box at all. So the contact-boundary work is almost certainly sitting on the
Mac as uncommitted changes or an unpushed local branch, and the file-level
"divergence" seen from here is just that work, unpushed.

**First thing to check, on the Mac:**

```bash
git status                  # uncommitted contact-boundary work?
git branch -a               # an unpushed local branch?
git log --oneline origin/main..HEAD
```

That single command set determines whether this is a real fork or simply
unpushed work — and most of the rest of this plan gets simpler if it is the
latter.

---

## 2. The useful discovery: the sync is independent

`pipeline/airtable_sync.py` has **no dependency on the undeployed line**.

Its imports — `config`, `database.client.db`, `database.retry`,
`enrichment.contact.filters.is_govt_entity`,
`enrichment.skip_filter._BANK_RE / is_lawyer_name` — all exist on `main`.
It references none of `contacts.status`, `ranked_property_contacts`,
`contact_observations`, `contact_scores`, `identity.py`, `scoring.py` or
`store.py`. It adds no migration. It queries only base tables
(`contacts`, `entities`, `property_roles`, `properties`).

**So it can ship to `main` on its own, ahead of any wider reconciliation.**
That is Phase 1, and it is the only phase blocking the PR.

---

## 3. Phase 1 — land the sync on `main` (small, clean PR)

Branch from `main`, not from this working copy.

### Files to add wholesale

| File | Note |
|---|---|
| `pipeline/airtable_sync.py` | new, self-contained |
| `docs/AIRTABLE_SYNC.md` | new |
| `docs/CRM_DEDUPLICATION.md` | new |
| `docs/RECONCILIATION_PLAN.md` | new (this file) |

`docs/` does not exist on `main` at all, so all four land without conflict.

### Files to hand-apply — do NOT copy from this working copy

These four differ between the lines for unrelated reasons. Re-apply the
*specific edits* onto `main`'s versions:

| File | Edit | Conflict risk |
|---|---|---|
| `config.py` | add the `AIRTABLE_MANAGEMENT_TABLE_ID` / `CONTACTS_TABLE_ID` / `ORT_TYPE_RECORD_ID` / `NEW_PIPELINE_STAGE` block after `AIRTABLE_HPD_FIELD_ID` | **none** — the Airtable block is byte-identical on both lines |
| `.env.example` | add the CRM-sync block after `AIRTABLE_HPD_FIELD_ID` | **none** — identical on both lines |
| `main.py` | add the `sync-airtable` command + 2 usage docstring lines | **low** — but this copy has `score-contacts` (contact-boundary line) and `main` has `enrich-company`. Copying the file wholesale would delete `enrich-company` and smuggle in `score-contacts`. |
| `CLAUDE.md` | add the sync section, commands, divergence note | **medium** — the two CLAUDE.md files describe different feature sets |

### One code change required before merging

**Narrow `_EXTRA_GOVT_RE` — do not delete it.** An earlier draft of this
plan said to drop it entirely because `main`'s `filters.py` covers those
patterns. Tested against `main`'s actual code, that is only partly true:

```
main is_govt_entity('COMMISIONER OF FINANCE')                   -> False   <-- gap
main is_govt_entity('SECY OF HOUSING & URBAN DVLPT')            -> True
main is_govt_entity('THE SECRETARY OF HOUSING AND URBAN DEV..') -> True
```

`filters.py` spells the misspellings `COMM(?:ISSIONER|ISSONER|ISSOINER)?` —
every alternative has a **double S**, so the single-S form ACRIS actually
records is not matched. Deleting the local pattern would have silently
readmitted Commissioner-of-Finance rows into the CRM.

So `_EXTRA_GOVT_RE` is reduced to `COMMIS+ION(?:ER)?\s+OF\s+FIN` (the one
real gap) and the redundant alternatives are dropped.

**Keep `_EXTRA_BANK_RE` in full.** Verified against both lines:
`skip_filter._BANK_RE` uses `\bSAVINGS\b` / `\bBANK\b`, which fail on
`GREEN POINT SAVINGSBANK` (no word boundary in the run-together form),
`AMERICAN BROKERS CONDUIT` and `CARVER FEDL SAVS & LOAN ASSN`.

**Better follow-up (separate PR):** fix both at source — widen the
Commissioner alternation in `filters.py` to `COMMIS+ION…`, and add the
run-together / ACRIS-abbreviation forms to `skip_filter._BANK_RE`. Then
both local patches can go. Kept out of this PR to avoid changing enrichment
behavior in a change that is otherwise additive.

### Verification before merge

```bash
python main.py sync-airtable --dry-run
```

Must report **zero creates, zero updates, zero merges** against the current
base. That is the convergence check, and it also proves the rebased
imports resolve.

---

## 4. Phase 2 — triage the contact-boundary line

Do this as a decision, not a merge. The work is: `016_enable_rls`,
`017_contact_boundary`, `018_broker_surface_v2`, `enrichment/contact/`
`identity.py` / `scoring.py` / `store.py`, plus edits to `orchestrator.py`,
`models.py`, `filters.py`, and the `score-contacts` command.

It is a real feature — the candidate→publish boundary that scores contacts
and publishes only what clears the bar (`docs/SPEC_contact_boundary.md`).
It is also two months stale and never deployed.

Three honest options:

**(a) Land it.** It is the designed fix for exactly the quality problem the
sync's gate works around. If it ships, `load_units()` should filter on
`status = 'published'` and most of the gate becomes redundant. Requires
Phase 3.

**(b) Shelve it deliberately.** Tag the branch, write down why, delete it
from the working tree so the two lines stop drifting. Cheapest option; the
risk is quietly losing real work.

**(c) Cherry-pick `016_enable_rls` only.** RLS is a security control and is
independent of the boundary feature. Worth landing on its own regardless of
what happens to (a).

**Recommendation: (c) now, then decide (a) vs (b) on its own merits.** Do
not let a security migration stay blocked behind an unrelated feature.

---

## 5. Phase 3 — if the contact-boundary line lands

### Renumber the collisions

`main` already occupies 016–022. Renumber this line to sit after:

| Current (here) | Becomes |
|---|---|
| `016_enable_rls.sql` | `023_enable_rls.sql` |
| `017_contact_boundary.sql` | `024_contact_boundary.sql` |
| `018_broker_surface_v2.sql` | `025_broker_surface_v2.sql` |

Then update the ordered migration list in `CLAUDE.md`, and the reference in
`docs/SPEC_contact_boundary.md` §7 and the `018` header comment (which
tells you to run `scripts/backfill_contact_boundary.py` first — still true,
just renumbered).

Check `025_broker_surface_v2` against `main`'s views before applying: it
replaces `broker_pitch_list`, while `main`'s Zapier work added
`zapier_enriched_contacts`. They are separate objects, so no conflict is
expected — but both read `contacts`, and `025` narrows the broker surface
to `status = 'published'`. Anything downstream of `broker_pitch_list`
will see fewer rows the moment it applies.

### Take `main`'s `filters.py`, not this copy's

`main`'s version is strictly newer — it has the government patterns *and*
`is_govt_email()`. This copy's is the older file. On rebase, keep `main`'s
and re-apply only genuinely boundary-specific changes on top, if any.

### Sequencing

`018/025` must not be applied until
`scripts/backfill_contact_boundary.py` has scored every existing contact
row — otherwise the broker surface empties out, because nothing is
`published` yet.

---

## 6. Phase 4 — reconcile the duplicated concerns

Once both lines are on one trunk, three things exist in two places:

| Concern | On `main` | In the sync | Resolution |
|---|---|---|---|
| Government filter | `filters.py` + `is_govt_name()` in SQL (022) | `_EXTRA_GOVT_RE` | widen `COMMIS+ION…` in `filters.py` **and** in SQL `is_govt_name()`, then delete the patch |
| Bank/lender filter | `skip_filter._BANK_RE` (has gaps) | `_EXTRA_BANK_RE` | fix `skip_filter.py`, then delete the patch |
| Company name resolution | 4-tier COALESCE in the Zapier view (022) | `entities.name` | adopt the view's cascade in the sync |
| Contact eligibility | `WHERE email/phone NOT NULL` in the view | the sync's quality gate | converge on `status='published'` if Phase 3 lands |

The company-name cascade is the one worth porting deliberately: employer →
LLC chain → owned LLC → direct entity is better than what the sync does
today, and it is the difference between a Management named
`WOODS, REGINALD R` and one named after the actual operating company.

---

## 7. Do not do these

- **Do not `git init` in this working copy.** Unison would propagate `.git`
  to the Mac, where a real repository already exists.
- **Do not copy `main.py`, `config.py`, `CLAUDE.md` or `.env.example`
  wholesale** in either direction. Each carries feature work from its own
  line; a wholesale copy silently deletes the other side's.
- **Do not apply this copy's `016/017/018`** to the production database.
  `main`'s 016–022 are already applied; same-numbered files are not the
  same migrations.
- **Do not run the zap and the cron simultaneously** against the same three
  Airtable tables (see `docs/CRM_DEDUPLICATION.md` §1).

---

## 8. Decisions needed

1. **Is the contact-boundary line an unpushed branch or abandoned work?**
   Answered by `git status` / `git branch -a` on the Mac (§1).
2. **Phase 2: land, shelve, or cherry-pick RLS only?**
3. **A GitHub PAT** with `contents:write` + `pull_requests:write` on
   `seboyer/owner-research-tool`. The existing
   `GITHUB_DIGITALOCEAN_DEPLOY_TOKEN` in `1-Resources/master.env` returns
   401 — expired, revoked, or not scoped to this repo.
4. **Zapier or cron** for the CRM write path — independent of all of the
   above, tracked in `docs/CRM_DEDUPLICATION.md` §7.

Phase 1 is unblocked by everything except #3.
