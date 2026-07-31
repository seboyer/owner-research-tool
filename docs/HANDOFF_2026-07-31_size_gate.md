# HANDOFF → the `contact-boundary` session

**Written 2026-07-31.** Read this before you rebase, and before you touch
`ingest/hpd.py`.

You are working on `contact-boundary` (Wave 0/1: the candidate→publish
boundary, observations, scoring, `network_distance` match confidence).
Six commits, unpushed, based on `main` at `43c4416` (2026-05-22).

While you were away, two PRs were opened against `main`'s line. **None of
your commits were touched, nothing was rebased, and `contact-boundary` was
not pushed.** But three files you own also changed on the other line, and
two things changed in the *live database* that affect code sitting right
next to yours.

---

## 1. What was opened

| PR | Branch | Base | What |
|---|---|---|---|
| [#44](https://github.com/seboyer/owner-research-tool/pull/44) | `airtable-crm-sync` | `main` | Airtable CRM sync (pre-existing, still open) |
| [#45](https://github.com/seboyer/owner-research-tool/pull/45) | `size-gate-and-sync-fixes` | `airtable-crm-sync` | Sync correctness + read-side size gate |
| [#46](https://github.com/seboyer/owner-research-tool/pull/46) | `ingest-size-gate` | `size-gate-and-sync-fixes` | Ingest-time size gate |

They are stacked: 44 → 45 → 46. Merge in that order.

---

## 2. The three files we both touch

### `ingest/hpd.py` — **the sharpest conflict. Read this one.**

Your branch refactors `ingest_hpd_registrations()` to write contacts via
`record_contact()` / `ContactHit`. PR #46 changes the *same function* on
`main`'s line, which still uses `upsert_contact()`:

- wraps the row stream in a new `buffered(rows, 500)` helper,
- adds a batched `pluto.admits()` size gate before the per-row loop,
- re-indents the whole loop body one level inside `for reg in batch:`,
- replaces `reg.get("unitcount")` / `reg.get("buildingclassid")` with
  `**pluto.property_fields(pluto.cached(bbl))`.

**When you rebase, re-apply your `record_contact` refactor on top of the
gate — do not take either whole file.** Taking yours drops the gate and
reinstates two dead field reads; taking theirs drops your refactor. The
re-indentation means git will almost certainly report a conflict across
the entire function rather than a clean hunk.

`buffered()` exists because the gate is a batched lookup — feeding it one
row at a time turns one request into one per building. Keep it.

### `main.py`

Their line has `enrich-company`; yours has `score-contacts`. **Both must
survive.** #45/#46 add two more commands that must also survive:
`ingest pluto` and `backfill-pluto`.

Note: your `.order("id")` paging fix belongs on your side only — the code
it patches (the `contacts.seed_property_id` scan) does not exist on
`main`'s line, so it was deliberately *not* carried over. See §4.

### `CLAUDE.md`

Both lines edited it. Theirs adds two new sections — *Building-size gate*
and *Paging Supabase reads* — plus an Airtable-sync paragraph. Yours has
the contact-boundary migration list. Merge additively; nothing overlaps
semantically.

---

## 3. Two things changed in the live database

These are **facts about production**, not about a branch. They are true
right now regardless of what you merge.

### `properties.unit_count` and `building_class` are now populated

They were **100% NULL across all 135,388 rows** — forever. `ingest/hpd.py`
read `reg.get("unitcount")` and `reg.get("buildingclassid")`, and the HPD
Registrations dataset (`tesw-yqqr`) has neither column. Every call
returned `None`. PLUTO is the only NYC source that publishes them.

`python main.py backfill-pluto` has been run: **131,047 of 135,388
populated**; 4,341 are not in PLUTO. Distribution — 1,747 at 0 residential
units, 6,097 at 1, 17,552 at 2, 105,651 at 3+.

### ⚠ This changes `skip_filter._evaluate_low_value()` behaviour — next to your scoring work

That scorer reads `unit_count`. With the column NULL:

- `+0.30 unit_count >= 3` **could never fire**;
- `-0.50 individual + no HPD reg + max_units <= 2` **always fired**, since
  `max_units` computed to 0.

Both now behave as designed for the first time. If you are calibrating
scores or thresholds against observed behaviour, **any baseline taken
before 2026-07-31 is not comparable.** `SKIP_LOW_VALUE_THRESHOLD` is
unchanged at 0.30, but what clears it has moved.

### 55 Managements / 72 Contacts / 55 Addresses deleted from Airtable

`scripts/purge_small_buildings.py --apply` retracted one- and two-family
owners the sync had already pushed. Only ORT-flagged records; 1 Management
was protected because a surviving unit still resolved to it. Idempotent —
a re-run finds nothing.

---

## 4. Something you inherit for free when you rebase

`contact-boundary` branched from `main` **before PR #43** (*Filter
government contacts*, merged 2026-05-24). So your `enrichment/contact/filters.py`
is missing, relative to current `main`:

- `is_govt_email()`, and
- 12 government patterns: `SECY OF HOUSING`, `HOUSING AND URBAN DEV`,
  `SECRETARY OF (THE) TREASURY`, `INTERNAL REVENUE`, `IRS`,
  `DEPARTMENT OF JUSTICE`, `DOJ`, `FBI`, `DEPARTMENT OF VETERANS`,
  `SOCIAL SECURITY ADMIN`, and `(?:^|\s)HUD\b` — which is stricter than
  your `\bHUD\b` and does not false-positive on `HUDSON`.

Rebasing picks all of that up. It is a real behaviour change: it drops two
more entities (`SECY OF HOUSING & URBAN DVLPT`,
`THE SECRETARY OF HOUSING AND URBAN DEVELOPMENT`) and three more contacts
from the CRM sync's eligible set. Verified those are the only two.

PR #45 adds `_COMMISSIONER` on top, fixing the single-S `COMMISIONER OF
FINANCE` that ACRIS actually records. **Take `main`'s `filters.py` and
re-apply anything boundary-specific on top** — this was already the
standing advice in `RECONCILIATION_PLAN.md` §5 Phase 3, and it is now
measured rather than assumed.

---

## 5. What was deliberately not done

- **`contact-boundary` was not pushed, rebased, or committed to.** It is
  exactly as you left it. Your six commits remain unpushed.
- **No migrations were added or changed.** `database/migrations/` is
  untouched at 17 files — the two PLUTO columns already existed in
  `schema.sql`. So none of this collides with the 016/017/018 numbering
  conflict, and Phases 2–3 of `RECONCILIATION_PLAN.md` are unaffected.
- **The working tree still holds the uncommitted session work.** PRs #45
  and #46 were built in a throwaway clone off `origin/airtable-crm-sync`,
  precisely so the synced `.git` and your WIP were never touched. The
  working-copy versions of `config.py`, `main.py`, `CLAUDE.md`,
  `filters.py`, `admin/routes.py` and `ingest/hpd.py` are **your line's
  versions plus session edits** — they are *not* what was pushed. Do not
  copy them onto `main`'s line.

---

## 6. One open question

The request that produced this work described the WIP as *"fable refactor
/ match confidence scores"*. The match-confidence work is unmistakable —
`scoring.py`, `network_distance`, the two 2026-07-31 commits. **No "fable
refactor" exists anywhere in the repository**: not in any branch, not in
any commit message, and `git log --all -S'fable'` returns nothing.

Assumed: "fable refactor" is the Wave 1 rewrite itself (authored in a
Fable session). If it is instead separate, unsynced work, then the
conflict analysis in §2 does not cover it and should be redone against it.
