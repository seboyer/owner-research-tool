# CRM Deduplication — findings, and the Zapier vs cron decision

Written after reseeding the LL Pipeline base from the Owner Research Tool
(July 2026). Two systems now write the same Airtable records from the same
Postgres data: a **Zapier zap** driven by a SQL query, and
**`python main.py sync-airtable`** (`pipeline/airtable_sync.py`).

This document records what deduplicating that data actually requires, so
the choice between them can be made on the real tradeoffs. It is a
decision aid, not an implementation — nothing here is wired up.

- Sync mechanics and field mapping: `docs/AIRTABLE_SYNC.md`
- Decision still open. See [§7](#7-making-the-decision).

---

## 1. Where things stand

The reseed is complete and verified. Current base contents:

| Table | Total | From ORT | From other sources (untouched) |
|---|---|---|---|
| Management | 647 | 522 | 125 |
| Contacts | 843 | 723 | 120 |
| Addresses | 740 | 624 | 116 |

1850 records written. A follow-up `--dry-run` reports zero creates, zero
updates and zero merges — a true fixed point, so re-running is safe.

### Two writers, one target — but no retroactive duplication

The zap ingests a SQL query against the same Postgres database the sync
reads. These are not complementary pipelines — they are two
implementations of one job, pointed at the same three tables.

**There is no retroactive duplication risk.** An earlier draft of this
document warned that the zap might re-create the 1850 reseeded records. It
will not: the zap uses a *New Row* trigger on `zapier_enriched_contacts`
keyed on `id`, and it has already passed every row the reseed covered.
Those are the same rows it originally pushed to Airtable, which were later
polluted by an unrelated tool and cleared. **The zap did not cause the
pollution**, and it only fires on rows it has not seen before. It runs from
a daily cron, when enabled.

**The overlap is forward-looking only.** For contacts enriched *from now
on*, both the zap and the sync would create Airtable records. That is the
open Zapier-vs-cron decision in §7, not an outstanding incident.

---

## 2. What deduplicating this data actually requires

Eight rules. Every one of them exists because of a specific failure
observed against live data — the first four are matching, the last four are
what prevent damage.

### 2.1 Normalize before comparing

Never compare raw values.

| Field | Rule | Example |
|---|---|---|
| Phone | strip to 10 digits, drop leading `1` | `+1 (718) 302-2171` → `7183022171` |
| Email | lowercase, trim, reject malformed/obfuscated | `Sam@PearNYC.com ` → `sam@pearnyc.com` |
| Name | uppercase, strip punctuation, collapse spaces | `Bawabeh Brothers, L.L.C.` → `BAWABEH BROTHERS L L C` |
| Address | same as name | `1100 Bedford Avenue, Brooklyn, NY` → `1100 BEDFORD AVENUE BROOKLYN NY` |

Not cosmetic. The pre-existing Bruman Realty contact stored
`+1 (718) 302-2171` while the tool held `7183022171`. A literal string
search matches neither to the other, so **every run produces a duplicate**.

### 2.2 Match on identity strength, not "a field matched"

- **Email is personal** → an email match establishes identity on its own.
- **A phone is a shared resource** → a phone match alone does *not*. It
  requires the names to agree.

An office switchboard belongs to everyone at the firm. Matching on phone
alone merged Joseph Brunner, Abe Mandel and Annette Mehal — three people
behind Bruman Realty's one number — into a single contact and silently
lost two of them.

A phone match is still fine for identifying the **company**. "Which
company is this?" and "which person is this?" are different questions and
need different lookups.

### 2.3 Index multi-valued, then choose

For each email/phone key, retrieve **every** record carrying that value,
then pick the one whose name agrees.

A single-match lookup returns the first person at a switchboard and hides
everyone behind them — so those people are never found and are
**re-created on every run**. This is unbounded duplicate growth, and it is
not hypothetical: five contacts wanted re-creating on the second pass
until the index was made multi-valued.

### 2.4 Name compatibility as a veto

```
tokenize(normalized name) → drop tokens ≤2 chars
                          → drop THE AND LLC INC CORP CO LP LTD MR MRS MS DR
                          → require ≥1 shared token
                          → empty on either side = compatible
```

| A | B | Result |
|---|---|---|
| `Joseph Brunner` | `JOSEPH BRUNNER` | compatible |
| `Elizabeth M Morris` | `Elizabeth Morris` | compatible |
| `JOSEPH BRUNNER` | `Bruman Realty` | **not** |
| `Lydia L Johnson` | `WOODS, REGINALD R` | **not** |

Deliberately loose. It is a veto on obviously-wrong merges, not a matcher —
it only has to catch "these are clearly different people."

### 2.5 Cascade in priority order

`contact identity → company domain → exact name → create`

Domain matching needs a stoplist covering free providers **and** consumer
ISPs (`bellsouth.net`, `usa.net`, `ix.netcom.com`, `gci.net`) and
placeholder domains (`address.com`, `email.com`). The base's own
`Company (Domain)` formula misses these, and each one silently merged
unrelated landlords who happened to share an ISP.

### 2.6 The merge guards

Refuse a merge when **the names disagree** *and* either:

1. the source entity already has a Management record of its own, or
2. the candidate was created by this same run for a different entity

Guard 1 is the subtle one. Records created by run 1 no longer look
"in-run" to run 2, so a guard written only against same-run merges
**silently lapses on the second run** — every merge it prevented the first
time then happens anyway. Guarding on "does this entity already own a
record" holds on every subsequent run.

Without these guards, five unrelated landlords — `ANDERSON, BERRIS`,
`ARIEL, AMI`, `ATTICO, WILLIS`, `COWELL, TAMU`, `FONTAINE, EDMEE` — folded
into one Management, because enrichment had attached the same Gmail
address to all five.

### 2.7 Additive writes

Fill blanks only. Never overwrite a non-empty field, never touch Pipeline,
never stamp origin markers on records that came from elsewhere.

### 2.8 Convergence is the real test

Run it, then dry-run it. The second pass must report **zero creates, zero
updates, zero new merges**. If it does not, the rules do not converge and
the next run will duplicate or re-merge. This single test catches nearly
every bug in the list above, and it is how each of the above was found.

---

## 3. Where each rule should live

The important insight: **most of these do not have to live in the
integration tool.** SQL and Airtable formulas are both auditable without a
developer, and both are already in the stack.

| Rule | Best home | Notes |
|---|---|---|
| 2.1 Normalization | **SQL query** + Airtable formula fields | Zero integration steps; both systems then agree on "same" |
| Quality gate (govt/bank/junk) | **SQL query** | It is a `WHERE` clause, not logic |
| 2.3 Find all sharing a phone | Zapier Find Many + Loop | Native |
| 2.2 Email = identity | Zapier Filter | Trivial |
| 2.2 Phone needs name | Zapier Filter on **Last Name** | Simplified — see §4 |
| 2.6 Own-record guard | Zapier: extra Find Many on Management | One more search step |
| 2.7 Fill blanks only | Create-only, or one Filter per field | Simplest: do not update at all |
| Duplicate detection | Airtable view | `Check Duplicates (Robot)` already exists |

### The SQL

Moves normalization and the quality gate into the query, so the integration
consumes clean columns:

```sql
WITH cleaned AS (
  SELECT
    e.id   AS entity_id,
    e.name AS company_name,
    c.full_name,
    c.title,
    c.phone_type,
    -- email: lowercase, drop obfuscated/masked
    CASE WHEN c.email ~* '^[^@\s]+@[^@\s]+\.[a-z]{2,}$'
          AND c.email !~ '\*'
          AND c.email !~* 'protected'
         THEN lower(trim(c.email)) END AS email_key,
    -- phone: 10 digits, leading 1 dropped
    CASE WHEN length(regexp_replace(c.phone,'\D','','g')) = 11
          AND left(regexp_replace(c.phone,'\D','','g'),1) = '1'
         THEN right(regexp_replace(c.phone,'\D','','g'),10)
         WHEN length(regexp_replace(c.phone,'\D','','g')) = 10
         THEN regexp_replace(c.phone,'\D','','g') END AS phone_key,
    -- name key for exact-match dedup
    trim(regexp_replace(upper(e.name),'[^A-Z0-9]+',' ','g')) AS company_key
  FROM contacts c
  JOIN entities e ON e.id = c.entity_id
  WHERE (c.email IS NOT NULL OR c.phone IS NOT NULL)
    AND e.name !~* 'CITY OF N|NYCHA|COMMIS+ION(ER)?\s+OF\s+FINANCE|SEC(Y|RETARY)\s+OF\s+HOUSING|HOUSING\s+AND\s+URBAN\s+DEV|URBAN\s+DVLPT'
    AND e.name !~* 'SAVINGS?\s*BANK|\mSAVS\M|BROKERS\s+CONDUIT|\mFEDL\M|FEDERAL\s+SAV|\mBANCORP\M'
)
SELECT * FROM cleaned
WHERE email_key IS NOT NULL OR phone_key IS NOT NULL;
```

**Not yet run against the database** — only REST access was available when
this was written. The regex semantics mirror the Python in
`pipeline/airtable_sync.py`, which is tested, but execute it once before
wiring it up.

Then mirror `phone_key` / `email_key` as formula fields on the Airtable
Contacts table and point lookups at *those*. This single change eliminates
the entire format-mismatch duplicate class described in §2.1. The base
already uses this pattern for `Domain` and `Company (Domain)`.

---

## 3b. What the zap actually does — and how far ahead it is

**This working copy is not on the same line of development as GitHub
`main`.** The zap is not a thin integration; it is backed by five
migrations on `main` that do not exist here:

| Migration (on `main`, absent here) | Purpose |
|---|---|
| `018_zapier_enriched_contacts_view` | the `zapier_enriched_contacts` view the zap polls |
| `019_zapier_view_employer_lookup` | resolve a contact's employer |
| `020_zapier_employs_backfill` | backfill `employs` relationships |
| `021_zapier_view_owned_llc_fallback` | fall back to an owned LLC for company name |
| `022_zapier_view_govt_filter` | in-SQL government filter |

**Zapier setup, per the migration header:** PostgreSQL app → *New Row*
trigger → table `zapier_enriched_contacts`, id column `id` (UUID),
ordered by `enriched_at`.

This matters for several claims made earlier in this document:

- **Normalization in SQL (§3) is partly built already.** The view is the
  natural home for it — `phone_type` is already inferred there per source.
- **The government filter is mostly solved on `main`, with one live gap.**
  `022` defines `is_govt_name()` / `is_govt_email()` in SQL, and `main`'s
  `enrichment/contact/filters.py` has matching Python patterns plus
  `is_govt_email()`. But both spell the misspellings
  `COMM(?:ISSIONER|ISSONER|ISSOINER)?` — every alternative double-S — so
  `COMMISIONER OF FINANCE`, the single-S form ACRIS actually records,
  returns `False` and passes straight through. `_EXTRA_GOVT_RE` in
  `pipeline/airtable_sync.py` is narrowed to exactly that gap; the fix
  belongs upstream in both `filters.py` and the SQL function.
- **The bank/lender patch is still needed.** `skip_filter._BANK_RE` uses
  `\bSAVINGS\b` / `\bBANK\b`, which do not match `GREEN POINT SAVINGSBANK`,
  `AMERICAN BROKERS CONDUIT` or `CARVER FEDL SAVS & LOAN ASSN`. Verified on
  both lines. `_EXTRA_BANK_RE` should survive the rebase.
- **Company resolution is more sophisticated in the view than in the sync.**
  `022` picks `contact_company` by four-tier COALESCE — employer →
  LLC chain → owned LLC → direct entity. The sync just uses
  `entities.name`. If the cron path wins, it should adopt that cascade.

**Still unknown:** the *Airtable-side* match step. The `New Row` trigger
dedups the **source** rows by UUID — it does not dedup against records
already in Airtable. Whatever the zap's action steps do for that is where
the eight rules in §2 apply, and it is not visible in this repo.

---

## 4. Option A — keep Zapier

**Why it wins:** it is maintainable by the person who owns it. Zapier
predates the current tooling in this stack and does not require a developer
— or an AI agent — to change. A pipeline that cannot be modified without
outside help is a dependency, and that cost is real and recurring.

It is also **further along than the sync in two respects** (§3b): the
government filter and the company-name cascade are both solved in SQL,
where they are auditable without touching Python.

Zapier supports Find Many Records and Looping, so §2.3 — the rule most
likely to cause runaway duplication — is expressible natively.

**Simplification worth making:** replace the token-intersection test (§2.4)
with **surname equality**. The Contacts table already has `First Name` and
`Last Name` formula fields, so the rule becomes "accept a phone match only
if Last Name matches" — one Filter step instead of tokenization. It still
catches the case that matters (Brunner / Mandel / Mehal are three different
surnames). It is slightly less tolerant of messy names; that is a fair
trade for something readable in the UI.

**What to watch:**

- **No dry run.** Zapier cannot preview what it is about to write. Since the
  damaging failure mode here is merges — awkward to reverse — compensate
  with an Airtable duplicate-detection view as a standing safety net, and by
  testing against a duplicated base before going live.
- **Update overwrites.** Zapier's Update Record writes whatever is mapped.
  Either restrict the zap to creates only, or add one Filter per field.
  Create-only is the simpler, safer default; the cost is that blanks never
  get backfilled.
- **Step cost.** The full rule set is roughly: search by email → search by
  phone → loop → surname filter → search Management by name → branch. That
  is a lot of tasks per source row at ~750 eligible contacts.

---

## 5. Option B — the cron job

`pipeline/airtable_sync.py` already implements all eight rules, is
deterministic (no AI at runtime), converges, and has a `--dry-run` that
prints exactly what it will change before changing it — the one control
Zapier cannot offer.

**The cost is ownership.** Changing a matching rule means editing Python,
which in practice means involving an agent. For a rule set that is now
stable that may be infrequent, but it is the honest tradeoff.

**What is missing today: an audit log.** The sync prints a run report to
stdout and emits structured log lines, but nothing durable is recorded. For
a scheduled job nobody is watching, that is not enough. §6 specifies what
would need building.

**Deployment:** the scheduler (`scheduler.py`) already runs in the Render
web process, so this would be an added APScheduler job rather than new
infrastructure.

---

## 6. Proposed: the deduplication log

Required before the cron option is viable. **Not built** — this is a spec.

### Design

One row per decision, plus a run summary. Follows the existing
`ingestion_log` convention.

```sql
-- database/migrations/019_airtable_sync_log.sql  (PROPOSED, not applied)

CREATE TABLE IF NOT EXISTS airtable_sync_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    dry_run         BOOLEAN NOT NULL DEFAULT FALSE,
    status          TEXT DEFAULT 'running',   -- running|success|failed
    contacts_seen   INTEGER,
    eligible        INTEGER,
    mgmt_created    INTEGER,
    mgmt_matched    INTEGER,
    contacts_created INTEGER,
    contacts_updated INTEGER,
    addresses_created INTEGER,
    merges          INTEGER,
    records_written INTEGER,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS airtable_sync_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          TEXT NOT NULL REFERENCES airtable_sync_runs(run_id) ON DELETE CASCADE,
    target_table    TEXT NOT NULL,   -- management|contact|address
    action          TEXT NOT NULL,   -- create|update|merge|skip|unchanged
    -- what it came from
    entity_id       UUID,
    contact_id      UUID,
    source_name     TEXT,            -- name as it appeared in Postgres
    -- what it resolved to
    airtable_id     TEXT,
    target_name     TEXT,            -- name of the Airtable record
    matched_by      TEXT,            -- email|phone+name|domain|name|null
    matched_value   TEXT,            -- the key that matched
    -- why
    reason          TEXT,            -- govt_entity | no_contact_info |
                                     -- names_disagree_own_record_exists | ...
    fields_written  JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_atsync_run    ON airtable_sync_log(run_id);
CREATE INDEX IF NOT EXISTS idx_atsync_entity ON airtable_sync_log(entity_id);
CREATE INDEX IF NOT EXISTS idx_atsync_action ON airtable_sync_log(action);

ALTER TABLE airtable_sync_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE airtable_sync_log  ENABLE ROW LEVEL SECURITY;
```

### Questions it must answer

| Question | Query |
|---|---|
| Why is this landlord not in the CRM? | `WHERE source_name = ... ` → read `reason` |
| What merged into this Management? | `WHERE action='merge' AND target_name = ...` |
| What did last night's run change? | `WHERE run_id = ... AND action <> 'unchanged'` |
| Is it still converging? | latest run with `action IN ('create','update')` should be empty |
| Which rule is doing the most work? | `GROUP BY matched_by` |

### Volume

At steady state most rows are `unchanged` (750 contacts, 623 addresses on
the last run). Log those as **counts on the run summary only**, and write
detail rows for `create` / `update` / `merge` / `skip` — a few dozen per
run rather than ~1900.

### Optional: mirror to Airtable

A small "Sync Log" table in the base holding just merges and creates would
make the audit trail visible without SQL. Recommended if the cron path is
chosen, since it restores the UI-inspectability that is Zapier's main
advantage.

### Worth noting

This log would be just as useful for auditing the **zap** — point it at the
same three questions and it tells you whether the zap's matching is
behaving. It is not strictly exclusive to Option B.

---

## 7. Making the decision

**Choose Zapier if** the priority is that the pipeline stay editable
without outside help. That is a legitimate and probably decisive reason.
Budget the work in §3 and §4 — the normalized SQL and the Airtable key
fields are the parts that matter most, and they are worth doing regardless
of which option wins.

**Choose the cron if** the priority is that matching be exactly right and
previewable before writing, and infrequent rule changes are acceptable.
Build §6 first.

**Either way, do these:**

1. Resolve the two-writer risk in §1 before the next scheduled run.
2. Move normalization into the SQL query and mirror it as Airtable formula
   fields (§3). Highest value, lowest maintenance, benefits both options.
3. Keep a convergence check (§2.8) in whatever form the chosen tool allows.

### Still open

- **The repo has diverged.** GitHub `main` (HEAD `6350706`, 2026-05-24) and
  this working copy are two parallel lines with *conflicting migration
  numbers*: `016/017/018` are `company_enrichment` /
  `company_enrich_indirect_view` / `zapier_enriched_contacts_view` on
  `main`, and `enable_rls` / `contact_boundary` / `broker_surface_v2` here.
  Reconciling these is a prerequisite for merging anything — including
  `pipeline/airtable_sync.py`, which adds no migration of its own but sits
  on top of the divergent tree.
- **What is the zap's Airtable-side match step keyed on?** Not a
  duplication risk for the reseeded records (§1), but it still decides how
  the zap dedups *new* rows against the base — which is what the eight
  rules in §2 govern. Not visible in this repo.
- The SQL in §3 has not been executed against the database.
- Migrations 017/018 *of this line* are unapplied, so neither path can
  filter on `contacts.status = 'published'` — the quality gate stands in
  for it.
- `_EXTRA_GOVT_RE` / `_EXTRA_BANK_RE` in `pipeline/airtable_sync.py` each
  cover a verified upstream gap and must stay until fixed at source (§3b).
