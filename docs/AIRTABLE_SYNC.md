# Airtable CRM Sync

Pushes Owner Research Tool results into the **LL Pipeline** Airtable base
(`appstQVl7JeMfr7d0`) — the sales pipeline.

    python main.py sync-airtable --dry-run   # report, write nothing
    python main.py sync-airtable             # apply
    python main.py sync-airtable --limit 25  # first 25 Management units

Implementation: `pipeline/airtable_sync.py`. Safe to re-run — a second run
over unchanged data writes zero records.

A Zapier zap writes these same three tables from a SQL query against the
same Postgres. Only one of the two should run at a time — see
`docs/CRM_DEDUPLICATION.md` for the comparison and the open decision.

This is the *outbound* half of the Airtable integration. The inbound half
(`webhook.py`) receives an address from an Airtable automation and writes
the BBL / HPD Building ID back to the Addresses table.

---

## 1. What gets written

| Airtable table | Source | Grain |
|---|---|---|
| **Management** | `entities` | one row per researched owner/management entity |
| **Contacts** | `contacts` | one row per person with a usable email or phone |
| **Addresses** | `properties` via `property_roles` | one row per current property |

Management is primary; Contacts and Addresses both link back to it.

### Field mapping

**Management** (`tblXSLY5l2ON0sChK`)

| Field | Value |
|---|---|
| Name | `entities.name` |
| Notes | `Created by Owner Research Tool` |
| Types | link to `recpeksnQCHm0qlRd` ("Owner Research Tool") |
| Owner Research Tool | `true` |
| Contact Company Type (ORT) | `individual` / `llc` / `unknown` from `entities.entity_type` |
| Pipeline | `New/Unsorted` |

**Contacts** (`tblid0IpZKpI6O14q`)

| Field | Value |
|---|---|
| Management | link to the resolved Management |
| Name | `contacts.full_name`, falling back to the company name |
| Email | normalized `contacts.email` |
| Phone | `contacts.phone` normalized to bare 10 digits |
| Phone Type | `Mobile` / `Land Line` (blank when unmapped) |
| Title | `contacts.title` |
| Notes | `Created by Owner Research Tool` |
| Owner Research Tool | `true` |

**Addresses** (`tblVOwshwfY0F3gSS`)

| Field | Value |
|---|---|
| Management | link to the resolved Management |
| Address (Non-Research) | `"1100 Bedford Avenue, Brooklyn, NY"` |

`entity_type` → Contact Company Type collapses `corporation`,
`management_company` and `partnership` into `llc`; anything unrecognised
becomes `unknown`, so the sync never mints a new select option.

---

## 2. Matching cascade

Per source entity, strongest signal first:

1. **Contact match** — an existing Airtable Contact with the same email, or
   the same phone. That contact's linked Management becomes the primary
   record for the entity.
2. **Domain match** — the entity's company-domain email (`sam@pearnyc.com`
   → `pearnyc.com`). Free/consumer providers and placeholder domains are
   excluded, so a shared ISP is never read as a shared employer.
3. **Exact name match** — normalized (uppercased, punctuation-stripped).
4. Otherwise **create**.

### Three rules that matter

Each of these was a bug found by running the sync against live data. They
are easy to reintroduce and each one corrupts the CRM in a different way.

**1. A phone match is not a personal identity.** An office switchboard is
shared by everyone at a firm. Matching on phone alone collapsed Joseph
Brunner, Abe Mandel and Annette Mehal — three people behind one number at
Bruman Realty — into a single contact, losing two of them. So:

- **email match** → same person (emails are personal)
- **phone match** → same person *only if the names agree*
- either match → good enough to identify the **Management**

Because a line is shared, `contact_by_email` / `contact_by_phone` map to a
**list** of contacts, not one. A single-entry index hides everyone but the
first person at that number, so the others are never found and get
re-created on *every* run — unbounded duplicate growth. Identity resolution
searches all candidates for the one whose name agrees.

**2. Two source entities do not merge on a weak signal.** Matching against a
record already in the base is the point of the sync. But when the names
disagree, the match rests on a shared mobile number or a shared
consumer-ISP domain, and fusing unrelated landlords is how the base got
polluted before. Without this guard five unrelated landlords —
`ANDERSON, BERRIS`, `ARIEL, AMI`, `ATTICO, WILLIS`, `COWELL, TAMU`,
`FONTAINE, EDMEE` — folded into one Management because enrichment had
attached the same Gmail to all five.

**3. A weak cross-entity signal never outranks an entity's own record.**
This is what makes the sync *stable across runs*, and it is subtle: records
created by run 1 no longer look "in-run" to run 2, so a guard written only
against same-run merges lapses the second time and every merge it prevented
happens anyway. Guarding on "does this entity already have a Management of
its own" holds on every subsequent run.

Every surviving merge is printed in the run report for review:

```
Entities folded into an existing Management (7) — review these:
  'DELEON, DOREEN'
      -> 'DE LEON, DOREEN'  [contact_match]
```

**Verify with a second dry run.** After a full sync, `--dry-run` must
report zero creates, zero updates and no new merges. Anything else means
the matching rules do not converge and the next run will duplicate or
re-merge records.

---

## 3. Write rules — additive only

The base holds records from other sources with their own Pipeline stages
and hand-written notes. Nothing this tool did not create is overwritten.

**On a record this tool creates:** the full field set above.

**On a record that already existed:**

- never overwrite a non-empty field
- never touch **Pipeline**
- never write the `Created by Owner Research Tool` note
- never set the **Owner Research Tool** checkbox, and never add the ORT
  **Types** link — the record did not originate here, so it must not claim
  to have
- the only field filled is **Contact Company Type (ORT)**, and only when
  empty
- a Contact's **Management** link is only set when it has none. Re-pointing
  or appending links on a contact owned by another source is what produces
  tangled many-to-many Management records.

Addresses are the one exception to "don't append": a building can
legitimately have several managements, so a matched Address gains the new
Management link alongside its existing ones.

---

## 4. Quality gate

Applied before anything is written. Deliberately narrow — it removes
records that are unusable or not landlords, and nothing else.

| Rule | Effect |
|---|---|
| No usable email **and** no phone | dropped |
| Obfuscated email (`[email protected]`, `s*****a@x.com`) | email discarded |
| Government entity | dropped |
| Bank / lender | dropped |
| Lawyer name | dropped |

Typical run: **778 contacts scanned → 750 eligible**, 530 Management units.

### The filter gap this works around

`enrichment/contact/filters.is_govt_entity()` misses forms that are live in
this data — `COMMISIONER OF FINANCE` (the ACRIS misspelling), `SECY OF
HOUSING & URBAN DVLPT`, `THE SECRETARY OF HOUSING AND URBAN DEVELOPMENT` —
and there is no bank/lender rule for ACRIS foreclosure deeds recording
`GREEN POINT SAVINGSBANK` or `AMERICAN BROKERS CONDUIT` as the "owner".

`_EXTRA_GOVT_RE` / `_EXTRA_BANK_RE` in `airtable_sync.py` supplement the
shared filters locally rather than editing them, so enrichment behavior is
unchanged. **The underlying gap in `filters.py` is still open** — fixing it
there would let this module drop its local patterns.

### What the gate does *not* remove

BatchData skip-traces the *property address*, so it returns whoever is
associated with that address — sometimes a tenant or prior resident rather
than the owner. **399 of 519** skip-trace contacts share no name token with
the entity they are attached to:

    'WOODS, REGINALD R'  ->  Lydia L Johnson
    'WILLIAMS, ELSA'     ->  Lee Evan Meyerson

These are kept, by decision — they are unverified, not wrong. Requiring the
name to match would cut the export from 761 contacts to 367.

The designed fix is the candidate→publish boundary in migrations **017/018**
(`contacts.status`, `relevance_score`, `ranked_property_contacts`), which
scores each contact and publishes only what clears the bar. **Neither
migration is applied to the live database**, so there is no `status` column
to filter on and this gate stands in for it. Once 017/018 and
`scripts/backfill_contact_boundary.py` have run, `load_units()` should
filter on `status = 'published'` and most of this gate becomes redundant.

---

## 5. Configuration

`config.py`, all overridable by environment variable:

| Setting | Default |
|---|---|
| `AIRTABLE_API_KEY` | — (required) |
| `AIRTABLE_BASE_ID` | `appstQVl7JeMfr7d0` |
| `AIRTABLE_MANAGEMENT_TABLE_ID` | `tblXSLY5l2ON0sChK` |
| `AIRTABLE_CONTACTS_TABLE_ID` | `tblid0IpZKpI6O14q` |
| `AIRTABLE_ADDRESS_TABLE_ID` | `tblVOwshwfY0F3gSS` |
| `AIRTABLE_ORT_TYPE_RECORD_ID` | `recpeksnQCHm0qlRd` |
| `AIRTABLE_NEW_PIPELINE_STAGE` | `New/Unsorted` |

The token needs `data.records:read` and `data.records:write` on the base.
Field IDs are constants in `airtable_sync.py` (`MgmtField`, `ContactField`,
`AddressField`) — IDs, not names, so renaming a column in the Airtable UI
does not break the sync.

---

## 6. Operational notes

- **Rate limit** — Airtable allows 5 requests/sec/base; the client paces
  itself at ~4.5/sec and batches writes 10 per request. A full run is a few
  minutes.
- **Reads are bulk, not per-record** — all three tables are pulled once into
  an in-memory index. Per-record Airtable searches would be far slower and
  would exhaust the rate limit.
- **Dry run** builds the same plan and reports identical counts, but issues
  no writes. Always the right first step after changing matching rules.
- **Address dedup is weak across sources.** The key is the normalized
  address string, and this tool emits `"616 Nostrand Avenue, Brooklyn, NY"`
  while other sources store unit numbers and ZIPs
  (`"112 Emerson Place SUITE 6E  Brooklyn NY 11205"`). Equivalent addresses
  in different formats will not match. Re-runs of *this* tool are
  consistent, so it does not duplicate its own rows.
- `properties.house_number`, `street_name`, `borough` and `bbl` are all
  available and could populate the Addresses table's structured columns —
  not written today, since only `Address (Non-Research)` was specified.
