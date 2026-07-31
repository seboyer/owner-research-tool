"""
airtable_sync.py — push Owner Research Tool results into the LL Pipeline
Airtable base.

Three tables are populated. Management is primary; Contacts and Addresses
both link back to it:

    Management  <- one row per researched owner/management entity
      |- Contacts   (email / phone / title for a person at that entity)
      `- Addresses  (a property the entity owns or manages)

Every write is update-or-create. The base holds records that came from
other sources, so the sync is *additive only*: it never overwrites a
non-empty field on a record it did not create, never touches Pipeline,
and never stamps the "Created by Owner Research Tool" note or the
Owner Research Tool checkbox onto a record that originated elsewhere.

Matching cascade, per the spec in docs/AIRTABLE_SYNC.md:

  1. Look for an existing Contact by normalized email, then by normalized
     phone. On a hit, that contact's linked Management becomes the primary
     record for the whole entity.
  2. Otherwise look for the Management itself — by email domain when the
     entity has a company-domain email, else by exact normalized name.
  3. Otherwise create the Management.

Usage:
    python main.py sync-airtable --dry-run     # report, write nothing
    python main.py sync-airtable               # apply
"""

from __future__ import annotations

import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog

from config import config
from database.client import db
from database.retry import retry_external
from enrichment.contact.filters import is_govt_entity
from enrichment.skip_filter import _BANK_RE, is_lawyer_name
from ingest.pluto import is_landlord_lot

log = structlog.get_logger(__name__)


# ============================================================
# Airtable field IDs
#
# Field IDs (not names) so a rename in the Airtable UI does not
# silently break the sync.
# ============================================================

class MgmtField:
    NAME = "fldQXmXgOAqMD5J5w"
    NOTES = "fldsOE6oYdxMoIqGK"
    TYPES = "fldF1jEQ29inQ3zJt"          # link -> Types table
    ORT_FLAG = "fldwOcku9yuftgJa4"       # checkbox "Owner Research Tool"
    COMPANY_TYPE = "fldg6c16GFMhjUUSO"   # singleSelect "Contact Company Type (ORT)"
    PIPELINE = "fldyhuOrocO19GVPZ"


class ContactField:
    NAME = "fldDr9aWdiCBm5mQa"
    EMAIL = "fldYJWVpqBy28Xmrg"
    PHONE = "fldnqKYA9tIqgv2H3"
    NOTES = "fldgr0fDTemjYAJx5"
    TITLE = "fldq8VUoNdDGfcjH6"
    PHONE_TYPE = "fldNaLoSUvx3Y789f"     # singleSelect
    MANAGEMENT = "fld1XfFThcxVVEus7"     # link -> Management
    ORT_FLAG = "fldNuhQnKm6Xwf9DW"


class AddressField:
    ADDRESS = "fldDqmiELcriDzXe8"        # "Address (Non-Research)"
    MANAGEMENT = "fldXuCgYIxeSR0rSH"     # link -> Management


ORT_NOTE = "Created by Owner Research Tool"

# Contact Company Type (ORT) choices that exist in the base. Anything the
# tool cannot classify lands in "unknown" rather than minting a new choice.
_COMPANY_TYPE_MAP = {
    "individual": "individual",
    "llc": "llc",
    "corporation": "llc",
    "management_company": "llc",
    "partnership": "llc",
}

# Phone Type choices that exist in the base. Values outside this map are
# left blank — writing an unknown choice would create a new select option.
_PHONE_TYPE_MAP = {
    "mobile": "Mobile",
    "land line": "Land Line",
    "landline": "Land Line",
}


# ============================================================
# Quality gate
#
# Gate "B": drop records that are unusable or that are not landlords at
# all. Deliberately does NOT drop BatchData skip-trace contacts whose
# name differs from the entity name — those are unverified but often
# still the right person.
# ============================================================

# ai_web_search scrapes pages where the email is obfuscated by Cloudflare
# ("[email protected]") or masked by the source ("s*******a@domain.com").
_UNUSABLE_EMAIL_RE = re.compile(r"\[email|protected\]|\*", re.IGNORECASE)

# Gaps in the shared filters that are live in this data set.
#
# _EXTRA_GOVT_RE is now REDUNDANT on the Python path: filters.py's
# _COMMISSIONER covers every spelling this matches, including the single-S
# "COMMISIONER OF FINANCE" that was the original reason for this patch. It
# is deliberately still here. Deleting it requires the same widening in SQL
# is_govt_name() (migration 022, which lives on `main` and not in this
# working copy), because reconciliation may take `main`'s filters.py and
# silently carry the gap back in. Delete both together or neither —
# docs/RECONCILIATION_PLAN.md §5 Phase 4, §6.
_EXTRA_GOVT_RE = re.compile(
    r"COMMIS+ION(?:ER)?\s+OF\s+FIN",
    re.IGNORECASE,
)
# skip_filter._BANK_RE uses \bBANK\b / \bSAVINGS\b, so a run-together form
# like SAVINGSBANK has no word boundary and slips through, as do the ACRIS
# abbreviations. All three of these are real "owners" on foreclosure deeds:
#     _BANK_RE('GREEN POINT SAVINGSBANK')      -> False
#     _BANK_RE('AMERICAN BROKERS CONDUIT')     -> False
#     _BANK_RE('CARVER FEDL SAVS & LOAN ASSN') -> False
_EXTRA_BANK_RE = re.compile(
    r"SAVINGS?\s*BANK"
    r"|\bSAVS\b"
    r"|BROKERS\s+CONDUIT"
    r"|\bFEDL\b"
    r"|FEDERAL\s+SAV"
    r"|\bBANCORP\b",
    re.IGNORECASE,
)


def _is_excluded_name(name: str | None) -> str | None:
    """Return the exclusion reason for a company/person name, or None."""
    if not name:
        return None
    if is_govt_entity(name) or _EXTRA_GOVT_RE.search(name):
        return "govt_entity"
    if _BANK_RE.search(name) or _EXTRA_BANK_RE.search(name):
        return "bank_or_lender"
    if is_lawyer_name(name):
        return "lawyer_name"
    return None


# ============================================================
# Normalization
# ============================================================

# Free/consumer email providers. The first block mirrors the "Company
# (Domain)" formula on the Contacts table so this tool and the base agree on
# what counts as a company domain.
_FREE_EMAIL_DOMAINS = (
    "gmail", "hotmail", "mac", "yahoo", "aol.com", "icloud", "ymail",
    "comcast", "sbcglobal", "msn.com", "live.com", "me.com", "outlook",
    "att.net", "verizon", "altavista", "juno.com", "optonline", "mail.com",
    "dell", "aim.com", ".edu", "netzero", "netscape",
    # Consumer ISPs and placeholder domains the base formula misses. Left
    # in, they read as "company domains" and merge unrelated landlords who
    # merely share an ISP.
    "bellsouth", "gci.net", "netcom.com", "usa.net", "address.com",
    "email.com", "insightbb", "cs.com", "earthlink", "rr.com", "cox.net",
    "charter.net", "roadrunner", "prodigy", "peoplepc", "web.tv", "gmx",
    "protonmail", "proton.me", "zoho", "yandex", "fastmail", "hush.com",
    "example.com", "test.com", "none.com", "domain.com",
    # Legacy dial-up/cable ISPs and free portals still attached to owners in
    # this data set. Each one was being read as a company domain, which both
    # invents a company for a private individual and — because the domain
    # index is what find_management() consults second — merges every landlord
    # who happened to use the same ISP.
    "onebox", "citlink", "qwest", "uswest", "concentric.net", "alltel",
    "starmedia", "cableone", "bright.net", "libertysurf", "impsat",
    "angelfire", "frontier.com", "windstream", "suddenlink", "centurylink",
    "sympatico", "btinternet",
    # "yaho.com" is the misspelling of yahoo.com, not a company. Matched in
    # full so it cannot swallow a real domain ending in "yaho".
    "yaho.com",
    # Alumni forwarding addresses belong to a university, not to a landlord.
    "alumni.",
)

# Domains that are real companies but never the landlord: the listing
# brokerage or the closing attorney. A brokerage address on an owner record
# means the owner used that broker, so treating it as their company domain
# both names them wrongly and — via mgmt_by_domain — collapses every client
# of that brokerage into one Management record.
_THIRD_PARTY_DOMAIN_RE = re.compile(
    # Residential/commercial brokerages.
    r"corcoran|bhsusa|halstead|stribling|elliman|century21|weichert"
    r"|citihabitats|cbrealty|coldwellbanker|sothebysrealty|nestseekers"
    r"|laffey|ngkf|remax|compass\.com"
    # Law firms. The dot is required so "lawrencerealty.com" and
    # "delawareholdings.com" are not caught by a bare "law".
    r"|law\.(?:com|net|org)|lawfirm|-law\.|legal\.(?:com|net|org)|attorney",
    re.IGNORECASE,
)

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$", re.IGNORECASE)


def normalize_email(value: str | None) -> str | None:
    """Lowercased email, or None when missing/obfuscated/malformed."""
    if not value:
        return None
    cleaned = value.strip().lower()
    # Non-breaking spaces show up inside scraped "[email protected]" strings.
    cleaned = cleaned.replace("\xa0", " ")
    if _UNUSABLE_EMAIL_RE.search(cleaned) or " " in cleaned:
        return None
    if not _EMAIL_RE.match(cleaned):
        return None
    return cleaned


def normalize_phone(value: str | None) -> str | None:
    """Bare 10-digit US phone, or None. Used both as the dedup key and as
    the value written to Airtable, so re-runs compare like with like."""
    if not value:
        return None
    digits = re.sub(r"\D", "", value)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10 or digits[0] in "01":
        return None
    return digits


def normalize_name(value: str | None) -> str:
    """Uppercased, punctuation-stripped name for exact-match dedup."""
    if not value:
        return ""
    collapsed = re.sub(r"[^A-Z0-9]+", " ", value.upper())
    return " ".join(collapsed.split())


def company_domain(email: str | None) -> str | None:
    """Domain of the email owner's *own* company, or None.

    None for free providers and consumer ISPs (not a company at all) and for
    brokerages and law firms (a company, but somebody else's).
    """
    email = normalize_email(email)
    if not email:
        return None
    domain = email.rsplit("@", 1)[1]
    if any(free in domain for free in _FREE_EMAIL_DOMAINS):
        return None
    if _THIRD_PARTY_DOMAIN_RE.search(domain):
        return None
    return domain


# Suffixes that stay at the end of a name rather than reading as a given
# name. The roman numerals are tracked separately so title-casing does not
# turn "III" into "Iii".
_ROMAN_SUFFIXES = frozenset({"II", "III", "IV", "V"})
_NAME_SUFFIXES = frozenset({"JR", "SR", "ESQ"}) | _ROMAN_SUFFIXES

# Tokens that mean a comma is separating something other than a surname —
# a company name, an estate, or an ACRIS annotation. Reordering around one
# of these produces nonsense, so the name is left exactly as recorded.
_NOT_A_PERSON_RE = re.compile(
    r"\b(?:LLC|L\.?L\.?C|INC|CORP|CORPORATION|CO|COMPANY|LP|LLP|LTD|TRUST|"
    r"ESTATE|ASSOC|ASSOCIATES|PARTNERS|PARTNERSHIP|REALTY|MANAGEMENT|MGMT|"
    r"PROPERTIES|GROUP|HOLDINGS|ENTERPRISES|BANK|CHURCH|HOUSING|ADMIN|"
    r"ADMINISTRATOR|EXEC|EXECUTOR|ETAL|ET\s+AL)\b",
    re.IGNORECASE,
)


def _titlecase(value: str) -> str:
    """Title-case a person's name, leaving roman-numeral suffixes alone."""
    return " ".join(
        word.upper() if word.rstrip(".").upper() in _ROMAN_SUFFIXES else word.title()
        for word in value.split()
    )


def format_person_name(value: str | None) -> str | None:
    """'SMITH, JOHN A' -> 'John A Smith'.

    ACRIS records natural persons surname-first, and the sync copies the
    entity name straight onto a record a human reads in the CRM. The
    reordering is presentational only — every match key below is derived
    from the tokens, not their order.

    Returns the input untouched whenever the "LAST, FIRST" reading is not
    clearly correct, so a company name containing a comma survives intact.
    """
    if not value:
        return value
    name = value.strip()
    # "JAMES, DOROTHY M/ADMIN OF" — a slash marks an ACRIS annotation, not
    # part of anyone's name.
    if "/" in name or _NOT_A_PERSON_RE.search(name):
        return value

    parts = [part.strip() for part in name.split(",")]
    suffix = ""
    if len(parts) == 2:
        last, given = parts
    elif len(parts) == 3 and parts[1].rstrip(".").upper() in _NAME_SUFFIXES:
        # "BERTH, JR., GEORGE"
        last, suffix, given = parts[0], parts[1].rstrip("."), parts[2]
    else:
        return value

    if not last or not given:
        return value

    # A suffix trailing the given names belongs at the end of the reordered
    # form: "AGNEW, ERNEST LLOYD JR" -> "Ernest Lloyd Agnew Jr".
    given_parts = given.split()
    if given_parts[-1].rstrip(".").upper() in _NAME_SUFFIXES:
        suffix = given_parts.pop().rstrip(".")
    if not given_parts:
        return value

    ordered = [*given_parts, last]
    if suffix:
        ordered.append(suffix)
    return _titlecase(" ".join(ordered))


def person_key(value: str | None) -> str:
    """Order-insensitive match key for a person's name.

    'SMITH, JOHN' and 'John Smith' are one person. Management records written
    by earlier runs carry the ACRIS surname-first spelling, so matching on
    normalize_name() alone would miss every one of them after the reordering
    above and create a duplicate record for each.

    Unrelated to enrichment.contact.identity.person_key(), which keys a
    person on name + email + linkedin + phone for the contacts table. This
    one is name-only and local to Airtable matching.
    """
    return " ".join(sorted(normalize_name(value).split()))


def format_address(address: str | None, borough: str | None) -> str | None:
    """Title-cased '123 Bedford Avenue, Brooklyn, NY' for the
    Address (Non-Research) column."""
    if not address:
        return None
    parts = [address.strip().title()]
    if borough:
        parts.append(borough.strip().title())
    parts.append("NY")
    return ", ".join(parts)


def address_key(value: str | None) -> str:
    """Dedup key for an address string."""
    return normalize_name(value)


# Tokens that carry no identifying signal when comparing two people.
_NAME_STOPWORDS = frozenset(
    {"THE", "AND", "LLC", "INC", "CORP", "CO", "LP", "LTD", "MR", "MRS", "MS", "DR"}
)


def _name_tokens(value: str | None) -> set[str]:
    return {
        token
        for token in normalize_name(value).split()
        if len(token) > 2 and token not in _NAME_STOPWORDS
    }


def names_compatible(a: str | None, b: str | None) -> bool:
    """Whether two contact names can denote the same person.

    Used to qualify phone matches. An office switchboard is shared by
    everyone at the company, so a phone hit alone is not an identity —
    without this, three people at one firm collapse into one record and
    two of them are lost.
    """
    tokens_a, tokens_b = _name_tokens(a), _name_tokens(b)
    if not tokens_a or not tokens_b:
        return True  # No name on one side; nothing contradicts the match.
    return bool(tokens_a & tokens_b)


# ============================================================
# Source records
# ============================================================

@dataclass
class SourceContact:
    contact_id: str
    name: str | None
    title: str | None
    email: str | None
    phone: str | None
    phone_type: str | None
    source: str | None


@dataclass
class ManagementUnit:
    """One researched entity and everything that hangs off it."""
    entity_id: str
    name: str
    entity_type: str | None
    domain: str | None
    contacts: list[SourceContact] = field(default_factory=list)
    addresses: list[str] = field(default_factory=list)

    @property
    def is_person(self) -> bool:
        return (self.entity_type or "").lower() == "individual"

    @property
    def company_type(self) -> str:
        return _COMPANY_TYPE_MAP.get((self.entity_type or "").lower(), "unknown")

    @property
    def domains(self) -> list[str]:
        """Company domains for this unit — the entity's own, plus any
        implied by its contacts' company emails."""
        found = []
        if self.domain:
            found.append(self.domain.strip().lower())
        for contact in self.contacts:
            domain = company_domain(contact.email)
            if domain:
                found.append(domain)
        return list(dict.fromkeys(found))


@dataclass
class GateStats:
    contacts_seen: int = 0
    dropped_no_contact_info: int = 0
    dropped_bad_contact_name: int = 0
    dropped_bad_entity: int = 0
    kept: int = 0
    # Building-size gate, applied per entity after addresses are attached.
    dropped_small_building: int = 0
    addresses_below_threshold: int = 0
    units_size_unknown: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "contacts_seen": self.contacts_seen,
            "dropped_no_contact_info": self.dropped_no_contact_info,
            "dropped_bad_contact_name": self.dropped_bad_contact_name,
            "dropped_bad_entity": self.dropped_bad_entity,
            "kept": self.kept,
            "dropped_small_building": self.dropped_small_building,
            "addresses_below_threshold": self.addresses_below_threshold,
            "units_size_unknown": self.units_size_unknown,
        }


_CONTACT_SELECT = (
    "id,full_name,title,email,phone,phone_type,source,entity_id,"
    "entities!contacts_entity_id_fkey(id,name,entity_type,domain)"
)
_PAGE = 1000


def load_units(limit: int | None = None) -> tuple[list[ManagementUnit], GateStats]:
    """Read contacts + their entities + their properties out of Supabase and
    fold them into Management units, applying the quality gate."""
    stats = GateStats()
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        page = (
            db().table("contacts")
            .select(_CONTACT_SELECT)
            .or_("email.not.is.null,phone.not.is.null")
            # Explicit order is required: paging an unordered select lets
            # Postgres return rows in a different order per request, which
            # silently drops and duplicates contacts once the table exceeds
            # one page.
            .order("id")
            .range(offset, offset + _PAGE - 1)
            .execute()
        )
        rows.extend(page.data or [])
        if len(page.data or []) < _PAGE:
            break
        offset += _PAGE

    units: dict[str, ManagementUnit] = {}
    excluded_entities: set[str] = set()

    for row in rows:
        stats.contacts_seen += 1
        entity = row.get("entities") or {}
        entity_id = row.get("entity_id")
        entity_name = entity.get("name")
        if not entity_id or not entity_name:
            stats.dropped_bad_entity += 1
            continue

        if entity_id in excluded_entities:
            stats.dropped_bad_entity += 1
            continue
        reason = _is_excluded_name(entity_name)
        if reason:
            excluded_entities.add(entity_id)
            units.pop(entity_id, None)
            stats.dropped_bad_entity += 1
            log.debug("airtable_sync.entity_excluded", entity=entity_name, reason=reason)
            continue

        if _is_excluded_name(row.get("full_name")):
            stats.dropped_bad_contact_name += 1
            continue

        email = normalize_email(row.get("email"))
        phone = normalize_phone(row.get("phone"))
        if not email and not phone:
            stats.dropped_no_contact_info += 1
            continue

        unit = units.get(entity_id)
        if unit is None:
            entity_type = entity.get("entity_type")
            name = entity_name.strip()
            if (entity_type or "").lower() == "individual":
                name = format_person_name(name) or name
            unit = ManagementUnit(
                entity_id=entity_id,
                name=name,
                entity_type=entity_type,
                domain=entity.get("domain"),
            )
            units[entity_id] = unit

        unit.contacts.append(
            SourceContact(
                contact_id=row["id"],
                # Fall back to the company name for a general company contact.
                # format_person_name is shape-gated, so a company name passes
                # through unchanged.
                name=format_person_name((row.get("full_name") or entity_name).strip()),
                title=(row.get("title") or "").strip() or None,
                email=email,
                phone=phone,
                phone_type=row.get("phone_type"),
                source=row.get("source"),
            )
        )
        stats.kept += 1

    _attach_addresses(units, stats)

    ordered = sorted(units.values(), key=lambda u: u.name)
    if limit:
        ordered = ordered[:limit]
    return ordered, stats


def _attach_addresses(units: dict[str, ManagementUnit], stats: GateStats) -> None:
    """Fill each unit's addresses from current property_roles, applying the
    building-size gate.

    The ingest-time gate in `ingest/pluto.py` only covers rows ingested after
    it existed; the table still holds ~25k lots below the threshold from
    before. This is the read-side counterpart, so those stop reaching the CRM
    without being deleted.

    The classification lives in `ingest.pluto.is_landlord_lot()` so the read
    side and the ingest side cannot drift apart: one-/two-family houses are
    out, mixed use with any apartment is in.

    Like the ingest gate it FAILS OPEN. A property whose unit_count is NULL
    (not in PLUTO, or ingested before the backfill) counts as qualifying —
    an unknown size is not evidence of a single-family house. An entity with
    no properties at all also survives: management companies reached via a
    contact's employer have no property_roles of their own, and they are the
    single most valuable thing in this pipeline.
    """
    gate_on = config.PLUTO_GATE_ENABLED

    # entity_id -> did any of its properties meet the threshold (or have an
    # unknown size)? Entities absent from this map own no properties.
    qualifies: dict[str, bool] = {}

    entity_ids = list(units.keys())
    for chunk in _chunks(entity_ids, 100):
        rows = (
            db().table("property_roles")
            .select("entity_id,properties(address,borough,unit_count,building_class)")
            .in_("entity_id", chunk)
            .eq("is_current", True)
            .order("id")
            .execute()
        ).data or []
        for row in rows:
            unit = units.get(row["entity_id"])
            prop = row.get("properties") or {}
            if not unit:
                continue

            units_res = prop.get("unit_count")
            if units_res is None:
                stats.units_size_unknown += 1
            keep = is_landlord_lot(units_res, prop.get("building_class"))
            qualifies[unit.entity_id] = qualifies.get(unit.entity_id, False) or keep

            if gate_on and not keep:
                stats.addresses_below_threshold += 1
                continue

            formatted = format_address(prop.get("address"), prop.get("borough"))
            if formatted and formatted not in unit.addresses:
                unit.addresses.append(formatted)

    if not gate_on:
        return

    for entity_id, ok in qualifies.items():
        if not ok:
            units.pop(entity_id, None)
            stats.dropped_small_building += 1


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ============================================================
# Airtable REST client
# ============================================================

class AirtableClient:
    """Minimal Airtable REST client: paged reads, batched writes, and the
    5 req/sec per-base rate limit respected by construction."""

    API_ROOT = "https://api.airtable.com/v0"
    BATCH_SIZE = 10          # Airtable's max records per write request
    MIN_INTERVAL = 0.22      # ~4.5 req/sec, just under the 5/sec cap

    def __init__(self, api_key: str, base_id: str, dry_run: bool = False):
        if not api_key:
            raise RuntimeError(
                "AIRTABLE_API_KEY is not set — add it to .env "
                "(personal access token with data.records:read/write)."
            )
        self.base_id = base_id
        self.dry_run = dry_run
        self._client = httpx.Client(
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )
        self._last_call = 0.0
        self.writes = 0

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> AirtableClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.MIN_INTERVAL:
            time.sleep(self.MIN_INTERVAL - elapsed)
        self._last_call = time.monotonic()

    @retry_external(max_attempts=4)
    def _request(self, method: str, url: str, **kwargs) -> dict:
        self._throttle()
        response = self._client.request(method, url, **kwargs)
        if response.status_code >= 400:
            # Airtable puts the actual reason in the body; without it a 422
            # is undebuggable.
            log.error(
                "airtable_sync.http_error",
                status=response.status_code,
                url=url,
                body=response.text[:1000],
            )
        response.raise_for_status()
        return response.json()

    def list_records(self, table_id: str, fields: list[str]) -> list[dict]:
        """Every record in a table, with cell values keyed by field ID."""
        url = f"{self.API_ROOT}/{self.base_id}/{table_id}"
        params: dict[str, Any] = {
            "pageSize": 100,
            "returnFieldsByFieldId": "true",
            "fields[]": fields,
        }
        records: list[dict] = []
        offset: str | None = None
        while True:
            if offset:
                params["offset"] = offset
            payload = self._request("GET", url, params=params)
            records.extend(payload.get("records", []))
            offset = payload.get("offset")
            if not offset:
                return records

    def create_records(self, table_id: str, records: list[dict]) -> list[dict]:
        """Create records in batches of 10. Returns the created records."""
        if self.dry_run or not records:
            return []
        url = f"{self.API_ROOT}/{self.base_id}/{table_id}"
        created: list[dict] = []
        for batch in _chunks(records, self.BATCH_SIZE):
            payload = self._request(
                "POST",
                url,
                json={"records": batch, "returnFieldsByFieldId": True},
            )
            created.extend(payload.get("records", []))
            self.writes += len(batch)
        return created

    def delete_records(self, table_id: str, record_ids: list[str]) -> int:
        """Delete records in batches of 10. Returns the count deleted.

        Only ever called by scripts/purge_small_buildings.py — the sync
        itself is additive and never deletes.
        """
        if self.dry_run or not record_ids:
            return 0
        real = [r for r in record_ids if str(r).startswith("rec")]
        if len(real) != len(record_ids):
            raise ValueError("refusing to delete non-record ids")
        url = f"{self.API_ROOT}/{self.base_id}/{table_id}"
        deleted = 0
        for batch in _chunks(real, self.BATCH_SIZE):
            payload = self._request("DELETE", url, params={"records[]": batch})
            deleted += len(payload.get("records", []))
            self.writes += len(batch)
        return deleted

    def update_records(self, table_id: str, records: list[dict]) -> None:
        """PATCH records in batches of 10 (leaves unlisted fields alone)."""
        if self.dry_run or not records:
            return
        # Placeholder IDs for not-yet-created records must never be sent —
        # Airtable rejects the whole batch with a 422.
        real = [r for r in records if str(r.get("id", "")).startswith("rec")]
        if len(real) != len(records):
            log.error(
                "airtable_sync.dropped_placeholder_update",
                table=table_id,
                dropped=[r.get("id") for r in records if r not in real],
            )
        records = real
        if not records:
            return
        url = f"{self.API_ROOT}/{self.base_id}/{table_id}"
        for batch in _chunks(records, self.BATCH_SIZE):
            self._request(
                "PATCH",
                url,
                json={"records": batch, "returnFieldsByFieldId": True},
            )
            self.writes += len(batch)


# ============================================================
# In-memory index of what is already in the base
# ============================================================

class BaseIndex:
    """Everything already in the three tables, indexed for dedup.

    Loaded once up front. Per-record Airtable searches would be far slower
    and would burn the rate limit; the tables are small enough to hold.
    """

    def __init__(self, client: AirtableClient):
        self.client = client
        self.mgmt_by_id: dict[str, dict] = {}
        self.mgmt_by_name: dict[str, str] = {}
        # Same records keyed order-insensitively, so a person already in the
        # base as "SMITH, JOHN" is still found once the sync writes them as
        # "John Smith". Consulted only for individuals.
        self.mgmt_by_person_key: dict[str, str] = {}
        self.mgmt_by_domain: dict[str, str] = {}
        # Email/phone -> every contact carrying it. Multi-valued on purpose:
        # a switchboard is shared by a whole firm, so a single-entry index
        # hides everyone but the first person and re-creates the rest on
        # every run.
        self.contact_by_email: dict[str, list[dict]] = defaultdict(list)
        self.contact_by_phone: dict[str, list[dict]] = defaultdict(list)
        self.address_by_key: dict[str, dict] = {}
        # Management records this run created -> the entity name that
        # created them. Guards in-run merging (see find_management).
        self.run_created: dict[str, str] = {}

    def load(self) -> None:
        for record in self.client.list_records(
            config.AIRTABLE_MANAGEMENT_TABLE_ID,
            [MgmtField.NAME, MgmtField.COMPANY_TYPE, MgmtField.TYPES, MgmtField.ORT_FLAG],
        ):
            fields = record.get("fields", {})
            self.mgmt_by_id[record["id"]] = fields
            key = normalize_name(fields.get(MgmtField.NAME))
            # First writer wins, so a re-run maps onto the same record.
            if key:
                self.mgmt_by_name.setdefault(key, record["id"])
                self.mgmt_by_person_key.setdefault(
                    person_key(fields.get(MgmtField.NAME)), record["id"]
                )

        for record in self.client.list_records(
            config.AIRTABLE_CONTACTS_TABLE_ID,
            [
                ContactField.NAME, ContactField.EMAIL, ContactField.PHONE,
                ContactField.TITLE, ContactField.PHONE_TYPE, ContactField.MANAGEMENT,
            ],
        ):
            fields = record.get("fields", {})
            entry = {"id": record["id"], "pending": False, "fields": fields}
            email = normalize_email(fields.get(ContactField.EMAIL))
            phone = normalize_phone(fields.get(ContactField.PHONE))
            if email:
                self.contact_by_email[email].append(entry)
            if phone:
                self.contact_by_phone[phone].append(entry)

            # A company-domain email on a contact identifies its Management.
            domain = company_domain(fields.get(ContactField.EMAIL))
            links = fields.get(ContactField.MANAGEMENT) or []
            if domain and links:
                self.mgmt_by_domain.setdefault(domain, links[0])

        for record in self.client.list_records(
            config.AIRTABLE_ADDRESS_TABLE_ID,
            [AddressField.ADDRESS, AddressField.MANAGEMENT],
        ):
            fields = record.get("fields", {})
            key = address_key(fields.get(AddressField.ADDRESS))
            if key:
                self.address_by_key.setdefault(
                    key, {"id": record["id"], "fields": fields}
                )

    # -- lookups ------------------------------------------------

    def _candidates(self, contact: SourceContact) -> list[dict]:
        """Every indexed contact sharing this contact's email or phone."""
        found: list[dict] = []
        if contact.email:
            found.extend(self.contact_by_email.get(contact.email, ()))
        if contact.phone:
            found.extend(self.contact_by_phone.get(contact.phone, ()))
        return found

    def find_contact_any(self, contact: SourceContact) -> dict | None:
        """Any existing contact reachable by email or phone.

        A phone hit may only mean "same switchboard", so this is used to
        locate the *Management*, not to establish personal identity.
        """
        candidates = self._candidates(contact)
        return candidates[0] if candidates else None

    def find_contact_identity(self, contact: SourceContact) -> dict | None:
        """The existing contact record that is the *same person*, if any.

        Among everyone sharing the email or phone, this is the one whose
        name agrees. When nobody's name agrees the contact is a different
        person on a shared line and gets a record of their own — which the
        next run then finds here by name, so repeated runs converge instead
        of duplicating.
        """
        for entry in self._candidates(contact):
            if names_compatible(contact.name, entry["fields"].get(ContactField.NAME)):
                return entry
        return None

    def _acceptable(self, mgmt_id: str, unit: ManagementUnit) -> bool:
        """Guard against folding two distinct source entities into one record.

        Matching against a record already in the base is the whole point of
        the sync, so a candidate whose name agrees is always fine. When the
        names disagree the match rests on a weak signal — a shared mobile
        number, a shared consumer-ISP domain — and fusing unrelated
        landlords is exactly how the base got polluted before.

        Two things then block the merge:

        1. The entity already has a Management of its own. A weak
           cross-entity signal must never outrank an entity's own record —
           without this the sync is not stable across runs: records created
           by run 1 stop looking "in-run" to run 2, and the merges this
           guard prevented the first time all happen the second time.
        2. The candidate was created by this same run for another entity,
           so the base never asserted the two belong together.
        """
        candidate_name = self.mgmt_by_id.get(mgmt_id, {}).get(MgmtField.NAME)
        if names_compatible(unit.name, candidate_name):
            return True

        own = self.mgmt_by_name.get(normalize_name(unit.name))
        if own is None and unit.is_person:
            # The person's own record may predate the surname-first rename,
            # and it still has to outrank a weak cross-entity signal — an
            # exact-name lookup alone would no longer find it.
            own = self.mgmt_by_person_key.get(person_key(unit.name))
        if own is not None and own != mgmt_id:
            return False

        return mgmt_id not in self.run_created

    def find_management(self, unit: ManagementUnit) -> tuple[str | None, str]:
        """Resolve the Management record for a unit.

        Returns (record_id, how) where `how` explains the match for logging.
        Candidates are tried strongest-signal first and each is checked
        against the in-run merge guard before being accepted.
        """
        # 1. Any contact we already know about points at its Management.
        #    A shared-switchboard hit is fine here: it still identifies the
        #    right company even when it is not the same person.
        for contact in unit.contacts:
            existing = self.find_contact_any(contact)
            if not existing:
                continue
            links = existing["fields"].get(ContactField.MANAGEMENT) or []
            if links and self._acceptable(links[0], unit):
                return links[0], "contact_match"

        # 2. Company domain.
        for domain in unit.domains:
            candidate = self.mgmt_by_domain.get(domain)
            if candidate and self._acceptable(candidate, unit):
                return candidate, "domain_match"

        # 3. Exact name.
        key = normalize_name(unit.name)
        candidate = self.mgmt_by_name.get(key) if key else None
        if candidate and self._acceptable(candidate, unit):
            return candidate, "name_match"

        # 4. The same person under the ACRIS surname-first spelling. Records
        #    written before names were reordered read "SMITH, JOHN"; without
        #    this every one of them gets a duplicate on the next run.
        if unit.is_person:
            candidate = self.mgmt_by_person_key.get(person_key(unit.name))
            if candidate and self._acceptable(candidate, unit):
                return candidate, "person_name_match"

        return None, "new"

    # -- registration of newly written records ------------------

    def register_management(self, record_id: str, unit: ManagementUnit) -> None:
        # Mirror what was actually written, so a later unit folding into this
        # record does not issue a redundant PATCH for fields already set.
        self.mgmt_by_id[record_id] = {
            MgmtField.NAME: unit.name,
            MgmtField.COMPANY_TYPE: unit.company_type,
        }
        self.run_created[record_id] = unit.name
        key = normalize_name(unit.name)
        if key:
            self.mgmt_by_name.setdefault(key, record_id)
            self.mgmt_by_person_key.setdefault(person_key(unit.name), record_id)
        for domain in unit.domains:
            self.mgmt_by_domain.setdefault(domain, record_id)

    def register_contact(
        self,
        record_id: str,
        contact: SourceContact,
        mgmt_id: str,
        pending: bool = False,
    ) -> dict:
        """Add a contact to the index and return its entry.

        `pending=True` reserves the email/phone for a record queued for
        creation but not yet written, so two identical source contacts are
        not both created. A pending entry carries a placeholder ID that must
        never reach the API; the caller mutates the returned entry in place
        once the create returns, which updates every index that holds it.
        """
        entry = {
            "id": record_id,
            "pending": pending,
            "fields": {
                ContactField.NAME: contact.name,
                ContactField.EMAIL: contact.email,
                ContactField.PHONE: contact.phone,
                ContactField.MANAGEMENT: [mgmt_id],
            },
        }
        if contact.email:
            self.contact_by_email[contact.email].append(entry)
        if contact.phone:
            self.contact_by_phone[contact.phone].append(entry)
        return entry

    def register_address(
        self, record_id: str, address: str, mgmt_id: str, pending: bool = False
    ) -> None:
        key = address_key(address)
        previous = self.address_by_key.get(key)
        if previous is not None and not (previous.get("pending") and not pending):
            return
        self.address_by_key[key] = {
            "id": record_id,
            "pending": pending,
            "fields": {
                AddressField.ADDRESS: address,
                AddressField.MANAGEMENT: [mgmt_id],
            },
        }


# ============================================================
# Sync
# ============================================================

@dataclass
class SyncReport:
    gate: dict[str, int] = field(default_factory=dict)
    units: int = 0
    mgmt_created: int = 0
    mgmt_matched: int = 0
    mgmt_updated: int = 0
    match_reasons: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    contacts_created: int = 0
    contacts_updated: int = 0
    contacts_unchanged: int = 0
    addresses_created: int = 0
    addresses_linked: int = 0
    addresses_unchanged: int = 0
    writes: int = 0
    # (source entity, Management it folded into, how) for every unit that did
    # not get its own record. Merges are the one action that is awkward to
    # undo by hand, so each one is reported rather than silently applied.
    merges: list[tuple[str, str, str]] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            "",
            "Quality gate (source -> eligible)",
            f"  contacts scanned            {self.gate.get('contacts_seen', 0)}",
            f"  dropped, no usable contact  {self.gate.get('dropped_no_contact_info', 0)}",
            f"  dropped, govt/bank/lawyer   "
            f"{self.gate.get('dropped_bad_entity', 0) + self.gate.get('dropped_bad_contact_name', 0)}",
            f"  eligible contacts           {self.gate.get('kept', 0)}",
            "",
            "Building-size gate (entities owning nothing at/above the threshold)",
            f"  entities dropped, too small {self.gate.get('dropped_small_building', 0)}",
            f"  addresses below threshold   {self.gate.get('addresses_below_threshold', 0)}",
            f"  properties of unknown size  {self.gate.get('units_size_unknown', 0)} (kept — fails open)",
            "",
            f"Management units              {self.units}",
            f"  created                     {self.mgmt_created}",
            f"  matched existing            {self.mgmt_matched}",
            f"  updated (empty fields only) {self.mgmt_updated}",
        ]
        for reason, count in sorted(self.match_reasons.items()):
            lines.append(f"    via {reason:<22} {count}")
        lines += [
            "",
            f"Contacts  created {self.contacts_created} | "
            f"updated {self.contacts_updated} | unchanged {self.contacts_unchanged}",
            f"Addresses created {self.addresses_created} | "
            f"linked {self.addresses_linked} | unchanged {self.addresses_unchanged}",
        ]
        if self.merges:
            lines += ["", f"Entities folded into an existing Management ({len(self.merges)}) — review these:"]
            for source, target, how in self.merges:
                lines.append(f"  {source!r}")
                lines.append(f"      -> {target!r}  [{how}]")
        lines += ["", f"Airtable records written      {self.writes}"]
        return "\n".join(lines)


def _management_create_fields(unit: ManagementUnit) -> dict[str, Any]:
    """Full field set for a Management this tool is creating."""
    return {
        MgmtField.NAME: unit.name,
        MgmtField.NOTES: ORT_NOTE,
        MgmtField.TYPES: [config.AIRTABLE_ORT_TYPE_RECORD_ID],
        MgmtField.ORT_FLAG: True,
        MgmtField.COMPANY_TYPE: unit.company_type,
        MgmtField.PIPELINE: config.AIRTABLE_NEW_PIPELINE_STAGE,
    }


def _management_update_fields(unit: ManagementUnit, existing: dict) -> dict[str, Any]:
    """Additive-only update for a Management that already existed.

    The record did not originate here, so it gets no ORT note, no ORT
    checkbox, no Types tag and no Pipeline change. The only thing filled in
    is Contact Company Type (ORT) — an ORT-owned column — and only when
    it is empty.
    """
    if existing.get(MgmtField.COMPANY_TYPE):
        return {}
    return {MgmtField.COMPANY_TYPE: unit.company_type}


def _contact_create_fields(contact: SourceContact, mgmt_id: str) -> dict[str, Any]:
    fields: dict[str, Any] = {
        ContactField.NAME: contact.name,
        ContactField.NOTES: ORT_NOTE,
        ContactField.ORT_FLAG: True,
        ContactField.MANAGEMENT: [mgmt_id],
    }
    if contact.email:
        fields[ContactField.EMAIL] = contact.email
    if contact.phone:
        fields[ContactField.PHONE] = contact.phone
    if contact.title:
        fields[ContactField.TITLE] = contact.title
    phone_type = _PHONE_TYPE_MAP.get((contact.phone_type or "").strip().lower())
    if phone_type:
        fields[ContactField.PHONE_TYPE] = phone_type
    return fields


def _contact_update_fields(contact: SourceContact, existing: dict, mgmt_id: str) -> dict[str, Any]:
    """Additive-only update: fill blanks, never overwrite, never claim origin.

    The Management link is only set when the contact has none — re-pointing
    or appending links on contacts owned by another source is what produces
    tangled many-to-many Management records.
    """
    fields = existing.get("fields", {})
    updates: dict[str, Any] = {}

    if contact.email and not normalize_email(fields.get(ContactField.EMAIL)):
        updates[ContactField.EMAIL] = contact.email
    if contact.phone and not normalize_phone(fields.get(ContactField.PHONE)):
        updates[ContactField.PHONE] = contact.phone
    if contact.title and not (fields.get(ContactField.TITLE) or "").strip():
        updates[ContactField.TITLE] = contact.title
    if not fields.get(ContactField.PHONE_TYPE):
        phone_type = _PHONE_TYPE_MAP.get((contact.phone_type or "").strip().lower())
        if phone_type:
            updates[ContactField.PHONE_TYPE] = phone_type
    if not (fields.get(ContactField.NAME) or "").strip():
        updates[ContactField.NAME] = contact.name
    if not (fields.get(ContactField.MANAGEMENT) or []):
        updates[ContactField.MANAGEMENT] = [mgmt_id]

    return updates


def sync(dry_run: bool = False, limit: int | None = None) -> SyncReport:
    """Push every eligible researched entity into the Airtable base."""
    report = SyncReport()

    log.info("airtable_sync.loading_source")
    units, gate = load_units(limit=limit)
    report.gate = gate.as_dict()
    report.units = len(units)
    log.info(
        "airtable_sync.source_loaded",
        units=len(units),
        eligible_contacts=gate.kept,
        dropped=gate.contacts_seen - gate.kept,
    )

    with AirtableClient(
        config.AIRTABLE_API_KEY, config.AIRTABLE_BASE_ID, dry_run=dry_run
    ) as client:
        index = BaseIndex(client)
        log.info("airtable_sync.indexing_base")
        index.load()
        log.info(
            "airtable_sync.base_indexed",
            managements=len(index.mgmt_by_id),
            contacts=len(index.contact_by_email) + len(index.contact_by_phone),
            addresses=len(index.address_by_key),
        )

        for unit in units:
            _sync_unit(unit, client, index, report, dry_run)

        report.writes = client.writes

    return report


def _sync_unit(
    unit: ManagementUnit,
    client: AirtableClient,
    index: BaseIndex,
    report: SyncReport,
    dry_run: bool,
) -> None:
    """Resolve one entity's Management, then its Contacts and Addresses."""
    mgmt_id, how = index.find_management(unit)
    report.match_reasons[how] += 1

    if mgmt_id is None:
        report.mgmt_created += 1
        if dry_run:
            # Stable placeholder so downstream counting stays honest.
            mgmt_id = f"dry-run:{unit.entity_id}"
        else:
            created = client.create_records(
                config.AIRTABLE_MANAGEMENT_TABLE_ID,
                [{"fields": _management_create_fields(unit)}],
            )
            if not created:
                log.warning("airtable_sync.management_create_failed", entity=unit.name)
                return
            mgmt_id = created[0]["id"]
        index.register_management(mgmt_id, unit)
    else:
        report.mgmt_matched += 1
        target = index.mgmt_by_id.get(mgmt_id, {}).get(MgmtField.NAME) or mgmt_id
        # Matching "John Smith" onto the record already reading "SMITH, JOHN"
        # is the same person, not a merge of two entities.
        same_record = normalize_name(target) == normalize_name(unit.name) or (
            unit.is_person and person_key(target) == person_key(unit.name)
        )
        if not same_record:
            report.merges.append((unit.name, target, how))
            log.info(
                "airtable_sync.entity_merged",
                entity=unit.name, into=target, via=how,
            )
        updates = _management_update_fields(unit, index.mgmt_by_id.get(mgmt_id, {}))
        if updates:
            report.mgmt_updated += 1
            client.update_records(
                config.AIRTABLE_MANAGEMENT_TABLE_ID,
                [{"id": mgmt_id, "fields": updates}],
            )

    _sync_contacts(unit, mgmt_id, client, index, report, dry_run)
    _sync_addresses(unit, mgmt_id, client, index, report, dry_run)


def _sync_contacts(
    unit: ManagementUnit,
    mgmt_id: str,
    client: AirtableClient,
    index: BaseIndex,
    report: SyncReport,
    dry_run: bool,
) -> None:
    to_create: list[tuple[SourceContact, dict]] = []
    pending_entries: list[dict] = []
    to_update: list[dict] = []

    for contact in unit.contacts:
        existing = index.find_contact_identity(contact)
        if existing and existing.get("pending"):
            # Already queued for creation earlier in this run — its record
            # does not exist yet, so there is nothing to patch.
            report.contacts_unchanged += 1
            continue
        if existing:
            updates = _contact_update_fields(contact, existing, mgmt_id)
            if updates:
                report.contacts_updated += 1
                to_update.append({"id": existing["id"], "fields": updates})
                existing["fields"].update(updates)
            else:
                report.contacts_unchanged += 1
            continue

        report.contacts_created += 1
        to_create.append((contact, {"fields": _contact_create_fields(contact, mgmt_id)}))
        # Reserve immediately so two identical contacts inside the same unit
        # do not both get created.
        pending_entries.append(
            index.register_contact(
                f"pending:{contact.contact_id}", contact, mgmt_id, pending=True
            )
        )

    if to_update:
        client.update_records(config.AIRTABLE_CONTACTS_TABLE_ID, to_update)

    if to_create:
        created = client.create_records(
            config.AIRTABLE_CONTACTS_TABLE_ID, [payload for _, payload in to_create]
        )
        if not dry_run:
            if len(created) != len(to_create):
                log.warning(
                    "airtable_sync.contact_create_count_mismatch",
                    requested=len(to_create), created=len(created), entity=unit.name,
                )
            # Promote the reservations in place: the same entry objects sit
            # in both the email and phone indexes, so mutating them here
            # makes the real record visible everywhere at once.
            for entry, record in zip(pending_entries, created, strict=False):
                entry["id"] = record["id"]
                entry["pending"] = False


def _sync_addresses(
    unit: ManagementUnit,
    mgmt_id: str,
    client: AirtableClient,
    index: BaseIndex,
    report: SyncReport,
    dry_run: bool,
) -> None:
    to_create: list[tuple[str, dict]] = []
    to_update: list[dict] = []

    for address in unit.addresses:
        existing = index.address_by_key.get(address_key(address))
        if existing and existing.get("pending"):
            # Queued for creation earlier in this run; no record to patch.
            report.addresses_unchanged += 1
            continue
        if existing:
            links = list(existing["fields"].get(AddressField.MANAGEMENT) or [])
            if mgmt_id in links:
                report.addresses_unchanged += 1
                continue
            # An address can legitimately have several managements; append.
            links.append(mgmt_id)
            report.addresses_linked += 1
            to_update.append({"id": existing["id"], "fields": {AddressField.MANAGEMENT: links}})
            existing["fields"][AddressField.MANAGEMENT] = links
            continue

        report.addresses_created += 1
        to_create.append(
            (
                address,
                {
                    "fields": {
                        AddressField.ADDRESS: address,
                        AddressField.MANAGEMENT: [mgmt_id],
                    }
                },
            )
        )
        index.register_address(
            f"pending:{address_key(address)}", address, mgmt_id, pending=True
        )

    if to_update:
        client.update_records(config.AIRTABLE_ADDRESS_TABLE_ID, to_update)

    if to_create:
        created = client.create_records(
            config.AIRTABLE_ADDRESS_TABLE_ID, [payload for _, payload in to_create]
        )
        if not dry_run:
            if len(created) != len(to_create):
                log.warning(
                    "airtable_sync.address_create_count_mismatch",
                    requested=len(to_create), created=len(created), entity=unit.name,
                )
            for (address, _), record in zip(to_create, created, strict=False):
                index.address_by_key[address_key(address)] = {
                    "id": record["id"],
                    "pending": False,
                    "fields": {
                        AddressField.ADDRESS: address,
                        AddressField.MANAGEMENT: [mgmt_id],
                    },
                }
