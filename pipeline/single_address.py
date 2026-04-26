"""
pipeline/single_address.py — Run the full ingest + pierce + enrich
pipeline for a single address.

Used by the Airtable webhook listener. The logic is lifted from
test_addresses.py — same six steps (geocode → HPD → ACRIS → pierce →
enrich → return summary) — but parameterized to accept a single address
or its components, and structured so it can be called from a FastAPI
handler (returns awaitable).

Dedup happens BEFORE running anything: if the BBL already exists in
Supabase `properties`, we short-circuit and return the existing record
without re-running the pipeline. This protects spend and avoids
duplicate work.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import httpx
import structlog

from config import config
from database.client import (
    db,
    upsert_property,
    upsert_entity,
    upsert_contact,
    upsert_property_role,
)
from enrichment.llc_piercer import pierce_entity
from enrichment.contact import enrich_pending_signers

log = structlog.get_logger(__name__)

ACRIS_DEED_DOC_TYPES = ("DEED", "DEED, LE", "DEED, RC", "DEED, TS", "DEED, PC")

HPD_CONTACT_TYPES = {
    "CorporateOwner": ("llc", "owner"),
    "Owner":           ("individual", "owner"),
    "IndividualOwner": ("individual", "owner"),
    "JointOwner":      ("individual", "owner"),
    "HeadOfficer":     ("individual", "owner"),
    "Agent":           ("management_company", "manager"),
    "Officer":         ("individual", "owner"),
}


# ============================================================
# Result types
# ============================================================

@dataclass
class AddressResult:
    """Structured result returned to the webhook caller."""
    address: str                         # normalized address
    bbl: Optional[str]                   # 10-digit Borough-Block-Lot
    hpd_reg_id: Optional[str]            # most recent HPD registration ID, if any
    property_id: Optional[str]           # Supabase properties.id (UUID)
    cached: bool                         # True if BBL already existed before this run
    geocoded: bool                       # True if NYC GeoSearch returned a BBL
    error: Optional[str] = None          # Human-readable error if pipeline aborted

    def to_dict(self) -> dict:
        return {
            "address": self.address,
            "bbl": self.bbl,
            "hpd_reg_id": self.hpd_reg_id,
            "property_id": self.property_id,
            "cached": self.cached,
            "geocoded": self.geocoded,
            "error": self.error,
        }


# ============================================================
# Step 1: Geocode → BBL
# ============================================================

async def _geocode(address: str, client: httpx.AsyncClient) -> Optional[dict]:
    """NYC Planning Labs GeoSearch → BBL + normalized address."""
    url = "https://geosearch.planninglabs.nyc/v2/search"
    try:
        resp = await client.get(url, params={"text": address, "size": 1}, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.error("geocode.error", address=address, error=str(e))
        return None

    features = resp.json().get("features", [])
    if not features:
        return None

    props = features[0]["properties"]
    pad_bbl = (
        (props.get("addendum") or {}).get("pad", {}).get("bbl", "")
        or props.get("pad_bbl", "")
    )
    if not pad_bbl or pad_bbl == "0":
        return None

    return {
        "bbl": pad_bbl,
        "address": props.get("label", address),
        "borough": props.get("borough", ""),
        "zip": props.get("postalcode", ""),
    }


# ============================================================
# Dedup: have we already researched this BBL?
# ============================================================

def _existing_property(bbl: str) -> Optional[dict]:
    """Return existing properties row for this BBL, if any."""
    res = (
        db().table("properties")
        .select("id, bbl, hpd_reg_id, address, house_number, street_name")
        .eq("bbl", bbl)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    return rows[0] if rows else None


# ============================================================
# Step 2: HPD ingest for a single BBL
# ============================================================

async def _ingest_hpd(
    bbl: str, address: str, borough: str, zip_code: str,
    client: httpx.AsyncClient,
) -> Optional[tuple[str, Optional[str]]]:
    """Returns (property_id, hpd_reg_id) or None if no HPD registration."""
    boro = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:].lstrip("0") or "0"

    params = {
        "$where": f"boroid='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 5,
        "$order": "lastregistrationdate DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(config.HPD_REGISTRATIONS_URL, params=params, timeout=20)
    regs = resp.json() if resp.status_code == 200 else []
    if not regs:
        return None

    reg = regs[0]
    house_num = reg.get("housenumber", "").strip()
    street_nm = reg.get("streetname", "").strip()
    prop_address = f"{house_num} {street_nm}".strip() or address
    hpd_reg_id = reg.get("registrationid")

    property_id = upsert_property(bbl, {
        "bbl": bbl,
        "borough": borough,
        "block": bbl[1:6],
        "lot": bbl[6:],
        "address": prop_address,
        "house_number": house_num or None,
        "street_name": street_nm or None,
        "zip_code": zip_code or reg.get("zipcode"),
        "unit_count": reg.get("unitcount"),
        "hpd_reg_id": hpd_reg_id,
        "raw_data": reg,
    })

    # Pull contacts for active registrations.
    reg_ids = [r["registrationid"] for r in regs]
    ids_clause = " OR ".join(f"registrationid='{rid}'" for rid in reg_ids)
    c_params = {"$where": ids_clause, "$limit": 50}
    if config.NYC_OPENDATA_APP_TOKEN:
        c_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    cresp = await client.get(config.HPD_CONTACTS_URL, params=c_params, timeout=20)
    contacts = cresp.json() if cresp.status_code == 200 else []

    for c in contacts:
        ctype = c.get("type", "")
        if ctype not in HPD_CONTACT_TYPES:
            continue
        entity_type, role = HPD_CONTACT_TYPES[ctype]

        corp = c.get("corporationname", "").strip()
        first = c.get("firstname", "").strip()
        last = c.get("lastname", "").strip()
        name = corp if corp else f"{first} {last}".strip()
        if not name:
            continue

        addr_parts = [c.get("businesshousenumber", ""), c.get("businessstreetname", "")]
        ent_addr = " ".join(p for p in addr_parts if p).strip()

        ent_id = upsert_entity(name, entity_type, extra={
            "address": ent_addr or None,
            "zip_code": c.get("businesszip") or None,
        })

        if first and last:
            upsert_contact(ent_id, {
                "first_name": first,
                "last_name": last,
                "full_name": f"{first} {last}",
                "source": "hpd",
                "confidence": 0.9,
            })

        upsert_property_role(property_id, ent_id, role, "hpd")

    return property_id, hpd_reg_id


# ============================================================
# Step 3: ACRIS mortgage data (supplements HPD owner data)
# ============================================================

async def _ingest_acris(bbl: str, property_id: str, client: httpx.AsyncClient) -> None:
    boro = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:].lstrip("0") or "0"

    params = {
        "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 20,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(config.ACRIS_LEGALS_URL, params=params, timeout=20)
    legals = resp.json() if resp.status_code == 200 else []
    if not legals:
        return

    doc_ids = [l["document_id"] for l in legals[:10] if l.get("document_id")]
    if not doc_ids:
        return

    ids_clause = " OR ".join(f"document_id='{did}'" for did in doc_ids)
    m_params = {
        "$where": f"({ids_clause}) AND doc_type IN ('MTGE','DEED','AL&R','AGMT')",
        "$limit": 10,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        m_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    mresp = await client.get(config.ACRIS_MASTER_URL, params=m_params, timeout=20)
    masters = mresp.json() if mresp.status_code == 200 else []
    if not masters:
        return

    for doc in masters[:5]:
        doc_id = doc.get("document_id", "")
        doc_type = doc.get("doc_type", "")
        doc_date = doc.get("doc_date", "")[:10] if doc.get("doc_date") else ""

        p_params = {"$where": f"document_id='{doc_id}'", "$limit": 20}
        if config.NYC_OPENDATA_APP_TOKEN:
            p_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        presp = await client.get(config.ACRIS_PARTIES_URL, params=p_params, timeout=20)
        parties = presp.json() if presp.status_code == 200 else []

        for party in parties:
            ptype = party.get("party_type", "")
            name = party.get("name", "").strip()
            if not name:
                continue

            if doc_type == "DEED" and ptype == "2":
                role = "owner"
                ent_type = "llc" if "LLC" in name.upper() else "individual"
            elif doc_type == "MTGE" and ptype == "1":
                role = "owner"
                ent_type = "llc" if "LLC" in name.upper() else "individual"
            else:
                continue

            addr_parts = [party.get("addr1", ""), party.get("addr2", ""),
                          party.get("city", ""), party.get("state", "")]
            ent_addr = ", ".join(p for p in addr_parts if p).strip(", ")

            ent_id = upsert_entity(name, ent_type, extra={"address": ent_addr or None})
            upsert_property_role(property_id, ent_id, role, "acris", extra={
                "doc_type": doc_type, "doc_date": doc_date,
            })


# ============================================================
# Step 3b: ACRIS deed fallback (no HPD registration)
# ============================================================

async def _ingest_acris_deed_fallback(
    bbl: str, address: str, borough: str, zip_code: str,
    client: httpx.AsyncClient,
) -> Optional[str]:
    boro = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:]))

    params = {
        "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 50,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(config.ACRIS_LEGALS_URL, params=params, timeout=20)
    legals = resp.json() if resp.status_code == 200 else []
    if not legals:
        return None

    doc_ids = list({l["document_id"] for l in legals if l.get("document_id")})[:30]

    types_clause = ",".join(f"'{t}'" for t in ACRIS_DEED_DOC_TYPES)
    ids_clause = ",".join(f"'{d}'" for d in doc_ids)
    m_params = {
        "$where": f"document_id in ({ids_clause}) AND doc_type in ({types_clause})",
        "$limit": 5,
        "$order": "recorded_datetime DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        m_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    mresp = await client.get(config.ACRIS_MASTER_URL, params=m_params, timeout=20)
    deeds = mresp.json() if mresp.status_code == 200 else []
    if not deeds:
        return None

    property_id = upsert_property(bbl, {
        "bbl": bbl,
        "borough": borough,
        "block": bbl[1:6],
        "lot": bbl[6:],
        "address": address,
        "zip_code": zip_code,
        "raw_data": {"source": "acris_deed_fallback"},
    })

    found_any = False
    for deed in deeds[:3]:
        doc_id = deed.get("document_id", "")
        doc_date = (deed.get("recorded_datetime") or deed.get("doc_date") or "")[:10]

        p_params = {
            "$where": f"document_id='{doc_id}' AND party_type='2'",
            "$limit": 10,
        }
        if config.NYC_OPENDATA_APP_TOKEN:
            p_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        presp = await client.get(config.ACRIS_PARTIES_URL, params=p_params, timeout=20)
        parties = presp.json() if presp.status_code == 200 else []

        for party in parties:
            name = party.get("name", "").strip()
            if not name:
                continue

            ent_type = "llc" if any(
                t in name.upper() for t in ("LLC", " CORP", " INC", " LP")
            ) else "individual"
            addr_parts = [party.get("addr1", ""), party.get("addr2", ""),
                          party.get("city", ""), party.get("state", "")]
            ent_addr = ", ".join(p for p in addr_parts if p).strip(", ")

            ent_id = upsert_entity(name, ent_type, extra={"address": ent_addr or None})
            upsert_property_role(property_id, ent_id, "owner", "acris_deed",
                                 extra={"doc_type": "DEED", "doc_date": doc_date})
            found_any = True

    return property_id if found_any else None


# ============================================================
# Step 4: Pierce LLCs on this property
# ============================================================

async def _pierce_property_entities(property_id: str) -> None:
    res = (
        db().table("property_roles")
        .select("entity_id, entities(*)")
        .eq("property_id", property_id)
        .eq("role", "owner")
        .execute()
    )
    llcs = [
        r["entities"] for r in (res.data or [])
        if r.get("entities", {}).get("entity_type") in ("llc", "corporation")
        and not r.get("entities", {}).get("is_pierced")
    ]

    for entity in llcs:
        try:
            await pierce_entity(entity)
        except Exception as e:
            log.error("pierce.error", entity=entity.get("name"), error=str(e))


# ============================================================
# Public entry point
# ============================================================

async def research_address(
    *,
    address: Optional[str] = None,
    house_number: Optional[str] = None,
    street_name: Optional[str] = None,
    borough: Optional[str] = None,
    skip_enrichment: bool = False,
) -> AddressResult:
    """
    Run the full pipeline for a single address.

    Either pass a free-form `address` string, or the structured fields
    `house_number` + `street_name` + `borough`. Structured fields are
    preferred — they geocode more reliably for NYC.

    Set skip_enrichment=True to skip the contact-enrichment phase, which
    is the slowest and most expensive step. Useful when called from a
    webhook context where we just need to surface the BBL/HPD ID quickly.
    """
    if not address:
        if not (house_number and street_name and borough):
            return AddressResult(
                address="(none)",
                bbl=None, hpd_reg_id=None, property_id=None,
                cached=False, geocoded=False,
                error="Must provide either `address` or `house_number`+`street_name`+`borough`",
            )
        address = f"{house_number} {street_name}, {borough}, NY"

    log.info("research_address.start", address=address)

    async with httpx.AsyncClient() as client:
        # 1. Geocode
        geo = await _geocode(address, client)
        if not geo:
            return AddressResult(
                address=address,
                bbl=None, hpd_reg_id=None, property_id=None,
                cached=False, geocoded=False,
                error="Could not geocode address — NYC GeoSearch returned no result",
            )

        bbl = geo["bbl"]

        # 2. Dedup — has this BBL already been researched?
        existing = _existing_property(bbl)
        if existing:
            log.info("research_address.cached", bbl=bbl, property_id=existing["id"])
            return AddressResult(
                address=existing.get("address") or geo["address"],
                bbl=bbl,
                hpd_reg_id=existing.get("hpd_reg_id"),
                property_id=existing["id"],
                cached=True,
                geocoded=True,
            )

        # 3. HPD ingest
        hpd_result = await _ingest_hpd(
            bbl, geo["address"], geo["borough"], geo["zip"], client
        )
        property_id: Optional[str] = None
        hpd_reg_id: Optional[str] = None
        if hpd_result:
            property_id, hpd_reg_id = hpd_result

        # 4. ACRIS deed fallback if no HPD record
        if not property_id:
            property_id = await _ingest_acris_deed_fallback(
                bbl, geo["address"], geo["borough"], geo["zip"], client
            )

        # 5. ACRIS supplemental ingest
        if property_id:
            await _ingest_acris(bbl, property_id, client)

        if not property_id:
            return AddressResult(
                address=geo["address"],
                bbl=bbl, hpd_reg_id=None, property_id=None,
                cached=False, geocoded=True,
                error="No HPD registration and no ACRIS deed found for this BBL",
            )

        # 6. LLC piercing
        try:
            await _pierce_property_entities(property_id)
        except Exception as e:
            log.error("research_address.pierce_error", error=str(e))

        # 7. Mark ACRIS PDF contacts as signers (so the enrichment step picks them up)
        try:
            db().table("contacts").update({"network_role": "signer"}).eq(
                "source", "acris_pdf"
            ).execute()
        except Exception as e:
            log.error("research_address.signer_mark_error", error=str(e))

    # 8. Contact enrichment — run in a thread pool because it uses time.sleep(90)
    #    to respect Anthropic rate limits, which would block the asyncio loop.
    if not skip_enrichment:
        try:
            await asyncio.to_thread(enrich_pending_signers, 20)
        except Exception as e:
            log.error("research_address.enrich_error", error=str(e))

    log.info("research_address.done", bbl=bbl, property_id=property_id)
    return AddressResult(
        address=geo["address"],
        bbl=bbl, hpd_reg_id=hpd_reg_id, property_id=property_id,
        cached=False, geocoded=True,
    )
