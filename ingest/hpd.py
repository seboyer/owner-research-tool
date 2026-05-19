"""
ingest/hpd.py — NYC HPD Registration Data Ingestion

HPD (Housing Preservation & Development) requires all buildings with 3+ units
to register annually, naming an owner and managing agent. This is our single
richest source of landlord data.

Datasets used:
  - HPD Registrations:        https://data.cityofnewyork.us/resource/tesw-yqqr.json
  - HPD Registration Contacts: https://data.cityofnewyork.us/resource/feu5-w2e2.json

Contact types we care about:
  CorporateOwner, Owner, HeadOfficer, Agent, IndividualOwner, JointOwner
"""

import asyncio
from datetime import date
from typing import AsyncIterator

import httpx
import structlog

from config import config
from database.client import (
    already_seen, mark_seen, checksum,
    upsert_property, upsert_entity, upsert_contact,
    upsert_property_role, start_ingestion_log, finish_ingestion_log,
)
from database.retry import retry_external

log = structlog.get_logger(__name__)

# Transient network errors caught inside per-row loops so a single dropped
# Supabase connection doesn't kill a multi-hundred-thousand-row ingestion.
# Skipped rows aren't mark_seen'd, so they re-attempt on the next pipeline
# run. Real HTTP errors (HTTPStatusError) and code bugs still propagate.
_TRANSIENT_NET_ERRORS = (httpx.NetworkError, httpx.TimeoutException)

# HPD contact types → our entity types
CONTACT_TYPE_MAP = {
    "CorporateOwner":  "llc",
    "Owner":           "individual",
    "IndividualOwner": "individual",
    "JointOwner":      "individual",
    "HeadOfficer":     "individual",
    "Agent":           "management_company",
    "Lessee":          "individual",
    "Officer":         "individual",
}

# Roles (HPD contact type → our property role)
ROLE_MAP = {
    "CorporateOwner":  "owner",
    "Owner":           "owner",
    "IndividualOwner": "owner",
    "JointOwner":      "owner",
    "HeadOfficer":     "owner",
    "Agent":           "manager",
    "Officer":         "owner",
}

SOCRATA_PAGE_SIZE = 5000


@retry_external(max_attempts=5)
async def _fetch_page(client: httpx.AsyncClient, url: str, offset: int, where: str = None) -> list[dict]:
    """Fetch a single paginated page from the Socrata API."""
    params = {
        "$limit": SOCRATA_PAGE_SIZE,
        "$offset": offset,
        "$order": ":id",
    }
    if where:
        params["$where"] = where
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(url, params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def paginate(url: str, where: str = None) -> AsyncIterator[dict]:
    """Async generator that yields all rows from a Socrata endpoint."""
    async with httpx.AsyncClient() as client:
        offset = 0
        while True:
            page = await _fetch_page(client, url, offset, where)
            if not page:
                break
            for row in page:
                yield row
            if len(page) < SOCRATA_PAGE_SIZE:
                break
            offset += SOCRATA_PAGE_SIZE
            await asyncio.sleep(0.1)  # be polite


# ============================================================
# Main HPD Ingestion
# ============================================================

async def ingest_hpd_contacts():
    """
    Pull all active HPD registration contacts and write entities,
    contacts, properties, and property_roles to Supabase.

    This is the primary ingestion job — run it first and then weekly.
    """
    log_id = start_ingestion_log("hpd_contacts")
    stats = {"records_fetched": 0, "records_created": 0, "records_updated": 0, "records_skipped": 0}

    try:
        # The HPD Contacts dataset (feu5-w2e2) has no date column to filter on,
        # so we pull all contacts and let already_seen() dedup against prior runs.
        # Stale contacts attached to expired registrations are filtered downstream
        # via property_roles.is_current.
        async for contact in paginate(config.HPD_CONTACTS_URL):
            stats["records_fetched"] += 1

            reg_id = contact.get("registrationid", "")
            contact_type = contact.get("type", "")

            if contact_type not in CONTACT_TYPE_MAP:
                continue  # skip types we don't care about

            try:
                # Build a dedup key from registration + contact type + name
                external_id = f"{reg_id}_{contact_type}_{contact.get('firstname', '')}_{contact.get('lastname', '')}_{contact.get('corporationname', '')}"
                chk = checksum({k: contact.get(k, "") for k in ["firstname", "lastname", "corporationname", "businesshousenumber", "businessstreetname"]})

                if already_seen("hpd_contact", external_id, chk):
                    stats["records_skipped"] += 1
                    continue

                # --- Determine entity name ---
                corp_name = contact.get("corporationname", "").strip()
                first = contact.get("firstname", "").strip()
                last = contact.get("lastname", "").strip()
                entity_name = corp_name if corp_name else f"{first} {last}".strip()

                if not entity_name:
                    mark_seen("hpd_contact", external_id, chk)
                    continue

                entity_type = CONTACT_TYPE_MAP[contact_type]
                role = ROLE_MAP.get(contact_type, "owner")

                # --- Build entity address ---
                addr_parts = [
                    contact.get("businesshousenumber", ""),
                    contact.get("businessstreetname", ""),
                ]
                entity_address = " ".join(p for p in addr_parts if p).strip()

                # --- Upsert Entity ---
                entity_id = upsert_entity(entity_name, entity_type, extra={
                    "address": entity_address or None,
                    "zip_code": contact.get("businesszip", None),
                    "state": contact.get("businessstate", "NY") or "NY",
                })

                # --- Upsert Contact (if individual with name) ---
                if first and last:
                    upsert_contact(entity_id, {
                        "first_name": first,
                        "last_name": last,
                        "full_name": f"{first} {last}",
                        "source": "hpd",
                        "confidence": 0.9,
                    })

                # --- Upsert Property ---
                # We need to build a BBL (borough+block+lot) from HPD reg to link property
                # HPD contacts table doesn't have BBL directly — we join via registrationid
                # We'll store the reg_id on the entity for now and resolve BBL in a separate pass
                # For now, store minimal property linkage

                # Try to get building info from the contact row itself
                bldg_id = contact.get("buildingid", "")
                if bldg_id:
                    bbl = f"hpd_bldg_{bldg_id}"  # Placeholder until we join with registrations table
                    property_id = upsert_property(bbl, {
                        "hpd_reg_id": reg_id,
                        "raw_data": {
                            "hpd_registration_id": reg_id,
                            "hpd_building_id": bldg_id,
                        },
                    })
                    upsert_property_role(property_id, entity_id, role, "hpd")

                mark_seen("hpd_contact", external_id, chk)
                stats["records_created"] += 1

            except _TRANSIENT_NET_ERRORS as e:
                log.warning(
                    "hpd_contacts.transient_skip",
                    reg_id=reg_id,
                    error=f"{type(e).__name__}: {e}".rstrip(": "),
                )
                continue

            if stats["records_fetched"] % 1000 == 0:
                log.info("hpd.progress", **stats)

        finish_ingestion_log(log_id, stats)
        log.info("hpd.complete", **stats)

    except Exception as e:
        # Capture exception type — str(e) is empty for some exceptions
        # (e.g. asyncio.TimeoutError, bare Exception()), which left us
        # with blank error_message rows that were hard to debug.
        err = f"{type(e).__name__}: {e}".rstrip(": ")
        log.error("hpd.error", error=err, exc_info=True)
        finish_ingestion_log(log_id, stats, status="failed", error=err)
        raise


async def ingest_hpd_registrations():
    """
    Pull HPD Registrations to get BBL (Borough-Block-Lot) for each building.
    This enriches the properties table with real BBLs and addresses.
    Run after ingest_hpd_contacts() to resolve the placeholder BBLs.
    """
    log_id = start_ingestion_log("hpd_registrations")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        # tesw-yqqr has no lifecyclestage column. Filter on registrationenddate
        # to skip expired registrations — currently-valid ones have an end date
        # in the future (HPD registrations are renewed annually).
        where_clause = f"registrationenddate >= '{date.today().isoformat()}'"

        async for reg in paginate(config.HPD_REGISTRATIONS_URL, where=where_clause):
            stats["records_fetched"] += 1

            reg_id = reg.get("registrationid", "")
            boro = reg.get("boroid", "")
            block = reg.get("block", "").zfill(5)
            lot = reg.get("lot", "").zfill(4)

            if not (boro and block and lot):
                continue

            try:
                bbl = f"{boro}{block}{lot}"
                # The Socrata column is `zip`, not `zipcode`. The previous name
                # silently returned None and left every property's zip_code NULL,
                # which in turn made the zipcode allowlist's unknown-zip bypass
                # match every entity. Adding zip to the checksum forces a
                # re-ingest of rows that were stored without it.
                zip_code = (reg.get("zip") or "").strip() or None
                chk = checksum({"boro": boro, "block": block, "lot": lot, "zip": zip_code or ""})

                if already_seen("hpd_registration", reg_id, chk):
                    stats["records_skipped"] += 1
                    continue

                borough_names = {"1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN", "4": "QUEENS", "5": "STATEN ISLAND"}

                address = f"{reg.get('housenumber', '').strip()} {reg.get('streetname', '').strip()}".strip()

                upsert_property(bbl, {
                    "bbl": bbl,
                    "borough": borough_names.get(str(boro), boro),
                    "block": block,
                    "lot": lot,
                    "address": address,
                    "zip_code": zip_code,
                    "unit_count": reg.get("unitcount", None),
                    "building_class": reg.get("buildingclassid", None),
                    "hpd_reg_id": reg_id,
                    "raw_data": reg,
                })

                mark_seen("hpd_registration", reg_id, chk)
                stats["records_created"] += 1

            except _TRANSIENT_NET_ERRORS as e:
                log.warning(
                    "hpd_registrations.transient_skip",
                    reg_id=reg_id,
                    error=f"{type(e).__name__}: {e}".rstrip(": "),
                )
                continue

        finish_ingestion_log(log_id, stats)
        log.info("hpd_registrations.complete", **stats)

    except Exception as e:
        err = f"{type(e).__name__}: {e}".rstrip(": ")
        log.error("hpd_registrations.error", error=err, exc_info=True)
        finish_ingestion_log(log_id, stats, status="failed", error=err)
        raise


async def run():
    """Entry point: run both HPD jobs.

    Sub-jobs are run independently — a failure in registrations doesn't
    block contacts (and vice versa). Each sub-job already records its
    own status to ingestion_log, so the stage-level error surfaces via
    aggregate re-raise at the end if any failed.
    """
    log.info("hpd.starting")
    errors: list[tuple[str, str]] = []

    for name, fn in (
        ("hpd_registrations", ingest_hpd_registrations),
        ("hpd_contacts", ingest_hpd_contacts),
    ):
        try:
            await fn()
        except Exception as e:
            err = f"{type(e).__name__}: {e}".rstrip(": ")
            log.error("hpd.subjob_failed", subjob=name, error=err, exc_info=True)
            errors.append((name, err))

    if errors:
        # Aggregate so the orchestrator's stage_hpd_full marks the stage failed.
        raise RuntimeError(
            "hpd subjob(s) failed: " + "; ".join(f"{n}: {e}" for n, e in errors)
        )
    log.info("hpd.done")
