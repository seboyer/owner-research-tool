"""
ingest/acris.py — NYC ACRIS Property Transfer Ingestion

ACRIS (Automated City Register Information System) records every property
deed, mortgage, and transfer in NYC. We mine it for:
  1. Recent deed transfers — who are the buyers? Many won't be in HPD.
  2. Historical purchase chains — useful for LLC piercing.

Datasets used:
  - ACRIS Real Property Master:  /resource/bnx9-e6tj.json  (doc metadata)
  - ACRIS Real Property Parties: /resource/636b-3b5g.json  (buyer/seller)
  - ACRIS Real Property Legals:  /resource/8h5j-fqxa.json  (lot/block/borough)

Strategy:
  - Filter Master for DEED documents in the last N days
  - For each deed, get buyer from Parties (party_type=1 = buyer)
  - Get BBL from Legals
  - Upsert buyer as entity, link to property
"""

import asyncio
from datetime import datetime, timedelta

import httpx
import structlog

from config import config
from database.client import (
    already_seen, mark_seen, checksum,
    upsert_property, upsert_entity, upsert_property_role,
    start_ingestion_log, finish_ingestion_log,
)
from database.retry import retry_external
from ingest import pluto
from ingest.hpd import _TRANSIENT_NET_ERRORS, paginate  # reuse the paginator + transient set

log = structlog.get_logger(__name__)

# ACRIS document types to ingest (deeds only — not mortgages/satisfactions)
DEED_DOC_TYPES = {"DEED", "DEED, BARGAIN AND SALE", "DEED, QUITCLAIM", "DEED, CORRECTION"}

# ACRIS party types: 1=buyer/grantee, 2=seller/grantor
BUYER_PARTY_TYPE = "1"
SELLER_PARTY_TYPE = "2"


async def _fetch_deed_doc_ids(since_date: datetime) -> list[str]:
    """Get all deed document IDs from ACRIS Master since a given date."""
    since_str = since_date.strftime("%Y-%m-%dT%H:%M:%S")
    where = f"doc_type in ('DEED', 'DEED, BARGAIN AND SALE', 'DEED, QUITCLAIM') AND good_through_date >= '{since_str}'"

    doc_ids = []
    async for row in paginate(config.ACRIS_MASTER_URL, where=where):
        doc_ids.append(row["document_id"])

    log.info("acris.deed_ids_fetched", count=len(doc_ids))
    return doc_ids


@retry_external(max_attempts=5)
async def _fetch_parties_chunk(client: httpx.AsyncClient, chunk: list[str]) -> list[dict]:
    """Fetch buyer parties for one chunk of document IDs from Socrata."""
    ids_clause = ",".join(f"'{d}'" for d in chunk)
    where = f"document_id in ({ids_clause}) AND party_type='{BUYER_PARTY_TYPE}'"
    params = {
        "$where": where,
        "$limit": 5000,
        "$order": "document_id",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN
    resp = await client.get(config.ACRIS_PARTIES_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def _fetch_parties_for_docs(doc_ids: list[str]) -> dict[str, list[dict]]:
    """
    Fetch buyer parties for a batch of document IDs.
    Returns dict: {document_id: [party_rows]}
    """
    parties: dict[str, list[dict]] = {}

    # Socrata $in clause
    chunk_size = 100
    async with httpx.AsyncClient() as client:
        for i in range(0, len(doc_ids), chunk_size):
            chunk = doc_ids[i : i + chunk_size]
            try:
                rows = await _fetch_parties_chunk(client, chunk)
            except Exception:
                rows = []
            for row in rows:
                doc_id = row["document_id"]
                parties.setdefault(doc_id, []).append(row)
            await asyncio.sleep(0.1)

    return parties


@retry_external(max_attempts=5)
async def _fetch_legals_chunk(client: httpx.AsyncClient, chunk: list[str]) -> list[dict]:
    """Fetch legals for one chunk of document IDs from Socrata."""
    ids_clause = ",".join(f"'{d}'" for d in chunk)
    where = f"document_id in ({ids_clause})"
    params = {
        "$where": where,
        "$limit": 5000,
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN
    resp = await client.get(config.ACRIS_LEGALS_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def _fetch_legals_for_docs(doc_ids: list[str]) -> dict[str, dict]:
    """
    Fetch legal (lot/block/borough) for each document ID.
    Returns dict: {document_id: legal_row}
    """
    legals: dict[str, dict] = {}

    chunk_size = 100
    async with httpx.AsyncClient() as client:
        for i in range(0, len(doc_ids), chunk_size):
            chunk = doc_ids[i : i + chunk_size]
            try:
                rows = await _fetch_legals_chunk(client, chunk)
            except Exception:
                rows = []
            for row in rows:
                doc_id = row["document_id"]
                if doc_id not in legals:
                    legals[doc_id] = row
            await asyncio.sleep(0.1)

    return legals


def _build_bbl(legal: dict) -> str | None:
    """Construct a BBL string from ACRIS legal row."""
    boro = legal.get("borough", "")
    block = legal.get("block", "").zfill(5)
    lot = legal.get("lot", "").zfill(4)
    if boro and block and lot:
        return f"{boro}{block}{lot}"
    return None


def _parse_buyer_name(party: dict) -> tuple[str, str]:
    """
    Extract entity name and type from an ACRIS party row.
    Returns (name, entity_type).
    """
    name = party.get("name", "").strip()
    # ACRIS stores names as "LAST NAME, FIRST NAME" for individuals
    # and as the full name for companies
    entity_type = "llc" if "LLC" in name.upper() else \
                  "corporation" if any(t in name.upper() for t in ["CORP", "INC", "CO.", "TRUST"]) else \
                  "individual" if "," in name else "unknown"
    return name, entity_type


# ============================================================
# Main ACRIS Ingestion
# ============================================================

async def ingest_acris_deeds():
    """
    Ingest recent deed transfers from ACRIS.

    Run this daily to catch new property purchases. New buyers who aren't
    in HPD yet (e.g., they just bought a building) will get queued for
    Zoominfo enrichment.
    """
    log_id = start_ingestion_log("acris_deeds")
    stats = {"records_fetched": 0, "records_created": 0, "records_skipped": 0}

    try:
        since = datetime.utcnow() - timedelta(days=config.ACRIS_LOOKBACK_DAYS)
        log.info("acris.starting", since=since.isoformat())

        # Step 1: Get all deed document IDs in window
        doc_ids = await _fetch_deed_doc_ids(since)
        if not doc_ids:
            log.info("acris.no_new_deeds")
            finish_ingestion_log(log_id, stats)
            return

        stats["records_fetched"] = len(doc_ids)

        # Filter already-seen documents
        new_doc_ids = [
            d for d in doc_ids
            if not already_seen("acris_deed", d)
        ]
        log.info("acris.new_docs", count=len(new_doc_ids), skipped=len(doc_ids) - len(new_doc_ids))
        stats["records_skipped"] = len(doc_ids) - len(new_doc_ids)

        if not new_doc_ids:
            finish_ingestion_log(log_id, stats)
            return

        # Step 2: Fetch buyers + lot info in parallel
        parties, legals = await asyncio.gather(
            _fetch_parties_for_docs(new_doc_ids),
            _fetch_legals_for_docs(new_doc_ids),
        )

        # Step 3: Resolve building size once for the whole batch.
        #
        # ACRIS covers every deed transfer, including 1-2 family homes whose
        # buyers are private homeowners rather than landlords. Gating here
        # rather than downstream is what keeps them out of the enrichment
        # spend entirely. Fails open — see ingest/pluto.py.
        batch_bbls = {
            bbl for d in new_doc_ids
            if (legal := legals.get(d)) and (bbl := _build_bbl(legal))
        }
        admitted = await pluto.admits(batch_bbls)
        gated_out = len(batch_bbls) - len(admitted)
        if gated_out:
            log.info(
                "acris.size_gate",
                bbls=len(batch_bbls),
                admitted=len(admitted),
                excluded=gated_out,
                min_units=config.PLUTO_MIN_RESIDENTIAL_UNITS,
            )

        # Step 4: Process each deed
        for doc_id in new_doc_ids:
            doc_parties = parties.get(doc_id, [])
            legal = legals.get(doc_id)

            if not doc_parties or not legal:
                mark_seen("acris_deed", doc_id)
                continue

            bbl = _build_bbl(legal)
            if not bbl:
                mark_seen("acris_deed", doc_id)
                continue

            if bbl not in admitted:
                # Below the unit threshold. mark_seen so the next run does
                # not re-fetch and re-gate the same document.
                mark_seen("acris_deed", doc_id)
                stats["records_skipped"] += 1
                continue

            try:
                # Upsert property
                borough_names = {"1": "MANHATTAN", "2": "BRONX", "3": "BROOKLYN", "4": "QUEENS", "5": "STATEN ISLAND"}
                property_id = upsert_property(bbl, {
                    "borough": borough_names.get(str(legal.get("borough", "")), ""),
                    "block": str(legal.get("block", "")).zfill(5),
                    "lot": str(legal.get("lot", "")).zfill(4),
                    "street_name": legal.get("street_name", ""),
                    "house_number": legal.get("address_number", ""),
                    "address": f"{legal.get('address_number', '')} {legal.get('street_name', '')}".strip(),
                    # The gate already resolved this BBL, so persisting it
                    # here costs nothing and spares the next run the lookup.
                    **pluto.property_fields(pluto.cached(bbl)),
                })

                # Upsert each buyer
                for party in doc_parties:
                    buyer_name, entity_type = _parse_buyer_name(party)
                    if not buyer_name or buyer_name in ("UNKNOWN", "N/A"):
                        continue

                    entity_id = upsert_entity(buyer_name, entity_type, extra={
                        "address": party.get("addr1", ""),
                        "city": party.get("city", ""),
                        "state": party.get("state", "NY"),
                        "zip_code": party.get("zip", ""),
                        "raw_data": {"acris_party": party, "acris_legal": legal},
                    })

                    upsert_property_role(property_id, entity_id, "owner", "acris", extra={
                        "raw_data": {"document_id": doc_id},
                    })

                mark_seen("acris_deed", doc_id)
                stats["records_created"] += 1

            except _TRANSIENT_NET_ERRORS as e:
                log.warning(
                    "acris.transient_skip",
                    doc_id=doc_id,
                    error=f"{type(e).__name__}: {e}".rstrip(": "),
                )
                continue

        finish_ingestion_log(log_id, stats)
        log.info("acris.complete", **stats)

    except Exception as e:
        # Capture exception type — str(e) is empty for some exceptions
        # (asyncio.TimeoutError, bare Exception()), which produced blank
        # error_message rows that were hard to debug.
        err = f"{type(e).__name__}: {e}".rstrip(": ")
        log.error("acris.error", error=err, exc_info=True)
        finish_ingestion_log(log_id, stats, status="failed", error=err)
        raise


async def run():
    await ingest_acris_deeds()
