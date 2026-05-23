"""
scripts/backfill_acris_owners.py — One-off ACRIS owner backfill by zip code.

The daily acris_deeds ingest only covers the last `ACRIS_LOOKBACK_DAYS` (30 by
default), so properties whose most recent deed predates that window sit in the
DB with no entity-owner linked via property_roles. This script targets a single
zip code, walks every property in it that has no current property_role, and
queries ACRIS directly (no date filter) for the most recent deed on that BBL.
The buyer becomes the entity-owner, linked via property_roles(role='owner',
source='acris'). The entity then flows into the normal enrichment queue via
`_determine_enrichment_types`.

Usage:
    python -m scripts.backfill_acris_owners --zip 11216
    python -m scripts.backfill_acris_owners --zip 11216 --dry-run
    python -m scripts.backfill_acris_owners --zip 11216 --concurrency 10

Idempotent: properties that already have a current property_role are skipped.
"""

import argparse
import asyncio
from typing import Any

import httpx
import structlog

from config import config
from database.client import (
    db, parse_bbl, upsert_entity, upsert_property_role,
)
from database.retry import retry_external
from ingest.acris import DEED_DOC_TYPES, BUYER_PARTY_TYPE, _parse_buyer_name

log = structlog.get_logger(__name__)


@retry_external(max_attempts=5)
async def _query_socrata(client: httpx.AsyncClient, url: str, params: dict) -> list[dict]:
    if config.NYC_OPENDATA_APP_TOKEN:
        params = {**params, "$$app_token": config.NYC_OPENDATA_APP_TOKEN}
    resp = await client.get(url, params=params, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


async def _find_most_recent_deed_buyer(
    client: httpx.AsyncClient, bbl: str
) -> tuple[str | None, dict | None]:
    """For a BBL, return (document_id, party_row) for the most recent DEED."""
    parsed = parse_bbl(bbl)
    if parsed is None:
        return None, None
    boro, block, lot = parsed

    # Step 1: Legals — get every doc_id ever recorded on this property.
    legals = await _query_socrata(
        client,
        config.ACRIS_LEGALS_URL,
        {
            "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
            "$select": "document_id",
            "$limit": 1000,
        },
    )
    doc_ids = list({row["document_id"] for row in legals if row.get("document_id")})
    if not doc_ids:
        return None, None

    # Step 2: Master — narrow to deed doc_types, take most recent by recorded_datetime.
    # ACRIS Socrata caps "$in" lists at a few hundred; chunk if necessary.
    deed_doc_types_clause = ",".join(f"'{t}'" for t in DEED_DOC_TYPES)
    masters: list[dict] = []
    for i in range(0, len(doc_ids), 100):
        chunk = doc_ids[i : i + 100]
        ids_clause = ",".join(f"'{d}'" for d in chunk)
        rows = await _query_socrata(
            client,
            config.ACRIS_MASTER_URL,
            {
                "$where": f"document_id in ({ids_clause}) AND doc_type in ({deed_doc_types_clause})",
                "$select": "document_id,doc_type,recorded_datetime",
                "$limit": 1000,
                "$order": "recorded_datetime DESC",
            },
        )
        masters.extend(rows)
    if not masters:
        return None, None

    masters.sort(key=lambda r: r.get("recorded_datetime", ""), reverse=True)
    top_doc_id = masters[0]["document_id"]

    # Step 3: Parties — buyer (party_type=1) for that deed.
    parties = await _query_socrata(
        client,
        config.ACRIS_PARTIES_URL,
        {
            "$where": f"document_id='{top_doc_id}' AND party_type='{BUYER_PARTY_TYPE}'",
            "$limit": 20,
        },
    )
    if not parties:
        return top_doc_id, None
    return top_doc_id, parties[0]


async def _backfill_one(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    prop: dict,
    dry_run: bool,
    stats: dict,
) -> None:
    bbl = prop.get("bbl", "")
    if not bbl or bbl.startswith("hpd_bldg_"):
        stats["bad_bbl"] += 1
        return
    async with sem:
        try:
            doc_id, party = await _find_most_recent_deed_buyer(client, bbl)
        except Exception as e:
            log.warning("backfill.query_error", bbl=bbl, error=f"{type(e).__name__}: {e}")
            stats["query_error"] += 1
            return

    if not party:
        if doc_id is None:
            stats["no_deeds_on_bbl"] += 1
        else:
            stats["no_buyer_on_deed"] += 1
        return

    buyer_name, entity_type = _parse_buyer_name(party)
    if not buyer_name or buyer_name in ("UNKNOWN", "N/A"):
        stats["bad_buyer_name"] += 1
        return

    if dry_run:
        log.info(
            "backfill.dry_run",
            bbl=bbl, address=prop.get("address"),
            buyer=buyer_name, entity_type=entity_type, doc_id=doc_id,
        )
        stats["would_link"] += 1
        return

    try:
        entity_id = upsert_entity(buyer_name, entity_type, extra={
            "address": party.get("addr1", ""),
            "city": party.get("city", ""),
            "state": party.get("state", "NY"),
            "zip_code": party.get("zip", ""),
            "raw_data": {"acris_party": party, "backfill_source": "scripts/backfill_acris_owners"},
        })
        upsert_property_role(prop["id"], entity_id, "owner", "acris", extra={
            "raw_data": {"document_id": doc_id, "backfill": True},
        })
        stats["linked"] += 1
        log.info("backfill.linked", bbl=bbl, buyer=buyer_name, entity_id=entity_id)
    except Exception as e:
        log.warning("backfill.write_error", bbl=bbl, error=f"{type(e).__name__}: {e}")
        stats["write_error"] += 1


async def backfill_zip(zip_code: str, dry_run: bool, concurrency: int) -> dict:
    """Walk every property in `zip_code` with no current property_role and backfill."""
    # Pull all properties in the zip
    props: list[dict] = []
    page = 0
    while True:
        r = (
            db().table("properties")
            .select("id, bbl, address")
            .eq("zip_code", zip_code)
            .order("id")
            .range(page * 1000, (page + 1) * 1000 - 1)
            .execute()
        )
        rows = r.data or []
        props.extend(rows)
        if len(rows) < 1000:
            break
        page += 1

    log.info("backfill.properties_in_zip", zip=zip_code, count=len(props))

    # Filter to those WITHOUT a current property_role
    prop_ids = [p["id"] for p in props]
    has_role: set[str] = set()
    for i in range(0, len(prop_ids), 100):
        chunk = prop_ids[i : i + 100]
        r = (
            db().table("property_roles")
            .select("property_id")
            .in_("property_id", chunk)
            .eq("is_current", True)
            .execute()
        )
        for row in (r.data or []):
            has_role.add(row["property_id"])
    targets = [p for p in props if p["id"] not in has_role]
    log.info(
        "backfill.targets",
        total_in_zip=len(props), already_linked=len(has_role), to_backfill=len(targets),
    )

    stats = {
        "to_backfill": len(targets),
        "linked": 0, "would_link": 0,
        "no_deeds_on_bbl": 0, "no_buyer_on_deed": 0,
        "bad_bbl": 0, "bad_buyer_name": 0,
        "query_error": 0, "write_error": 0,
    }

    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[
            _backfill_one(client, sem, p, dry_run, stats) for p in targets
        ])

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill ACRIS owners for a zip code")
    parser.add_argument("--zip", required=True, help="5-digit zip code, e.g. 11216")
    parser.add_argument("--dry-run", action="store_true", help="Log proposed changes without writing")
    parser.add_argument("--concurrency", type=int, default=10, help="Parallel Socrata queries")
    args = parser.parse_args()

    stats = asyncio.run(backfill_zip(args.zip, args.dry_run, args.concurrency))

    print("\n=== backfill complete ===")
    for k, v in stats.items():
        print(f"  {k:24} {v}")


if __name__ == "__main__":
    main()
