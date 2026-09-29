"""
ingest/pluto.py — NYC PLUTO lot characteristics, and the building-size gate.

PLUTO is the only NYC source that publishes a residential unit count and a
building class per BBL. HPD Registrations does not: the `tesw-yqqr` dataset
has no `unitcount` and no `buildingclassid` column at all, so the two
`reg.get(...)` calls that used to populate `properties.unit_count` and
`properties.building_class` always returned None and every row in the table
was NULL for both.

That mattered because the size signal is load-bearing in two places:

  * `enrichment/skip_filter.py` scores `unit_count >= 3` and penalises
    `max_units <= 2`. With the column NULL the first could never fire and
    the second always did, so the filter degraded to an `hpd_reg_id` check.
  * Nothing stopped ACRIS from storing 1-2 family homes. ACRIS records every
    deed transfer regardless of building size, so the single-family owners it
    brings in cost enrichment spend and land in the CRM as "Managements".

This module therefore does two jobs:

  1. `lookup()` — a cached, batched BBL -> PlutoLot resolver. Used as an
     ingest-time gate so lots below the unit threshold are never stored.
  2. `backfill_properties()` — fill the two columns for rows already stored.

The gate FAILS OPEN. A BBL that PLUTO does not know, or a PLUTO request that
errors, is admitted rather than dropped: silently discarding real ownership
data is far worse than carrying a few small buildings.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass

import httpx
import structlog

from config import config
from database.client import db
from database.retry import retry_external

log = structlog.get_logger(__name__)

# Socrata caps a single response at 50k rows. The gate queries by explicit
# BBL, so the practical limit is URL length, not row count.
_LOOKUP_BATCH = 150
_PAGE = 50000


# NYC building-class prefixes. The target is landlords; the thing to exclude
# is the owner-occupied home.
#
#   A = one-family dwelling   -> never a landlord property
#   B = two-family dwelling   -> the classic owner-upstairs/tenant-downstairs
#                                house, so out even though it has a tenant
#   S = primarily residential with a store below
#   K = store building, sometimes with apartments above
#
# S and K qualify on any residential unit at all: commercial space under the
# apartments means the owner is running a building, not living in a house.
# That is why they are not subject to the unit threshold.
_HOME_CLASSES = ("A", "B")
_MIXED_USE_CLASSES = ("S", "K")


def is_landlord_lot(units_res: int | None, bldg_class: str | None) -> bool:
    """Whether a lot looks like a landlord's building rather than a home.

    Fails open: an unknown size and an unknown class both qualify, because
    absence of evidence is not evidence of a single-family house.
    """
    klass = (bldg_class or "").strip().upper()
    if klass.startswith(_HOME_CLASSES):
        return False
    if klass.startswith(_MIXED_USE_CLASSES):
        # Mixed use is a landlord signal, but a store with no apartments is
        # just a shop — this is a residential-landlord pipeline.
        return (units_res or 0) >= 1
    if units_res is None:
        return True
    return units_res >= config.PLUTO_MIN_RESIDENTIAL_UNITS


@dataclass(frozen=True)
class PlutoLot:
    bbl: str
    units_res: int
    units_total: int
    bldg_class: str | None

    @property
    def qualifies(self) -> bool:
        """Whether this lot may be stored / synced. See is_landlord_lot().

        Reads `units_res`, not `units_total`: a 1-family house with a garage
        is still a 1-family house.
        """
        return is_landlord_lot(self.units_res, self.bldg_class)


# BBLs resolved during this process. PLUTO is a static annual release, so a
# result is good for the life of the run.
_CACHE: dict[str, PlutoLot | None] = {}


def _norm_bbl(value) -> str | None:
    """PLUTO returns bbl as a float-formatted string ('2054800111.00000000');
    ours are 10-char zero-padded text. Normalise to the latter."""
    if value is None:
        return None
    try:
        return f"{int(float(value)):010d}"
    except (TypeError, ValueError):
        return None


def _to_int(value) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


@retry_external(max_attempts=5)
async def _fetch_batch(client: httpx.AsyncClient, bbls: list[str]) -> list[dict]:
    """One PLUTO request for an explicit set of BBLs."""
    # PLUTO stores bbl as a number, so it must be compared numerically —
    # bbl='1000010001' matches nothing.
    where = " OR ".join(f"bbl={int(b)}" for b in bbls)
    params = {
        "$select": "bbl,unitsres,unitstotal,bldgclass",
        "$where": where,
        "$limit": len(bbls) * 2,
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN
    resp = await client.get(config.PLUTO_URL, params=params, timeout=60.0)
    resp.raise_for_status()
    return resp.json()


async def lookup(bbls: Iterable[str]) -> dict[str, PlutoLot]:
    """Resolve BBLs to PLUTO lot characteristics.

    Checks the in-process cache, then the `properties` table (already
    backfilled rows need no request), then PLUTO itself. Results found via
    the API are written back to `properties` so the next run is free.

    BBLs PLUTO does not know are absent from the returned dict — callers
    must treat "missing" as "unknown", never as "too small".
    """
    wanted = {b for b in bbls if b}
    found: dict[str, PlutoLot] = {}

    unresolved = set()
    for bbl in wanted:
        if bbl in _CACHE:
            if _CACHE[bbl] is not None:
                found[bbl] = _CACHE[bbl]
        else:
            unresolved.add(bbl)

    if unresolved:
        for chunk in _chunks(sorted(unresolved), 200):
            rows = (
                db().table("properties")
                .select("bbl,unit_count,building_class")
                .in_("bbl", chunk)
                .not_.is_("unit_count", "null")
                .execute()
            ).data or []
            for row in rows:
                lot = PlutoLot(
                    bbl=row["bbl"],
                    units_res=_to_int(row.get("unit_count")),
                    # properties.unit_count stores unitsres only, so a lot
                    # rehydrated from the table cannot distinguish the two.
                    # Nothing in the gate reads units_total; if that changes,
                    # this path needs its own column.
                    units_total=_to_int(row.get("unit_count")),
                    bldg_class=row.get("building_class"),
                )
                _CACHE[lot.bbl] = lot
                found[lot.bbl] = lot
                unresolved.discard(lot.bbl)

    if unresolved:
        fetched: dict[str, PlutoLot] = {}
        async with httpx.AsyncClient() as client:
            for chunk in _chunks(sorted(unresolved), _LOOKUP_BATCH):
                try:
                    rows = await _fetch_batch(client, chunk)
                except Exception as exc:  # gate must fail open, whatever broke
                    log.warning(
                        "pluto.lookup_failed",
                        count=len(chunk),
                        error=f"{type(exc).__name__}: {exc}".rstrip(": "),
                    )
                    continue
                for row in rows:
                    bbl = _norm_bbl(row.get("bbl"))
                    if not bbl:
                        continue
                    fetched[bbl] = PlutoLot(
                        bbl=bbl,
                        units_res=_to_int(row.get("unitsres")),
                        units_total=_to_int(row.get("unitstotal")),
                        bldg_class=(row.get("bldgclass") or "").strip() or None,
                    )
                await asyncio.sleep(0.1)  # be polite

        found.update(fetched)
        for bbl in unresolved:
            # Cache misses too, so an unknown BBL is not re-requested all run.
            _CACHE[bbl] = fetched.get(bbl)

    return found


async def admits(bbls: Iterable[str]) -> set[str]:
    """The subset of `bbls` that may be stored.

    A BBL is admitted when PLUTO says it is a landlord property, OR when
    PLUTO has no record of it (fail open).
    """
    wanted = {b for b in bbls if b}
    if not wanted or not config.PLUTO_GATE_ENABLED:
        return wanted
    lots = await lookup(wanted)
    return {b for b in wanted if b not in lots or lots[b].qualifies}


def cached(bbl: str) -> PlutoLot | None:
    """The already-resolved lot for a BBL, or None.

    Only ever returns what a preceding lookup()/admits() call put in the
    cache — it never issues a request, so callers inside a per-row loop
    cannot accidentally turn one batched fetch into thousands.
    """
    return _CACHE.get(bbl)


def property_fields(lot: PlutoLot | None) -> dict:
    """The `properties` columns PLUTO owns, for merging into an upsert."""
    if lot is None:
        return {}
    return {"unit_count": lot.units_res, "building_class": lot.bldg_class}


def _chunks(items, size):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ============================================================
# Backfill
# ============================================================

async def backfill_properties(limit: int | None = None, only_missing: bool = True) -> dict:
    """Populate unit_count / building_class for properties already stored.

    Streams the whole PLUTO release once rather than querying BBL-by-BBL:
    at ~860k lots that is ~18 requests against ~900 batched ones.
    """
    stats = {"properties": 0, "matched": 0, "updated": 0, "unmatched": 0}

    rows: list[dict] = []
    offset = 0
    while True:
        # A fresh builder per page, and an explicit order: paging with
        # .range() over an unordered select lets Postgres return rows in a
        # different order per request, which silently skips and duplicates.
        query = db().table("properties").select("id,bbl,unit_count,building_class").order("id")
        if only_missing:
            query = query.is_("unit_count", "null")
        page = (query.range(offset, offset + 999).execute()).data or []
        rows.extend(page)
        if len(page) < 1000 or (limit and len(rows) >= limit):
            break
        offset += 1000
    if limit:
        rows = rows[:limit]

    by_bbl = {r["bbl"]: r for r in rows if r.get("bbl")}
    stats["properties"] = len(by_bbl)
    log.info("pluto.backfill_start", properties=len(by_bbl))
    if not by_bbl:
        return stats

    updates: list[dict] = []
    async with httpx.AsyncClient() as client:
        offset = 0
        while True:
            params = {
                "$select": "bbl,unitsres,unitstotal,bldgclass",
                "$limit": _PAGE,
                "$offset": offset,
                "$order": ":id",
            }
            if config.NYC_OPENDATA_APP_TOKEN:
                params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN
            resp = await client.get(config.PLUTO_URL, params=params, timeout=120.0)
            resp.raise_for_status()
            page = resp.json()
            if not page:
                break

            for row in page:
                bbl = _norm_bbl(row.get("bbl"))
                target = by_bbl.get(bbl) if bbl else None
                if target is None:
                    continue
                stats["matched"] += 1
                units = _to_int(row.get("unitsres"))
                klass = (row.get("bldgclass") or "").strip() or None
                if target.get("unit_count") == units and target.get("building_class") == klass:
                    continue
                updates.append({"id": target["id"], "unit_count": units, "building_class": klass})

            log.info("pluto.backfill_progress", scanned=offset + len(page),
                     matched=stats["matched"], pending_writes=len(updates))
            if len(page) < _PAGE:
                break
            offset += _PAGE
            await asyncio.sleep(0.1)

    for chunk in _chunks(updates, 500):
        db().table("properties").upsert(chunk, on_conflict="id").execute()
        stats["updated"] += len(chunk)

    stats["unmatched"] = stats["properties"] - stats["matched"]
    log.info("pluto.backfill_done", **stats)
    return stats


async def run():
    """CLI entry point: backfill every property missing lot characteristics."""
    return await backfill_properties()
