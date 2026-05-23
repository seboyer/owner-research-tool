"""Admin API router — zipcode allowlist management + pipeline run viewer.

All endpoints require HTTP Basic Auth. Password is compared against
config.ADMIN_PASSWORD. Returns:
  - 500 if ADMIN_PASSWORD is not configured
  - 401 (with WWW-Authenticate header) if Authorization header is missing/malformed
  - 403 if password is wrong
"""

from __future__ import annotations

import base64
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from admin.allowlist import _borough_from_bbl, get_allowlist, invalidate_cache
from admin.template import ADMIN_HTML
from config import config
from database.client import db

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# ============================================================
# Auth
# ============================================================

def _check_admin_auth(authorization: Optional[str] = None) -> None:
    """Validate HTTP Basic Auth against config.ADMIN_PASSWORD."""
    if not config.ADMIN_PASSWORD:
        raise HTTPException(status_code=500, detail="ADMIN_PASSWORD not configured")

    if not authorization or not authorization.startswith("Basic "):
        raise HTTPException(
            status_code=401,
            detail="Missing or malformed Authorization header",
            headers={"WWW-Authenticate": 'Basic realm="admin"'},
        )

    try:
        decoded = base64.b64decode(authorization.removeprefix("Basic ").strip()).decode()
        _user, _, password = decoded.partition(":")
    except Exception:
        raise HTTPException(
            status_code=401,
            detail="Malformed Basic Auth credentials",
            headers={"WWW-Authenticate": 'Basic realm="admin"'},
        )

    if password != config.ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid admin password")


def _auth(authorization: Optional[str] = Header(default=None)) -> None:
    _check_admin_auth(authorization)


# ============================================================
# HTML page
# ============================================================

@router.get("", response_class=HTMLResponse)
async def admin_page(_auth: None = Depends(_auth)) -> HTMLResponse:
    """Serve the admin dashboard."""
    return HTMLResponse(content=ADMIN_HTML)


# ============================================================
# Status
# ============================================================

@router.get("/api/status")
async def api_status(_auth: None = Depends(_auth)) -> dict:
    """Return worker state (from the scheduler_status heartbeat) + queue depths.

    auto_search_enabled reflects the WORKER's env var, not this web service's.
    worker_stale is True when no heartbeat has arrived in >120s (poll interval
    is 30s, so 4 missed ticks).
    """
    from datetime import datetime, timedelta, timezone
    from database.client import get_scheduler_status

    queue_counts: dict[str, int] = {}
    for etype in ("llc_pierce", "company_enrich", "multi_source"):
        res = (
            db()
            .table("enrichment_queue")
            .select("id", count="exact")
            .eq("enrichment_type", etype)
            .execute()
        )
        queue_counts[etype] = res.count or 0

    # Wrap in try/except so a missing scheduler_status table (migration 009
    # not yet run) doesn't 500 the whole endpoint and break the dashboard.
    try:
        sched = get_scheduler_status()
    except Exception as e:
        log.warning("admin.api_status.scheduler_status_unavailable", error=str(e))
        sched = None

    auto_search_enabled = False
    weekly_pipeline_day = None
    worker_last_seen_at = None
    worker_stale = True

    if sched:
        auto_search_enabled = bool(sched.get("auto_search_enabled", False))
        weekly_pipeline_day = sched.get("weekly_pipeline_day")
        worker_last_seen_at = sched.get("last_seen_at")
        if worker_last_seen_at:
            last_dt = (
                datetime.fromisoformat(worker_last_seen_at.replace("Z", "+00:00"))
                if isinstance(worker_last_seen_at, str)
                else worker_last_seen_at
            )
            worker_stale = (datetime.now(timezone.utc) - last_dt) > timedelta(seconds=120)

    return {
        "auto_search_enabled": auto_search_enabled,
        "weekly_pipeline_day": weekly_pipeline_day,
        "worker_last_seen_at": worker_last_seen_at,
        "worker_stale": worker_stale,
        "queue": queue_counts,
    }


# ============================================================
# Zipcodes
# ============================================================

class ZipToggleBody(BaseModel):
    zip_code: str
    enabled: bool


def _fetch_all_property_zips() -> list[str]:
    """Fetch every non-null zip_code from properties via pagination.

    PostgREST defaults to a 1000-row response cap, so a single .execute()
    silently truncated the result and only the first 1000 properties (all
    Manhattan, in insertion order) contributed to the zipcode counts.
    """
    page_size = 1000
    offset = 0
    all_zips: list[str] = []
    while True:
        res = (
            db()
            .table("properties")
            .select("zip_code")
            .neq("zip_code", "")
            .not_.is_("zip_code", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = res.data or []
        for row in rows:
            z = (row.get("zip_code") or "").strip()
            if z:
                all_zips.append(z)
        if len(rows) < page_size:
            break
        offset += page_size
    return all_zips


@router.get("/api/zipcodes")
async def api_zipcodes(_auth: None = Depends(_auth)) -> list[dict]:
    """Discover all zips in the properties table, upsert missing ones, return full list."""
    all_zips = _fetch_all_property_zips()
    known_zips: set[str] = set(all_zips)

    # Upsert any new zips (disabled by default — must be opted in via admin)
    existing_res = db().table("zipcode_allowlist").select("zip_code").execute()
    existing_zips: set[str] = {r["zip_code"] for r in (existing_res.data or [])}
    new_zips = known_zips - existing_zips
    if new_zips:
        db().table("zipcode_allowlist").upsert(
            [{"zip_code": z, "enabled": False} for z in new_zips],
            on_conflict="zip_code",
        ).execute()
        log.info("admin.zipcodes.discovered", count=len(new_zips))
        invalidate_cache()

    # Step 2: return joined list with property counts
    allowlist_res = db().table("zipcode_allowlist").select("zip_code, enabled, updated_at").execute()
    allowlist_map: dict[str, dict] = {
        r["zip_code"]: r for r in (allowlist_res.data or [])
    }

    # Count properties per zip
    zip_counts: dict[str, int] = {}
    for z in all_zips:
        zip_counts[z] = zip_counts.get(z, 0) + 1

    result = []
    for zip_code, entry in allowlist_map.items():
        result.append({
            "zip_code": zip_code,
            "enabled": entry["enabled"],
            "property_count": zip_counts.get(zip_code, 0),
            "updated_at": entry.get("updated_at"),
        })

    result.sort(key=lambda x: x["zip_code"])
    return result


@router.post("/api/zipcodes/toggle")
async def api_zipcode_toggle(
    body: ZipToggleBody,
    _auth: None = Depends(_auth),
) -> dict:
    """Enable or disable a single zipcode."""
    db().table("zipcode_allowlist").upsert(
        {"zip_code": body.zip_code, "enabled": body.enabled, "updated_at": "now()"},
        on_conflict="zip_code",
    ).execute()
    invalidate_cache()
    log.info("admin.zipcode.toggled", zip_code=body.zip_code, enabled=body.enabled)
    return {"ok": True}


@router.post("/api/zipcodes/bulk")
async def api_zipcode_bulk(
    enabled: bool = Query(..., description="true to enable all, false to disable all"),
    _auth: None = Depends(_auth),
) -> dict:
    """Set all zipcodes to the given enabled state."""
    db().table("zipcode_allowlist").update(
        {"enabled": enabled, "updated_at": "now()"}
    ).neq("zip_code", "").execute()
    invalidate_cache()
    log.info("admin.zipcodes.bulk_toggle", enabled=enabled)
    return {"ok": True}


# ============================================================
# Boroughs (fallback gate for properties with NULL zip)
# ============================================================

class BoroughToggleBody(BaseModel):
    borough_code: str
    enabled: bool


def _count_null_zip_properties_by_borough() -> dict[str, int]:
    """Count NULL/empty-zip properties grouped by borough (BBL first digit).

    These are exactly the properties whose enrichment is gated by the borough
    fallback toggles, so the counts give admins useful "how many am I
    affecting" context next to each switch.
    """
    page_size = 1000
    offset = 0
    counts: dict[str, int] = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    while True:
        # Supabase .or_() lets us match zip_code IS NULL OR zip_code = ''
        res = (
            db()
            .table("properties")
            .select("bbl")
            .or_("zip_code.is.null,zip_code.eq.")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        rows = res.data or []
        for row in rows:
            boro = _borough_from_bbl(row.get("bbl"))
            if boro:
                counts[boro] += 1
        if len(rows) < page_size:
            break
        offset += page_size
    return counts


@router.get("/api/boroughs")
async def api_boroughs(_auth: None = Depends(_auth)) -> list[dict]:
    """Return the 5 borough switches with their enabled state + count of
    NULL-zip properties (the rows actually gated by each switch)."""
    res = (
        db()
        .table("borough_allowlist")
        .select("borough_code, borough_name, enabled, updated_at")
        .execute()
    )
    rows = res.data or []

    null_zip_counts = _count_null_zip_properties_by_borough()
    for r in rows:
        r["null_zip_property_count"] = null_zip_counts.get(r["borough_code"], 0)

    rows.sort(key=lambda r: r["borough_code"])
    return rows


@router.post("/api/boroughs/toggle")
async def api_borough_toggle(
    body: BoroughToggleBody,
    _auth: None = Depends(_auth),
) -> dict:
    """Enable or disable a single borough fallback switch."""
    if body.borough_code not in ("1", "2", "3", "4", "5"):
        raise HTTPException(status_code=400, detail="borough_code must be '1'-'5'")
    db().table("borough_allowlist").update(
        {"enabled": body.enabled, "updated_at": "now()"}
    ).eq("borough_code", body.borough_code).execute()
    invalidate_cache()
    log.info("admin.borough.toggled", borough_code=body.borough_code, enabled=body.enabled)
    return {"ok": True}


# ============================================================
# Pipeline runs
# ============================================================

@router.get("/api/runs")
async def api_runs(_auth: None = Depends(_auth)) -> list[dict]:
    """Return the last 50 ingestion_log rows, most recent first."""
    res = (
        db()
        .table("ingestion_log")
        .select(
            "id, source, run_started_at, run_finished_at, "
            "records_fetched, records_created, records_updated, "
            "records_skipped, records_no_match, "
            "status, error_message, cost_estimated_usd, stopped_by_cost_cap"
        )
        .order("run_started_at", desc=True)
        .limit(50)
        .execute()
    )
    return res.data or []


# ============================================================
# Manual pipeline triggers
#
# The web service can't run the pipeline directly — sync DB calls would
# block the event loop and trip Render's /health timeout. Instead we
# insert into pipeline_triggers and the worker service polls it every
# 30s (see scheduler._job_poll_triggers).
# ============================================================

@router.get("/api/run-status")
async def api_run_status(_auth: None = Depends(_auth)) -> dict:
    """Return current trigger state per pipeline: 'idle', 'pending', or 'running'."""
    from database.client import get_active_triggers
    return get_active_triggers()


def _queue_trigger(pipeline: str) -> dict:
    from database.client import get_active_triggers, request_pipeline_trigger
    active = get_active_triggers()
    if active.get(pipeline) in ("pending", "running"):
        raise HTTPException(
            status_code=409,
            detail=f"{pipeline.capitalize()} pipeline is already {active[pipeline]}",
        )
    row = request_pipeline_trigger(pipeline)
    log.info("admin.manual_run.queued", pipeline=pipeline, trigger_id=row["id"])
    return {"ok": True, "queued": pipeline, "trigger_id": row["id"]}


@router.post("/run/daily")
async def run_daily(_auth: None = Depends(_auth)) -> dict:
    return _queue_trigger("daily")


@router.post("/run/weekly")
async def run_weekly(_auth: None = Depends(_auth)) -> dict:
    return _queue_trigger("weekly")


# ============================================================
# Skipped entities
# ============================================================

class RequeueBody(BaseModel):
    entity_id: Optional[str] = None
    reason: Optional[str] = None


@router.get("/api/skipped")
async def api_skipped(
    reason: Optional[str] = Query(default=None),
    min_score: Optional[float] = Query(default=None, ge=0.0, le=1.0),
    max_score: Optional[float] = Query(default=None, ge=0.0, le=1.0),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    sort: str = Query(default="score_desc"),
    _auth: None = Depends(_auth),
) -> dict:
    """Return currently-skipped entities (requeued_at IS NULL) with optional filters."""
    try:
        # Count query
        count_q = (
            db()
            .table("enrichment_skip_log")
            .select("entity_id", count="exact")
            .is_("requeued_at", "null")
        )
        if reason:
            count_q = count_q.eq("reason", reason)
        if min_score is not None:
            count_q = count_q.gte("score", min_score)
        if max_score is not None:
            count_q = count_q.lte("score", max_score)
        count_res = count_q.execute()
        total = count_res.count or 0

        # Data query
        data_q = (
            db()
            .table("enrichment_skip_log")
            .select("entity_id, reason, score, evidence, skipped_at")
            .is_("requeued_at", "null")
        )
        if reason:
            data_q = data_q.eq("reason", reason)
        if min_score is not None:
            data_q = data_q.gte("score", min_score)
        if max_score is not None:
            data_q = data_q.lte("score", max_score)
        if sort == "skipped_at_desc":
            data_q = data_q.order("skipped_at", desc=True)
        else:
            data_q = data_q.order("score", desc=True)  # score_desc — highest score first (closest to threshold)
        data_q = data_q.range(offset, offset + limit - 1)
        data_res = data_q.execute()
        rows = data_res.data or []

        # Enrich with entity name/type via a batched lookup
        entity_ids = [r["entity_id"] for r in rows if r.get("entity_id")]
        entity_map: dict[str, dict] = {}
        if entity_ids:
            ent_res = (
                db()
                .table("entities")
                .select("id, name, entity_type")
                .in_("id", entity_ids)
                .execute()
            )
            for e in (ent_res.data or []):
                entity_map[e["id"]] = e

        result_rows = []
        for r in rows:
            eid = r.get("entity_id")
            ent = entity_map.get(eid, {})
            result_rows.append({
                "entity_id": eid,
                "name": ent.get("name"),
                "entity_type": ent.get("entity_type"),
                "reason": r.get("reason"),
                "score": r.get("score"),
                "evidence": r.get("evidence"),
                "skipped_at": r.get("skipped_at"),
            })

        return {"rows": result_rows, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        log.warning("admin.api_skipped.error", error=str(e))
        return {"rows": [], "total": 0, "limit": limit, "offset": offset}


@router.post("/api/skipped/requeue")
async def api_skipped_requeue(
    body: RequeueBody,
    _auth: None = Depends(_auth),
) -> dict:
    """Re-queue a single skipped entity or all entities with a given reason."""
    from database.client import requeue_skipped

    if not body.entity_id and not body.reason:
        raise HTTPException(status_code=400, detail="Must provide entity_id or reason")

    if body.entity_id:
        types = requeue_skipped(body.entity_id)
        log.info("admin.requeue_skipped.single", entity=body.entity_id, types=types)
        return {"requeued": [body.entity_id], "types": types}

    # Bulk by reason
    res = (
        db()
        .table("enrichment_skip_log")
        .select("entity_id")
        .eq("reason", body.reason)
        .is_("requeued_at", "null")
        .limit(1000)
        .execute()
    )
    entity_ids = [r["entity_id"] for r in (res.data or [])]
    requeued = []
    for eid in entity_ids:
        try:
            requeue_skipped(eid)
            requeued.append(eid)
        except Exception as e:
            log.warning("admin.requeue_skipped.bulk_item_error", entity=eid, error=str(e))
    log.info("admin.requeue_skipped.bulk", reason=body.reason, count=len(requeued))
    return {"requeued": requeued, "count": len(requeued)}


@router.get("/api/skipped/summary")
async def api_skipped_summary(_auth: None = Depends(_auth)) -> dict:
    """Return counts of currently-skipped entities grouped by reason."""
    try:
        res = (
            db()
            .table("enrichment_skip_log")
            .select("reason")
            .is_("requeued_at", "null")
            .execute()
        )
        rows = res.data or []
        by_reason: dict[str, int] = {}
        for r in rows:
            reason = r.get("reason") or "unknown"
            by_reason[reason] = by_reason.get(reason, 0) + 1
        total = sum(by_reason.values())
        return {"by_reason": by_reason, "total": total}
    except Exception as e:
        log.warning("admin.api_skipped_summary.error", error=str(e))
        return {"by_reason": {}, "total": 0}
