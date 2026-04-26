"""
webhook.py — FastAPI app for the Airtable address-feed integration.

Endpoints:
    GET  /                       -> liveness probe
    GET  /health                 -> deeper health (DB reachable, env sane)
    POST /webhook/airtable       -> receive an address from an Airtable
                                    automation, run the pipeline, write
                                    BBL + HPD ID back to the record.

Auth:
    Airtable automations send an `Authorization: Bearer <secret>` header.
    The secret is configured in Render as AIRTABLE_WEBHOOK_SECRET. Set the
    same value in the Airtable automation HTTP action.

Concurrency:
    Pipeline runs are launched as background tasks so the webhook returns
    202 quickly. Airtable's HTTP step has a tight timeout — running the
    full pipeline (which can take 1–3 minutes) inline would time out.
    The background task does the work and writes back to Airtable when done.

Flow:
    1. Airtable record's "Research" checkbox flips on -> record enters the
       "Research (Robot)" view.
    2. Airtable automation POSTs the row's house number, street name,
       borough, and record ID to /webhook/airtable.
    3. Service responds 202 immediately.
    4. Background task: dedup-check Supabase, run the pipeline if new,
       PATCH the BBL Number + HPD Building ID fields back to Airtable.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import structlog
from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from config import config, validate_required_config
from pipeline.single_address import research_address, AddressResult
from scheduler import build_async_scheduler, register_jobs

log = structlog.get_logger(__name__)


# ============================================================
# Lifespan: start the APScheduler alongside FastAPI
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Boot the scheduler when the FastAPI app starts; shut it down on exit."""
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.LOG_LEVEL, logging.INFO)
        ),
    )

    missing = validate_required_config()
    if missing:
        # Log loudly but don't kill the process — we still want /health
        # to come up so Render can show what's wrong.
        log.error("config.missing_required_keys", missing=missing)

    sched = build_async_scheduler()
    registered = register_jobs(sched)
    sched.start()
    log.info("webhook.startup", auto_search=config.AUTO_SEARCH_ENABLED, jobs=registered)
    app.state.scheduler = sched

    try:
        yield
    finally:
        log.info("webhook.shutdown")
        sched.shutdown(wait=False)


app = FastAPI(
    title="Owner Research Tool",
    description="Webhook listener + scheduler for the NYC property-owner research pipeline.",
    version="1.0.0",
    lifespan=lifespan,
)


# ============================================================
# Models
# ============================================================

class AirtableWebhookPayload(BaseModel):
    """Payload sent by the Airtable automation HTTP step.

    The automation should send:
        - record_id    : Airtable record ID (rec...)
        - house_number : "1100"
        - street_name  : "Bedford Ave"
        - borough      : "Brooklyn" (or one of the 5)
        - address (optional): full string fallback if structured fields missing

    Any other fields are ignored.
    """
    record_id: str = Field(..., description="Airtable record ID (rec...)")
    house_number: Optional[str] = None
    street_name: Optional[str] = None
    borough: Optional[str] = None
    address: Optional[str] = None  # free-form fallback

    def has_structured_address(self) -> bool:
        return bool(self.house_number and self.street_name and self.borough)


# ============================================================
# Auth
# ============================================================

def _check_auth(authorization: Optional[str]) -> None:
    """Validate the Bearer token sent by the Airtable automation."""
    if not config.AIRTABLE_WEBHOOK_SECRET:
        # Misconfigured — better to fail loudly than to accept any request.
        log.error("webhook.no_secret_configured")
        raise HTTPException(status_code=500, detail="Webhook secret not configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = authorization.removeprefix("Bearer ").strip()
    if token != config.AIRTABLE_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid webhook token")


# ============================================================
# Airtable writeback
# ============================================================

async def _writeback_to_airtable(
    record_id: str, bbl: Optional[str], hpd_reg_id: Optional[str]
) -> None:
    """PATCH the Airtable row with the discovered BBL Number + HPD Building ID."""
    if not config.AIRTABLE_API_KEY:
        log.warn("airtable.writeback_skipped_no_key", record_id=record_id)
        return
    if not (bbl or hpd_reg_id):
        log.info("airtable.writeback_skipped_no_data", record_id=record_id)
        return

    fields: dict = {}
    if bbl:
        fields[config.AIRTABLE_BBL_FIELD_ID] = bbl
    if hpd_reg_id:
        fields[config.AIRTABLE_HPD_FIELD_ID] = str(hpd_reg_id)

    url = (
        f"https://api.airtable.com/v0/{config.AIRTABLE_BASE_ID}/"
        f"{config.AIRTABLE_ADDRESS_TABLE_ID}/{record_id}"
    )
    headers = {
        "Authorization": f"Bearer {config.AIRTABLE_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {"fields": fields}

    async with httpx.AsyncClient() as client:
        try:
            resp = await client.patch(url, headers=headers, json=body, timeout=15)
            if resp.status_code >= 400:
                log.error(
                    "airtable.writeback_failed",
                    status=resp.status_code,
                    body=resp.text[:500],
                    record_id=record_id,
                )
            else:
                log.info(
                    "airtable.writeback_ok",
                    record_id=record_id, bbl=bbl, hpd_reg_id=hpd_reg_id,
                )
        except Exception as e:
            log.error("airtable.writeback_exception", error=str(e), record_id=record_id)


# ============================================================
# Background pipeline runner
# ============================================================

async def _run_pipeline_and_writeback(payload: AirtableWebhookPayload) -> None:
    """Run the full pipeline for one address and PATCH Airtable on completion."""
    log.info("webhook.pipeline_start", record_id=payload.record_id)

    try:
        kwargs: dict = {}
        if payload.has_structured_address():
            kwargs["house_number"] = payload.house_number
            kwargs["street_name"] = payload.street_name
            kwargs["borough"] = payload.borough
        elif payload.address:
            kwargs["address"] = payload.address
        else:
            log.error("webhook.no_address_in_payload", record_id=payload.record_id)
            return

        result: AddressResult = await research_address(**kwargs)

        log.info(
            "webhook.pipeline_done",
            record_id=payload.record_id,
            bbl=result.bbl,
            cached=result.cached,
            error=result.error,
        )

        await _writeback_to_airtable(payload.record_id, result.bbl, result.hpd_reg_id)
    except Exception as e:
        log.error("webhook.pipeline_exception", record_id=payload.record_id, error=str(e))


# ============================================================
# Routes
# ============================================================

@app.get("/")
async def root():
    return {"service": "owner-research-tool", "status": "ok"}


@app.get("/health")
async def health():
    """Lightweight health check — verifies env + DB reachability."""
    missing = validate_required_config()
    db_ok = True
    db_error: Optional[str] = None
    try:
        from database.client import db
        db().table("entities").select("id", count="exact").limit(1).execute()
    except Exception as e:
        db_ok = False
        db_error = str(e)[:200]

    return {
        "status": "ok" if (not missing and db_ok) else "degraded",
        "auto_search_enabled": config.AUTO_SEARCH_ENABLED,
        "missing_config": missing,
        "database_reachable": db_ok,
        "database_error": db_error,
    }


@app.post("/webhook/airtable", status_code=202)
async def airtable_webhook(
    payload: AirtableWebhookPayload,
    background_tasks: BackgroundTasks,
    authorization: Optional[str] = Header(default=None),
):
    """Receive an address from Airtable, kick off the pipeline asynchronously."""
    _check_auth(authorization)

    if not payload.has_structured_address() and not payload.address:
        raise HTTPException(
            status_code=400,
            detail=(
                "Payload must include either `address` (free-form) or all of "
                "`house_number` + `street_name` + `borough`."
            ),
        )

    log.info(
        "webhook.received",
        record_id=payload.record_id,
        structured=payload.has_structured_address(),
    )

    background_tasks.add_task(_run_pipeline_and_writeback, payload)

    return {
        "status": "accepted",
        "record_id": payload.record_id,
        "message": "Pipeline started; results will be written back to Airtable when complete.",
    }


# ============================================================
# Manual run helper (dev/debug)
# ============================================================

@app.post("/internal/run-now")
async def run_now(
    payload: AirtableWebhookPayload,
    authorization: Optional[str] = Header(default=None),
):
    """
    Synchronous run endpoint for debugging — waits for the pipeline to
    finish before returning. Same auth as the Airtable webhook. Don't
    point Airtable at this; it will time out.
    """
    _check_auth(authorization)
    if not payload.has_structured_address() and not payload.address:
        raise HTTPException(status_code=400, detail="Address fields missing")

    kwargs: dict = (
        {"house_number": payload.house_number, "street_name": payload.street_name,
         "borough": payload.borough}
        if payload.has_structured_address()
        else {"address": payload.address}
    )
    result = await research_address(**kwargs)
    await _writeback_to_airtable(payload.record_id, result.bbl, result.hpd_reg_id)
    return result.to_dict()


# ============================================================
# Standalone uvicorn entrypoint (production)
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(
        "webhook:app",
        host="0.0.0.0",
        port=port,
        log_level=config.LOG_LEVEL.lower(),
    )
