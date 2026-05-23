"""
pipeline/orchestrator.py — Pipeline Orchestrator

Defines the full pipeline as a sequence of stages, each of which can be
run independently or as part of the full daily/weekly schedule.

Stage order and rationale:
  1. HPD Full Sync       (weekly)   — foundational: ~80K buildings, owners, agents
  2. ACRIS Delta         (daily)    — new property transfers from last N days
  3. WoW Portfolio       (weekly)   — group LLCs by apparent owner
  4. LLC Piercing        (ongoing)  — find real owners of building LLCs
     4a. ACRIS PDF Signers           — mortgage doc signature extraction
     4b. AI Agentic Reasoning        — Claude web research
  5. Company Enrich     (ongoing)  — multi-source cascade for corporate entities
  6. Multi-Source Enrich (ongoing)  — Whitepages/PropertyRadar/Web for individuals
"""

import asyncio
from datetime import datetime
from typing import Callable

import structlog

from config import config

log = structlog.get_logger(__name__)


# ============================================================
# Stage Definitions
# ============================================================

async def stage_hpd_full():
    """HPD full sync — run weekly on Sundays."""
    from ingest.hpd import run
    log.info("pipeline.stage_start", stage="hpd_full")
    await run()
    log.info("pipeline.stage_done", stage="hpd_full")


async def stage_acris_delta():
    """ACRIS delta sync — run daily."""
    from ingest.acris import run
    log.info("pipeline.stage_start", stage="acris_delta")
    await run()
    log.info("pipeline.stage_done", stage="acris_delta")


async def stage_wow_portfolio():
    """Who Owns What portfolio enrichment — run weekly."""
    from ingest.whoownswhat import run
    log.info("pipeline.stage_start", stage="wow_portfolio")
    await run()
    log.info("pipeline.stage_done", stage="wow_portfolio")


async def stage_llc_piercing(batch_size: int = 20):
    """LLC piercing — run daily."""
    from enrichment.llc_piercer import run_batch
    log.info("pipeline.stage_start", stage="llc_piercing")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="llc_piercing")


async def stage_acris_pdf_pierce(batch_size: int = 15):
    """ACRIS PDF signer extraction — run daily."""
    from enrichment.acris_pdf import run_batch_pdf_pierce
    log.info("pipeline.stage_start", stage="acris_pdf_pierce")
    await run_batch_pdf_pierce(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="acris_pdf_pierce")


async def stage_company_enrich(batch_size: int = None):
    """Company enrichment cascade — run daily (FREE→BUDGET→STANDARD→PREMIUM waterfall)."""
    from enrichment.company.orchestrator import run_batch
    log.info("pipeline.stage_start", stage="company_enrich")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="company_enrich")


async def stage_multi_source_enrich(batch_size: int = 100):
    """Multi-source enrichment for individuals — run daily."""
    from enrichment.multi_source import run_batch
    log.info("pipeline.stage_start", stage="multi_source_enrich")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="multi_source_enrich")


# ============================================================
# Stage wrapper — per-stage timeout so a stuck job can't run forever
# ============================================================

# Hard timeouts in seconds. If a stage exceeds this, asyncio.TimeoutError is
# raised and the next start_ingestion_log() call for the same source closes
# any orphaned 'running' row.
STAGE_TIMEOUTS: dict[str, int] = {
    "hpd_full":             4 * 3600,   # ~400k contacts + registrations
    "acris_delta":          1 * 3600,   # normal: minutes
    "wow_portfolio":        2 * 3600,
    "llc_piercing":         2 * 3600,
    "acris_pdf_pierce":     2 * 3600,   # Claude vision is slow
    "company_enrich":       1 * 3600,
    "multi_source_enrich":  2 * 3600,
}

_DEFAULT_STAGE_TIMEOUT = 3600


async def _run_stage_with_timeout(stage_name: str, stage_fn: Callable) -> None:
    """Run a stage with a hard timeout. Caller is responsible for catching exceptions."""
    timeout = STAGE_TIMEOUTS.get(stage_name, _DEFAULT_STAGE_TIMEOUT)
    try:
        await asyncio.wait_for(stage_fn(), timeout=timeout)
    except asyncio.TimeoutError:
        log.error("pipeline.stage_timeout", stage=stage_name, timeout_s=timeout)
        raise


# ============================================================
# Cost cap — shared across enrichment stages within a single run
# ============================================================

# Per-entity cost estimates by stage (configurable in config.py).
# Stages call tracker.add(stage_name) after each processed entity; when the
# tracker's total crosses DAILY_ENRICHMENT_COST_CAP_USD, the stage breaks
# out of its drain loop and unprocessed entities roll over to the next run.
COST_PER_ENTITY: dict[str, float] = {
    "llc_pierce":          config.COST_PER_ENTITY_LLC_PIERCE,
    "acris_pdf_pierce":    config.COST_PER_ENTITY_ACRIS_PDF,
    "company_enrich":      config.COST_PER_ENTITY_COMPANY_ENRICH,
    "multi_source_enrich": config.COST_PER_ENTITY_MULTI_SOURCE,
}


class CostTracker:
    """Tracks estimated enrichment spend within a single pipeline run.

    Not thread-safe — the worker runs one pipeline at a time, gated by
    scheduler._pipeline_lock.
    """

    def __init__(self, cap_usd: float = 0.0):
        # cap_usd <= 0 means no cap (unlimited).
        self.cap_usd = cap_usd
        self.spent_by_stage: dict[str, float] = {}

    @property
    def total_spent(self) -> float:
        return sum(self.spent_by_stage.values())

    @property
    def cap_hit(self) -> bool:
        return self.cap_usd > 0 and self.total_spent >= self.cap_usd

    def stage_spent(self, stage: str) -> float:
        return self.spent_by_stage.get(stage, 0.0)

    def add(self, stage: str, amount: float | None = None) -> None:
        if amount is None:
            amount = COST_PER_ENTITY.get(stage, 0.0)
        self.spent_by_stage[stage] = self.spent_by_stage.get(stage, 0.0) + amount


_tracker: CostTracker | None = None


def get_cost_tracker() -> CostTracker:
    """Return the current tracker, creating an uninitialized one if needed.
    Stages call this; the orchestrator's pipeline entrypoints reset it."""
    global _tracker
    if _tracker is None:
        _tracker = CostTracker(cap_usd=config.DAILY_ENRICHMENT_COST_CAP_USD)
    return _tracker


def reset_cost_tracker() -> CostTracker:
    """Start a fresh tracker for a new top-level pipeline run."""
    global _tracker
    _tracker = CostTracker(cap_usd=config.DAILY_ENRICHMENT_COST_CAP_USD)
    log.info("pipeline.cost_tracker_reset", cap_usd=_tracker.cap_usd)
    return _tracker


# ============================================================
# Composite Pipelines
# ============================================================

async def run_initial_full_load():
    """
    One-time full load — run this ONCE when first setting up the system.
    Ingests all HPD data, then runs enrichment.

    This will take a while (~hours for all of NYC).
    Monitor progress in the ingestion_log table.
    """
    log.info("pipeline.full_load_start")
    reset_cost_tracker()
    start = datetime.utcnow()

    stages = [
        ("hpd_full",           stage_hpd_full),
        ("acris_delta",        stage_acris_delta),
        ("wow_portfolio",      stage_wow_portfolio),
        ("llc_piercing",       lambda: stage_llc_piercing(batch_size=50)),
        ("acris_pdf_pierce",   lambda: stage_acris_pdf_pierce(batch_size=30)),
        ("company_enrich",      lambda: stage_company_enrich()),
        ("multi_source_enrich", lambda: stage_multi_source_enrich(batch_size=200)),
    ]

    for stage_name, stage_fn in stages:
        try:
            log.info("pipeline.running_stage", stage=stage_name)
            await _run_stage_with_timeout(stage_name, stage_fn)
        except Exception as e:
            log.error("pipeline.stage_failed",
                      stage=stage_name, error=str(e))
            # Continue with next stage rather than aborting

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("pipeline.full_load_complete", elapsed_seconds=elapsed)


async def run_daily_pipeline(reset_tracker: bool = True):
    """
    Daily pipeline — runs every day to catch new data and enrich.
    Lighter than the full load — only processes deltas.

    reset_tracker defaults to True for standalone calls (Run Daily / cron).
    run_weekly_pipeline passes False so the weekly's tracker carries
    through the embedded daily run instead of restarting mid-pipeline.
    """
    log.info("pipeline.daily_start")
    if reset_tracker:
        reset_cost_tracker()
    start = datetime.utcnow()

    stages = [
        ("acris_delta",        stage_acris_delta),
        ("llc_piercing",       lambda: stage_llc_piercing(batch_size=15)),
        ("acris_pdf_pierce",   lambda: stage_acris_pdf_pierce(batch_size=10)),
        ("company_enrich",      lambda: stage_company_enrich()),
        ("multi_source_enrich", lambda: stage_multi_source_enrich(batch_size=50)),
    ]

    for stage_name, stage_fn in stages:
        try:
            await _run_stage_with_timeout(stage_name, stage_fn)
        except Exception as e:
            log.error("pipeline.daily_stage_failed",
                      stage=stage_name, error=str(e))

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("pipeline.daily_complete", elapsed_seconds=elapsed)


async def run_weekly_pipeline():
    """
    Weekly pipeline — heavier sync run on Tuesday at 2 AM ET.
    Refreshes HPD + WoW data, which change slowly.
    """
    log.info("pipeline.weekly_start")
    reset_cost_tracker()

    stages = [
        ("hpd_full",      stage_hpd_full),
        ("wow_portfolio", stage_wow_portfolio),
    ]
    for stage_name, stage_fn in stages:
        try:
            await _run_stage_with_timeout(stage_name, stage_fn)
        except Exception as e:
            log.error("pipeline.weekly_stage_failed",
                      stage=stage_name, error=str(e))

    # Then run the daily pipeline on top. Pass reset_tracker=False so the
    # weekly's tracker carries through (single per-run cap, not per-call).
    await run_daily_pipeline(reset_tracker=False)

    log.info("pipeline.weekly_complete")


async def run_enrichment_only():
    """
    Enrichment-only run — useful if ingestion is up to date
    but enrichment is behind (e.g., after adding a new API key).
    """
    log.info("pipeline.enrichment_only_start")
    reset_cost_tracker()
    stages = [
        ("llc_piercing",       lambda: stage_llc_piercing(batch_size=25)),
        ("acris_pdf_pierce",   lambda: stage_acris_pdf_pierce(batch_size=20)),
        ("company_enrich",      lambda: stage_company_enrich()),
        ("multi_source_enrich", lambda: stage_multi_source_enrich(batch_size=100)),
    ]
    for stage_name, stage_fn in stages:
        try:
            await _run_stage_with_timeout(stage_name, stage_fn)
        except Exception as e:
            log.error("pipeline.enrichment_only_stage_failed",
                      stage=stage_name, error=str(e))
    log.info("pipeline.enrichment_only_complete")


# ============================================================
# Stats / Monitoring
# ============================================================

async def print_stats():
    """Print a summary of current database stats."""
    from database.client import db

    tables = ["entities", "properties", "contacts", "property_roles", "entity_relationships"]
    print("\n=== Owner Research Tool — Database Stats ===")
    for table in tables:
        res = db().table(table).select("id", count="exact").execute()
        print(f"  {table:30s}: {res.count:>8,} rows")

    # Enrichment breakdown
    print("\n  Enrichment status breakdown:")
    status_rows = db().table("entities").select("enrichment_status").execute().data or []
    breakdown: dict[str, int] = {}
    for row in status_rows:
        status = row.get("enrichment_status") or "(null)"
        breakdown[status] = breakdown.get(status, 0) + 1
    for status in ("pending", "in_progress", "done", "failed"):
        if status in breakdown:
            print(f"    {status:14s}: {breakdown.pop(status):>8,}")
    for status, count in sorted(breakdown.items()):
        print(f"    {status:14s}: {count:>8,}")

    # Building LLCs
    llc_res = db().table("entities").select("id", count="exact")\
        .eq("is_building_llc", True).execute()
    pierced_res = db().table("entities").select("id", count="exact")\
        .eq("is_building_llc", True).eq("is_pierced", True).execute()
    print(f"\n  Building LLCs:   {llc_res.count:>8,}")
    print(f"  Pierced LLCs:    {pierced_res.count:>8,}")

    # Queue breakdown by type
    print("\n  Enrichment queue (by type):")
    for t in ("llc_pierce", "company_enrich", "multi_source"):
        c = db().table("enrichment_queue")\
            .select("id", count="exact")\
            .eq("enrichment_type", t)\
            .limit(0)\
            .execute().count
        print(f"    {t:14s}: {c:>6,}")
    print("============================================\n")
