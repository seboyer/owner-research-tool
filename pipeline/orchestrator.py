"""
pipeline/orchestrator.py — Pipeline Orchestrator

Defines the full pipeline as a sequence of stages, each of which can be
run independently or as part of the full daily/weekly schedule.

Stage order and rationale:
  1. HPD Full Sync       (weekly)   — foundational: ~80K buildings, owners, agents
  2. ACRIS Delta         (daily)    — new property transfers from last N days
  3. WoW Portfolio       (weekly)   — group LLCs by apparent owner
  4. OpenCorporates      (ongoing)  — LLC registered agent data
  5. LLC Piercing        (ongoing)  — find real owners of building LLCs
     5a. ACRIS PDF Signers           — mortgage doc signature extraction
     5b. AI Agentic Reasoning        — Claude web research
  6. Zoominfo Enrich     (ongoing)  — contact enrichment for corporate entities
  7. Multi-Source Enrich (ongoing)  — Whitepages/PropertyRadar/Web for individuals
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


async def stage_opencorporates(batch_size: int = 50):
    """OpenCorporates LLC lookup — run daily."""
    from ingest.opencorporates import run_batch
    log.info("pipeline.stage_start", stage="opencorporates")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="opencorporates")


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


async def stage_zoominfo_enrich(batch_size: int = None):
    """Zoominfo enrichment — run daily (respects ENRICHMENT_BATCH_SIZE from config)."""
    from enrichment.zoominfo import run_batch
    log.info("pipeline.stage_start", stage="zoominfo_enrich")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="zoominfo_enrich")


async def stage_multi_source_enrich(batch_size: int = 100):
    """Multi-source enrichment for individuals — run daily."""
    from enrichment.multi_source import run_batch
    log.info("pipeline.stage_start", stage="multi_source_enrich")
    await run_batch(batch_size=batch_size)
    log.info("pipeline.stage_done", stage="multi_source_enrich")


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
    start = datetime.utcnow()

    stages = [
        ("hpd_full",           stage_hpd_full),
        ("acris_delta",        stage_acris_delta),
        ("wow_portfolio",      stage_wow_portfolio),
        ("opencorporates",     lambda: stage_opencorporates(batch_size=100)),
        ("llc_piercing",       lambda: stage_llc_piercing(batch_size=50)),
        ("acris_pdf_pierce",   lambda: stage_acris_pdf_pierce(batch_size=30)),
        ("zoominfo_enrich",    lambda: stage_zoominfo_enrich()),
        ("multi_source_enrich",lambda: stage_multi_source_enrich(batch_size=200)),
    ]

    for stage_name, stage_fn in stages:
        try:
            log.info("pipeline.running_stage", stage=stage_name)
            await stage_fn()
        except Exception as e:
            log.error("pipeline.stage_failed",
                      stage=stage_name, error=str(e))
            # Continue with next stage rather than aborting

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("pipeline.full_load_complete", elapsed_seconds=elapsed)


async def run_daily_pipeline():
    """
    Daily pipeline — runs every day to catch new data and enrich.
    Lighter than the full load — only processes deltas.
    """
    log.info("pipeline.daily_start")
    start = datetime.utcnow()

    stages = [
        ("acris_delta",        stage_acris_delta),
        ("opencorporates",     lambda: stage_opencorporates(batch_size=30)),
        ("llc_piercing",       lambda: stage_llc_piercing(batch_size=15)),
        ("acris_pdf_pierce",   lambda: stage_acris_pdf_pierce(batch_size=10)),
        ("zoominfo_enrich",    lambda: stage_zoominfo_enrich()),
        ("multi_source_enrich",lambda: stage_multi_source_enrich(batch_size=50)),
    ]

    for stage_name, stage_fn in stages:
        try:
            await stage_fn()
        except Exception as e:
            log.error("pipeline.daily_stage_failed",
                      stage=stage_name, error=str(e))

    elapsed = (datetime.utcnow() - start).total_seconds()
    log.info("pipeline.daily_complete", elapsed_seconds=elapsed)


async def run_weekly_pipeline():
    """
    Weekly pipeline — heavier sync run on Sundays.
    Refreshes HPD + WoW data, which change slowly.
    """
    log.info("pipeline.weekly_start")

    # Run heavy ingestion jobs
    await stage_hpd_full()
    await stage_wow_portfolio()

    # Then run the daily pipeline on top
    await run_daily_pipeline()

    log.info("pipeline.weekly_complete")


async def run_enrichment_only():
    """
    Enrichment-only run — useful if ingestion is up to date
    but enrichment is behind (e.g., after adding a new API key).
    """
    log.info("pipeline.enrichment_only_start")
    await stage_opencorporates(batch_size=50)
    await stage_llc_piercing(batch_size=25)
    await stage_acris_pdf_pierce(batch_size=20)
    await stage_zoominfo_enrich()
    await stage_multi_source_enrich(batch_size=100)
    log.info("pipeline.enrichment_only_complete")


# ============================================================
# Stats / Monitoring
# ============================================================

async def print_stats():
    """Print a summary of current database stats."""
    from database.client import db

    tables = ["entities", "properties", "contacts", "property_roles", "entity_relationships"]
    print("\n=== NYC Landlord Finder — Database Stats ===")
    for table in tables:
        res = db().table(table).select("id", count="exact").execute()
        print(f"  {table:30s}: {res.count:>8,} rows")

    # Enrichment breakdown
    res = db().table("entities")\
        .select("enrichment_status", count="exact")\
        .execute()
    print("\n  Enrichment status breakdown:")
    status_res = db().rpc("get_enrichment_breakdown", {}).execute()

    # Building LLCs
    llc_res = db().table("entities").select("id", count="exact")\
        .eq("is_building_llc", True).execute()
    pierced_res = db().table("entities").select("id", count="exact")\
        .eq("is_building_llc", True).eq("is_pierced", True).execute()
    print(f"\n  Building LLCs:   {llc_res.count:>8,}")
    print(f"  Pierced LLCs:    {pierced_res.count:>8,}")

    # Queue
    queue_res = db().table("enrichment_queue").select("id", count="exact").execute()
    print(f"  Enrichment queue:{queue_res.count:>8,} pending")
    print("============================================\n")
