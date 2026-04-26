"""
scheduler.py — APScheduler-based persistent job scheduler

Two ways to use:

  1. Standalone (legacy):
        python scheduler.py
     Creates its own asyncio loop and runs the BlockingScheduler. Used only
     for local testing of the cron jobs in isolation.

  2. Inside the FastAPI app (production):
        from scheduler import build_async_scheduler, register_jobs
        sched = build_async_scheduler()
        register_jobs(sched)
        sched.start()
     This is what the Render Web Service uses — the scheduler shares the
     FastAPI event loop and runs alongside the webhook listener.

Cron jobs (only registered if AUTO_SEARCH_ENABLED=true):
  - Daily   (3:00 AM ET): ACRIS delta + enrichment
  - Weekly  (Sun 2:00 AM ET): HPD full sync + WoW + daily pipeline
  - Hourly health check: logs queue depth (always registered)
"""

import asyncio
import logging
import signal

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import config

log = structlog.get_logger(__name__)


# ============================================================
# Job wrappers
# ============================================================

async def _job_daily():
    """Daily: ACRIS delta + enrichment."""
    from pipeline.orchestrator import run_daily_pipeline
    log.info("scheduler.job_start", job="daily")
    try:
        await run_daily_pipeline()
        log.info("scheduler.job_done", job="daily")
    except Exception as e:
        log.error("scheduler.job_error", job="daily", error=str(e))
        raise


async def _job_weekly():
    """Weekly: Full HPD + WoW + enrichment."""
    from pipeline.orchestrator import run_weekly_pipeline
    log.info("scheduler.job_start", job="weekly")
    try:
        await run_weekly_pipeline()
        log.info("scheduler.job_done", job="weekly")
    except Exception as e:
        log.error("scheduler.job_error", job="weekly", error=str(e))
        raise


def _job_health_check():
    """Hourly: log queue depth and basic stats."""
    from database.client import db
    try:
        queue_count = db().table("enrichment_queue").select("id", count="exact").execute().count
        entity_count = db().table("entities").select("id", count="exact").execute().count
        contact_count = db().table("contacts").select("id", count="exact").execute().count
        log.info(
            "scheduler.health",
            queue=queue_count,
            entities=entity_count,
            contacts=contact_count,
            auto_search_enabled=config.AUTO_SEARCH_ENABLED,
        )
    except Exception as e:
        log.error("scheduler.health_error", error=str(e))


# ============================================================
# Public API — used by app.py (FastAPI integration)
# ============================================================

def build_async_scheduler() -> AsyncIOScheduler:
    """Construct an AsyncIOScheduler bound to the current event loop."""
    return AsyncIOScheduler(timezone="America/New_York")


def register_jobs(scheduler) -> list[str]:
    """
    Register all cron and interval jobs on the given scheduler.

    Cron jobs (daily/weekly auto-search) are only registered when
    AUTO_SEARCH_ENABLED is true. The hourly health check is always
    registered so we have visibility regardless of the toggle.

    Returns the list of registered job IDs (for logging).
    """
    registered: list[str] = []

    if config.AUTO_SEARCH_ENABLED:
        scheduler.add_job(
            _job_daily,
            trigger=CronTrigger(hour=3, minute=0, timezone="America/New_York"),
            id="daily_pipeline",
            name="Daily Pipeline",
            misfire_grace_time=3600,
            coalesce=True,
        )
        registered.append("daily_pipeline")

        scheduler.add_job(
            _job_weekly,
            trigger=CronTrigger(
                day_of_week="sun", hour=2, minute=0, timezone="America/New_York"
            ),
            id="weekly_pipeline",
            name="Weekly Full Pipeline",
            misfire_grace_time=7200,
            coalesce=True,
        )
        registered.append("weekly_pipeline")

        log.info("scheduler.auto_search_enabled", jobs=registered)
    else:
        log.info(
            "scheduler.auto_search_disabled",
            note="cron jobs not registered; flip AUTO_SEARCH_ENABLED=true to enable",
        )

    # Always register the health check so we get visibility.
    scheduler.add_job(
        _job_health_check,
        trigger=IntervalTrigger(hours=1),
        id="health_check",
        name="Health Check",
    )
    registered.append("health_check")

    return registered


# ============================================================
# Standalone entrypoint (legacy `python scheduler.py`)
# ============================================================

def main():
    """Run the scheduler as a standalone blocking process.

    The production deployment uses the FastAPI app instead (app.py).
    This entrypoint exists for local debugging only.
    """
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    )

    log.info("scheduler.starting_standalone")

    # Use BlockingScheduler in standalone mode — no async context needed.
    # The async job functions are wrapped to run in a fresh event loop.
    scheduler = BlockingScheduler(timezone="America/New_York")

    def _run_async(coro_factory):
        """Run an async job inside a fresh event loop, propagating errors."""
        def _wrapper():
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(coro_factory())
            except Exception as e:
                log.error("scheduler.job_error", error=str(e))
                # Don't re-raise — APScheduler will mark the job missed and
                # we want the next scheduled run to still fire.
            finally:
                loop.close()
        return _wrapper

    if config.AUTO_SEARCH_ENABLED:
        scheduler.add_job(
            _run_async(_job_daily),
            trigger=CronTrigger(hour=3, minute=0, timezone="America/New_York"),
            id="daily_pipeline",
            misfire_grace_time=3600,
            coalesce=True,
        )
        scheduler.add_job(
            _run_async(_job_weekly),
            trigger=CronTrigger(
                day_of_week="sun", hour=2, minute=0, timezone="America/New_York"
            ),
            id="weekly_pipeline",
            misfire_grace_time=7200,
            coalesce=True,
        )

    scheduler.add_job(
        _job_health_check,
        trigger=IntervalTrigger(hours=1),
        id="health_check",
    )

    # Graceful shutdown on SIGTERM (Render sends this on deploy).
    def _shutdown(signum, frame):
        log.info("scheduler.stopping", signal=signum)
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("scheduler.started", jobs=[j.id for j in scheduler.get_jobs()])
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("scheduler.stopped")


if __name__ == "__main__":
    main()
