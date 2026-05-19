"""
scheduler.py — APScheduler-based persistent job scheduler

Runs in the dedicated Render worker service via `python main.py schedule`.
The web service does NOT run the scheduler (see render.yaml + webhook.py).

Cron jobs (only registered if AUTO_SEARCH_ENABLED=true):
  - Daily   (3:00 AM ET): ACRIS delta + enrichment
  - Weekly  (configurable day, 2:00 AM ET): HPD + WoW + daily pipeline

Always-on jobs:
  - Hourly health check: logs queue depth
  - 30s trigger poller: drains the pipeline_triggers table that the web
    service inserts into when admins click Run Daily/Run Weekly.
"""

import asyncio
import logging
import signal
import threading

import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from config import config

log = structlog.get_logger(__name__)

# Single-flight pipeline lock. Cron daily/weekly and the manual-trigger
# polling job all acquire this before invoking the orchestrator so that
# two pipeline runs can't overlap in the worker.
_pipeline_lock = threading.Lock()


# ============================================================
# Job wrappers
# ============================================================

async def _job_daily():
    """Daily: ACRIS delta + enrichment."""
    if not _pipeline_lock.acquire(blocking=False):
        log.info("scheduler.daily_skipped_pipeline_busy")
        return
    try:
        from pipeline.orchestrator import run_daily_pipeline
        log.info("scheduler.job_start", job="daily")
        try:
            await run_daily_pipeline()
            log.info("scheduler.job_done", job="daily")
        except Exception as e:
            log.error("scheduler.job_error", job="daily", error=str(e))
            raise
    finally:
        _pipeline_lock.release()


async def _job_weekly():
    """Weekly: Full HPD + WoW + enrichment."""
    if not _pipeline_lock.acquire(blocking=False):
        log.info("scheduler.weekly_skipped_pipeline_busy")
        return
    try:
        from pipeline.orchestrator import run_weekly_pipeline
        log.info("scheduler.job_start", job="weekly")
        try:
            await run_weekly_pipeline()
            log.info("scheduler.job_done", job="weekly")
        except Exception as e:
            log.error("scheduler.job_error", job="weekly", error=str(e))
            raise
    finally:
        _pipeline_lock.release()


async def _job_poll_triggers():
    """Poll pipeline_triggers every 30s and run any pending request.
    Skips if another pipeline is already running on this worker."""
    if not _pipeline_lock.acquire(blocking=False):
        return  # pipeline already busy; try again next tick

    try:
        from database.client import claim_next_pipeline_trigger, finish_pipeline_trigger
        trigger = claim_next_pipeline_trigger()
        if not trigger:
            return

        log.info("scheduler.manual_trigger_claimed",
                 id=trigger["id"], pipeline=trigger["pipeline"])
        try:
            if trigger["pipeline"] == "daily":
                from pipeline.orchestrator import run_daily_pipeline
                await run_daily_pipeline()
            else:
                from pipeline.orchestrator import run_weekly_pipeline
                await run_weekly_pipeline()
            finish_pipeline_trigger(trigger["id"], success=True)
            log.info("scheduler.manual_trigger_done", id=trigger["id"])
        except Exception as e:
            finish_pipeline_trigger(trigger["id"], success=False, error=str(e))
            log.error("scheduler.manual_trigger_failed", id=trigger["id"], error=str(e))
    finally:
        _pipeline_lock.release()


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
                day_of_week=config.WEEKLY_PIPELINE_DAY, hour=2, minute=0, timezone="America/New_York"
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

    # Always register the manual-trigger poller — independent of AUTO_SEARCH_ENABLED
    # so admins can run a one-off pipeline even when scheduled cron is disabled.
    scheduler.add_job(
        _job_poll_triggers,
        trigger=IntervalTrigger(seconds=30),
        id="poll_triggers",
        name="Manual trigger poller",
        max_instances=1,
        coalesce=True,
    )
    registered.append("poll_triggers")

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
                day_of_week=config.WEEKLY_PIPELINE_DAY, hour=2, minute=0, timezone="America/New_York"
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

    scheduler.add_job(
        _run_async(_job_poll_triggers),
        trigger=IntervalTrigger(seconds=30),
        id="poll_triggers",
        max_instances=1,
        coalesce=True,
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
