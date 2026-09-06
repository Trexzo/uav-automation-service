from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from collections.abc import Callable

from . import market_service
from .config import settings
from .discord_service import run_discord_bot
from .engine import AutomationEngine
from .webhook_service import webhook_worker

logger = logging.getLogger(__name__)
shutdown_event = threading.Event()


def _request_shutdown(signum=None, frame=None) -> None:
    if not shutdown_event.is_set():
        logger.info("Shutdown requested%s", f" by signal {signum}" if signum else "")
        shutdown_event.set()


def _thread_wrapper(name: str, target: Callable, *args) -> None:
    try:
        target(*args)
    except Exception:
        logger.exception("Background component %s crashed", name)
        shutdown_event.set()
    finally:
        market_service.close_thread_connection()


def _start_thread(name: str, target: Callable, *args) -> threading.Thread:
    thread = threading.Thread(
        name=name,
        target=_thread_wrapper,
        args=(name, target, *args),
        daemon=True,
    )
    thread.start()
    return thread


def _join_thread(thread: threading.Thread | None, timeout: float) -> bool:
    if thread is None:
        return True
    thread.join(timeout=timeout)
    if thread.is_alive():
        logger.warning("Component %s did not stop within %.0fs", thread.name, timeout)
        return False
    return True


def main() -> int:
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(threadName)s %(name)s: %(message)s",
    )
    settings.ensure_directories()

    errors = settings.validate_runtime()
    if errors:
        for error in errors:
            logger.error(error)
        logger.error("Copy .env.example to .env or configure /etc/uav-automation-service/service.env")
        return 2

    missing_webhooks = [
        name for name, url in settings.webhook_urls.items() if not url
    ]
    if missing_webhooks:
        logger.warning(
            "Webhook destinations not configured (matching alerts will be skipped): %s",
            ", ".join(sorted(missing_webhooks)),
        )

    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    logger.info("Initializing database at %s", settings.db_path)
    market_service.start_accepting_writer_batches()
    market_service.init_db()
    market_service.load_items_cache()
    market_service.load_backfilled_set(init_seen_ids=False)
    market_service.load_tracked_users()
    engine = AutomationEngine()

    webhook_thread = _start_thread("webhook-worker", webhook_worker, shutdown_event)
    writer_thread = _start_thread("database-writer", market_service._db_writer_loop)
    tracker_thread = _start_thread(
        "user-tracker", market_service.user_tracker_loop, shutdown_event
    )
    scraper_thread: threading.Thread | None = None

    if settings.scraper_enabled:
        scraper_thread = _start_thread(
            "market-scraper", market_service.poller_logic, shutdown_event
        )
    else:
        logger.warning("Market scraper is disabled by SCRAPER_ENABLED=false")

    api_thread: threading.Thread | None = None
    if settings.api_enabled:
        api_thread = _start_thread(
            "flask-api", market_service.run_api_server, shutdown_event
        )

    return_code = 0
    try:
        run_discord_bot(engine, shutdown_event)
    except KeyboardInterrupt:
        _request_shutdown()
        return_code = 0
    except Exception:
        logger.exception("Discord bot stopped unexpectedly")
        shutdown_event.set()
        return_code = 1
    else:
        return_code = 0
    finally:
        shutdown_event.set()

        # Keep the complete cleanup sequence within systemd's 90-second stop
        # budget. All components observe shutdown_event concurrently; the shared
        # deadline prevents sequential join caps from accidentally exceeding it.
        shutdown_deadline = time.monotonic() + 80.0

        def remaining(cap: float) -> float:
            return max(0.0, min(cap, shutdown_deadline - time.monotonic()))

        # Stop components that can produce database writes before stopping the
        # dedicated writer. This prevents rows being queued behind the sentinel.
        scraper_stopped = _join_thread(scraper_thread, timeout=remaining(30))
        tracker_stopped = _join_thread(tracker_thread, timeout=remaining(15))
        if not scraper_stopped or not tracker_stopped:
            logger.error(
                "One or more producers did not stop cleanly; blocking new writer batches"
            )
        # Finish the optional API before shutting down the writer so no request
        # can race application teardown with a future queue-backed mutation.
        _join_thread(api_thread, timeout=remaining(10))
        market_service.stop_accepting_writer_batches()

        try:
            timeout = remaining(10)
            if timeout <= 0:
                raise TimeoutError("shutdown budget exhausted before writer sentinel")
            market_service._write_queue.put(None, timeout=timeout)
        except Exception:
            logger.exception("Could not signal the database writer to stop cleanly")
        _join_thread(writer_thread, timeout=remaining(25))

        # The webhook worker has already been draining while the database and
        # API were joined. Give it the final remaining budget.
        _join_thread(webhook_thread, timeout=remaining(30))
        market_service.close_thread_connection()
        logger.info("Application stopped")

    return return_code


if __name__ == "__main__":
    sys.exit(main())
