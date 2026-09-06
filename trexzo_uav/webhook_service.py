from __future__ import annotations

import logging
import os
import queue
import random
import re
import threading
import time
from dataclasses import dataclass
from typing import Optional

import requests

from .config import settings

logger = logging.getLogger(__name__)
_ROLE_ID_RE = re.compile(r"<@&(\d+)>")


def _route_config(name: str) -> dict:
    return settings.profile().get("routes", {}).get(name, {})


def _allowed_role_ids(job_name: str) -> list[str]:
    route = _route_config(job_name)
    env_name = str(route.get("mention_env", "")).strip()
    value = os.getenv(env_name, "").strip() if env_name else ""
    return [match.group(1) for match in _ROLE_ID_RE.finditer(value)]


def route_mention(job_name: str) -> str:
    route = _route_config(job_name)
    env_name = str(route.get("mention_env", "")).strip()
    return os.getenv(env_name, "").strip() if env_name else ""


@dataclass
class WebhookJob:
    name: str
    content: str
    username: Optional[str] = None
    avatar_url: Optional[str] = None


_webhook_queue: queue.Queue[WebhookJob] = queue.Queue(maxsize=settings.webhook_queue_size)


def queue_webhook(
    name: str,
    content: str,
    *,
    username: str | None = None,
    avatar_url: str | None = None,
    block: bool = True,
) -> bool:
    if not content:
        return False
    routes = settings.profile().get("routes", {})
    if name not in routes:
        logger.error("Unknown webhook route: %s", name)
        return False
    try:
        _webhook_queue.put(
            WebhookJob(name=name, content=content, username=username, avatar_url=avatar_url),
            block=block,
            timeout=2 if block else 0,
        )
        return True
    except queue.Full:
        logger.error("Webhook queue is full; dropped message for %s", name)
        return False


def _retry_wait(shutdown_event: threading.Event | None, seconds: float, job_name: str) -> None:
    if shutdown_event is None:
        time.sleep(seconds)
        return
    if shutdown_event.wait(seconds):
        raise RuntimeError(f"Webhook retry interrupted by shutdown for {job_name}")


def _deliver(session: requests.Session, job: WebhookJob, shutdown_event: threading.Event | None = None) -> None:
    route = _route_config(job.name)
    env_name = str(route.get("webhook_env", "")).strip()
    url = os.getenv(env_name, "").strip() if env_name else ""
    if not url:
        logger.warning("Webhook route %s is not configured; message skipped", job.name)
        return

    payload: dict[str, object] = {
        "content": job.content[:2000],
        "allowed_mentions": {"parse": [], "roles": _allowed_role_ids(job.name)},
    }
    if job.username:
        payload["username"] = job.username[:80]
    if job.avatar_url:
        payload["avatar_url"] = job.avatar_url

    last_status = None
    for attempt in range(6):
        try:
            timeout = (3, 7) if shutdown_event is not None and shutdown_event.is_set() else (5, 15)
            response = session.post(url, json=payload, timeout=timeout)
        except requests.RequestException as exc:
            error_type = type(exc).__name__
            if shutdown_event is not None and shutdown_event.is_set():
                raise RuntimeError(f"Webhook shutdown delivery failed for {job.name} ({error_type})") from None
            if attempt == 5:
                raise RuntimeError(f"Webhook network failure for {job.name} ({error_type})") from None
            _retry_wait(shutdown_event, min(30.0, (2**attempt) + random.random()), job.name)
            continue

        last_status = response.status_code
        if response.status_code in {200, 204}:
            return
        if response.status_code == 429:
            if shutdown_event is not None and shutdown_event.is_set():
                raise RuntimeError(f"Webhook {job.name} remained rate-limited during shutdown")
            try:
                retry_after = float(response.json().get("retry_after", 1.0))
            except (ValueError, TypeError, requests.JSONDecodeError):
                retry_after = 1.0
            if attempt == 5:
                break
            _retry_wait(shutdown_event, max(0.25, min(60.0, retry_after)), job.name)
            continue
        if response.status_code in {500, 502, 503, 504} and attempt < 5 and not (shutdown_event is not None and shutdown_event.is_set()):
            _retry_wait(shutdown_event, min(30.0, (2**attempt) + random.random()), job.name)
            continue
        raise RuntimeError(f"Webhook {job.name} failed with HTTP {response.status_code}: {response.text[:300]}")

    raise RuntimeError(f"Webhook {job.name} exhausted retries; last HTTP status was {last_status}")


def webhook_worker(shutdown_event: threading.Event) -> None:
    session = requests.Session()
    session.headers.update({"User-Agent": "Trexzo-UAV/2.0"})
    logger.info("Webhook worker started")
    while not shutdown_event.is_set() or not _webhook_queue.empty():
        try:
            job = _webhook_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            _deliver(session, job, shutdown_event)
        except Exception:
            logger.exception("Webhook delivery failed for %s", job.name)
        finally:
            _webhook_queue.task_done()
    session.close()
    logger.info("Webhook worker stopped")


def pending_webhooks() -> int:
    return _webhook_queue.qsize()
