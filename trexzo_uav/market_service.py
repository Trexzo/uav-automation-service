import json
import queue
import sqlite3
import requests
import os
import sys
from requests.adapters import HTTPAdapter
import time
import threading
import random
import re
import secrets
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, jsonify, request
from flask_cors import CORS

from .config import settings

app = Flask(__name__)
if settings.api_cors_origins:
    CORS(
        app,
        origins=list(settings.api_cors_origins),
        allow_headers=["Content-Type", "X-API-Token", "Authorization"],
    )


def _require_api_token():
    """Protect every data/API route, including read and mutation endpoints."""
    if not request.path.startswith("/api/"):
        return None
    if request.method == "OPTIONS":
        return None
    expected = settings.api_token
    if not expected:
        return jsonify({"error": "API is not configured with API_TOKEN", "status": 503}), 503
    supplied = request.headers.get("X-API-Token", "").strip()
    authorization = request.headers.get("Authorization", "").strip()
    if not supplied and authorization.lower().startswith("bearer "):
        supplied = authorization[7:].strip()
    if not supplied or not secrets.compare_digest(supplied, expected):
        return jsonify({"error": "unauthorized", "status": 401}), 401
    return None


if hasattr(app, "before_request"):
    app.before_request(_require_api_token)


def _close_api_resources(_error=None):
    close_thread_connection()


if hasattr(app, "teardown_appcontext"):
    app.teardown_appcontext(_close_api_resources)

try:
    APP_VERSION = (settings.project_root / "VERSION").read_text(encoding="utf-8").strip()
except OSError:
    APP_VERSION = "unknown"

# --- CONFIG ---
BASE_DIR = str(settings.data_dir)
DB_PATH = str(settings.db_path)
ITEMS_JSON = str(settings.items_json)
AWS_URL = settings.tradingpost_url
WORKERS = max(1, settings.scraper_workers)
MAX_WORKERS = max(WORKERS, settings.scraper_max_workers)
POLLING_INTERVAL = max(5, settings.polling_interval)
TRACKING_INTERVAL = max(5, settings.tracking_interval)
API_MAX_PAGE_LIMIT = 5_000
API_MAX_EXPORT_ROWS = 50_000
API_MAX_ANALYTICS_SCAN = 250_000
API_MAX_BULK_ITEMS = 500

PAGE_WARN_MILESTONES = {100, 500, 1000, 5000, 10000, 25000, 50000}
THROTTLE_CODES = {429, 502, 503, 504}

# Completeness guarantees
# When an already-backfilled item gets 0 new trades on a page, require this many
# consecutive zero-new-trades pages before declaring "caught up".
CONSECUTIVE_EMPTY_THRESHOLD = 2
# If fetch_page returns None after its internal retries, retry the same page.
# After this many consecutive full-page failures, stop the item without marking
# a new historical backfill complete. No page is intentionally skipped.
MAX_PAGE_FAILURES = 3
# Items that were backfilled with 0 pages (API returned nothing at the time) are
# automatically reset after this many days so they get a full re-scrape attempt.
# Covers items that had no trades when first seen but have traded since.
STALE_EMPTY_RESET_DAYS = 7

# --- THREAD-LOCAL STORAGE ---
_thread_local = threading.local()

def normalize_username(name):
    if not name:
        return None
    return name.strip().lower().replace("_", " ")


def get_conn():
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute("PRAGMA cache_size=-8000;")
        _thread_local.conn = conn
    return _thread_local.conn


def close_thread_connection():
    """Close SQLite and HTTP resources owned by the current thread, if any."""
    conn = getattr(_thread_local, "conn", None)
    if conn is not None:
        conn.close()
        _thread_local.conn = None
    session = getattr(_thread_local, "session", None)
    if session is not None:
        session.close()
        _thread_local.session = None


def get_session():
    if getattr(_thread_local, "session", None) is None:
        s = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=200,
            pool_maxsize=200,
            max_retries=0  # we handle retries manually
        )
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        s.headers.update({"User-Agent": "TradeScraper/3.0"})
        _thread_local.session = s
    return _thread_local.session


# --- GLOBAL METRICS / STATE ---
processed_count = 0
counter_lock = threading.Lock()

metrics_lock = threading.Lock()
cycle_metrics = {
    "success": 0, "throttled": 0, "failed": 0,
    "timeouts": 0, "empty": 0, "pages_fetched": 0,
    "new_trades": 0, "latencies": []
}

current_workers_lock = threading.Lock()
current_workers = WORKERS

# --- ITEMS CACHE ---
_items_cache = None
_items_by_name = {}
_items_cache_lock = threading.Lock()

# --- BACKFILL CACHE ---
_backfilled_set = set()
_backfilled_lock = threading.Lock()

# A bounded queue provides backpressure during large backfills.
_write_queue = queue.Queue(maxsize=100)
_pending_ids = set()
_pending_ids_lock = threading.Lock()
_writer_accepting = threading.Event()
_writer_accepting.set()
# Serializes the final accept/reject decision with queue insertion. Without this
# gate, a producer can pass the Event check, get descheduled, and enqueue a
# batch after shutdown has already placed the writer sentinel.
_writer_gate_lock = threading.Lock()


def stop_accepting_writer_batches():
    """Atomically prevent new batches from being placed behind the sentinel."""
    with _writer_gate_lock:
        _writer_accepting.clear()


def start_accepting_writer_batches():
    """Enable queue writes during application startup and isolated tests."""
    with _writer_gate_lock:
        _writer_accepting.set()


class _WriterBarrier:
    """Queue marker used to wait until all earlier rows are durably committed."""

    def __init__(self):
        self.event = threading.Event()
        self.error = None


def _db_writer_loop():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    pending = []
    last_commit = time.time()

    def flush_pending():
        nonlocal last_commit
        if pending:
            committed_ids = [row[0] for row in pending]
            conn.executemany("INSERT OR IGNORE INTO trades VALUES (?,?,?,?,?,?,?,?)", pending)
            conn.commit()
            pending.clear()
            with _pending_ids_lock:
                _pending_ids.difference_update(committed_ids)
            last_commit = time.time()

    try:
        while True:
            try:
                rows = _write_queue.get(timeout=0.5)
            except queue.Empty:
                flush_pending()
                continue

            try:
                if rows is None:
                    flush_pending()
                    break
                if isinstance(rows, _WriterBarrier):
                    try:
                        flush_pending()
                    except Exception as exc:
                        rows.error = exc
                        raise
                    finally:
                        rows.event.set()
                    continue
                pending.extend(rows)
                # Commit every 500 rows or every 2 seconds.
                if len(pending) >= 500 or (time.time() - last_commit) >= 2:
                    flush_pending()
            finally:
                _write_queue.task_done()
    finally:
        try:
            flush_pending()
        finally:
            conn.close()


def flush_database_writer(timeout=30.0):
    """Wait until every row queued before this call has been committed."""
    barrier = _WriterBarrier()
    try:
        with _writer_gate_lock:
            if not _writer_accepting.is_set():
                raise RuntimeError("database writer is shutting down")
            _write_queue.put(barrier, timeout=min(5.0, timeout))
    except queue.Full as exc:
        raise RuntimeError("database writer queue stayed full while flushing") from exc
    if not barrier.event.wait(timeout):
        raise RuntimeError("database writer did not acknowledge flush in time")
    if barrier.error is not None:
        raise RuntimeError("database writer failed while flushing") from barrier.error


def _strip_display_name(raw_key):
    name = raw_key
    if name.lower().startswith("@gre@"):
        name = name[5:]
    name = re.sub(r'_\d+$', '', name)
    return name.strip()


def load_items_cache():
    global _items_cache, _items_by_name
    with _items_cache_lock:
        if _items_cache is not None:
            return _items_cache
        with open(ITEMS_JSON, 'r') as f:
            raw = json.load(f)
        items = []
        # Support three formats:
        #   1. Old dict:  {"@gre@ItemName": id, ...}  — @gre@ prefix = tradeable
        #   2. New array: [{"id": N, "name": "...", "tradeable": true}, ...]
        #   3. Plain array without tradeable field: [{"id": N, "name": "..."}, ...]
        if isinstance(raw, list):
            for entry in raw:
                item_id  = entry.get("id")
                name     = entry.get("name", "")
                # If "tradeable" key is present, respect it; if absent (old plain dump),
                # fall back to checking whether the name contains "@gre@"
                if "tradeable" in entry:
                    if not entry["tradeable"]:
                        continue
                    display = re.sub(r'_\d+$', '', name).strip()
                else:
                    # Plain array without tradeable field — use @gre@ in name as signal
                    if "@gre@" not in name:
                        continue
                    name    = name.replace("@gre@", "").strip()
                    display = re.sub(r'_\d+$', '', name).strip()
                if not display:
                    continue
                search = display.lower().replace(" ", "_")
                item = {
                    "id": item_id,
                    "display_name": display,
                    "search_name": search,
                    "db_name": name,
                    "db_name_clean": re.sub(r'_\d+$', '', name).strip(),
                }
                items.append(item)
                _items_by_name[display.lower()] = item
        else:
            # Old dict format: {"@gre@ItemName": id, ...}
            for raw_key, item_id in raw.items():
                if not raw_key.lower().startswith("@gre@"):
                    continue
                name    = raw_key[5:]  # strip @gre@
                display = re.sub(r'_\d+$', '', name).strip()
                search  = display.lower().replace(" ", "_")
                item = {
                    "id": item_id,
                    "display_name": display,
                    "search_name": search,
                    "db_name": name,
                    "db_name_clean": re.sub(r'_\d+$', '', name).strip(),
                }
                items.append(item)
                _items_by_name[display.lower()] = item
        # Deduplicate by display_name (case-insensitive) — the new items.json often
        # contains multiple IDs for the same item (noted variants, re-used names, etc).
        # The API search is by name so hitting it twice for the same name wastes bandwidth
        # and pollutes backfill_status with duplicate rows.
        seen_names = set()
        deduped = []
        for item in items:
            key = item["display_name"].lower()
            if key not in seen_names:
                seen_names.add(key)
                deduped.append(item)
        removed = len(items) - len(deduped)
        if removed > 0:
            print(f"[Items] Deduplicated {removed} duplicate item names ({len(deduped)} unique items remaining)")
        _items_cache = deduped
        return _items_cache


def get_items():
    return load_items_cache()


def get_market_items():
    items = get_items()
    return [i["db_name"] for i in items if i["db_name"]]


def load_backfilled_set(init_seen_ids=False):
    # init_seen_ids remains in the signature for compatibility, but the old
    # full-database Python set was intentionally removed to keep memory bounded.
    global _backfilled_set
    conn = get_conn()
    rows = conn.execute("SELECT item_name FROM backfill_status").fetchall()
    with _backfilled_lock:
        _backfilled_set = {r[0].lower() for r in rows}


def is_backfilled(item_name):
    with _backfilled_lock:
        return item_name.lower() in _backfilled_set


def mark_backfilled(item_name, total_pages):
    conn = get_conn()
    conn.execute(
        "INSERT OR REPLACE INTO backfill_status VALUES (?, ?, ?)",
        (item_name, time.strftime('%Y-%m-%d %H:%M:%S'), total_pages)
    )
    conn.commit()
    with _backfilled_lock:
        _backfilled_set.add(item_name.lower())


def reset_stale_empty_items():
    """
    Deletes backfill_status rows for items that were scraped but yielded
    0 pages (i.e. the API had no data for them at the time) and whose
    completed_at timestamp is older than STALE_EMPTY_RESET_DAYS.

    This forces a full re-scrape on the next cycle, catching items that
    had no trades when first seen but have since been traded.
    """
    cutoff = time.strftime(
        '%Y-%m-%d %H:%M:%S',
        time.gmtime(time.time() - STALE_EMPTY_RESET_DAYS * 86400)
    )
    conn = get_conn()
    rows = conn.execute(
        "SELECT item_name FROM backfill_status WHERE total_pages = 0 AND completed_at < ?",
        (cutoff,)
    ).fetchall()
    if not rows:
        return
    names = [r[0] for r in rows]
    conn.execute(
        "DELETE FROM backfill_status WHERE total_pages = 0 AND completed_at < ?",
        (cutoff,)
    )
    conn.commit()
    with _backfilled_lock:
        for name in names:
            _backfilled_set.discard(name.lower())
    print(f"[{time.strftime('%H:%M:%S')}] [StaleReset] Reset {len(names)} zero-page item(s) "
          f"older than {STALE_EMPTY_RESET_DAYS}d → will full-scrape next cycle")


# --- DB INIT ---
def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute('''CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY, timestamp TEXT, item_name TEXT,
                  quantity INTEGER, price INTEGER, currency INTEGER,
                  seller TEXT, buyer TEXT)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS backfill_status
                 (item_name TEXT PRIMARY KEY, completed_at TEXT, total_pages INTEGER)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS tracked_users
                 (username TEXT PRIMARY KEY, added_at TEXT, last_checked TEXT,
                  last_trade_id INTEGER, discord_user_id INTEGER)''')
    tracked_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(tracked_users)").fetchall()
    }
    if "discord_user_id" not in tracked_columns:
        conn.execute("ALTER TABLE tracked_users ADD COLUMN discord_user_id INTEGER")
    conn.execute('''CREATE TABLE IF NOT EXISTS trade_alerts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, trade_id INTEGER,
                  timestamp TEXT, item_name TEXT, quantity INTEGER, price INTEGER,
                  currency INTEGER, role TEXT, seen INTEGER DEFAULT 0,
                  alerted_at TEXT)''')
    # Remove historical duplicates before enforcing idempotent alert inserts.
    conn.execute('''DELETE FROM trade_alerts
                    WHERE id NOT IN (
                        SELECT MIN(id) FROM trade_alerts GROUP BY username, trade_id
                    )''')
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_name ON trades(item_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_seller ON trades(seller)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_buyer ON trades(buyer)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON trades(timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_username ON trade_alerts(username)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_seen ON trade_alerts(seen)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_user_trade ON trade_alerts(username, trade_id)")
    # LOWER() expression indexes — these are what actually get used by LOWER(col) = ? queries
    conn.execute("CREATE INDEX IF NOT EXISTS idx_seller_lower ON trades(LOWER(seller))")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_buyer_lower  ON trades(LOWER(buyer))")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_lower   ON trades(LOWER(item_name))")
    # Composite index for leaderboard GROUP BY — covers both seller/buyer scans
    conn.execute("CREATE INDEX IF NOT EXISTS idx_seller_ts ON trades(seller, timestamp)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_buyer_ts  ON trades(buyer,  timestamp)")
    conn.commit()
    conn.close()


# --- SCRAPER ---
def record_metric(key, value=1):
    with metrics_lock:
        if key == "latency":
            cycle_metrics["latencies"].append(value)
        else:
            cycle_metrics[key] += value


LAMBDA_ERROR_MSG = b'"Internal server error"'

def _wait_or_sleep(shutdown_event, seconds):
    if shutdown_event is None:
        time.sleep(seconds)
        return False
    return shutdown_event.wait(seconds)


def fetch_page(item_name, page, shutdown_event=None):
    params = {"search_text": item_name}
    if page > 1:
        params["page"] = page
    session = get_session()
    for attempt in range(5):
        if shutdown_event is not None and shutdown_event.is_set():
            return None, 0.0
        start = time.time()
        try:
            resp = session.get(AWS_URL, params=params, timeout=(5, 12))
            latency = time.time() - start
            record_metric("latency", latency)
        except requests.exceptions.Timeout:
            record_metric("timeouts")
            if _wait_or_sleep(shutdown_event, 0.2 * (2 ** attempt)):
                return None, 0.0
            continue
        except requests.exceptions.RequestException as e:
            record_metric("failed")
            print(f"[{time.strftime('%H:%M:%S')}] Request error on {item_name} p{page}: {e}")
            if _wait_or_sleep(shutdown_event, 0.2 * (2 ** attempt)):
                return None, 0.0
            continue

        if resp.status_code in THROTTLE_CODES:
            record_metric("throttled")
            print(f"[{time.strftime('%H:%M:%S')}] Throttled ({resp.status_code}) on {item_name} p{page}, attempt {attempt+1}")
            if _wait_or_sleep(shutdown_event, 0.5 * (2 ** attempt)):
                return None, 0.0
            continue

        if resp.status_code == 200:
            try:
                payload = resp.json()
            except requests.exceptions.JSONDecodeError:
                record_metric("failed")
                print(f"[{time.strftime('%H:%M:%S')}] Invalid JSON on {item_name} p{page}, attempt {attempt+1}")
                if _wait_or_sleep(shutdown_event, 0.2 * (2 ** attempt)):
                    return None, 0.0
                continue
            if not isinstance(payload, list):
                record_metric("failed")
                print(f"[{time.strftime('%H:%M:%S')}] Unexpected JSON shape on {item_name} p{page}, attempt {attempt+1}")
                if _wait_or_sleep(shutdown_event, 0.2 * (2 ** attempt)):
                    return None, 0.0
                continue
            return payload, latency

        # Lambda timeout/crash returns 500 with {"message": "Internal server error"}.
        if resp.status_code == 500 and LAMBDA_ERROR_MSG in resp.content:
            wait = 2.0 * (2 ** attempt)
            print(f"[{time.strftime('%H:%M:%S')}] Lambda error on {item_name} p{page} "
                  f"(attempt {attempt+1}/5) — retrying in {wait:.0f}s")
            record_metric("throttled")
            if _wait_or_sleep(shutdown_event, wait):
                return None, 0.0
            continue

        record_metric("failed")
        print(f"[{time.strftime('%H:%M:%S')}] HTTP {resp.status_code} on {item_name} p{page}, attempt {attempt+1}")
        if _wait_or_sleep(shutdown_event, 0.2 * (2 ** attempt)):
            return None, 0.0

    return None, 0.0


def insert_trades(data, commit=False):
    if not _writer_accepting.is_set():
        raise RuntimeError("database writer is shutting down")
    # Deduplicate IDs within the current API page before querying SQLite.
    unique_rows = {}
    for trade in data:
        row = (
            trade['id'], trade['time'], trade['item_name'], trade['amount'],
            trade['price'], trade['currency'],
            normalize_username(trade.get('seller')),
            normalize_username(trade.get('buyer')),
        )
        unique_rows.setdefault(row[0], row)
    rows = list(unique_rows.values())
    if not rows:
        return 0

    # Query in chunks so correctness does not depend on SQLite's host-parameter
    # limit or on the upstream API's page size.
    ids = [row[0] for row in rows]
    existing = set()
    conn = get_conn()
    for start in range(0, len(ids), 500):
        chunk = ids[start:start + 500]
        placeholders = ",".join("?" * len(chunk))
        existing.update(
            row[0] for row in conn.execute(
                f"SELECT id FROM trades WHERE id IN ({placeholders})", chunk
            ).fetchall()
        )

    # The gate makes the final acceptance decision and queue insertion atomic
    # with stop_accepting_writer_batches(). The pending-ID lock prevents two
    # workers from queueing the same not-yet-committed trade.
    with _writer_gate_lock:
        if not _writer_accepting.is_set():
            raise RuntimeError("database writer is shutting down")
        with _pending_ids_lock:
            new_rows = [
                row for row in rows
                if row[0] not in existing and row[0] not in _pending_ids
            ]
            _pending_ids.update(row[0] for row in new_rows)
        if new_rows:
            try:
                _write_queue.put(new_rows, timeout=5)
            except queue.Full as exc:
                with _pending_ids_lock:
                    _pending_ids.difference_update(row[0] for row in new_rows)
                raise RuntimeError("database writer queue stayed full for 5 seconds") from exc
    return len(new_rows)


def fetch_item(item_name, shutdown_event=None):
    global processed_count
    initial_delay = random.uniform(0.1, 0.6)
    if shutdown_event is not None:
        if shutdown_event.wait(initial_delay):
            return
    else:
        time.sleep(initial_delay)
    item_already_backfilled = is_backfilled(item_name)
    total_new = 0
    total_pages = 0
    consecutive_no_new = 0   # pages with 0 new trades in a row (early-stop guard)
    consecutive_failed  = 0  # consecutive pages that returned None
    unresolved_fetch_failure = False
    reached_natural_end = False
    previous_page_ids = None
    try:
        page = 0
        while shutdown_event is None or not shutdown_event.is_set():
            page += 1
            if page > 1:
                page_delay = random.uniform(0, 0.3)
                if shutdown_event is not None:
                    if shutdown_event.wait(page_delay):
                        break
                else:
                    time.sleep(page_delay)
            data, _ = fetch_page(item_name, page, shutdown_event)

            # Skip rather than abort on a fetch failure
            if data is None:
                if shutdown_event is not None and shutdown_event.is_set():
                    break
                consecutive_failed += 1
                if consecutive_failed >= MAX_PAGE_FAILURES:
                    unresolved_fetch_failure = True
                    print(f"[{time.strftime('%H:%M:%S')}] {item_name}: "
                          f"{MAX_PAGE_FAILURES} consecutive fetch failures at p{page}, stopping")
                    break
                print(f"[{time.strftime('%H:%M:%S')}] {item_name}: "
                      f"fetch failed p{page}, retrying the same page "
                      f"({consecutive_failed}/{MAX_PAGE_FAILURES})")
                page -= 1
                continue
            consecutive_failed = 0  # reset on any response

            # Genuine end-of-results from API
            if not isinstance(data, list) or len(data) == 0:
                reached_natural_end = True
                if page == 1:
                    record_metric("empty")
                break

            # Adjacent result pages must not contain the exact same trade IDs.
            # A broken pagination token or upstream cache can otherwise trap a
            # first historical backfill in an endless loop. Treat a repeat as
            # incomplete so the item is retried in a later cycle.
            try:
                page_ids = frozenset(trade["id"] for trade in data)
            except (KeyError, TypeError):
                raise ValueError(f"Malformed trade page for {item_name} p{page}")
            if previous_page_ids is not None and page_ids == previous_page_ids:
                unresolved_fetch_failure = True
                print(f"[{time.strftime('%H:%M:%S')}] {item_name}: repeated trade IDs on p{page}; stopping incomplete backfill")
                break
            previous_page_ids = page_ids

            record_metric("pages_fetched")
            total_pages += 1
            new_count = insert_trades(data, commit=False)
            total_new += new_count
            if new_count > 0:
                record_metric("new_trades", new_count)
                consecutive_no_new = 0
            if page in PAGE_WARN_MILESTONES:
                print(f"[{time.strftime('%H:%M:%S')}] Notice: {item_name} at page {page} ({total_new} new trades so far)")

            # Require CONSECUTIVE_EMPTY_THRESHOLD zero-new-trades pages before
            # early-stopping, instead of bailing on the very first one
            if item_already_backfilled and new_count == 0:
                consecutive_no_new += 1
                if consecutive_no_new >= CONSECUTIVE_EMPTY_THRESHOLD:
                    break

        if shutdown_event is not None and shutdown_event.is_set():
            return

        if not item_already_backfilled:
            if reached_natural_end and not unresolved_fetch_failure:
                # Do not record completion until every row queued by this item is
                # durably visible in SQLite. Otherwise a crash can leave a
                # completed marker ahead of missing historical rows.
                flush_database_writer()
                mark_backfilled(item_name, total_pages)
                print(f"[{time.strftime('%H:%M:%S')}] Backfill complete: {item_name} — {total_pages} pages, {total_new} trades")
            else:
                print(f"[{time.strftime('%H:%M:%S')}] Backfill incomplete: {item_name} — "
                      f"not marking complete because history did not reach a clean end")
        if total_new > 0:
            record_metric("success")
            print(f"[{time.strftime('%H:%M:%S')}] +{total_new} new trades over {total_pages} pages: {item_name}")
    except Exception as e:
        record_metric("failed")
        print(f"[{time.strftime('%H:%M:%S')}] Error on {item_name}: {e}")
    finally:
        with counter_lock:
            processed_count += 1
            if processed_count % 50 == 0:
                print(f"Progress: {processed_count} items processed this cycle...")


def summarize_cycle(duration, item_count):
    with metrics_lock:
        m = dict(cycle_metrics)
        for k in cycle_metrics:
            cycle_metrics[k] = [] if k == "latencies" else 0

    avg_latency = (sum(m["latencies"]) / len(m["latencies"])) if m["latencies"] else 0.0
    trades_per_min = (m["new_trades"] / duration * 60) if duration > 0 else 0.0
    print(
        f"[{time.strftime('%H:%M:%S')}] Cycle summary: "
        f"items={item_count}, duration={duration:.2f}s, "
        f"pages_fetched={m['pages_fetched']}, new_trades={m['new_trades']} ({trades_per_min:.1f}/min), "
        f"success={m['success']}, throttled={m['throttled']}, failed={m['failed']}, "
        f"timeouts={m['timeouts']}, empty={m['empty']}, "
        f"avg_latency={avg_latency*1000:.1f}ms"
    )
    return m


def adjust_workers(metrics):
    global WORKERS, current_workers
    pages = max(1, metrics["pages_fetched"])
    throttle_rate = metrics["throttled"] / pages
    fail_rate = metrics["failed"] / pages
    if throttle_rate > 0.05 or fail_rate > 0.05:
        new_workers = max(1, WORKERS - 1)
        if new_workers < WORKERS:
            print(f"[{time.strftime('%H:%M:%S')}] Throttle/fail rate high ({throttle_rate:.2%}/{fail_rate:.2%}). Reducing workers: {WORKERS} -> {new_workers}")
            WORKERS = new_workers
    elif throttle_rate == 0 and fail_rate == 0:
        new_workers = min(MAX_WORKERS, WORKERS + 1)
        if new_workers > WORKERS:
            print(f"[{time.strftime('%H:%M:%S')}] Clean cycle. Increasing workers: {WORKERS} -> {new_workers}")
            WORKERS = new_workers
    with current_workers_lock:
        current_workers = WORKERS


def poller_logic(shutdown_event=None):
    global processed_count
    print(f"Starting Scraper with {WORKERS} workers (pagination enabled)...")
    print(f"First cycle will backfill ALL history. Subsequent cycles use early-stop per item.")
    cycle_num = 0
    while shutdown_event is None or not shutdown_event.is_set():
        cycle_num += 1
        items = get_market_items()
        reset_stale_empty_items()
        load_backfilled_set()
        processed_count = 0
        start_time = time.time()
        print(f"\n[{time.strftime('%H:%M:%S')}] --- Cycle #{cycle_num} ({len(items)} items, {WORKERS} workers) ---")
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = []
            for item in items:
                if shutdown_event is not None and shutdown_event.is_set():
                    break
                futures.append(executor.submit(fetch_item, item, shutdown_event))
                if shutdown_event is not None:
                    if shutdown_event.wait(0.02):
                        break
                else:
                    time.sleep(0.02)
            for f in futures:
                f.result()
        duration = time.time() - start_time
        metrics = summarize_cycle(duration, len(items))
        adjust_workers(metrics)
        print(f"Cycle #{cycle_num} finished in {duration:.2f}s. Sleeping {POLLING_INTERVAL}s...\n")
        try:
            if cycle_num % 10 == 0:
                get_conn().execute("PRAGMA wal_checkpoint(RESTART);")
            else:
                get_conn().execute("PRAGMA wal_checkpoint(PASSIVE);")
        except Exception:
            pass
        if shutdown_event is not None:
            shutdown_event.wait(POLLING_INTERVAL)
        else:
            time.sleep(POLLING_INTERVAL)


# =============================================================================
# USER TRACKING
# =============================================================================

_tracked_users_lock = threading.Lock()
_tracked_users = {}

_alerts_lock = threading.Lock()
_recent_alerts = []
MAX_ALERT_BUFFER = 500


def load_tracked_users():
    global _tracked_users
    conn = get_conn()
    rows = conn.execute("SELECT username, last_trade_id FROM tracked_users").fetchall()
    with _tracked_users_lock:
        _tracked_users.clear()
        for username, last_trade_id in rows:
            _tracked_users[username] = {"last_trade_id": last_trade_id or 0}
    print(f"[Tracker] Loaded {len(_tracked_users)} tracked users from DB")


def add_tracked_user(username, discord_user_id=None):
    username = normalize_username(username)
    if not username:
        return False, "Invalid username"
    conn = get_conn()
    row = conn.execute(
        "SELECT MAX(id) FROM trades WHERE seller = ? OR buyer = ?", (username, username)
    ).fetchone()
    last_trade_id = row[0] or 0
    # A fresh registration starts at the current trade cursor; old unseen
    # alerts for a previous registration must not be delivered later.
    conn.execute("UPDATE trade_alerts SET seen = 1 WHERE username = ?", (username,))
    conn.execute(
        "INSERT INTO tracked_users "
        "(username, added_at, last_checked, last_trade_id, discord_user_id) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(username) DO UPDATE SET "
        "last_checked=excluded.last_checked, last_trade_id=excluded.last_trade_id, "
        "discord_user_id=COALESCE(excluded.discord_user_id, tracked_users.discord_user_id)",
        (
            username,
            time.strftime('%Y-%m-%d %H:%M:%S'),
            None,
            last_trade_id,
            int(discord_user_id) if discord_user_id else None,
        ),
    )
    conn.commit()
    with _tracked_users_lock:
        _tracked_users[username] = {"last_trade_id": last_trade_id}
    print(f"[Tracker] Now tracking: {username} (seeded at trade id {last_trade_id})")
    return True, f"Now tracking {username}"


def remove_tracked_user(username):
    username = normalize_username(username)
    conn = get_conn()
    conn.execute("UPDATE trade_alerts SET seen = 1 WHERE username = ?", (username,))
    conn.execute("DELETE FROM tracked_users WHERE username = ?", (username,))
    conn.commit()
    with _tracked_users_lock:
        _tracked_users.pop(username, None)
    print(f"[Tracker] Stopped tracking: {username}")
    return True, f"Stopped tracking {username}"


def check_tracked_user(username, last_trade_id):
    conn = get_conn()
    uname_lower = username.lower()
    rows = conn.execute(
        "SELECT id, timestamp, item_name, quantity, price, currency, seller, buyer "
        "FROM trades WHERE (LOWER(seller) = ? OR LOWER(buyer) = ?) AND id > ? ORDER BY id ASC",
        (uname_lower, uname_lower, last_trade_id)
    ).fetchall()

    alerts = []
    for r in rows:
        role = "seller" if (r[6] or "").lower() == uname_lower else "buyer"
        alerts.append({
            "username":   username,
            "trade_id":   r[0],
            "timestamp":  r[1],
            "item_name":  r[2],
            "quantity":   r[3],
            "price":      r[4],
            "currency":   r[5],
            "seller":     r[6],
            "buyer":      r[7],
            "role":       role,
            "alerted_at": time.strftime('%Y-%m-%d %H:%M:%S'),
            "seen":       False,
        })
    return alerts


def persist_alerts(alerts):
    if not alerts:
        return
    conn = get_conn()
    conn.executemany(
        "INSERT OR IGNORE INTO trade_alerts (username, trade_id, timestamp, item_name, quantity, price, currency, role, seen, alerted_at) "
        "VALUES (?,?,?,?,?,?,?,?,0,?)",
        [(a["username"], a["trade_id"], a["timestamp"], a["item_name"],
          a["quantity"], a["price"], a["currency"], a["role"], a["alerted_at"]) for a in alerts]
    )
    conn.commit()


def user_tracker_loop(shutdown_event=None):
    print(f"[Tracker] Starting user tracker (interval: {TRACKING_INTERVAL}s)")
    while shutdown_event is None or not shutdown_event.is_set():
        if shutdown_event is not None:
            if shutdown_event.wait(TRACKING_INTERVAL):
                break
        else:
            time.sleep(TRACKING_INTERVAL)
        with _tracked_users_lock:
            snapshot = dict(_tracked_users)

        if not snapshot:
            continue

        new_alerts = []
        for username, state in snapshot.items():
            try:
                alerts = check_tracked_user(username, state["last_trade_id"])
                if alerts:
                    max_id = max(a["trade_id"] for a in alerts)
                    # Persist idempotent alerts before advancing the cursor. If a
                    # later update fails, the next pass safely retries without
                    # losing or duplicating alert rows.
                    persist_alerts(alerts)
                    conn = get_conn()
                    conn.execute(
                        "UPDATE tracked_users SET last_trade_id = ?, last_checked = ? WHERE username = ?",
                        (max_id, time.strftime('%Y-%m-%d %H:%M:%S'), username)
                    )
                    conn.commit()
                    with _tracked_users_lock:
                        if username in _tracked_users:
                            _tracked_users[username]["last_trade_id"] = max_id
                    new_alerts.extend(alerts)
                    print(f"[Tracker] {username}: {len(alerts)} new trade(s)")
                else:
                    conn = get_conn()
                    conn.execute(
                        "UPDATE tracked_users SET last_checked = ? WHERE username = ?",
                        (time.strftime('%Y-%m-%d %H:%M:%S'), username)
                    )
                    conn.commit()
            except Exception as e:
                print(f"[Tracker] Error checking {username}: {e}")

        if new_alerts:
            with _alerts_lock:
                _recent_alerts.extend(new_alerts)
                if len(_recent_alerts) > MAX_ALERT_BUFFER:
                    del _recent_alerts[:-MAX_ALERT_BUFFER]


def get_pending_discord_alerts(limit=50):
    """Return unseen tracker alerts that have a Discord DM recipient."""
    limit = max(1, min(int(limit), 200))
    rows = get_conn().execute(
        "SELECT a.id, a.username, a.trade_id, a.timestamp, a.item_name, "
        "a.quantity, a.price, a.currency, a.role, u.discord_user_id "
        "FROM trade_alerts a JOIN tracked_users u ON u.username = a.username "
        "WHERE a.seen = 0 AND u.discord_user_id IS NOT NULL "
        "ORDER BY a.id ASC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "alert_id": row[0], "username": row[1], "trade_id": row[2],
            "timestamp": row[3], "item_name": row[4], "quantity": row[5],
            "price": row[6], "currency": row[7], "role": row[8],
            "discord_user_id": row[9],
        }
        for row in rows
    ]


def mark_trade_alert_seen(alert_id):
    conn = get_conn()
    conn.execute("UPDATE trade_alerts SET seen = 1 WHERE id = ?", (int(alert_id),))
    conn.commit()


# =============================================================================
# FLASK API
# =============================================================================

def paginate(total, page, limit):
    return {"page": page, "limit": limit, "total": total,
            "total_pages": max(1, (total + limit - 1) // limit)}


def trade_row(row):
    return {
        "id": row[0], "timestamp": row[1], "item_name": row[2],
        "quantity": row[3], "price": row[4], "currency": row[5],
        "seller": row[6], "buyer": row[7]
    }


def err(msg, code=500):
    return jsonify({"error": msg, "status": code,
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')}), code


def get_page_limit(max_limit=API_MAX_PAGE_LIMIT):
    page = max(1, int(request.args.get('page', 1)))
    limit = min(max_limit, max(1, int(request.args.get('limit', 50))))
    offset = (page - 1) * limit
    return page, limit, offset


@app.route('/health')
def health():
    return jsonify({"status": "healthy",
                    "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                    "version": APP_VERSION})


@app.route('/api/debug/chart')
def debug_chart():
    """Quick diagnostic — shows the last 3 trades, the 12h cutoff, and raw bucket counts."""
    try:
        conn = get_conn()
        now = time.time()
        cutoff_1h  = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now - 3600))
        cutoff_12h = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now - 12 * 3600))
        last3 = conn.execute(
            "SELECT id, timestamp FROM trades ORDER BY timestamp DESC LIMIT 3"
        ).fetchall()
        count_1h  = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE timestamp >= ?", (cutoff_1h,)
        ).fetchone()[0]
        count_12h = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE timestamp >= ?", (cutoff_12h,)
        ).fetchone()[0]
        return jsonify({
            "now_utc":      time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(now)),
            "cutoff_1h":    cutoff_1h,
            "cutoff_12h":   cutoff_12h,
            "count_last_1h":  count_1h,
            "count_last_12h": count_12h,
            "last_3_trades":  [{"id": r[0], "timestamp": r[1]} for r in last3],
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/stats')
def stats():
    try:
        conn = get_conn()
        total_trades = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        backfilled_items = conn.execute("SELECT COUNT(*) FROM backfill_status").fetchone()[0]
        total_users = conn.execute("""
            SELECT COUNT(DISTINCT username) FROM (
                SELECT seller AS username FROM trades WHERE seller IS NOT NULL AND seller != ''
                UNION
                SELECT buyer  AS username FROM trades WHERE buyer  IS NOT NULL AND buyer  != ''
            )
        """).fetchone()[0]
        top_items = conn.execute(
            "SELECT item_name, COUNT(*) FROM trades GROUP BY item_name ORDER BY COUNT(*) DESC LIMIT 10"
        ).fetchall()

        with metrics_lock:
            snap_trades = cycle_metrics["new_trades"]
            snap_pages = cycle_metrics["pages_fetched"]
            snap_throttled = cycle_metrics["throttled"]
            lats = cycle_metrics["latencies"][:]

        avg_lat = (sum(lats) / len(lats) * 1000) if lats else 0.0

        with current_workers_lock:
            workers_snap = current_workers

        return jsonify({
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "database": {
                "total_trades":    total_trades,
                "total_items":     len(get_items()),
                "total_users":     total_users,
                "backfilled_items": backfilled_items,
                "top_items":       [{"item": r[0], "trades": r[1]} for r in top_items]
            },
            "poller": {
                "worker_count": workers_snap,
                "current_cycle_new_trades": snap_trades,
                "current_cycle_pages_fetched": snap_pages,
                "current_cycle_throttled": snap_throttled,
                "avg_latency_ms": round(avg_lat, 1)
            }
        })

    except Exception as e:
        return err(str(e))


# ---------------------------------------------------------------------------
# Dashboard aggregation endpoints — pre-computed server-side so the frontend
# never needs to download raw trade rows for display purposes.
# ---------------------------------------------------------------------------
_dash_cache      = {}
_dash_cache_lock = threading.Lock()
DASH_CACHE_TTL   = 300  # 5 minutes


def _dash_cached(key, compute_fn):
    with _dash_cache_lock:
        entry = _dash_cache.get(key)
        if entry and time.time() - entry["ts"] < DASH_CACHE_TTL:
            return entry["data"]
    data = compute_fn()
    with _dash_cache_lock:
        _dash_cache[key] = {"data": data, "ts": time.time()}
    return data


@app.route('/api/dashboard/hot-items')
def dashboard_hot_items():
    try:
        hours = max(0, min(8_760, int(request.args.get("hours", 0))))
        key   = "hot_items_{}".format(hours)

        def compute():
            conn = get_conn()
            if hours > 0:
                cutoff = time.strftime(
                    '%Y-%m-%d %H:%M:%S',
                    time.gmtime(time.time() - hours * 3600)
                )
                rows = conn.execute("""
                    SELECT item_name, COUNT(*) AS cnt
                    FROM trades WHERE timestamp >= ?
                    GROUP BY item_name ORDER BY cnt DESC LIMIT 50
                """, (cutoff,)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT item_name, COUNT(*) AS cnt
                    FROM trades
                    GROUP BY item_name ORDER BY cnt DESC LIMIT 50
                """).fetchall()
            return [{"item": r[0], "trades": r[1]} for r in rows]

        return jsonify({"items": _dash_cached(key, compute), "hours": hours})
    except Exception as e:
        return err(str(e))


@app.route('/api/dashboard/chart')
def dashboard_chart():
    """
    ?hours=12  → 12 hourly buckets (default, last 12h)
    ?hours=24  → 24 hourly buckets
    ?hours=168 → 7 daily buckets (one per day)
    """
    try:
        import datetime
        hours  = max(1, min(720, int(request.args.get("hours", 12))))
        is_7d  = hours >= 48

        def compute():
            conn = get_conn()
            now  = time.time()

            if is_7d:
                bucket_size = 86400
                n_buckets   = hours // 24
            else:
                bucket_size = 3600
                n_buckets   = hours

            buckets = []
            for i in range(n_buckets - 1, -1, -1):
                t0 = time.strftime('%Y-%m-%d %H:%M:%S',
                                   time.gmtime(now - (i + 1) * bucket_size))
                ts = now - (i + 1) * bucket_size
                if i == 0:
                    count = conn.execute(
                        "SELECT COUNT(*) FROM trades WHERE timestamp >= ?", (t0,)
                    ).fetchone()[0]
                else:
                    t1 = time.strftime('%Y-%m-%d %H:%M:%S',
                                       time.gmtime(now - i * bucket_size))
                    count = conn.execute(
                        "SELECT COUNT(*) FROM trades WHERE timestamp >= ? AND timestamp < ?",
                        (t0, t1)
                    ).fetchone()[0]
                if is_7d:
                    label = datetime.datetime.utcfromtimestamp(ts).strftime('%a')
                else:
                    label = datetime.datetime.utcfromtimestamp(ts).strftime('%H:%M')
                buckets.append({"label": label, "count": count})
            return buckets

        key = "chart_{}h".format(hours)
        ttl = 60 if hours <= 24 else 300
        with _dash_cache_lock:
            entry = _dash_cache.get(key)
            # Reject cached entry if it's expired OR if all buckets are zero
            # (all-zero means it was cached during a broken period and must be recomputed)
            cached_valid = (
                entry is not None
                and time.time() - entry["ts"] < ttl
                and any(b["count"] > 0 for b in entry["data"])
            )
            if cached_valid:
                return jsonify({"buckets": entry["data"]})
        data = compute()
        if any(b["count"] > 0 for b in data):
            with _dash_cache_lock:
                _dash_cache[key] = {"data": data, "ts": time.time()}
        return jsonify({"buckets": data})

    except Exception as e:
        return err(str(e))


_leaderboard_cache = None
_leaderboard_cache_time = 0
_leaderboard_cache_lock = threading.Lock()
LEADERBOARD_CACHE_TTL = 120


def compute_leaderboard(limit=1000, sort="net_pnl"):
    sort_col = {
        "net_pnl":     "net_pnl DESC",
        "earned":      "earned DESC",
        "spent":       "spent DESC",
        "trade_count": "trade_count DESC",
    }.get(sort, "net_pnl DESC")

    conn = get_conn()
    rows = conn.execute(f"""
    SELECT
        username,
        SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END) AS earned,
        SUM(CASE WHEN role='buyer'  THEN coin_value ELSE 0 END) AS spent,
        SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END)
          - SUM(CASE WHEN role='buyer' THEN coin_value ELSE 0 END) AS net_pnl,
        SUM(CASE WHEN role='seller' THEN 1 ELSE 0 END) AS sell_count,
        SUM(CASE WHEN role='buyer'  THEN 1 ELSE 0 END) AS buy_count,
        COUNT(*) AS trade_count,
        MAX(timestamp) AS last_trade
    FROM (
        SELECT LOWER(seller) AS username, 'seller' AS role, timestamp,
            (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
              * CAST(quantity AS REAL) AS coin_value
        FROM trades WHERE seller IS NOT NULL AND seller != ''
        UNION ALL
        SELECT LOWER(buyer) AS username, 'buyer' AS role, timestamp,
            (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
              * CAST(quantity AS REAL) AS coin_value
        FROM trades WHERE buyer IS NOT NULL AND buyer != ''
    )
    GROUP BY username
    ORDER BY {sort_col}
    LIMIT ?
""", (limit,)).fetchall()

    result = []
    for i, r in enumerate(rows):
        result.append({
            "rank":        i + 1,
            "username":    r[0],
            "earned":      r[1] or 0,
            "spent":       r[2] or 0,
            "net_pnl":     r[3] or 0,
            "sell_count":  r[4] or 0,
            "buy_count":   r[5] or 0,
            "trade_count": r[6] or 0,
            "last_trade":  r[7],
        })
    return result


@app.route('/api/leaderboard')
def leaderboard():
    global _leaderboard_cache, _leaderboard_cache_time
    try:
        limit = min(2000, max(10, int(request.args.get('limit', 1000))))
        sort  = request.args.get('sort', 'net_pnl')
        if sort not in ('net_pnl', 'earned', 'spent', 'trade_count'):
            sort = 'net_pnl'
        force = request.args.get('refresh', '0') == '1'
        cache_key = f"leaderboard_{limit}_{sort}"

        with _dash_cache_lock:
            entry = _dash_cache.get(cache_key)
            age   = time.time() - (entry["ts"] if entry else 0)
            if not force and entry and age < LEADERBOARD_CACHE_TTL:
                data = entry["data"]
            else:
                data = compute_leaderboard(limit, sort)
                _dash_cache[cache_key] = {"data": data, "ts": time.time()}
                _leaderboard_cache      = data
                _leaderboard_cache_time = time.time()

        return jsonify({
            "timestamp":   time.strftime('%Y-%m-%d %H:%M:%S'),
            "total":       len(data),
            "cached_age":  round(time.time() - _leaderboard_cache_time),
            "users":       data
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/leaderboard/user/<path:username>')
def leaderboard_user_stats(username):
    """
    Compute leaderboard stats for a single user directly — no LIMIT constraint.
    Used when a user isn't in the cached top-N leaderboard.
    """
    try:
        conn = get_conn()
        uname = username.strip().lower()
        row = conn.execute("""
            SELECT
                SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END) AS earned,
                SUM(CASE WHEN role='buyer'  THEN coin_value ELSE 0 END) AS spent,
                SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END)
                  - SUM(CASE WHEN role='buyer' THEN coin_value ELSE 0 END) AS net_pnl,
                SUM(CASE WHEN role='seller' THEN 1 ELSE 0 END) AS sell_count,
                SUM(CASE WHEN role='buyer'  THEN 1 ELSE 0 END) AS buy_count,
                COUNT(*) AS trade_count,
                MAX(timestamp) AS last_trade
            FROM (
                SELECT 'seller' AS role, timestamp,
                    (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
                    * CAST(quantity AS REAL) AS coin_value
                FROM trades WHERE LOWER(seller) = ?
                UNION ALL
                SELECT 'buyer' AS role, timestamp,
                    (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
                    * CAST(quantity AS REAL) AS coin_value
                FROM trades WHERE LOWER(buyer) = ?
            )
        """, (uname, uname)).fetchone()

        if not row or not row[6]:  # no last_trade = no trades at all
            return jsonify({"found": False, "username": uname})

        return jsonify({
            "found":       True,
            "username":    uname,
            "earned":      row[0] or 0,
            "spent":       row[1] or 0,
            "net_pnl":     row[2] or 0,
            "sell_count":  row[3] or 0,
            "buy_count":   row[4] or 0,
            "trade_count": row[5] or 0,
            "last_trade":  row[6],
            "rank":        None,   # rank unknown — not in cached leaderboard
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/leaderboard/<path:username>/items')
def leaderboard_user_items(username):
    try:
        limit = min(100, max(5, int(request.args.get('limit', 20))))
        conn = get_conn()
        rows = conn.execute("""
            SELECT item_name,
                SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END)
                  - SUM(CASE WHEN role='buyer' THEN coin_value ELSE 0 END) AS net_pnl,
                SUM(CASE WHEN role='seller' THEN coin_value ELSE 0 END) AS earned,
                SUM(CASE WHEN role='buyer'  THEN coin_value ELSE 0 END) AS spent,
                SUM(CASE WHEN role='seller' THEN 1 ELSE 0 END) AS sell_count,
                SUM(CASE WHEN role='buyer'  THEN 1 ELSE 0 END) AS buy_count
            FROM (
                SELECT item_name, 'seller' AS role,
                    (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
                    * CAST(quantity AS REAL) AS coin_value
                FROM trades WHERE LOWER(seller) = LOWER(?)
                UNION ALL
                SELECT item_name, 'buyer' AS role,
                    (CASE WHEN currency=1 THEN CAST(price AS REAL)*100000000 ELSE CAST(price AS REAL) END)
                    * CAST(quantity AS REAL) AS coin_value
                FROM trades WHERE LOWER(buyer) = LOWER(?)
            )
            GROUP BY item_name
            ORDER BY net_pnl DESC
            LIMIT ?
        """, (username, username, limit)).fetchall()

        return jsonify({
            "username": username,
            "items": [{
                "item_name":  r[0],
                "net_pnl":    r[1] or 0,
                "earned":     r[2] or 0,
                "spent":      r[3] or 0,
                "sell_count": r[4] or 0,
                "buy_count":  r[5] or 0,
            } for r in rows]
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/items')
def api_items():
    try:
        page, limit, _ = get_page_limit()
        items = get_items()
        total = len(items)
        start = (page - 1) * limit
        page_items = items[start:start + limit]
        return jsonify({
            "data": [{"id": i["id"], "display_name": i["display_name"],
                      "search_name": i["search_name"]} for i in page_items],
            "pagination": paginate(total, page, limit)
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/items/<path:name>')
def api_item_detail(name):
    try:
        name_lower = name.lower().replace("%20", " ").replace("+", " ")
        items = get_items()
        match = next((i for i in items if i["display_name"].lower() == name_lower), None)
        if not match:
            match = next((i for i in items if name_lower in i["display_name"].lower()), None)
        if not match:
            return err("Item not found", 404)
        return jsonify({"id": match["id"], "display_name": match["display_name"],
                        "search_name": match["search_name"]})
    except Exception as e:
        return err(str(e))


@app.route('/api/search/items')
def search_items():
    try:
        q = request.args.get('q', '').lower().strip()
        if not q:
            return err("q parameter required", 400)
        page, limit, _ = get_page_limit()
        items = get_items()
        results = [i for i in items if q in i["display_name"].lower()]
        total = len(results)
        start = (page - 1) * limit
        page_results = results[start:start + limit]
        return jsonify({
            "data": [{"id": i["id"], "display_name": i["display_name"],
                      "search_name": i["search_name"]} for i in page_results],
            "pagination": paginate(total, page, limit)
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/search/users')
def search_users():
    try:
        q = request.args.get('q', '').strip().lower()
        if not q:
            return err("q parameter required", 400)
        page, limit, offset = get_page_limit()
        # Prefix pattern on LOWER() column — uses the LOWER() expression index
        prefix  = f"{q}%"
        # Also support contains-search for short queries via a second pattern
        # We union both so typing "cos" finds "cosmicbot" (prefix) and "mycosmic" (contains)
        contains = f"%{q}%"
        conn = get_conn()
        total = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT DISTINCT LOWER(seller) AS username FROM trades
                WHERE LOWER(seller) LIKE ?
                UNION
                SELECT DISTINCT LOWER(buyer) AS username FROM trades
                WHERE LOWER(buyer) LIKE ?
            )
        """, (contains, contains)).fetchone()[0]

        rows = conn.execute("""
            SELECT username,
                SUM(CASE WHEN role='seller' THEN 1 ELSE 0 END) as sell_count,
                SUM(CASE WHEN role='buyer'  THEN 1 ELSE 0 END) as buy_count,
                COUNT(*) as trade_count,
                MAX(timestamp) as last_traded
            FROM (
                SELECT LOWER(seller) AS username, 'seller' AS role, timestamp
                FROM trades WHERE LOWER(seller) LIKE ?
                UNION ALL
                SELECT LOWER(buyer) AS username, 'buyer' AS role, timestamp
                FROM trades WHERE LOWER(buyer) LIKE ?
            )
            GROUP BY username
            ORDER BY trade_count DESC
            LIMIT ? OFFSET ?
        """, (contains, contains, limit, offset)).fetchall()

        return jsonify({
            "data": [{"username": r[0], "sell_count": r[1], "buy_count": r[2],
                      "trade_count": r[3], "last_traded": r[4]} for r in rows],
            "pagination": paginate(total, page, limit)
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/trades')
def api_trades():
    try:
        page, limit, offset = get_page_limit()
        conn = get_conn()
        total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
        return jsonify({"data": [trade_row(r) for r in rows],
                        "pagination": paginate(total, page, limit)})
    except Exception as e:
        return err(str(e))


@app.route('/api/trades/item/<path:name>')
def trades_by_item(name):
    try:
        name = name.replace("%20", " ").replace("+", " ")
        page, limit, offset = get_page_limit()
        items = get_items()
        name_lower = name.lower()
        match = next((i for i in items if i["display_name"].lower() == name_lower), None)
        if not match:
            match = next((i for i in items if name_lower in i["display_name"].lower()), None)

        conn = get_conn()
        if match:
            db_name = match["db_name_clean"]
            total = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE LOWER(item_name) = LOWER(?)", (db_name,)
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
                "FROM trades WHERE LOWER(item_name) = LOWER(?) ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (db_name, limit, offset)
            ).fetchall()
        else:
            pattern = f"%{name}%"
            total = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE LOWER(item_name) LIKE LOWER(?)", (pattern,)
            ).fetchone()[0]
            rows = conn.execute(
                "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
                "FROM trades WHERE LOWER(item_name) LIKE LOWER(?) ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (pattern, limit, offset)
            ).fetchall()

        if total == 0 and not match:
            return err("Item not found", 404)

        item_info = ({"id": match["id"], "display_name": match["display_name"],
                      "search_name": match["search_name"]} if match else {"display_name": name})
        return jsonify({"item": item_info, "trades": [trade_row(r) for r in rows],
                        "pagination": paginate(total, page, limit)})
    except Exception as e:
        return err(str(e))


@app.route('/api/trades/user/<path:username>')
def trades_by_user(username):
    try:
        page, limit, offset = get_page_limit()
        clean = username.strip().lower()
        conn = get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE LOWER(buyer) = ? OR LOWER(seller) = ?",
            (clean, clean)
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades WHERE LOWER(buyer) = ? OR LOWER(seller) = ? "
            "ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (clean, clean, limit, offset)
        ).fetchall()
        return jsonify({"username": clean, "type": "both",
                        "trades": [trade_row(r) for r in rows],
                        "pagination": paginate(total, page, limit)})
    except Exception as e:
        return err(str(e))


@app.route('/api/trades/buyer/<path:username>')
def trades_by_buyer(username):
    try:
        page, limit, offset = get_page_limit()
        clean = username.strip().lower()
        conn = get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE LOWER(buyer) = ?", (clean,)
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades WHERE LOWER(buyer) = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (clean, limit, offset)
        ).fetchall()
        return jsonify({"username": clean, "type": "buyer",
                        "trades": [trade_row(r) for r in rows],
                        "pagination": paginate(total, page, limit)})
    except Exception as e:
        return err(str(e))


@app.route('/api/trades/seller/<path:username>')
def trades_by_seller(username):
    try:
        page, limit, offset = get_page_limit()
        clean = username.strip().lower()
        conn = get_conn()
        total = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE LOWER(seller) = ?", (clean,)
        ).fetchone()[0]
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades WHERE LOWER(seller) = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
            (clean, limit, offset)
        ).fetchall()
        return jsonify({"username": clean, "type": "seller",
                        "trades": [trade_row(r) for r in rows],
                        "pagination": paginate(total, page, limit)})
    except Exception as e:
        return err(str(e))


@app.route('/api/dump/items')
def dump_items():
    try:
        items = get_items()
        return jsonify({
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "total": len(items),
            "items": [{"id": i["id"], "display_name": i["display_name"],
                       "search_name": i["search_name"]} for i in items]
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/items/db')
def items_in_db():
    try:
        items_cache = get_items()
        items_by_db = {i["db_name"].lower(): i for i in items_cache}
        conn = get_conn()
        rows = conn.execute(
            "SELECT LOWER(item_name) as name, COUNT(*) as cnt FROM trades GROUP BY LOWER(item_name)"
        ).fetchall()
        result = []
        for db_name, cnt in rows:
            meta = items_by_db.get(db_name, {})
            result.append({
                "display_name": meta.get("display_name", db_name),
                "search_name": meta.get("search_name", db_name),
                "db_name": db_name,
                "trade_count": cnt
            })
        result.sort(key=lambda x: -x["trade_count"])
        return jsonify({"items": result, "total": len(result)})
    except Exception as e:
        return err(str(e))


@app.route('/api/items/stats/<path:name>')
def item_stats(name):
    try:
        name = name.replace("%20", " ").replace("+", " ")
        items = get_items()
        name_lower = name.lower()
        matches = [i for i in items if i["display_name"].lower() == name_lower]
        db_names = [i["db_name_clean"] for i in matches] if matches else [name]

        conn = get_conn()
        placeholders = ",".join("?" * len(db_names))
        db_names_lower = [d.lower() for d in db_names]
        agg = conn.execute(
            "SELECT MIN(CASE WHEN currency=1 THEN price*100000000 ELSE price END), "
            "MAX(CASE WHEN currency=1 THEN price*100000000 ELSE price END), "
            "AVG(CASE WHEN currency=1 THEN price*100000000 ELSE price END), COUNT(*) "
            "FROM trades WHERE LOWER(item_name) IN ({})".format(placeholders),
            db_names_lower
        ).fetchone()
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades WHERE LOWER(item_name) IN ({}) ORDER BY timestamp DESC LIMIT 2000".format(placeholders),
            db_names_lower
        ).fetchall()
        return jsonify({
            "item": db_names[0],
            "min_price": agg[0], "max_price": agg[1],
            "avg_price": round(agg[2]) if agg[2] else 0,
            "volume": agg[3], "currency": 0,
            "trades": [trade_row(r) for r in rows]
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/items/stats/bulk', methods=['POST'])
def item_stats_bulk():
    try:
        body = request.get_json() or {}
        names = body.get('names', [])
        if not isinstance(names, list):
            return err("names must be a list", 400)
        names = list(dict.fromkeys(
            str(name).strip() for name in names if str(name).strip()
        ))
        if len(names) > API_MAX_BULK_ITEMS:
            return err(f"at most {API_MAX_BULK_ITEMS} names may be requested at once", 400)
        hours = max(1, min(720, int(body.get('hours', 4))))
        if not names:
            return jsonify({"items": {}})

        cutoff          = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() - hours * 3600))
        fallback_cutoff = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() - 30 * 24 * 3600))
        currency_tokens = {"vintage point ticket", "blood money"}

        conn = get_conn()
        names_lower = [n.lower() for n in names]
        ph = ",".join("?" * len(names_lower))

        # --- Bulk query 1: all items within the requested window ---
        window_rows = conn.execute(
            "SELECT LOWER(item_name), "
            "AVG(CASE WHEN currency = 1 THEN price * 100000000 ELSE price END), "
            "COUNT(*), MAX(timestamp) "
            f"FROM trades WHERE LOWER(item_name) IN ({ph}) AND timestamp >= ? "
            "GROUP BY LOWER(item_name)",
            names_lower + [cutoff]
        ).fetchall()
        window_map = {r[0]: {"avg": r[1], "count": r[2], "last": r[3]} for r in window_rows}

        # --- Bulk query 2: fallback window (30d) for items missing from window ---
        missing = [n for n in names_lower if n not in window_map or window_map[n]["count"] == 0]
        # currency tokens always use fallback window
        missing += [n for n in names_lower if n in currency_tokens and n not in missing]
        fallback_map = {}
        if missing:
            ph2 = ",".join("?" * len(missing))
            fb_rows = conn.execute(
                "SELECT LOWER(item_name), "
                "AVG(CASE WHEN currency = 1 THEN price * 100000000 ELSE price END), "
                "COUNT(*), MAX(timestamp) "
                f"FROM trades WHERE LOWER(item_name) IN ({ph2}) AND timestamp >= ? "
                "GROUP BY LOWER(item_name)",
                missing + [fallback_cutoff]
            ).fetchall()
            fallback_map = {r[0]: {"avg": r[1], "count": r[2], "last": r[3]} for r in fb_rows}

        result = {}
        for name_lower in names_lower:
            is_currency_token = name_lower in currency_tokens
            if is_currency_token:
                entry = fallback_map.get(name_lower) or window_map.get(name_lower)
                within_window = False
            else:
                entry = window_map.get(name_lower)
                within_window = bool(entry and entry["count"] > 0)
                if not within_window:
                    entry = fallback_map.get(name_lower)
            if entry and entry["count"] > 0:
                result[name_lower] = {
                    "price":         entry["avg"],
                    "within_window": within_window,
                    "last_trade":    entry["last"],
                    "trade_count":   entry["count"]
                }

        return jsonify({"items": result})
    except Exception as e:
        return err(str(e))


@app.route('/api/currencies')
def get_currencies():
    try:
        with open(os.path.join(BASE_DIR, "currencies.json"), 'r') as f:
            data = json.load(f)
        return jsonify(data)
    except Exception as e:
        return err(str(e))


@app.route('/api/rwt')
def rwt_outliers():
    """
    Server-side RWT outlier computation.
    Computes per-item avg price and returns only trades that deviate beyond threshold.
    ?threshold=0.5  — fraction deviation (default 0.5 = 50%)
    ?days=all|14|30|60|90|ytd — time window (default 30)
    ?limit=5000     — max outlier rows to return (capped at 10,000)
    ?scan_limit=100000 — recent rows to inspect (capped at 250,000)
    """
    try:
        threshold = float(request.args.get('threshold', 0.5))
        days      = request.args.get('days', '30')
        limit_out = min(10_000, max(1, int(request.args.get('limit', 5_000))))
        scan_limit = min(
            API_MAX_ANALYTICS_SCAN,
            max(1_000, int(request.args.get('scan_limit', 100_000))),
        )
        conn      = get_conn()

        cutoff_clause = ""
        params: list = []
        if days != 'all':
            if days == 'ytd':
                import datetime
                cutoff = datetime.datetime(datetime.datetime.utcnow().year, 1, 1).strftime('%Y-%m-%d %H:%M:%S')
            else:
                cutoff = time.strftime('%Y-%m-%d %H:%M:%S',
                    time.gmtime(time.time() - int(days) * 86400))
            cutoff_clause = "AND timestamp >= ?"
            params.append(cutoff)

        # Step 1: compute per-item avg in one pass
        avg_rows = conn.execute(
            f"SELECT LOWER(item_name), "
            f"AVG(CASE WHEN currency=1 THEN price*100000000.0 ELSE CAST(price AS REAL) END) "
            f"FROM trades WHERE price > 0 {cutoff_clause} "
            f"GROUP BY LOWER(item_name)",
            params
        ).fetchall()
        avg_map = {r[0]: r[1] for r in avg_rows if r[1] and r[1] > 0}

        if not avg_map:
            return jsonify({"trades": [], "total": 0})

        # Step 2: fetch a bounded recent slice then filter in Python.
        # This avoids a slow correlated subquery without exhausting VM memory.
        trade_rows = conn.execute(
            f"SELECT id, timestamp, item_name, quantity, price, currency, seller, buyer "
            f"FROM trades WHERE price > 0 {cutoff_clause} "
            f"ORDER BY timestamp DESC LIMIT ?",
            list(params) + [scan_limit]
        ).fetchall()

        outliers = []
        for r in trade_rows:
            key = (r[2] or "").lower()
            avg = avg_map.get(key)
            if not avg:
                continue
            qty = r[3] or 1
            p   = r[4] * 100000000 if r[5] == 1 else r[4]
            dev = (p - avg) / avg
            if abs(dev) > threshold:
                outliers.append({
                    "item_name":   r[2],
                    "price":       p,
                    "avg":         avg,
                    "dev":         dev,
                    "total_value": p * qty,
                    "seller":      r[6],
                    "buyer":       r[7],
                    "timestamp":   r[1],
                    "quantity":    qty,
                    "id":          r[0],
                })

        outliers.sort(key=lambda x: -abs(x["dev"]))
        return jsonify({
            "trades": outliers[:limit_out],
            "total": len(outliers),
            "rows_scanned": len(trade_rows),
            "scan_truncated": len(trade_rows) >= scan_limit,
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/dump/trades')
def dump_trades():
    try:
        limit = min(
            API_MAX_EXPORT_ROWS,
            max(1, int(request.args.get('limit', 10_000))),
        )
        conn = get_conn()
        total = conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        rows = conn.execute(
            "SELECT id,timestamp,item_name,quantity,price,currency,seller,buyer "
            "FROM trades ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return jsonify({
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
            "total": total,
            "returned": len(rows),
            "truncated": total > len(rows),
            "trades": [trade_row(r) for r in rows],
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/merchables')
def get_merchables():
    try:
        import math, statistics as _stats
        days       = max(1, min(365, int(request.args.get('days', 30))))
        min_trades = max(1, int(request.args.get('min_trades', 10)))
        limit      = max(1, min(500, int(request.args.get('limit', 100))))
        scan_limit = min(
            API_MAX_ANALYTICS_SCAN,
            max(1_000, int(request.args.get('scan_limit', 100_000))),
        )
        sort_by    = request.args.get('sort', 'margin')

        cutoff = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() - days * 86400))
        conn   = get_conn()

        rows = conn.execute(
            "SELECT LOWER(item_name), "
            "CASE WHEN currency = 1 THEN price * 100000000 ELSE price END as norm_price "
            "FROM trades WHERE timestamp >= ? AND price > 0 "
            "ORDER BY timestamp DESC LIMIT ?",
            (cutoff, scan_limit)
        ).fetchall()

        if not rows:
            return jsonify({"items": [], "total": 0, "days": days})

        item_prices = {}
        for item_name, price in rows:
            if item_name not in item_prices:
                item_prices[item_name] = []
            item_prices[item_name].append(price)

        results = []
        for item_name, prices in item_prices.items():
            if len(prices) < min_trades:
                continue

            prices_sorted = sorted(prices)
            n = len(prices_sorted)

            def percentile(p, ps=prices_sorted, cnt=n):
                idx = (p / 100) * (cnt - 1)
                lo, hi = int(idx), min(int(idx) + 1, cnt - 1)
                return ps[lo] + (ps[hi] - ps[lo]) * (idx - lo)

            p10    = percentile(10)
            p90    = percentile(90)
            median = percentile(50)
            mean   = sum(prices) / n

            if median <= 0 or mean <= 0:
                continue

            spread_pct = (p90 - p10) / median * 100
            try:
                stddev = _stats.stdev(prices)
            except Exception:
                stddev = 0
            cv          = (stddev / mean * 100) if mean > 0 else 0
            reliability = max(0.0, min(100.0, 100.0 - cv))
            vol_weight   = math.log10(n + 1)
            margin_score = spread_pct  * vol_weight
            rel_score    = reliability * vol_weight

            results.append({
                "item":         item_name,
                "low":          round(p10),
                "high":         round(p90),
                "median":       round(median),
                "spread_pct":   round(spread_pct, 1),
                "reliability":  round(reliability, 1),
                "volume":       n,
                "margin_score": round(margin_score, 2),
                "rel_score":    round(rel_score, 2),
            })

        if sort_by == 'reliability':
            results.sort(key=lambda x: -x['rel_score'])
        else:
            results.sort(key=lambda x: -x['margin_score'])

        return jsonify({
            "items": results[:limit],
            "total": len(results),
            "days": days,
            "sort": sort_by,
            "rows_scanned": len(rows),
            "scan_truncated": len(rows) >= scan_limit,
        })
    except Exception as e:
        return err(str(e))


# =============================================================================
# TRACKING API
# =============================================================================

@app.route('/api/tracking/add', methods=['POST'])
def tracking_add():
    try:
        body = request.get_json()
        username = (body or {}).get('username', '').strip()
        if not username:
            return err("username required", 400)
        ok, msg = add_tracked_user(username)
        return jsonify({"success": ok, "message": msg, "username": normalize_username(username)})
    except Exception as e:
        return err(str(e))


@app.route('/api/tracking/remove', methods=['POST'])
def tracking_remove():
    try:
        body = request.get_json()
        username = (body or {}).get('username', '').strip()
        if not username:
            return err("username required", 400)
        ok, msg = remove_tracked_user(username)
        return jsonify({"success": ok, "message": msg})
    except Exception as e:
        return err(str(e))


@app.route('/api/tracking/list')
def tracking_list():
    try:
        conn = get_conn()
        rows = conn.execute(
            "SELECT username, added_at, last_checked, last_trade_id FROM tracked_users ORDER BY added_at DESC"
        ).fetchall()
        return jsonify({
            "tracked": [
                {"username": r[0], "added_at": r[1], "last_checked": r[2], "last_trade_id": r[3]}
                for r in rows
            ],
            "total": len(rows)
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/tracking/alerts')
def tracking_alerts():
    try:
        unseen_only = request.args.get('unseen', '0') == '1'
        username    = request.args.get('username', '').strip().lower() or None
        limit       = min(500, max(1, int(request.args.get('limit', 50))))
        mark_seen   = request.args.get('mark_seen', '0') == '1'

        conn = get_conn()
        query = "SELECT id, username, trade_id, timestamp, item_name, quantity, price, currency, role, seen, alerted_at FROM trade_alerts WHERE 1=1"
        params = []
        if unseen_only:
            query += " AND seen = 0"
        if username:
            query += " AND username = ?"
            params.append(username)
        query += " ORDER BY alerted_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        alerts = [
            {"id": r[0], "username": r[1], "trade_id": r[2], "timestamp": r[3],
             "item_name": r[4], "quantity": r[5], "price": r[6], "currency": r[7],
             "role": r[8], "seen": bool(r[9]), "alerted_at": r[10]}
            for r in rows
        ]

        if mark_seen and alerts:
            ids = [a["id"] for a in alerts]
            conn.execute(
                "UPDATE trade_alerts SET seen = 1 WHERE id IN ({})".format(",".join("?" * len(ids))),
                ids
            )
            conn.commit()

        unseen_count = conn.execute("SELECT COUNT(*) FROM trade_alerts WHERE seen = 0").fetchone()[0]
        return jsonify({"alerts": alerts, "total": len(alerts), "unseen_count": unseen_count})
    except Exception as e:
        return err(str(e))


@app.route('/api/tracking/alerts/clear', methods=['POST'])
def tracking_alerts_clear():
    try:
        username = (request.get_json() or {}).get('username', '').strip().lower() or None
        conn = get_conn()
        if username:
            conn.execute("UPDATE trade_alerts SET seen = 1 WHERE username = ?", (username,))
        else:
            conn.execute("UPDATE trade_alerts SET seen = 1")
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        return err(str(e))


# =============================================================================
# COVERAGE / COMPLETENESS
# =============================================================================

@app.route('/api/coverage/gaps')
def coverage_gaps():
    """
    Analyses the database for items that may have incomplete trade data.
    Returns three categories:
      - never_backfilled : items in items.json that have no backfill_status row
      - zero_trades      : backfilled with pages > 0 but 0 trades in DB
      - low_yield        : trades-per-page below 5 (API normally returns ~25)
    """
    try:
        conn      = get_conn()
        all_items = get_market_items()

        bf_rows = conn.execute(
            "SELECT item_name, completed_at, total_pages FROM backfill_status"
        ).fetchall()
        bf_map = {r[0]: {"completed_at": r[1], "total_pages": r[2] or 0} for r in bf_rows}

        trade_rows = conn.execute(
            "SELECT LOWER(item_name), COUNT(*) FROM trades GROUP BY LOWER(item_name)"
        ).fetchall()
        trade_counts = {r[0]: r[1] for r in trade_rows}

        never_backfilled = [i for i in all_items if i not in bf_map]

        zero_trades = [
            i for i, info in bf_map.items()
            if info["total_pages"] > 0 and trade_counts.get(i.lower(), 0) == 0
        ]

        low_yield = []
        for item, info in bf_map.items():
            pages = info["total_pages"]
            if pages < 3:
                continue
            trades   = trade_counts.get(item.lower(), 0)
            per_page = trades / pages
            if per_page < 5:
                low_yield.append({
                    "item":         item,
                    "trades":       trades,
                    "pages":        pages,
                    "per_page":     round(per_page, 1),
                    "completed_at": info["completed_at"],
                })
        low_yield.sort(key=lambda x: x["per_page"])

        return jsonify({
            "timestamp":              time.strftime('%Y-%m-%d %H:%M:%S'),
            "total_items":            len(all_items),
            "never_backfilled_count": len(never_backfilled),
            "zero_trades_count":      len(zero_trades),
            "low_yield_count":        len(low_yield),
            "never_backfilled_list":  never_backfilled[:200],
            "zero_trades_list":       zero_trades[:200],
            "low_yield_items":        low_yield[:200],
        })
    except Exception as e:
        return err(str(e))


@app.route('/api/coverage/reset', methods=['POST'])
def coverage_reset():
    """
    Clears backfill_status for the given items so the scraper will
    do a full re-scrape on the next cycle.

    Body: { "items": ["item1", ...], "mode": "specific" }
      or  { "mode": "all" }  to reset everything
    """
    try:
        body  = request.get_json() or {}
        items = body.get('items', [])
        mode  = body.get('mode', 'specific')
        conn  = get_conn()

        if mode == 'all':
            conn.execute("DELETE FROM backfill_status")
            conn.commit()
            with _backfilled_lock:
                _backfilled_set.clear()
            return jsonify({"success": True, "message": "All backfill status cleared", "count": -1})

        if not items:
            return err("items list required for mode=specific", 400)
        if not isinstance(items, list):
            return err("items must be a list", 400)
        items = list(dict.fromkeys(
            str(item).strip() for item in items if str(item).strip()
        ))
        if len(items) > API_MAX_BULK_ITEMS:
            return err(f"at most {API_MAX_BULK_ITEMS} items may be reset at once", 400)

        placeholders = ','.join('?' * len(items))
        conn.execute(f"DELETE FROM backfill_status WHERE item_name IN ({placeholders})", items)
        conn.commit()
        with _backfilled_lock:
            for item in items:
                _backfilled_set.discard(str(item).lower())
        return jsonify({"success": True, "message": f"Reset {len(items)} item(s)", "count": len(items)})
    except Exception as e:
        return err(str(e))


# =============================================================================
# SYSTEM INFO & REBUILD
# =============================================================================
_server_start_time = time.time()


@app.route('/api/system')
def system_info():
    db_size_bytes = 0
    try:
        db_size_bytes = os.path.getsize(DB_PATH)
    except Exception:
        pass
    uptime_s = int(time.time() - _server_start_time)
    h, rem   = divmod(uptime_s, 3600)
    m, s_val = divmod(rem, 60)
    return jsonify({
        "python_version":    sys.version.split()[0],
        "db_path":           DB_PATH,
        "db_size_bytes":     db_size_bytes,
        "db_size_mb":        round(db_size_bytes / 1024 / 1024, 2),
        "items_json_path":   ITEMS_JSON,
        "uptime_seconds":    uptime_s,
        "uptime_formatted":  f"{h}h {m}m {s_val}s",
        "workers":           WORKERS,
        "polling_interval":  POLLING_INTERVAL,
        "tracking_interval": TRACKING_INTERVAL,
        "aws_url":           AWS_URL,
    })


# =============================================================================
# MAIN
# =============================================================================
def run_api_server(shutdown_event=None):
    """Run the optional API with an interruptible production WSGI server."""
    from werkzeug.serving import make_server

    server = make_server(
        settings.api_host,
        settings.api_port,
        app,
        # One API thread avoids orphaned request workers during process shutdown.
        # The Discord bot and scraper remain independent background components.
        threaded=False,
    )
    server.timeout = 1.0
    try:
        while shutdown_event is None or not shutdown_event.is_set():
            server.handle_request()
    finally:
        server.server_close()
