from __future__ import annotations

import json
import queue
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path
from unittest import mock


# The production install includes Flask. These minimal stubs let the database and
# scraper core be tested in restricted build environments where Flask is absent.
try:
    import flask  # noqa: F401
except ImportError:
    flask_module = types.ModuleType("flask")

    class DummyFlask:
        def __init__(self, *args, **kwargs):
            pass

        def route(self, *args, **kwargs):
            return lambda function: function

        def run(self, *args, **kwargs):
            return None

    class DummyArgs(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    flask_module.Flask = DummyFlask
    flask_module.jsonify = lambda value=None, **kwargs: value if value is not None else kwargs
    flask_module.request = types.SimpleNamespace(
        args=DummyArgs(), get_json=lambda: {}, remote_addr="127.0.0.1"
    )
    sys.modules["flask"] = flask_module

    flask_cors_module = types.ModuleType("flask_cors")
    flask_cors_module.CORS = lambda app: app
    sys.modules["flask_cors"] = flask_cors_module

from uav_service import market_service


class MarketServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.db_path = root / "entiredatabase.db"
        self.items_path = root / "items.json"
        self.items_path.write_text(
            json.dumps(
                [
                    {"id": 1, "name": "Example", "tradeable": True},
                    {"id": 2, "name": "Example", "tradeable": True},
                    {"id": 3, "name": "Hidden", "tradeable": False},
                ]
            ),
            encoding="utf-8",
        )
        market_service.DB_PATH = str(self.db_path)
        market_service.ITEMS_JSON = str(self.items_path)
        market_service.BASE_DIR = str(root)
        market_service._thread_local = threading.local()
        market_service._items_cache = None
        market_service._items_by_name = {}
        market_service._backfilled_set = set()
        market_service._write_queue = queue.Queue(maxsize=100)
        market_service._pending_ids = set()
        market_service.start_accepting_writer_batches()
        market_service.processed_count = 0
        market_service.init_db()

    def tearDown(self) -> None:
        conn = getattr(market_service._thread_local, "conn", None)
        if conn is not None:
            conn.close()
        self.temp_dir.cleanup()

    @staticmethod
    def trade(trade_id: int = 1) -> dict:
        return {
            "id": trade_id,
            "time": "2026-08-06 12:00:00",
            "item_name": "Example",
            "amount": 1,
            "price": 100,
            "currency": 0,
            "seller": "Seller",
            "buyer": "Buyer",
        }

    def test_item_loader_filters_and_deduplicates(self) -> None:
        items = market_service.load_items_cache()
        self.assertEqual([item["display_name"] for item in items], ["Example"])

    def test_insert_trades_uses_database_duplicate_check(self) -> None:
        writer = threading.Thread(target=market_service._db_writer_loop)
        writer.start()
        try:
            self.assertEqual(market_service.insert_trades([self.trade()]), 1)
            market_service.flush_database_writer(timeout=5)
            check_conn = sqlite3.connect(self.db_path)
            try:
                count = check_conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
            finally:
                check_conn.close()
            self.assertEqual(count, 1)
            self.assertEqual(market_service.insert_trades([self.trade()]), 0)
        finally:
            market_service._write_queue.put(None)
            writer.join(timeout=5)
        self.assertFalse(writer.is_alive())

    def test_pending_ids_prevent_duplicate_queueing_before_commit(self) -> None:
        self.assertEqual(market_service.insert_trades([self.trade(), self.trade()]), 1)
        self.assertEqual(market_service.insert_trades([self.trade()]), 0)



    def test_shutdown_gate_is_atomic_with_queue_insertion(self) -> None:
        entered_query = threading.Event()
        release_query = threading.Event()
        errors: list[Exception] = []

        class FakeResult:
            def fetchall(self):
                entered_query.set()
                release_query.wait(5)
                return []

        class FakeConn:
            def execute(self, sql, params=()):
                return FakeResult()

        def producer():
            try:
                market_service.insert_trades([self.trade(77)])
            except Exception as exc:
                errors.append(exc)

        with mock.patch.object(market_service, "get_conn", return_value=FakeConn()):
            thread = threading.Thread(target=producer)
            thread.start()
            self.assertTrue(entered_query.wait(2))
            market_service.stop_accepting_writer_batches()
            market_service._write_queue.put(None)
            release_query.set()
            thread.join(5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIn("shutting down", str(errors[0]))
        self.assertIsNone(market_service._write_queue.get_nowait())
        self.assertTrue(market_service._write_queue.empty())

    def test_large_api_page_is_checked_in_sqlite_safe_chunks(self) -> None:
        calls: list[tuple] = []

        class FakeResult:
            def fetchall(self):
                return []

        class FakeConn:
            def execute(self, sql, params=()):
                calls.append(tuple(params))
                return FakeResult()

        trades = [self.trade(trade_id) for trade_id in range(1, 1202)]
        with mock.patch.object(market_service, "get_conn", return_value=FakeConn()):
            self.assertEqual(market_service.insert_trades(trades), 1201)
        self.assertEqual([len(params) for params in calls], [500, 500, 201])
        queued = market_service._write_queue.get_nowait()
        self.assertEqual(len(queued), 1201)

    def test_http_session_is_recreated_after_thread_cleanup(self) -> None:
        old_session = mock.Mock()
        new_session = mock.Mock()
        new_session.headers = mock.Mock()
        market_service._thread_local.session = old_session
        market_service.close_thread_connection()
        with mock.patch.object(market_service.requests, "Session", return_value=new_session):
            self.assertIs(market_service.get_session(), new_session)
        old_session.close.assert_called_once()

    def test_backfilled_item_stops_after_two_existing_pages(self) -> None:
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
            (1, "2026-08-06 12:00:00", "Example", 1, 100, 0, "seller", "buyer"),
        )
        conn.commit()
        conn.close()
        market_service._backfilled_set.add("example")
        pages: list[int] = []

        def fake_fetch(item_name, page, shutdown_event=None):
            pages.append(page)
            return [self.trade()], 0.01

        with mock.patch.object(market_service, "fetch_page", side_effect=fake_fetch), mock.patch.object(
            market_service.random, "uniform", return_value=0
        ):
            market_service.fetch_item("Example")

        self.assertEqual(pages, [1, 2])

    def test_repeated_page_ids_stop_incomplete_backfill(self) -> None:
        pages: list[int] = []

        def fake_fetch(item_name, page, shutdown_event=None):
            pages.append(page)
            return [self.trade()], 0.01

        with mock.patch.object(market_service, "fetch_page", side_effect=fake_fetch), mock.patch.object(
            market_service.random, "uniform", return_value=0
        ):
            market_service.fetch_item("Example")

        self.assertEqual(pages, [1, 2])
        self.assertFalse(market_service.is_backfilled("Example"))

    def test_shutdown_does_not_mark_partial_backfill_complete(self) -> None:
        shutdown = threading.Event()

        def fake_fetch(item_name, page, shutdown_event=None):
            shutdown.set()
            return [self.trade()], 0.01

        with mock.patch.object(market_service, "fetch_page", side_effect=fake_fetch), mock.patch.object(
            market_service.random, "uniform", return_value=0
        ):
            market_service.fetch_item("Example", shutdown)

        self.assertFalse(market_service.is_backfilled("Example"))

    def test_unresolved_page_failure_does_not_mark_backfill_complete(self) -> None:
        responses = iter([([self.trade()], 0.01), (None, 0.0), (None, 0.0), (None, 0.0)])

        def fake_fetch(item_name, page, shutdown_event=None):
            return next(responses)

        with mock.patch.object(market_service, "fetch_page", side_effect=fake_fetch), mock.patch.object(
            market_service.random, "uniform", return_value=0
        ):
            market_service.fetch_item("Example")

        self.assertFalse(market_service.is_backfilled("Example"))

    def test_transient_page_failure_can_recover_and_complete(self) -> None:
        responses = iter([([self.trade()], 0.01), (None, 0.0), ([], 0.01)])
        writer = threading.Thread(target=market_service._db_writer_loop)
        writer.start()

        def fake_fetch(item_name, page, shutdown_event=None):
            return next(responses)

        try:
            with mock.patch.object(market_service, "fetch_page", side_effect=fake_fetch), mock.patch.object(
                market_service.random, "uniform", return_value=0
            ):
                market_service.fetch_item("Example")
            self.assertTrue(market_service.is_backfilled("Example"))
            conn = sqlite3.connect(self.db_path)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0], 1)
            finally:
                conn.close()
        finally:
            market_service._write_queue.put(None)
            writer.join(timeout=5)

    def test_backfill_marker_is_written_only_after_rows_are_durable(self) -> None:
        responses = iter([([self.trade()], 0.01), ([], 0.01)])
        writer = threading.Thread(target=market_service._db_writer_loop)
        writer.start()
        observed_counts: list[int] = []
        original_mark = market_service.mark_backfilled

        def checking_mark(item_name, total_pages):
            conn = sqlite3.connect(self.db_path)
            try:
                observed_counts.append(conn.execute("SELECT COUNT(*) FROM trades").fetchone()[0])
            finally:
                conn.close()
            original_mark(item_name, total_pages)

        try:
            with mock.patch.object(market_service, "fetch_page", side_effect=lambda *args, **kwargs: next(responses)),                  mock.patch.object(market_service.random, "uniform", return_value=0),                  mock.patch.object(market_service, "mark_backfilled", side_effect=checking_mark):
                market_service.fetch_item("Example")
        finally:
            market_service._write_queue.put(None)
            writer.join(timeout=5)
        self.assertEqual(observed_counts, [1])

    def test_fetch_page_rejects_non_list_success_payload(self) -> None:
        response = mock.Mock(status_code=200, content=b"{}")
        response.json.return_value = {"message": "unexpected"}
        session = mock.Mock()
        session.get.return_value = response
        with mock.patch.object(market_service, "get_session", return_value=session), mock.patch.object(
            market_service, "_wait_or_sleep", return_value=False
        ):
            payload, latency = market_service.fetch_page("Example", 1)
        self.assertIsNone(payload)
        self.assertEqual(session.get.call_count, 5)

    def test_mark_backfilled_updates_case_insensitive_cache_immediately(self) -> None:
        market_service.mark_backfilled("Mixed Case Item", 3)
        self.assertTrue(market_service.is_backfilled("mixed case item"))
        self.assertTrue(market_service.is_backfilled("MIXED CASE ITEM"))

    def test_init_db_migrates_old_tracking_schema_and_removes_duplicate_alerts(self) -> None:
        market_service.close_thread_connection()
        self.db_path.unlink()
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "CREATE TABLE tracked_users "
            "(username TEXT PRIMARY KEY, added_at TEXT, last_checked TEXT, last_trade_id INTEGER)"
        )
        conn.execute(
            "CREATE TABLE trade_alerts "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT, trade_id INTEGER, "
            "timestamp TEXT, item_name TEXT, quantity INTEGER, price INTEGER, "
            "currency INTEGER, role TEXT, seen INTEGER DEFAULT 0, alerted_at TEXT)"
        )
        alert_values = (
            "seller", 7, "2026-08-06 12:00:00", "Example", 1, 100, 0,
            "seller", 0, "2026-08-06 12:01:00",
        )
        conn.execute(
            "INSERT INTO trade_alerts "
            "(username,trade_id,timestamp,item_name,quantity,price,currency,role,seen,alerted_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            alert_values,
        )
        conn.execute(
            "INSERT INTO trade_alerts "
            "(username,trade_id,timestamp,item_name,quantity,price,currency,role,seen,alerted_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            alert_values,
        )
        conn.commit()
        conn.close()

        market_service.init_db()
        conn = sqlite3.connect(self.db_path)
        try:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(tracked_users)")}
            count = conn.execute("SELECT COUNT(*) FROM trade_alerts").fetchone()[0]
            indexes = {row[1] for row in conn.execute("PRAGMA index_list(trade_alerts)")}
        finally:
            conn.close()
        self.assertIn("discord_user_id", columns)
        self.assertEqual(count, 1)
        self.assertIn("idx_alerts_user_trade", indexes)

    def test_untrack_suppresses_old_pending_alerts(self) -> None:
        market_service.add_tracked_user("seller", discord_user_id=12345)
        alert = {
            "username": "seller", "trade_id": 99, "timestamp": "2026-08-06 12:00:00",
            "item_name": "Example", "quantity": 1, "price": 100, "currency": 0,
            "role": "seller", "alerted_at": "2026-08-06 12:01:00",
        }
        market_service.persist_alerts([alert])
        self.assertEqual(len(market_service.get_pending_discord_alerts()), 1)
        market_service.remove_tracked_user("seller")
        self.assertEqual(market_service.get_pending_discord_alerts(), [])

    def test_writer_shutdown_gate_rejects_late_batches(self) -> None:
        market_service.stop_accepting_writer_batches()
        with self.assertRaisesRegex(RuntimeError, "shutting down"):
            market_service.insert_trades([self.trade()])

    def test_tracked_user_alert_has_dm_recipient_and_is_idempotent(self) -> None:
        market_service.add_tracked_user("seller", discord_user_id=12345)
        alert = {
            "username": "seller", "trade_id": 99, "timestamp": "2026-08-06 12:00:00",
            "item_name": "Example", "quantity": 1, "price": 100, "currency": 0,
            "role": "seller", "alerted_at": "2026-08-06 12:01:00",
        }
        market_service.persist_alerts([alert, alert])
        pending = market_service.get_pending_discord_alerts()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["discord_user_id"], 12345)
        market_service.mark_trade_alert_seen(pending[0]["alert_id"])
        self.assertEqual(market_service.get_pending_discord_alerts(), [])


    def test_api_token_guard_rejects_missing_and_accepts_valid_token(self) -> None:
        original_settings = market_service.settings
        original_request = market_service.request
        original_jsonify = market_service.jsonify
        try:
            market_service.settings = types.SimpleNamespace(api_token="top-secret")
            market_service.jsonify = lambda value=None, **kwargs: value if value is not None else kwargs
            market_service.request = types.SimpleNamespace(
                path="/api/stats",
                headers={},
                method="GET",
            )
            body, status = market_service._require_api_token()
            self.assertEqual(status, 401)
            self.assertEqual(body["error"], "unauthorized")

            market_service.request.headers = {"X-API-Token": "top-secret"}
            self.assertIsNone(market_service._require_api_token())

            market_service.request.headers = {"Authorization": "Bearer top-secret"}
            self.assertIsNone(market_service._require_api_token())

            market_service.request.path = "/health"
            market_service.request.headers = {}
            self.assertIsNone(market_service._require_api_token())

            market_service.request.path = "/api/stats"
            market_service.request.method = "OPTIONS"
            self.assertIsNone(market_service._require_api_token())
        finally:
            market_service.settings = original_settings
            market_service.request = original_request
            market_service.jsonify = original_jsonify

    def test_api_export_and_bulk_requests_are_bounded(self) -> None:
        original_request = market_service.request
        original_jsonify = market_service.jsonify
        original_get_conn = market_service.get_conn
        try:
            calls = []

            class FakeResult:
                def __init__(self, row=None, rows=None):
                    self._row = row
                    self._rows = rows or []

                def fetchone(self):
                    return self._row

                def fetchall(self):
                    return self._rows

            class FakeConn:
                def execute(self, sql, params=()):
                    calls.append((sql, params))
                    if "COUNT(*)" in sql:
                        return FakeResult(row=(123456,))
                    return FakeResult(rows=[])

            market_service.get_conn = lambda: FakeConn()
            market_service.jsonify = lambda value=None, **kwargs: value if value is not None else kwargs
            market_service.request = types.SimpleNamespace(
                args=types.SimpleNamespace(get=lambda key, default=None: "999999999" if key == "limit" else default),
                get_json=lambda: {},
            )
            result = market_service.dump_trades()
            self.assertEqual(result["returned"], 0)
            select_call = next(call for call in calls if "ORDER BY timestamp DESC LIMIT" in call[0])
            self.assertEqual(select_call[1], (market_service.API_MAX_EXPORT_ROWS,))

            market_service.request = types.SimpleNamespace(
                args={},
                get_json=lambda: {"names": [f"item-{i}" for i in range(501)]},
            )
            body, status = market_service.item_stats_bulk()
            self.assertEqual(status, 400)
            self.assertIn("at most", body["error"])
        finally:
            market_service.request = original_request
            market_service.jsonify = original_jsonify
            market_service.get_conn = original_get_conn


    def test_api_server_stops_when_shutdown_event_is_set(self) -> None:
        shutdown = threading.Event()
        calls: list[str] = []

        class FakeServer:
            timeout = None

            def handle_request(self):
                calls.append("handle")
                shutdown.set()

            def server_close(self):
                calls.append("close")

        serving = types.ModuleType("werkzeug.serving")
        serving.make_server = lambda *args, **kwargs: FakeServer()
        werkzeug = types.ModuleType("werkzeug")
        werkzeug.serving = serving
        with mock.patch.dict(
            sys.modules,
            {"werkzeug": werkzeug, "werkzeug.serving": serving},
        ):
            market_service.run_api_server(shutdown)
        self.assertEqual(calls, ["handle", "close"])


if __name__ == "__main__":
    unittest.main()
