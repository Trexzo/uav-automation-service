from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path

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

    flask_module.Flask = DummyFlask
    flask_module.jsonify = lambda value=None, **kwargs: value if value is not None else kwargs
    flask_module.request = types.SimpleNamespace(
        args={}, get_json=lambda: {}, remote_addr="127.0.0.1"
    )
    sys.modules["flask"] = flask_module
    flask_cors_module = types.ModuleType("flask_cors")
    flask_cors_module.CORS = lambda app: app
    sys.modules["flask_cors"] = flask_cors_module

from uav_service import market_service, queries


class QueryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.db_path = root / "entiredatabase.db"
        items = [
            {"id": 1, "name": "Example sword", "tradeable": True},
            {"id": 2, "name": "Currency token", "tradeable": True},
        ]
        items_path = root / "items.json"
        items_path.write_text(json.dumps(items), encoding="utf-8")
        market_service.DB_PATH = str(self.db_path)
        market_service.ITEMS_JSON = str(items_path)
        market_service.BASE_DIR = str(root)
        market_service._thread_local = threading.local()
        market_service._items_cache = None
        market_service._items_by_name = {}
        market_service._backfilled_set = set()
        market_service.init_db()
        conn = sqlite3.connect(self.db_path)
        base = datetime.now(timezone.utc) - timedelta(days=1)
        stamp = lambda hours: (base + timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        conn.executemany(
            "INSERT INTO trades VALUES (?,?,?,?,?,?,?,?)",
            [
                (1, stamp(0), "Example sword", 1, 100, 0, "alice", "bob"),
                (2, stamp(1), "Example sword", 2, 200, 0, "bob", "alice"),
                (3, stamp(2), "Currency token", 1, 2, 1, "alice", "carol"),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self) -> None:
        market_service.close_thread_connection()
        self.temp_dir.cleanup()

    def test_item_stats_and_currency_normalization(self) -> None:
        stats = queries.get_item_stats("Example sword", 3650)
        self.assertIsNotNone(stats)
        self.assertEqual(stats["median"], 150)
        self.assertEqual(stats["latest"], 200)
        self.assertFalse(stats["sample_limited"])
        currency = queries.get_item_stats("Currency token", 3650)
        self.assertEqual(currency["latest"], 200_000_000)

    def test_player_and_recent_queries(self) -> None:
        player = queries.get_player_trades("Alice", 10)
        self.assertEqual(len(player), 3)
        self.assertEqual({row["role"] for row in player}, {"buyer", "seller"})
        recent = queries.get_recent_item_trades("Example", 10)
        self.assertEqual([row["id"] for row in recent], [2, 1])

    def test_deterministic_ask_router(self) -> None:
        answer = queries.ask_database("price of Example sword")
        self.assertIn("Example sword", answer)
        self.assertIn("median", answer)


if __name__ == "__main__":
    unittest.main()
