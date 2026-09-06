from __future__ import annotations

import asyncio
import sys
import time
import types
import unittest
from types import SimpleNamespace


# Minimal Flask stubs are needed because discord_service imports market_service.
if "flask" not in sys.modules:
    flask = types.ModuleType("flask")

    class DummyFlask:
        def __init__(self, *args, **kwargs):
            pass

        def route(self, *args, **kwargs):
            return lambda fn: fn

    flask.Flask = DummyFlask
    flask.jsonify = lambda value=None, **kwargs: value if value is not None else kwargs
    flask.request = SimpleNamespace(
        path="/", method="GET", headers={}, args={}, get_json=lambda: {}
    )
    sys.modules["flask"] = flask
    flask_cors = types.ModuleType("flask_cors")
    flask_cors.CORS = lambda *args, **kwargs: None
    sys.modules["flask_cors"] = flask_cors


# Lightweight discord.py stubs let us exercise UAVBot's internal ordering
# and duplicate handling without network access or third-party installation.
if "discord" not in sys.modules:
    discord = types.ModuleType("discord")

    class Intents:
        @classmethod
        def default(cls):
            return cls()

    class AllowedMentions:
        @classmethod
        def none(cls):
            return cls()

    class DummyTree:
        async def sync(self, **kwargs):
            return []

        def copy_global_to(self, **kwargs):
            return None

        def command(self, *args, **kwargs):
            return lambda fn: fn

        def error(self, fn):
            return fn

    class Bot:
        def __init__(self, *args, **kwargs):
            self.tree = DummyTree()
            self.user = None

        async def close(self):
            return None

        def is_closed(self):
            return False

        async def wait_until_ready(self):
            return None

        async def process_commands(self, message):
            return None

        def get_user(self, user_id):
            return None

    discord.Intents = Intents
    discord.AllowedMentions = AllowedMentions
    discord.Object = lambda **kwargs: SimpleNamespace(**kwargs)
    discord.TextChannel = type("TextChannel", (), {})
    discord.Thread = type("Thread", (), {})
    discord.Forbidden = type("Forbidden", (Exception,), {})
    discord.NotFound = type("NotFound", (Exception,), {})
    discord.Message = object
    discord.Interaction = object

    app_commands = types.ModuleType("discord.app_commands")
    app_commands.AppCommandError = type("AppCommandError", (Exception,), {})
    app_commands.MissingPermissions = type("MissingPermissions", (Exception,), {})
    app_commands.Choice = lambda **kwargs: SimpleNamespace(**kwargs)
    app_commands.command = lambda *args, **kwargs: (lambda fn: fn)
    app_commands.describe = lambda *args, **kwargs: (lambda fn: fn)
    app_commands.autocomplete = lambda *args, **kwargs: (lambda fn: fn)
    app_commands.default_permissions = lambda *args, **kwargs: (lambda fn: fn)
    app_commands.checks = SimpleNamespace(
        has_permissions=lambda *args, **kwargs: (lambda fn: fn)
    )

    ext = types.ModuleType("discord.ext")
    commands = types.ModuleType("discord.ext.commands")
    commands.Bot = Bot
    ext.commands = commands
    discord.app_commands = app_commands
    discord.ext = ext
    sys.modules.update({
        "discord": discord,
        "discord.app_commands": app_commands,
        "discord.ext": ext,
        "discord.ext.commands": commands,
    })

from uav_service.discord_service import UAVBot


class DiscordServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_messages_are_serialized_and_deduplicated(self) -> None:
        calls: list[str] = []

        class Engine:
            def process_message(self, title):
                calls.append(title)
                time.sleep(0.01)
                return []

        bot = UAVBot(Engine(), SimpleNamespace(is_set=lambda: False, set=lambda: None))
        first = SimpleNamespace(id=1, embeds=[SimpleNamespace(title="first")])
        second = SimpleNamespace(id=2, embeds=[SimpleNamespace(title="second")])
        await asyncio.gather(
            bot._process_source_embeds(first),
            bot._process_source_embeds(second),
        )
        self.assertEqual(calls, ["first", "second"])
        self.assertFalse(await bot._process_source_embeds(first))
        self.assertEqual(calls, ["first", "second"])

    async def test_failed_source_processing_can_retry_duplicate_delivery(self) -> None:
        attempts = 0

        class Engine:
            def process_message(self, title):
                nonlocal attempts
                attempts += 1
                if attempts == 1:
                    raise RuntimeError("temporary failure")
                return []

        bot = UAVBot(Engine(), SimpleNamespace(is_set=lambda: False, set=lambda: None))
        message = SimpleNamespace(id=9, embeds=[SimpleNamespace(title="retry")])
        with self.assertRaises(RuntimeError):
            await bot._process_source_embeds(message)
        self.assertTrue(await bot._process_source_embeds(message))
        self.assertEqual(attempts, 2)


    async def test_retry_of_multi_embed_message_skips_already_successful_embeds(self) -> None:
        calls: list[str] = []
        bad_attempts = 0

        class Engine:
            def process_message(self, title):
                nonlocal bad_attempts
                calls.append(title)
                if title == "bad":
                    bad_attempts += 1
                    if bad_attempts == 1:
                        raise RuntimeError("temporary second-embed failure")
                return []

        bot = UAVBot(Engine(), SimpleNamespace(is_set=lambda: False, set=lambda: None))
        message = SimpleNamespace(
            id=42,
            embeds=[SimpleNamespace(title="good"), SimpleNamespace(title="bad")],
        )
        with self.assertRaises(RuntimeError):
            await bot._process_source_embeds(message)
        self.assertTrue(await bot._process_source_embeds(message))
        self.assertEqual(calls, ["good", "bad", "bad"])

    def test_source_id_cache_is_bounded(self) -> None:
        bot = UAVBot(SimpleNamespace(), SimpleNamespace())
        for message_id in range(6000):
            self.assertTrue(bot._mark_source_message(message_id))
        self.assertEqual(len(bot._processed_source_ids), 5000)
        self.assertEqual(len(bot._processed_source_id_set), 5000)
        self.assertNotIn(0, bot._processed_source_id_set)
        self.assertIn(5999, bot._processed_source_id_set)


if __name__ == "__main__":
    unittest.main()
