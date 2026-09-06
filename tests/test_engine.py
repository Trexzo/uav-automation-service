from __future__ import annotations

import json
from pathlib import Path

from uav_service.engine import AutomationEngine


def profile() -> dict:
    return {
        "version": 1,
        "message_format": {
            "player_pattern": r":notice:\s*(?P<subject>.*?)\s+received\s+.*$",
            "drop_pattern": r"received\s+x?\d+.*from\s+.+\(\d+\s+kc\)",
        },
        "routes": {
            "primary": {"webhook_env": "WEBHOOK_PRIMARY"},
            "secondary": {"webhook_env": "WEBHOOK_SECONDARY"},
            "control": {"webhook_env": "WEBHOOK_CONTROL"},
        },
        "control_reply_route": "control",
        "counter_reply_route": "control",
        "simulation_markers": [":notice:", "received"],
        "simulation_command": "test",
        "rules": [
            {
                "name": "drop",
                "route": "primary",
                "contains_any": ["sample source"],
                "require_drop": True,
            },
            {
                "name": "announcement",
                "route": "secondary",
                "contains_any": ["sample announcement"],
            },
        ],
        "timed_rules": [
            {
                "name": "timed",
                "route": "secondary",
                "contains_any": ["sample timed event"],
                "label": "Timed event",
                "duration_seconds": 60,
            }
        ],
        "counters": [
            {
                "name": "rewards",
                "route": "secondary",
                "title": "Rewards",
                "daily_title": "Daily rewards",
                "rewards": {"sample reward": "Sample Reward"},
                "fusion_rewards": [],
                "totals_aliases": ["rewards"],
                "daily_aliases": ["rewardsdaily"],
            }
        ],
        "named_lists": [
            {
                "name": "tracked_group",
                "state_file": "tracked_group.json",
                "route": "primary",
                "aliases": ["group"],
                "require_drop": True,
            }
        ],
        "rotations": [
            {
                "name": "cycle",
                "state_file": "cycle.json",
                "route": "secondary",
                "aliases": ["setcycle"],
                "locations": ["A", "B", "C"],
                "triggers": ["cycle starts in 5 min", "cycle starts in 1 min"],
                "advance_contains": ["1 min"],
            }
        ],
        "history_counters": [
            {
                "name": "metric",
                "aliases": ["metriccount"],
                "route": "control",
                "search_term": "sample reward",
                "label": "Metric",
            }
        ],
    }


def make_engine(tmp_path: Path):
    queued: list[tuple[str, str]] = []
    engine = AutomationEngine(
        profile(),
        state_dir=tmp_path,
        webhook_queue=lambda name, content, **kwargs: queued.append((name, content)) or True,
    )
    return engine, queued


def test_static_routing_and_ignore(tmp_path: Path) -> None:
    engine, queued = make_engine(tmp_path)
    message = ":notice: Alice received x1 thing from sample source (12 kc)"
    engine.process_message(message)
    assert queued[-1][0] == "primary"
    engine.ignore_names.add("alice")
    queued.clear()
    engine.process_message(message)
    assert queued == []


def test_watch_creates_dm(tmp_path: Path) -> None:
    engine, _ = make_engine(tmp_path)
    handled, replies, _ = engine.handle_control_command("!watch add alice | thing discord:456")
    assert handled and replies
    dms = engine.process_message(":notice: Alice received x1 thing from sample source (12 kc)")
    assert len(dms) == 1
    assert dms[0].user_id == 456


def test_counter_persists(tmp_path: Path) -> None:
    engine, queued = make_engine(tmp_path)
    engine.process_message(":notice: Alice received 2x sample reward from a chest")
    assert any(route == "secondary" for route, _ in queued)
    state = json.loads((tmp_path / "counters.json").read_text(encoding="utf-8"))
    assert state["groups"]["rewards"]["totals"]["Sample Reward"] == 2
    restored, _ = make_engine(tmp_path)
    assert "Sample Reward**: 2" in restored.counter_totals_text("rewards")


def test_named_list_and_rotation_are_profile_driven(tmp_path: Path) -> None:
    engine, queued = make_engine(tmp_path)
    handled, _, _ = engine.handle_control_command("!group add bob")
    assert handled
    engine.process_message(":notice: Bob received x1 thing from anywhere (5 kc)", count_rewards=False)
    assert queued[-1][0] == "primary"

    assert engine.set_rotation_location("cycle", "B")
    engine.process_message("Cycle starts in 1 min", count_rewards=False)
    assert engine.rotation_indexes["cycle"] == 2
    assert json.loads((tmp_path / "cycle.json").read_text())["index"] == 2


def test_history_metric_and_simulation_helpers(tmp_path: Path) -> None:
    engine, _ = make_engine(tmp_path)
    assert engine.history_counter("metriccount")["name"] == "metric"
    assert engine.looks_like_simulated_input("received something")
    assert engine.process_test_message(":notice: A received x1 x from sample source (1 kc)")
