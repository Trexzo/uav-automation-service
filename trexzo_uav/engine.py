from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable

from .config import settings
from .profile import aliases, contains_match
from .state_store import JsonStore
from .webhook_service import queue_webhook


@dataclass(frozen=True)
class DmRequest:
    user_id: int
    content: str


class AutomationEngine:
    """Profile-driven message routing, counters, rotations and watch rules.

    Operational match terms live in the JSON profile rather than Python source.
    This keeps the public engine reusable while a deployment can keep its exact
    rules, route names and state private.
    """

    def __init__(
        self,
        profile: dict[str, Any] | None = None,
        *,
        state_dir: Path | None = None,
        webhook_queue: Callable[..., bool] = queue_webhook,
    ) -> None:
        self.profile = profile or settings.profile()
        self.state_dir = Path(state_dir or settings.state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.queue_webhook = webhook_queue
        self._lock = threading.RLock()

        message_format = self.profile.get("message_format", {})
        player_pattern = str(message_format.get("player_pattern", "")).strip()
        drop_pattern = str(message_format.get("drop_pattern", "")).strip()
        self.player_pattern = re.compile(player_pattern, re.IGNORECASE) if player_pattern else None
        self.drop_pattern = re.compile(drop_pattern, re.IGNORECASE) if drop_pattern else None

        self.members_store = JsonStore(self.state_dir / "members.json", {"tracked_members": []})
        self.ignore_store = JsonStore(self.state_dir / "ignore_names.json", [])
        self.watch_store = JsonStore(self.state_dir / "watch_list.json", [])
        self.counter_store = JsonStore(self.state_dir / "counters.json", {"version": 1, "groups": {}})

        self.ignore_names = self._load_ignore_names()
        self.watch_list = self._load_watch_list()

        self.named_list_stores: dict[str, JsonStore] = {}
        self.named_lists: dict[str, set[str]] = {}
        for entry in self.profile.get("named_lists", []):
            name = str(entry.get("name", "")).strip()
            if not name:
                continue
            state_file = str(entry.get("state_file", f"{name}.json"))
            store = JsonStore(self.state_dir / state_file, [])
            raw = store.load()
            values = raw if isinstance(raw, list) else []
            self.named_list_stores[name] = store
            self.named_lists[name] = {str(value).strip().lower() for value in values if str(value).strip()}

        self.rotation_stores: dict[str, JsonStore] = {}
        self.rotation_indexes: dict[str, int] = {}
        for entry in self.profile.get("rotations", []):
            name = str(entry.get("name", "")).strip()
            locations = [str(v) for v in entry.get("locations", [])]
            if not name or not locations:
                continue
            state_file = str(entry.get("state_file", f"{name}_rotation.json"))
            store = JsonStore(self.state_dir / state_file, {"index": 0})
            raw = store.load()
            try:
                index = int(raw.get("index", 0)) if isinstance(raw, dict) else 0
            except (TypeError, ValueError):
                index = 0
            self.rotation_stores[name] = store
            self.rotation_indexes[name] = index % len(locations)

        self.counter_state = self._load_counter_state()
        self._reset_daily_if_needed()

    def _load_ignore_names(self) -> set[str]:
        values: set[str] = set()
        members = self.members_store.load()
        if isinstance(members, dict) and isinstance(members.get("tracked_members"), list):
            values.update(str(v).strip().lower() for v in members["tracked_members"] if str(v).strip())
        ignored = self.ignore_store.load()
        if isinstance(ignored, list):
            values.update(str(v).strip().lower() for v in ignored if str(v).strip())
        return values

    def _load_watch_list(self) -> list[dict[str, Any]]:
        raw = self.watch_store.load()
        result: list[dict[str, Any]] = []
        if not isinstance(raw, list):
            return result
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            subject = str(entry.get("subject", entry.get("player", ""))).strip().lower()
            keyword = str(entry.get("keyword", "")).strip().lower()
            try:
                user_id = int(entry.get("discord_id") or 0)
            except (TypeError, ValueError):
                user_id = 0
            if subject and keyword:
                result.append({"subject": subject, "keyword": keyword, "discord_id": user_id or None})
        return result

    def _counter_defaults(self) -> dict[str, Any]:
        groups: dict[str, Any] = {}
        today = date.today().isoformat()
        for group in self.profile.get("counters", []):
            name = str(group.get("name", "")).strip()
            rewards = group.get("rewards", {})
            if not name or not isinstance(rewards, dict):
                continue
            canonical = {str(v) for v in rewards.values()}
            groups[name] = {
                "totals": {value: 0 for value in canonical},
                "daily": {**{value: 0 for value in canonical}, "last_reset": today},
            }
        return {"version": 1, "groups": groups}

    @staticmethod
    def _safe_count(value: Any) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    def _load_counter_state(self) -> dict[str, Any]:
        defaults = self._counter_defaults()
        raw = self.counter_store.load()
        groups_raw = raw.get("groups", {}) if isinstance(raw, dict) else {}
        result = defaults
        for group in self.profile.get("counters", []):
            name = str(group.get("name", "")).strip()
            if not name or name not in result["groups"]:
                continue
            source = groups_raw.get(name, {}) if isinstance(groups_raw, dict) else {}
            totals = source.get("totals", {}) if isinstance(source, dict) else {}
            daily = source.get("daily", {}) if isinstance(source, dict) else {}
            for key in result["groups"][name]["totals"]:
                result["groups"][name]["totals"][key] = self._safe_count(totals.get(key, 0)) if isinstance(totals, dict) else 0
                result["groups"][name]["daily"][key] = self._safe_count(daily.get(key, 0)) if isinstance(daily, dict) else 0
            if isinstance(daily, dict) and isinstance(daily.get("last_reset"), str):
                result["groups"][name]["daily"]["last_reset"] = daily["last_reset"]
        self.counter_store.save(result)
        return result

    def _reset_daily_if_needed(self) -> None:
        today = date.today().isoformat()
        changed = False
        with self._lock:
            for group in self.counter_state.get("groups", {}).values():
                daily = group.get("daily", {})
                if daily.get("last_reset") == today:
                    continue
                for key in list(daily):
                    if key != "last_reset":
                        daily[key] = 0
                daily["last_reset"] = today
                changed = True
            if changed:
                self.counter_store.save(self.counter_state)

    def _mention(self, route: str) -> str:
        route_config = self.profile.get("routes", {}).get(route, {})
        env_name = str(route_config.get("mention_env", "")).strip()
        return os.getenv(env_name, "").strip() if env_name else ""

    def _render(self, template: str, **values: Any) -> str:
        safe = {key: str(value) for key, value in values.items()}
        try:
            return template.format_map(safe).strip()
        except (KeyError, ValueError):
            return str(values.get("message", "")).strip()

    def extract_subject(self, text: str) -> str | None:
        if self.player_pattern is None:
            return None
        match = self.player_pattern.search(text)
        if not match:
            return None
        value = match.groupdict().get("subject") if match.groupdict() else None
        if value is None and match.groups():
            value = match.group(1)
        return str(value).strip().lower() if value else None

    def _counter_match(self, text: str, group: dict[str, Any]) -> tuple[str, int] | None:
        rewards = group.get("rewards", {})
        if not isinstance(rewards, dict):
            return None
        for trigger, canonical in rewards.items():
            match = re.search(rf"\breceived\s+(\d+)x\s+{re.escape(str(trigger))}(?:\b|$)", text, re.IGNORECASE)
            if match:
                return str(canonical), int(match.group(1))
        for trigger in group.get("fusion_rewards", []):
            if re.search(rf"\bfused\s+the\s+{re.escape(str(trigger))}(?:\s*!|\b)", text, re.IGNORECASE):
                canonical = str(rewards.get(trigger, trigger))
                return canonical, 1
        return None

    def _queue(self, route: str, content: str, *, mention: bool = False) -> bool:
        prefix = self._mention(route) if mention else ""
        final = f"{prefix} {content}".strip() if prefix else content.strip()
        return bool(self.queue_webhook(route, final))

    def _process_static_rules(self, original: str) -> bool:
        matched = False
        for rule in self.profile.get("rules", []):
            if not contains_match(original, rule, self.drop_pattern):
                continue
            route = str(rule.get("route", ""))
            template = str(rule.get("template", "{message}"))
            content = self._render(template, message=original, mention=self._mention(route))
            self._queue(route, content, mention=bool(rule.get("mention")) and "{mention}" not in template)
            matched = True
        return matched

    def _process_named_lists(self, original: str) -> bool:
        lower = original.lower()
        matched = False
        for entry in self.profile.get("named_lists", []):
            name = str(entry.get("name", ""))
            values = tuple(self.named_lists.get(name, ()))
            if not values or not any(value in lower for value in values):
                continue
            if not contains_match(original, entry, self.drop_pattern):
                continue
            route = str(entry.get("route", ""))
            self._queue(route, original, mention=bool(entry.get("mention")))
            matched = True
        return matched

    def process_message(self, text: str, *, count_rewards: bool = True) -> list[DmRequest]:
        self._reset_daily_if_needed()
        original = text.strip()
        lower = original.lower()
        subject = self.extract_subject(original)
        with self._lock:
            if subject and subject in self.ignore_names:
                return []
            watch_snapshot = [dict(entry) for entry in self.watch_list]

        dms: list[DmRequest] = []
        for entry in watch_snapshot:
            if subject and subject == entry.get("subject") and str(entry.get("keyword", "")) in lower:
                user_id = int(entry.get("discord_id") or 0)
                if user_id:
                    dms.append(DmRequest(user_id, f"🎯 **{subject.upper()}** matched a configured watch: {original}"))

        if count_rewards:
            for group in self.profile.get("counters", []):
                match = self._counter_match(original, group)
                if not match:
                    continue
                reward, quantity = match
                name = str(group.get("name", ""))
                with self._lock:
                    state = self.counter_state["groups"].setdefault(name, {"totals": {}, "daily": {"last_reset": date.today().isoformat()}})
                    state["totals"][reward] = self._safe_count(state["totals"].get(reward, 0)) + quantity
                    state["daily"][reward] = self._safe_count(state["daily"].get(reward, 0)) + quantity
                    total = state["totals"][reward]
                    daily = state["daily"][reward]
                    self.counter_store.save(self.counter_state)
                route = str(group.get("route", ""))
                template = str(group.get("template", "{message}\n**{reward} total:** {total} (+{daily} today)"))
                self._queue(route, self._render(template, message=original, reward=reward, total=total, daily=daily), mention=bool(group.get("mention")))

        self._process_static_rules(original)
        self._process_named_lists(original)

        for rule in self.profile.get("timed_rules", []):
            if not contains_match(original, rule, self.drop_pattern):
                continue
            duration = max(0, int(rule.get("duration_seconds", 0)))
            expires = int(time.time()) + duration
            relative = f"<t:{expires}:R>" if duration else ""
            route = str(rule.get("route", ""))
            label = str(rule.get("label", "Configured event"))
            template = str(rule.get("template", "{label} — {expires}"))
            content = self._render(template, message=original, label=label, expires=relative, mention=self._mention(route))
            self._queue(route, content, mention=bool(rule.get("mention")) and "{mention}" not in template)
            if rule.get("dm_owner") and settings.owner_user_id:
                dms.append(DmRequest(settings.owner_user_id, f"{label} — {relative}".strip()))

        for rotation in self.profile.get("rotations", []):
            if not contains_match(original, rotation, self.drop_pattern):
                continue
            triggers = [str(v).lower() for v in rotation.get("triggers", [])]
            if triggers and not any(trigger in lower for trigger in triggers):
                continue
            name = str(rotation.get("name", ""))
            locations = [str(v) for v in rotation.get("locations", [])]
            if not name or not locations:
                continue
            with self._lock:
                index = self.rotation_indexes.get(name, 0) % len(locations)
                location = locations[index]
                route = str(rotation.get("route", ""))
                template = str(rotation.get("template", "{message} Location: {location}"))
                content = self._render(template, message=original, location=location, mention=self._mention(route))
                self._queue(route, content, mention=bool(rotation.get("mention")) and "{mention}" not in template)
                advance_on = [str(v).lower() for v in rotation.get("advance_contains", [])]
                if advance_on and any(value in lower for value in advance_on):
                    next_index = (index + 1) % len(locations)
                    self.rotation_indexes[name] = next_index
                    self.rotation_stores[name].save({"index": next_index})

        return dms

    # Compatibility name for older integrations.
    process_embed = process_message

    def process_test_message(self, text: str) -> bool:
        original = text.strip()
        subject = self.extract_subject(original)
        with self._lock:
            if subject and subject in self.ignore_names:
                return False
        static = self._process_static_rules(original)
        named = self._process_named_lists(original)
        return static or named

    def looks_like_simulated_input(self, text: str) -> bool:
        lower = text.lower()
        return any(str(marker).lower() in lower for marker in self.profile.get("simulation_markers", []))

    def counter_totals_text(self, group_name: str | None = None) -> str:
        group = self._find_counter(group_name)
        if group is None:
            return "No counter group is configured."
        name = str(group.get("name"))
        title = str(group.get("title", "Configured totals"))
        with self._lock:
            totals = dict(self.counter_state["groups"].get(name, {}).get("totals", {}))
        return f"**{title}**\n" + "\n".join(f"**{key}**: {value}" for key, value in totals.items())

    def counter_daily_text(self, group_name: str | None = None) -> str:
        self._reset_daily_if_needed()
        group = self._find_counter(group_name)
        if group is None:
            return "No counter group is configured."
        name = str(group.get("name"))
        title = str(group.get("daily_title", "Daily configured totals"))
        with self._lock:
            daily = dict(self.counter_state["groups"].get(name, {}).get("daily", {}))
        return f"**{title}**\n" + "\n".join(f"**{key}**: +{value} today" for key, value in daily.items() if key != "last_reset")

    def _find_counter(self, name: str | None) -> dict[str, Any] | None:
        counters = self.profile.get("counters", [])
        if not counters:
            return None
        if not name:
            return counters[0]
        needle = name.strip().lower()
        for group in counters:
            if str(group.get("name", "")).lower() == needle or needle in aliases(group):
                return group
        return None

    def set_rotation_location(self, rotation_name: str, location: str) -> bool:
        for rotation in self.profile.get("rotations", []):
            if str(rotation.get("name", "")).lower() != rotation_name.lower():
                continue
            locations = [str(v) for v in rotation.get("locations", [])]
            mapping = {value.lower(): index for index, value in enumerate(locations)}
            index = mapping.get(location.strip().lower())
            if index is None:
                return False
            name = str(rotation.get("name"))
            with self._lock:
                self.rotation_indexes[name] = index
                self.rotation_stores[name].save({"index": index})
            return True
        return False

    def handle_control_command(self, content: str) -> tuple[bool, list[str], list[DmRequest]]:
        raw = content.strip()
        prefix = settings.command_prefix
        if not raw.startswith(prefix):
            return False, [], []
        command_line = raw[len(prefix):].strip()
        if not command_line:
            return False, [], []
        command, _, rest = command_line.partition(" ")
        cmd = command.lower()
        replies: list[str] = []
        dms: list[DmRequest] = []

        if cmd == "ignore":
            return self._handle_ignore(rest, replies, dms)
        if cmd == "watch":
            return self._handle_watch(rest, replies, dms)

        for rotation in self.profile.get("rotations", []):
            if cmd in aliases(rotation):
                name = str(rotation.get("name", ""))
                if not rest.strip():
                    replies.append("⚠️ A configured location is required")
                elif self.set_rotation_location(name, rest.strip()):
                    replies.append("✅ Rotation updated")
                else:
                    replies.append("⚠️ Unknown configured location")
                return True, replies, dms

        for named in self.profile.get("named_lists", []):
            if cmd in aliases(named):
                return self._handle_named_list(named, rest, replies, dms)

        for counter in self.profile.get("counters", []):
            total_aliases = {str(v).lower() for v in counter.get("totals_aliases", [])}
            daily_aliases = {str(v).lower() for v in counter.get("daily_aliases", [])}
            if cmd in total_aliases:
                replies.append(self.counter_totals_text(str(counter.get("name"))))
                return True, replies, dms
            if cmd in daily_aliases:
                replies.append(self.counter_daily_text(str(counter.get("name"))))
                return True, replies, dms

        if cmd == str(self.profile.get("simulation_command", "test")).lower():
            matched = self.process_test_message(rest)
            replies.append("🧪 Message matched a configured route" if matched else "🧪 Message did not match a configured route")
            return True, replies, dms

        if cmd == "list" and rest:
            list_name, _, list_args = rest.partition(" ")
            for named in self.profile.get("named_lists", []):
                if str(named.get("name", "")).lower() == list_name.lower():
                    return self._handle_named_list(named, list_args, replies, dms)
        if cmd == "rotation" and rest:
            rotation_name, _, location = rest.partition(" ")
            if location and self.set_rotation_location(rotation_name, location):
                replies.append("✅ Rotation updated")
            else:
                replies.append("⚠️ Unknown rotation or location")
            return True, replies, dms
        if cmd == "totals":
            replies.append(self.counter_totals_text(rest.strip() or None))
            return True, replies, dms
        if cmd == "daily":
            replies.append(self.counter_daily_text(rest.strip() or None))
            return True, replies, dms

        return False, replies, dms

    def _handle_ignore(self, rest: str, replies: list[str], dms: list[DmRequest]):
        action, _, value = rest.strip().partition(" ")
        action = action.lower()
        name = value.strip().lower()
        with self._lock:
            if action == "add" and name:
                self.ignore_names.add(name)
                self.ignore_store.save(sorted(self.ignore_names))
                replies.append("✅ Added entry to ignore list")
            elif action == "remove" and name:
                existed = name in self.ignore_names
                self.ignore_names.discard(name)
                self.ignore_store.save(sorted(self.ignore_names))
                replies.append("✅ Removed entry from ignore list" if existed else "⚠️ Entry was not in ignore list")
            elif action == "list":
                replies.extend(self._chunked_list("📋 **Ignore list:**", sorted(self.ignore_names)))
            else:
                replies.append("Usage: `!ignore add <name>`, `!ignore remove <name>`, `!ignore list`")
        return True, replies, dms

    def _handle_watch(self, rest: str, replies: list[str], dms: list[DmRequest]):
        args = rest.strip()
        action, _, body = args.partition(" ")
        action = action.lower()
        if not action:
            replies.append("👁️ `!watch add <subject> | <keyword> discord:<user_id>`, `remove`, `clear`, `list`")
            return True, replies, dms
        if action == "add":
            dm_match = re.search(r"\bdiscord:(\d+)\s*$", body, re.IGNORECASE)
            if not dm_match or "|" not in body[: dm_match.start()]:
                replies.append("⚠️ Usage: `!watch add <subject> | <keyword> discord:<user_id>`")
                return True, replies, dms
            user_id = int(dm_match.group(1))
            subject, keyword = [part.strip().lower() for part in body[: dm_match.start()].split("|", 1)]
            if not subject or not keyword:
                replies.append("⚠️ Subject and keyword cannot be empty")
                return True, replies, dms
            with self._lock:
                exists = any(e.get("subject") == subject and e.get("keyword") == keyword for e in self.watch_list)
                if not exists:
                    self.watch_list.append({"subject": subject, "keyword": keyword, "discord_id": user_id})
                    self.watch_store.save(self.watch_list)
            replies.append("⚠️ Watch already exists" if exists else "✅ Added watch")
            return True, replies, dms
        if action == "remove" and "|" in body:
            subject, keyword = [part.strip().lower() for part in body.split("|", 1)]
            with self._lock:
                before = len(self.watch_list)
                self.watch_list[:] = [e for e in self.watch_list if not (e.get("subject") == subject and e.get("keyword") == keyword)]
                removed = len(self.watch_list) < before
                if removed:
                    self.watch_store.save(self.watch_list)
            replies.append("✅ Removed watch" if removed else "⚠️ Watch not found")
            return True, replies, dms
        if action == "clear":
            subject = body.strip().lower()
            with self._lock:
                before = len(self.watch_list)
                self.watch_list[:] = [e for e in self.watch_list if e.get("subject") != subject]
                removed = before - len(self.watch_list)
                if removed:
                    self.watch_store.save(self.watch_list)
            replies.append(f"✅ Cleared {removed} watch(es)" if removed else "⚠️ No watches found")
            return True, replies, dms
        if action == "list":
            with self._lock:
                lines = [f"• **{e.get('subject')}** / `{e.get('keyword')}` → DM <@{e.get('discord_id')}>" for e in self.watch_list]
            replies.extend(self._chunked_lines("👁️ **Active watches:**", lines) if lines else ["👁️ Watch list is empty"])
            return True, replies, dms
        replies.append("⚠️ Invalid watch command")
        return True, replies, dms

    def _handle_named_list(self, entry: dict[str, Any], rest: str, replies: list[str], dms: list[DmRequest]):
        name = str(entry.get("name", ""))
        values = self.named_lists.setdefault(name, set())
        action, _, value = rest.strip().partition(" ")
        action = action.lower()
        item = value.strip().lower()
        with self._lock:
            if action == "add" and item:
                existed = item in values
                values.add(item)
                self.named_list_stores[name].save(sorted(values))
                replies.append("⚠️ Entry already exists" if existed else "✅ Added entry")
            elif action == "remove" and item:
                existed = item in values
                values.discard(item)
                if existed:
                    self.named_list_stores[name].save(sorted(values))
                replies.append("✅ Removed entry" if existed else "⚠️ Entry not found")
            elif action == "list":
                replies.extend(self._chunked_list(f"📋 **{name} list:**", sorted(values)))
            else:
                replies.append(f"Usage: `!{next(iter(aliases(entry)), name)} add <name>`, `remove`, `list`")
        return True, replies, dms

    def history_counter(self, name_or_alias: str) -> dict[str, Any] | None:
        needle = name_or_alias.strip().lower()
        for metric in self.profile.get("history_counters", []):
            metric_aliases = aliases(metric)
            if str(metric.get("name", "")).lower() == needle or needle in metric_aliases:
                return metric
        return None

    def history_counter_names(self) -> list[str]:
        return [str(metric.get("name")) for metric in self.profile.get("history_counters", []) if metric.get("name")]

    @staticmethod
    def _chunked_list(header: str, values: Iterable[str], chunk_size: int = 20) -> list[str]:
        values_list = list(values)
        if not values_list:
            return [header + " empty"]
        return [header + "\n" + ", ".join(values_list[i:i + chunk_size]) for i in range(0, len(values_list), chunk_size)]

    @staticmethod
    def _chunked_lines(header: str, lines: list[str], max_chars: int = 1800) -> list[str]:
        messages: list[str] = []
        current = header
        for line in lines:
            candidate = current + "\n" + line
            if len(candidate) > max_chars:
                messages.append(current)
                current = header + "\n" + line
            else:
                current = candidate
        if current:
            messages.append(current)
        return messages


UavEngine = AutomationEngine
