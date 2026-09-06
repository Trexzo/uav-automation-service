from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


class ProfileError(ValueError):
    pass


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProfileError("expected a list")
    return [str(item) for item in value]


def _validate_route_name(name: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", name):
        raise ProfileError(f"invalid route name: {name!r}")


def validate_profile(profile: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(profile, dict):
        raise ProfileError("profile root must be an object")
    if int(profile.get("version", 0)) != 1:
        raise ProfileError("profile version must be 1")

    routes = profile.get("routes", {})
    if not isinstance(routes, dict) or not routes:
        raise ProfileError("profile must define at least one route")
    for name, route in routes.items():
        _validate_route_name(str(name))
        if not isinstance(route, dict):
            raise ProfileError(f"route {name!r} must be an object")
        env_name = str(route.get("webhook_env", "")).strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", env_name):
            raise ProfileError(f"route {name!r} has invalid webhook_env")
        mention_env = str(route.get("mention_env", "")).strip()
        if mention_env and not re.fullmatch(r"[A-Z][A-Z0-9_]*", mention_env):
            raise ProfileError(f"route {name!r} has invalid mention_env")

    known_routes = set(routes)
    for section in ("rules", "timed_rules", "named_lists", "rotations", "counters"):
        entries = profile.get(section, [])
        if not isinstance(entries, list):
            raise ProfileError(f"{section} must be a list")
        for entry in entries:
            if not isinstance(entry, dict):
                raise ProfileError(f"{section} entries must be objects")
            route = str(entry.get("route", ""))
            if route and route not in known_routes:
                raise ProfileError(f"{section} references unknown route {route!r}")

    message_format = profile.setdefault("message_format", {})
    if not isinstance(message_format, dict):
        raise ProfileError("message_format must be an object")
    for key in ("player_pattern", "drop_pattern"):
        raw = str(message_format.get(key, "")).strip()
        if raw:
            try:
                re.compile(raw, re.IGNORECASE)
            except re.error as exc:
                raise ProfileError(f"invalid {key}: {exc}") from exc

    profile.setdefault("rules", [])
    profile.setdefault("timed_rules", [])
    profile.setdefault("named_lists", [])
    profile.setdefault("rotations", [])
    profile.setdefault("counters", [])
    profile.setdefault("history_counters", [])
    profile.setdefault("simulation_markers", [])
    profile.setdefault("simulation_command", "test")
    profile.setdefault("control_reply_route", next(iter(routes)))
    profile.setdefault("counter_reply_route", profile["control_reply_route"])

    if profile["control_reply_route"] not in known_routes:
        raise ProfileError("control_reply_route references an unknown route")
    if profile["counter_reply_route"] not in known_routes:
        raise ProfileError("counter_reply_route references an unknown route")

    return profile


@lru_cache(maxsize=8)
def load_profile(path: str | Path) -> dict[str, Any]:
    profile_path = Path(path).expanduser().resolve()
    try:
        with profile_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise ProfileError(f"profile not found: {profile_path}") from exc
    except json.JSONDecodeError as exc:
        raise ProfileError(f"invalid JSON profile {profile_path}: {exc}") from exc
    return validate_profile(data)


def clear_profile_cache() -> None:
    load_profile.cache_clear()


def route_env(profile: dict[str, Any], route_name: str) -> tuple[str, str]:
    route = profile.get("routes", {}).get(route_name)
    if not isinstance(route, dict):
        raise ProfileError(f"unknown route {route_name!r}")
    return str(route["webhook_env"]), str(route.get("mention_env", ""))


def aliases(entry: dict[str, Any]) -> set[str]:
    values = entry.get("aliases", [])
    if isinstance(values, str):
        values = [values]
    return {str(value).strip().lower() for value in values if str(value).strip()}


def contains_match(text: str, rule: dict[str, Any], drop_pattern: re.Pattern[str] | None) -> bool:
    lower = text.lower()
    contains_any = _as_list(rule.get("contains_any"))
    contains_all = _as_list(rule.get("contains_all"))
    contains_none = _as_list(rule.get("contains_none"))
    regex_any = _as_list(rule.get("regex_any"))
    regex_all = _as_list(rule.get("regex_all"))

    if contains_any and not any(value.lower() in lower for value in contains_any):
        return False
    if contains_all and not all(value.lower() in lower for value in contains_all):
        return False
    if any(value.lower() in lower for value in contains_none):
        return False
    if regex_any and not any(re.search(pattern, text, re.IGNORECASE) for pattern in regex_any):
        return False
    if regex_all and not all(re.search(pattern, text, re.IGNORECASE) for pattern in regex_all):
        return False
    if rule.get("require_drop") and (drop_pattern is None or not drop_pattern.search(text)):
        return False
    return True
