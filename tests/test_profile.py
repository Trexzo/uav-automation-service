from __future__ import annotations

import json
from pathlib import Path

import pytest

from uav_service.profile import ProfileError, load_profile


def test_example_profile_loads() -> None:
    root = Path(__file__).resolve().parents[1]
    profile = load_profile(root / "config" / "profile.example.json")
    assert profile["version"] == 1
    assert profile["routes"]


def test_unknown_route_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "profile.json"
    path.write_text(json.dumps({
        "version": 1,
        "routes": {"ok": {"webhook_env": "WEBHOOK_OK"}},
        "rules": [{"name": "bad", "route": "missing"}],
    }))
    with pytest.raises(ProfileError):
        load_profile(path)
