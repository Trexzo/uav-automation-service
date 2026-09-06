from __future__ import annotations

import re
from pathlib import Path


def test_public_tree_excludes_private_deployment_files() -> None:
    root = Path(__file__).resolve().parents[1]
    assert not (root / ".env").exists()
    assert not (root / "config" / "profile.json").exists()
    assert not (root / "runtime").exists()


def test_public_examples_have_no_real_looking_ids_or_webhook_credentials() -> None:
    root = Path(__file__).resolve().parents[1]
    env = (root / ".env.example").read_text(encoding="utf-8")
    assert not re.search(r"(?<!\d)\d{17,20}(?!\d)", env)
    all_text = "\n".join(
        p.read_text(encoding="utf-8", errors="ignore")
        for p in root.rglob("*")
        if p.is_file() and ".git" not in p.parts
    )
    assert not re.search(r"https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/\d+/[A-Za-z0-9_-]+", all_text)
