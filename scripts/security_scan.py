from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    failures: list[str] = []
    forbidden_paths = [ROOT / ".env", ROOT / "config" / "profile.json", ROOT / "config" / "private", ROOT / "runtime"]
    for path in forbidden_paths:
        if path.exists():
            failures.append(f"private runtime/config path is present: {path.relative_to(ROOT)}")

    webhook_re = re.compile(r"https://(?:canary\.|ptb\.)?discord(?:app)?\.com/api/webhooks/\d+/[A-Za-z0-9_-]+")
    snowflake_re = re.compile(r"(?<!\d)\d{17,20}(?!\d)")
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in {".git", ".venv", "dist", "build"} for part in path.parts):
            continue
        if path.suffix.lower() not in {".py", ".md", ".json", ".yml", ".yaml", ".toml", ".txt", ".example", ""}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if webhook_re.search(text):
            failures.append(f"Discord webhook credential pattern in {path.relative_to(ROOT)}")
        if path.name == ".env.example" and snowflake_re.search(text):
            failures.append("real-looking numeric IDs found in .env.example")

    if failures:
        print("Public-repo security scan failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Public-repo security scan passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
