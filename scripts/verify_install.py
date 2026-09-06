from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from uav_service.profile import load_profile


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify UAV Automation Service configuration and runtime data")
    parser.add_argument("--profile", type=Path, default=Path("config/profile.json"))
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument("--skip-db", action="store_true")
    args = parser.parse_args()

    failures: list[str] = []
    try:
        profile = load_profile(args.profile)
        print(f"Profile: {args.profile} ({len(profile.get('routes', {}))} routes)")
    except Exception as exc:
        failures.append(f"profile: {exc}")

    items = args.data_dir / "items.json"
    if items.exists():
        try:
            json.loads(items.read_text(encoding="utf-8"))
        except Exception as exc:
            failures.append(f"items.json: {exc}")

    if not args.skip_db:
        db = args.data_dir / "entiredatabase.db"
        if db.exists():
            try:
                conn = sqlite3.connect(str(db), timeout=30)
                try:
                    result = conn.execute("PRAGMA quick_check").fetchone()[0]
                    if result != "ok":
                        failures.append(f"database quick_check: {result}")
                finally:
                    conn.close()
            except Exception as exc:
                failures.append(f"database: {exc}")
        else:
            print("Database: not created yet (okay for a fresh install)")

    if failures:
        print("Verification failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
