"""Create an integrity-checked standalone SQLite backup."""
from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path


def _progress(status: int, remaining: int, total: int) -> None:
    if total:
        percent = (total - remaining) / total * 100
        print(f"\rBacking up: {percent:6.2f}%", end="", flush=True)
        if remaining == 0:
            print()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path, help="Path to the SQLite database")
    parser.add_argument("destination", type=Path, nargs="?", help="Output path")
    args = parser.parse_args()

    source = args.source.expanduser().resolve()
    destination = args.destination.expanduser().resolve() if args.destination else source.with_name("entiredatabase_backup.db")
    if not source.exists():
        raise SystemExit(f"Source database not found: {source}")
    if source == destination:
        raise SystemExit("Destination must be different from source")

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")
    partial.unlink(missing_ok=True)
    src = sqlite3.connect(str(source), timeout=30)
    dst = sqlite3.connect(str(partial), timeout=30)
    try:
        try:
            src.execute("PRAGMA query_only=ON")
            src.backup(dst, pages=10_000, progress=_progress)
            dst.execute("PRAGMA journal_mode=DELETE")
            dst.commit()
            result = dst.execute("PRAGMA integrity_check").fetchone()[0]
            if result != "ok":
                raise RuntimeError(f"Backup integrity check failed: {result}")
        finally:
            dst.close()
            src.close()
        os.replace(partial, destination)
    except Exception:
        partial.unlink(missing_ok=True)
        raise

    print(f"Backup complete: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
