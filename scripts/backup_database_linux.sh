#!/usr/bin/env bash
set -euo pipefail
umask 027

DATA_DIR="${DATA_DIR:-/srv/uav-automation-service}"
SOURCE="$DATA_DIR/entiredatabase.db"
BACKUP_DIR="$DATA_DIR/backups"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$BACKUP_DIR/entiredatabase-$STAMP.db"
PARTIAL="$DEST.partial"

mkdir -p "$BACKUP_DIR"
if [[ ! -f "$SOURCE" ]]; then
  echo "Database not found: $SOURCE" >&2
  exit 1
fi

python3 - "$SOURCE" "$DEST" "$PARTIAL" <<'PY'
import os, sqlite3, sys
source, dest, partial = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    if os.path.exists(partial):
        os.unlink(partial)
    src = sqlite3.connect(source, timeout=30)
    dst = sqlite3.connect(partial, timeout=30)
    try:
        src.backup(dst)
        dst.execute("PRAGMA journal_mode=DELETE")
        dst.commit()
        result = dst.execute("PRAGMA integrity_check").fetchone()[0]
        if result != "ok":
            raise SystemExit(f"integrity_check failed: {result}")
    finally:
        dst.close()
        src.close()
    os.replace(partial, dest)
except BaseException:
    try:
        os.unlink(partial)
    except FileNotFoundError:
        pass
    raise
print(dest)
PY

find "$BACKUP_DIR" -maxdepth 1 -type f -name 'entiredatabase-*.db' -mtime +14 -delete
