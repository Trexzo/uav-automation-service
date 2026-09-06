from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any


class JsonStore:
    """Small atomic JSON store for UAV state files."""

    def __init__(self, path: Path, default: Any):
        self.path = path
        self.default = default
        self._lock = threading.RLock()

    def load(self) -> Any:
        with self._lock:
            if not self.path.exists():
                self.save(self.default)
                return json.loads(json.dumps(self.default))
            try:
                with self.path.open("r", encoding="utf-8") as handle:
                    return json.load(handle)
            except (OSError, json.JSONDecodeError):
                corrupt = self.path.with_suffix(self.path.suffix + ".corrupt")
                try:
                    self.path.replace(corrupt)
                except OSError:
                    pass
                self.save(self.default)
                return json.loads(json.dumps(self.default))

    def save(self, value: Any) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent)
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(value, handle, indent=2, ensure_ascii=False)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, self.path)
                # Persist the directory entry as well as file contents where supported.
                try:
                    directory_fd = os.open(str(self.path.parent), os.O_RDONLY)
                except OSError:
                    directory_fd = None
                if directory_fd is not None:
                    try:
                        os.fsync(directory_fd)
                    finally:
                        os.close(directory_fd)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
