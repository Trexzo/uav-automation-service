from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
source = ROOT / "config" / "profile.example.json"
destination = ROOT / "config" / "profile.json"
if destination.exists():
    raise SystemExit(f"Refusing to overwrite {destination}")
shutil.copy2(source, destination)
print(f"Created {destination}. Edit it before running the service.")
