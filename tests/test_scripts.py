from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_security_scan_passes() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, str(root / "scripts" / "security_scan.py")], cwd=root, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
