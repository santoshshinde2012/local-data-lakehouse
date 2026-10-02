"""Puts tests/ on sys.path; importing support.ldl then adds src/ and scripts/."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import support.ldl  # noqa: E402,F401  (side effect: src/ and scripts/ on sys.path)
