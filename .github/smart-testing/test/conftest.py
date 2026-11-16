"""Put `.github/smart-testing` on the import path, so tests import its scripts."""

import sys
from pathlib import Path

SMART_TESTING_DIR = Path(__file__).resolve().parent.parent
if str(SMART_TESTING_DIR) not in sys.path:
    sys.path.insert(0, str(SMART_TESTING_DIR))
