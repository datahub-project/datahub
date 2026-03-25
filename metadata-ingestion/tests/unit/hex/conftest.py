import json
import os
from pathlib import Path


def load_json_data(filename):
    """Load test data from JSON files in the test_data directory."""
    test_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    file_path = test_dir / "test_data" / filename
    with open(file_path, "r") as f:
        return json.load(f)
