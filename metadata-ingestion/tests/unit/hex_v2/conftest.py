import json
import os
from typing import Any


def load_fixture(filename: str) -> Any:
    path = os.path.join(os.path.dirname(__file__), "test_data", filename)
    with open(path) as f:
        if filename.endswith(".json"):
            return json.load(f)
        return f.read()
