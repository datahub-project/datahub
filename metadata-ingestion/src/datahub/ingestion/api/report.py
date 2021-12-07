import json
import pprint
import sys
from dataclasses import dataclass
from typing import Dict

# The sort_dicts option was added in Python 3.8.
if sys.version_info >= (3, 8):
    PPRINT_OPTIONS = {"sort_dicts": False}
else:
    PPRINT_OPTIONS: Dict = {}


@dataclass
class Report:
    def as_obj(self) -> dict:
        return {
            key: value.as_obj() if hasattr(value, "as_obj") else value
            for (key, value) in self.__dict__.items()
        }

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj(), width=150, **PPRINT_OPTIONS)

    def as_json(self) -> str:
        return json.dumps(self.as_obj())
