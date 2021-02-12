from dataclasses import dataclass
import json
import pprint


@dataclass
class Report:
    def as_obj(self) -> dict:
        return self.__dict__

    def as_string(self) -> str:
        return pprint.pformat(self.as_obj())

    def as_json(self) -> str:
        return json.dumps(self.as_obj())
