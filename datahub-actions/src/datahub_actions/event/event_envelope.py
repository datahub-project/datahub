# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict

from datahub_actions.event.event import Event
from datahub_actions.event.event_registry import event_registry

logger = logging.getLogger(__name__)


# An object representation of the actual change event.
@dataclass
class EventEnvelope:
    # The type of the event. This corresponds to the shape of the payload.
    event_type: str

    # The event itself
    event: Event

    # Arbitrary metadata about the event
    meta: Dict[str, Any]

    # Convert an enveloped event to JSON representation
    def as_json(self) -> str:
        # Be careful about converting meta bag, since anything can be put inside at runtime.
        meta_json = None
        try:
            if self.meta is not None:
                meta_json = json.dumps(self.meta)
        except Exception:
            logger.warning(
                f"Failed to serialize meta field of EventEnvelope to json {self.meta}. Ignoring it during serialization."
            )
        result = f'{{ "event_type": "{self.event_type}", "event": {self.event.as_json()}, "meta": {meta_json if meta_json is not None else "null"} }}'
        return result

    # Convert a json event envelope back into the object.
    @classmethod
    def from_json(cls, json_str: str) -> "EventEnvelope":
        json_obj = json.loads(json_str)
        event_type = json_obj["event_type"]
        event_class = event_registry.get(event_type)
        event = event_class.from_json(json.dumps(json_obj["event"]))
        meta = json_obj["meta"] if "meta" in json_obj else {}
        return EventEnvelope(event_type=event_type, event=event, meta=meta)
