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
from typing import Any, Dict, List, Optional

from pydantic import Field

from datahub.configuration import ConfigModel
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.filter import match_util
from datahub_actions.filter.filter import Filter
from datahub_actions.pipeline.pipeline_context import PipelineContext

logger = logging.getLogger(__name__)


class EventTypeFilterSpec(ConfigModel):
    """Body predicates for a single event type.

    `event` is a list of body predicate dicts combined with OR semantics:
    the event passes if it satisfies *any* predicate dict.  Within a single
    predicate dict all key/value pairs must match (AND semantics).

    Omitting `event` means "pass on type match alone".
    """

    event: Optional[List[Dict[str, Any]]] = Field(default=None)


class EventTypeFilterConfig(ConfigModel):
    """
    Maps event_type strings to per-type body predicates.

    An event passes this filter when its event_type appears as a key in
    `filter` and satisfies the associated body predicate (if any).

    Example:
        filter:
          MetadataChangeLogEvent_v1:
            event:
              - entityType: schemaField
                aspectName: documentation
              - entityType: dataset
                aspectName: documentation
          EntityChangeEvent_v1:
            event:
              - category: DOCUMENTATION
                entityType: schemaField
    """

    filter: Dict[str, Optional[EventTypeFilterSpec]]


class EventTypeFilter(Filter):
    """
    Passes events whose type is listed in the filter config and whose body
    satisfies the associated predicate (if any).

    Semantics:
    - Across event_type keys: OR — the event must match *any* listed type.
    - Across body predicate list items: OR — the event body must satisfy
      *any* predicate dict in the list.
    - Across keys within a single predicate dict: AND — every key/value pair
      must match.
    """

    def __init__(self, config: EventTypeFilterConfig) -> None:
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventTypeFilter":
        config = EventTypeFilterConfig.model_validate(config_dict)
        return cls(config)

    def matches(self, event: EventEnvelope) -> bool:
        if event.event_type not in self.config.filter:
            logger.debug(
                "EventTypeFilter: dropping event (type %s not in filter config)",
                event.event_type,
            )
            return False
        spec = self.config.filter[event.event_type]
        if spec is None or spec.event is None:
            logger.debug(
                "EventTypeFilter: passing event (type %s matches, no body predicate)",
                event.event_type,
            )
            return True
        body: Dict[str, Any] = json.loads(event.event.as_json())
        result = any(
            all(match_util.matches(v, body.get(k)) for k, v in predicate.items())
            for predicate in spec.event
        )
        logger.debug(
            "EventTypeFilter: %s event (type %s, body predicate result: %s)",
            "passing" if result else "dropping",
            event.event_type,
            result,
        )
        return result
