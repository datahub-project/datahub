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
from typing import Any, Dict, List, Optional, Union

from avrogen.dict_wrapper import DictWrapper
from pydantic import Field

from datahub.configuration import ConfigModel
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.transform.transformer import Transformer

logger = logging.getLogger(__name__)


class FilterTransformerConfig(ConfigModel):
    event_type: Union[str, List[str]]
    event: Optional[Dict[str, Any]] = Field(default=None)


class FilterTransformer(Transformer):
    def __init__(self, config: FilterTransformerConfig):
        self.config: FilterTransformerConfig = config
        # Extract simple top-level fields for fast path optimization
        self._simple_field_checks = self._extract_simple_fields()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        config = FilterTransformerConfig.model_validate(config_dict)
        return cls(config)

    def _extract_simple_fields(self) -> Dict[str, Any]:
        """
        Extract simple top-level string fields for fast rejection path.

        These fields (entityType, entityUrn, aspectName, changeType) are:
        - Always simple strings in MCL events
        - Most commonly used in filters
        - Can be checked via direct attribute access without dict conversion

        This optimization avoids expensive as_json() + json.loads() conversion
        for events that fail these basic checks.
        """
        if self.config.event is None:
            return {}

        # Only these 4 fields - guaranteed to be simple strings in MCL
        SIMPLE_FIELDS = {"entityType", "entityUrn", "aspectName", "changeType"}

        return {
            key: val for key, val in self.config.event.items() if key in SIMPLE_FIELDS
        }

    def transform(self, env_event: EventEnvelope) -> Optional[EventEnvelope]:
        logger.debug(f"Preparing to filter event {env_event}")

        # Match Event Type.
        if not self._matches(self.config.event_type, env_event.event_type):
            return None

        # Fast rejection path - check simple fields directly on typed object
        # This avoids as_json() + json.loads() conversion for events that fail basic checks
        if self._simple_field_checks:
            if not self._check_simple_fields_fast(env_event.event):
                logger.debug("Event rejected by fast path filter")
                return None  # Early rejection - no dict conversion needed

        # Match Event Body - full filter on all configured fields
        if self.config.event is not None:
            body_as_dict = self._event_to_dict(env_event.event)
            for key, val in self.config.event.items():
                if not self._matches(val, body_as_dict.get(key)):
                    return None
        return env_event

    def _check_simple_fields_fast(self, event: Any) -> bool:
        """
        Fast rejection check on simple string fields.
        Direct attribute access, no dict conversion needed.
        """
        for key, expected_val in self._simple_field_checks.items():
            actual_val = getattr(event, key, None)
            if not self._matches(expected_val, actual_val):
                return False
        return True

    def _event_to_dict(self, event: Any) -> Dict[str, Any]:
        """
        Convert event to dictionary representation.

        Uses to_obj() if available (via DictWrapper inheritance) to avoid the JSON
        serialization round-trip that happens with json.loads(as_json()). The round-trip
        means: dict -> json.dumps() -> JSON string -> json.loads() -> dict, which involves
        expensive serialization/deserialization. Calling to_obj() directly returns the dict
        without the intermediate JSON string step.

        Falls back to json.loads(as_json()) for Event implementations that don't inherit
        from DictWrapper (though all current implementations do).

        TODO: This runtime check could be removed by adding to_obj() as an abstract
        method to the Event base class (which currently only defines as_json/from_json).

        All current Event implementations inherit from avrogen-generated classes that
        provide to_obj() via DictWrapper:
        - MetadataChangeLogEvent (from MetadataChangeLogClass -> DictWrapper)
        - EntityChangeEvent (from EntityChangeEventClass -> DictWrapper)
        - RelationshipChangeEvent (from RelationshipChangeEventClass -> DictWrapper)
        Making to_obj() part of the Event interface would eliminate this runtime check.

        Args:
            event: The event object to convert

        Returns:
            Dictionary representation of the event
        """
        if isinstance(event, DictWrapper):
            # Optimized path - direct dict conversion without JSON serialization
            return event.to_obj()
        else:
            # Fallback for Event implementations that don't inherit from DictWrapper
            return json.loads(event.as_json())

    def _matches(self, match_val: Any, match_val_to: Any) -> bool:
        if isinstance(match_val, dict):
            return self._matches_dict(match_val, match_val_to)
        if isinstance(match_val, list):
            return self._matches_list(match_val, match_val_to)
        return match_val == match_val_to

    def _matches_list(self, match_filters: List, match_with: Any) -> bool:
        """When matching lists we do ANY not ALL match"""
        if not isinstance(match_with, str):
            return False
        for filter in match_filters:
            if filter == match_with:
                return True
        return False

    def _matches_dict(self, match_filters: Dict[str, Any], match_with: Any) -> bool:
        if isinstance(match_with, str):
            try:
                match_with = json.loads(match_with)
            except ValueError:
                pass
        if not isinstance(match_with, dict):
            return False
        for key, val in match_filters.items():
            curr = match_with.get(key)
            if not self._matches(val, curr):
                return False
        return True
