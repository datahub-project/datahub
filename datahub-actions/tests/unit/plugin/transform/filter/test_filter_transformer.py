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
from typing import Any

from datahub.metadata.schema_classes import DictWrapper
from datahub_actions.event.event import Event
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
)
from datahub_actions.plugin.transform.filter.filter_transformer import (
    FilterTransformer,
    FilterTransformerConfig,
)


class TestEvent(Event, DictWrapper):
    def __init__(self, field1: Any, field2: Any):
        super().__init__()
        self._inner_dict["field1"] = field1
        self._inner_dict["field2"] = field2

    @classmethod
    def from_obj(cls, obj: dict, tuples: bool = False) -> "TestEvent":
        return cls(obj["field1"], obj["field2"])

    def to_obj(self, tuples: bool = False) -> dict:
        return self._inner_dict

    @classmethod
    def from_json(cls, json_str: str) -> "TestEvent":
        json_obj = json.loads(json_str)
        return TestEvent.from_obj(json_obj)

    def as_json(self) -> str:
        return json.dumps(self.to_obj())


def test_returns_none_when_diff_event_type():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "b")

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE, event=test_event, meta={}
        )
    )

    assert result is None


def test_does_exact_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "b")

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )

    assert result is not None


def test_returns_none_when_no_match():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "c")

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None


def test_matches_on_nested_event():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": {"nested_b": "a"}, "nested_2": "c"}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is not None


def test_returns_none_when_no_match_nested_event():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": {"nested_b": "b"}, "nested_2": "c"}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None


def test_returns_none_when_different_data_type():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": {"nested_b": "a"}}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": ["a"]}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None


def test_returns_match_when_either_is_present():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": ["a", "b"]}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": "a"}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is not None


def test_returns_none_when_neither_is_present():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": ["a", "b"]}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": "c"}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None


def test_no_match_when_list_filter_on_dict_obj():
    filter_transformer_config = FilterTransformerConfig.parse_obj(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": {"nested_1": ["a", "b"]}},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)
    test_event = TestEvent({"nested_1": {"a": "B"}}, None)
    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None
