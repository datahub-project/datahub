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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "b")

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )

    assert result is not None


def test_returns_none_when_no_match():
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "a", "field2": "b"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    test_event = TestEvent("a", "c")

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )
    assert result is None


def test_matches_on_nested_event():
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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
    filter_transformer_config = FilterTransformerConfig.model_validate(
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


class TestEventWithSimpleFields(Event, DictWrapper):
    """Test event that simulates MCL structure with simple top-level fields."""

    def __init__(
        self,
        entity_type: str,
        entity_urn: str,
        aspect_name: str,
        change_type: str,
        nested_field: Any = None,
    ):
        super().__init__()
        self._inner_dict["entityType"] = entity_type
        self._inner_dict["entityUrn"] = entity_urn
        self._inner_dict["aspectName"] = aspect_name
        self._inner_dict["changeType"] = change_type
        if nested_field is not None:
            self._inner_dict["aspect"] = nested_field

    @property
    def entityType(self) -> str:
        return self._inner_dict["entityType"]

    @property
    def entityUrn(self) -> str:
        return self._inner_dict["entityUrn"]

    @property
    def aspectName(self) -> str:
        return self._inner_dict["aspectName"]

    @property
    def changeType(self) -> str:
        return self._inner_dict["changeType"]

    @classmethod
    def from_obj(cls, obj: dict, tuples: bool = False) -> "TestEventWithSimpleFields":
        return cls(
            obj["entityType"],
            obj["entityUrn"],
            obj["aspectName"],
            obj["changeType"],
            obj.get("aspect"),
        )

    def to_obj(self, tuples: bool = False) -> dict:
        return self._inner_dict

    @classmethod
    def from_json(cls, json_str: str) -> "TestEventWithSimpleFields":
        json_obj = json.loads(json_str)
        return TestEventWithSimpleFields.from_obj(json_obj)

    def as_json(self) -> str:
        return json.dumps(self.to_obj())


def test_fast_path_rejects_on_entity_type_mismatch():
    """Test that fast path correctly rejects based on entityType without dict conversion."""
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {
            "event_type": "MetadataChangeLogEvent_v1",
            "event": {
                "entityType": "dataHubExecutionRequest",
                "aspectName": "dataHubExecutionRequestInput",
            },
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # Event with different entityType should be rejected by fast path
    test_event = TestEventWithSimpleFields(
        entity_type="dataset",  # Doesn't match filter
        entity_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,test,PROD)",
        aspect_name="dataHubExecutionRequestInput",
        change_type="UPSERT",
    )

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE, event=test_event, meta={}
        )
    )

    assert result is None


def test_fast_path_rejects_on_aspect_name_mismatch():
    """Test that fast path correctly rejects based on aspectName."""
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {
            "event_type": "MetadataChangeLogEvent_v1",
            "event": {
                "entityType": "dataHubExecutionRequest",
                "aspectName": [
                    "dataHubExecutionRequestInput",
                    "dataHubExecutionRequestSignal",
                ],
            },
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # Event with different aspectName should be rejected by fast path
    test_event = TestEventWithSimpleFields(
        entity_type="dataHubExecutionRequest",
        entity_urn="urn:li:dataHubExecutionRequest:test",
        aspect_name="someOtherAspect",  # Doesn't match filter
        change_type="UPSERT",
    )

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE, event=test_event, meta={}
        )
    )

    assert result is None


def test_fast_path_passes_then_full_filter_applies():
    """Test that fast path passes simple checks, then full filter checks nested fields."""
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {
            "event_type": "MetadataChangeLogEvent_v1",
            "event": {
                "entityType": "dataHubExecutionRequest",
                "aspectName": "dataHubExecutionRequestInput",
                "changeType": "UPSERT",
                "aspect": {"value": {"executorId": "default"}},  # Nested field
            },
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # Event passes fast path but fails on nested field
    test_event = TestEventWithSimpleFields(
        entity_type="dataHubExecutionRequest",
        entity_urn="urn:li:dataHubExecutionRequest:test",
        aspect_name="dataHubExecutionRequestInput",
        change_type="UPSERT",
        nested_field={"value": {"executorId": "other"}},  # Doesn't match nested filter
    )

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE, event=test_event, meta={}
        )
    )

    assert result is None


def test_fast_path_and_full_filter_both_pass():
    """Test that event passes both fast path and full filter."""
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {
            "event_type": "MetadataChangeLogEvent_v1",
            "event": {
                "entityType": "dataHubExecutionRequest",
                "aspectName": "dataHubExecutionRequestInput",
                "changeType": "UPSERT",
                "aspect": {"value": {"executorId": "default"}},
            },
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # Event passes both fast path and full filter
    test_event = TestEventWithSimpleFields(
        entity_type="dataHubExecutionRequest",
        entity_urn="urn:li:dataHubExecutionRequest:test",
        aspect_name="dataHubExecutionRequestInput",
        change_type="UPSERT",
        nested_field={"value": {"executorId": "default"}},  # Matches nested filter
    )

    result = filter_transformer.transform(
        EventEnvelope(
            event_type=METADATA_CHANGE_LOG_EVENT_V1_TYPE, event=test_event, meta={}
        )
    )

    assert result is not None
    assert result.event == test_event


class NonDictWrapperEvent(Event):
    """Event implementation that does NOT inherit from DictWrapper.

    This simulates a hypothetical Event implementation that only implements
    the Event interface (as_json/from_json) but not to_obj(). Used to test
    the fallback path in FilterTransformer._event_to_dict().
    """

    def __init__(self, field1: Any, field2: Any):
        self.field1 = field1
        self.field2 = field2

    @classmethod
    def from_json(cls, json_str: str) -> "NonDictWrapperEvent":
        obj = json.loads(json_str)
        return cls(obj["field1"], obj["field2"])

    def as_json(self) -> str:
        """Only implements as_json() - no to_obj() method."""
        return json.dumps({"field1": self.field1, "field2": self.field2})


def test_fallback_path_for_non_dictwrapper_event():
    """Test that FilterTransformer works with Events that don't inherit from DictWrapper.

    This verifies the fallback path in _event_to_dict() which uses json.loads(as_json())
    when to_obj() is not available via DictWrapper inheritance.
    """
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {
            "event_type": "EntityChangeEvent_v1",
            "event": {"field1": "test", "field2": 123},
        }
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # This event only has as_json(), not to_obj()
    test_event = NonDictWrapperEvent("test", 123)

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )

    # Should match using the fallback path and return the same event
    assert result is not None
    assert result.event == test_event


def test_fallback_path_rejects_non_match():
    """Test that fallback path correctly rejects events that don't match filter."""
    filter_transformer_config = FilterTransformerConfig.model_validate(
        {"event_type": "EntityChangeEvent_v1", "event": {"field1": "expected"}}
    )
    filter_transformer = FilterTransformer(filter_transformer_config)

    # This event only has as_json(), not to_obj()
    test_event = NonDictWrapperEvent("different", 123)

    result = filter_transformer.transform(
        EventEnvelope(event_type=ENTITY_CHANGE_EVENT_V1_TYPE, event=test_event, meta={})
    )

    # Should reject using the fallback path
    assert result is None
