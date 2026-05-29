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
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.filter.event_type_filter import EventTypeFilter


class SimpleEvent(Event, DictWrapper):
    def __init__(self, **kwargs):
        super().__init__()
        self._inner_dict.update(kwargs)

    @classmethod
    def from_obj(cls, obj: dict, tuples: bool = False) -> "SimpleEvent":
        e = cls()
        e._inner_dict.update(obj)
        return e

    def to_obj(self, tuples: bool = False) -> dict:
        return self._inner_dict

    @classmethod
    def from_json(cls, json_str: str) -> "SimpleEvent":
        return cls.from_obj(json.loads(json_str))

    def as_json(self) -> str:
        return json.dumps(self._inner_dict)


_ctx = PipelineContext(pipeline_name="test", graph=None)


def _make_filter(config_dict: dict) -> EventTypeFilter:
    return EventTypeFilter.create(config_dict, _ctx)


def _env(event_type: str, **fields: Any) -> EventEnvelope:
    return EventEnvelope(event_type=event_type, event=SimpleEvent(**fields), meta={})


# ── type matching ─────────────────────────────────────────────────────────────


def test_unknown_event_type_is_rejected():
    f = _make_filter({"filter": {"EntityChangeEvent_v1": {}}})
    assert not f.matches(_env(METADATA_CHANGE_LOG_EVENT_V1_TYPE))


def test_type_match_no_body_spec_passes():
    f = _make_filter({"filter": {"EntityChangeEvent_v1": {}}})
    assert f.matches(_env(ENTITY_CHANGE_EVENT_V1_TYPE))


def test_type_match_null_body_spec_passes():
    f = _make_filter({"filter": {"EntityChangeEvent_v1": None}})
    assert f.matches(_env(ENTITY_CHANGE_EVENT_V1_TYPE))


def test_multiple_types_or_semantics():
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {},
                ENTITY_CHANGE_EVENT_V1_TYPE: {},
            }
        }
    )
    assert f.matches(_env(METADATA_CHANGE_LOG_EVENT_V1_TYPE))
    assert f.matches(_env(ENTITY_CHANGE_EVENT_V1_TYPE))


# ── body predicates ───────────────────────────────────────────────────────────


def test_body_predicate_scalar_match():
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [{"entityType": "dataset", "aspectName": "documentation"}]
                }
            }
        }
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataset",
            aspectName="documentation",
        )
    )


def test_body_predicate_scalar_no_match():
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [{"entityType": "dataset", "aspectName": "documentation"}]
                }
            }
        }
    )
    assert not f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataset",
            aspectName="ownership",
        )
    )


def test_body_predicate_list_any_match():
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {
                            "aspectName": [
                                "dataHubExecutionRequestInput",
                                "dataHubExecutionRequestSignal",
                            ]
                        }
                    ]
                }
            }
        }
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE, aspectName="dataHubExecutionRequestInput"
        )
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            aspectName="dataHubExecutionRequestSignal",
        )
    )
    assert not f.matches(
        _env(METADATA_CHANGE_LOG_EVENT_V1_TYPE, aspectName="ownership")
    )


def test_body_predicate_or_across_predicate_list():
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {"aspectName": "documentation", "entityType": "schemaField"},
                        {"aspectName": "documentation", "entityType": "dataset"},
                    ]
                }
            }
        }
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="schemaField",
            aspectName="documentation",
        )
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataset",
            aspectName="documentation",
        )
    )
    assert not f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="chart",
            aspectName="documentation",
        )
    )


def test_empty_predicate_list_rejects_all():
    f = _make_filter({"filter": {METADATA_CHANGE_LOG_EVENT_V1_TYPE: {"event": []}}})
    assert not f.matches(_env(METADATA_CHANGE_LOG_EVENT_V1_TYPE, entityType="dataset"))


# ── nested/deep matching ──────────────────────────────────────────────────────


def test_body_predicate_nested_dict_match():
    """Predicates can match nested dictionary structures."""
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {
                            "entityType": "dataHubExecutionRequest",
                            "aspect": {"value": {"executorId": "default"}},
                        }
                    ]
                }
            }
        }
    )
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataHubExecutionRequest",
            aspect={"value": {"executorId": "default"}},
        )
    )


def test_body_predicate_nested_dict_no_match():
    """Nested dictionary predicates correctly reject non-matches."""
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {
                            "entityType": "dataHubExecutionRequest",
                            "aspect": {"value": {"executorId": "default"}},
                        }
                    ]
                }
            }
        }
    )
    assert not f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataHubExecutionRequest",
            aspect={"value": {"executorId": "custom"}},  # wrong executorId
        )
    )


def test_body_predicate_mixed_scalar_and_nested():
    """Predicates can combine scalar and nested dictionary matching."""
    f = _make_filter(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {
                            "entityType": "dataHubExecutionRequest",
                            "changeType": "UPSERT",
                            "aspectName": [
                                "dataHubExecutionRequestInput",
                                "dataHubExecutionRequestSignal",
                            ],
                            "aspect": {"value": {"executorId": "default"}},
                        }
                    ]
                }
            }
        }
    )

    # All conditions match
    assert f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataHubExecutionRequest",
            changeType="UPSERT",
            aspectName="dataHubExecutionRequestInput",
            aspect={"value": {"executorId": "default"}},
        )
    )

    # Scalar mismatch (wrong changeType)
    assert not f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataHubExecutionRequest",
            changeType="DELETE",
            aspectName="dataHubExecutionRequestInput",
            aspect={"value": {"executorId": "default"}},
        )
    )

    # Nested mismatch (wrong executorId)
    assert not f.matches(
        _env(
            METADATA_CHANGE_LOG_EVENT_V1_TYPE,
            entityType="dataHubExecutionRequest",
            changeType="UPSERT",
            aspectName="dataHubExecutionRequestInput",
            aspect={"value": {"executorId": "custom"}},
        )
    )
