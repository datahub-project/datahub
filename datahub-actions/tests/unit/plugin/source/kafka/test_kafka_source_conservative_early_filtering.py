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

"""Tests for MCL pre-deserialization filter OR semantics and conservative behavior."""

from unittest.mock import patch

from datahub_actions.event.event_registry import (
    ENTITY_CHANGE_EVENT_V1_TYPE,
    METADATA_CHANGE_LOG_EVENT_V1_TYPE,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.filter.event_type_filter import EventTypeFilter
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)
from tests.unit.test_helpers import TestMessage

_MCL_MSG = {
    "auditHeader": None,
    "entityType": "dataset",
    "entityUrn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
    "entityKeyAspect": None,
    "changeType": "UPSERT",
    "aspectName": "dataPlatformInstance",
    "aspect": (
        "com.linkedin.pegasus2avro.mxe.GenericAspect",
        {
            "value": b'{"platform":"urn:li:dataPlatform:hdfs"}',
            "contentType": "application/json",
        },
    ),
    "systemMetadata": (
        "com.linkedin.pegasus2avro.mxe.SystemMetadata",
        {
            "lastObserved": 1651593943881,
            "runId": "file-2022_05_03-21_35_43",
            "registryName": None,
            "registryVersion": None,
            "properties": None,
        },
    ),
    "previousAspectValue": None,
    "previousSystemMetadata": None,
    "created": (
        "com.linkedin.pegasus2avro.common.AuditStamp",
        {
            "time": 1651593944068,
            "actor": "urn:li:corpuser:UNKNOWN",
            "impersonator": None,
        },
    ),
}


def _make_source(
    enable_mcl_pre_deserialization_filter: bool = False,
    async_commit_enabled: bool = True,
) -> KafkaEventSource:
    config = KafkaEventSourceConfig(
        enable_mcl_pre_deserialization_filter=enable_mcl_pre_deserialization_filter,
        async_commit_enabled=async_commit_enabled,
    )
    ctx = PipelineContext(pipeline_name="test-kafka", graph=None)
    with (
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
        ),
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
        ),
    ):
        return KafkaEventSource(config, ctx)


def test_or_semantics_multiple_predicates():
    """Multiple predicates with extractable fields - optimization enabled with OR semantics."""
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {"entityType": "dataset"},
                        {"entityType": "chart", "aspectName": "documentation"},
                    ]
                }
            }
        },
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])

    # Assert criteria extracted correctly
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == [
        {"entityType": "dataset"},
        {"entityType": "chart", "aspectName": "documentation"},
    ]

    # Test handle_mcl: matches first predicate
    msg_dataset = TestMessage(_MCL_MSG)  # entityType=dataset
    assert len(list(source.handle_mcl(msg_dataset))) == 1, (
        "Should pass (matches predicate 1)"
    )

    # Test handle_mcl: matches second predicate
    msg_chart = TestMessage(
        {**_MCL_MSG, "entityType": "chart", "aspectName": "documentation"}
    )
    assert len(list(source.handle_mcl(msg_chart))) == 1, (
        "Should pass (matches predicate 2)"
    )

    # Test handle_mcl: matches neither predicate
    msg_dashboard = TestMessage({**_MCL_MSG, "entityType": "dashboard"})
    assert list(source.handle_mcl(msg_dashboard)) == [], (
        "Should reject (no predicate match)"
    )

    # Test handle_mcl: matches entityType but not aspectName of predicate 2
    msg_chart_wrong_aspect = TestMessage(
        {**_MCL_MSG, "entityType": "chart", "aspectName": "ownership"}
    )
    assert list(source.handle_mcl(msg_chart_wrong_aspect)) == [], (
        "Should reject (predicate 2 needs both fields)"
    )


def test_conservative_partial_extraction():
    """CONSERVATIVE: Predicate with some extractable + some non-extractable fields
    still enables optimization (extracts what it can, ignores the rest)."""
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
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
                            "aspect": {
                                "value": {"executorId": "default"}
                            },  # NOT extractable
                        }
                    ]
                }
            }
        },
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])

    # Assert criteria extracted correctly (aspect.value.executorId ignored)
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == [
        {
            "entityType": "dataHubExecutionRequest",
            "changeType": "UPSERT",
            "aspectName": [
                "dataHubExecutionRequestInput",
                "dataHubExecutionRequestSignal",
            ],
        }
    ]

    # Test handle_mcl: matches extractable criteria (CONSERVATIVE: passes even if executorId might be wrong)
    msg_match = TestMessage(
        {
            **_MCL_MSG,
            "entityType": "dataHubExecutionRequest",
            "changeType": "UPSERT",
            "aspectName": "dataHubExecutionRequestInput",
        }
    )
    assert len(list(source.handle_mcl(msg_match))) == 1, (
        "Should pass (matches extractable criteria)"
    )

    # Test handle_mcl: wrong changeType
    msg_wrong_change = TestMessage(
        {
            **_MCL_MSG,
            "entityType": "dataHubExecutionRequest",
            "changeType": "DELETE",
            "aspectName": "dataHubExecutionRequestInput",
        }
    )
    assert list(source.handle_mcl(msg_wrong_change)) == [], (
        "Should reject (changeType mismatch)"
    )

    # Test handle_mcl: aspectName not in list
    msg_wrong_aspect = TestMessage(
        {
            **_MCL_MSG,
            "entityType": "dataHubExecutionRequest",
            "changeType": "UPSERT",
            "aspectName": "someOtherAspect",
        }
    )
    assert list(source.handle_mcl(msg_wrong_aspect)) == [], (
        "Should reject (aspectName not in list)"
    )


def test_disabled_when_predicate_has_no_extractable_fields():
    """If ANY predicate has no extractable fields, optimization must be disabled
    (OR semantics: message might match the unoptimizable predicate)."""
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [
                        {"entityType": "dataset"},  # predicate 1: extractable
                        {
                            "aspect": {"value": {"executorId": "default"}}
                        },  # predicate 2: NOT extractable
                    ]
                }
            }
        },
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])

    # Assert optimization disabled
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == [], "Optimization should be disabled"

    # Test handle_mcl: all messages pass through (no early filtering)
    msg_dataset = TestMessage(_MCL_MSG)  # entityType=dataset
    assert len(list(source.handle_mcl(msg_dataset))) == 1, (
        "Should pass (no early filter)"
    )

    msg_chart = TestMessage({**_MCL_MSG, "entityType": "chart"})
    assert len(list(source.handle_mcl(msg_chart))) == 1, "Should pass (no early filter)"


# A message dropped by the early filter must still advance the committed offset, so the
# committed position tracks the log tip on a stream where matching events are rare
# (otherwise lag grows unboundedly and every restart replays the whole backlog).

_EXEC_REQUEST_FILTER = {
    "filter": {
        METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
            "event": [{"entityType": "dataHubExecutionRequest"}]
        }
    }
}


def _rejected_msg() -> TestMessage:
    # entityType=dataset does not match the execution-request filter -> definite miss.
    return TestMessage(
        {
            **_MCL_MSG,
            "topic": "MetadataChangeLog_Versioned_v1",
            "partition": 3,
            "offset": 100,
        }
    )


def test_rejected_message_advances_offset_async():
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(_EXEC_REQUEST_FILTER, ctx)
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])

    assert list(source.handle_mcl(_rejected_msg())) == [], (
        "Should reject (definite miss)"
    )

    source.consumer.store_offsets.assert_called_once()
    tp = source.consumer.store_offsets.call_args.kwargs["offsets"][0]
    # Kafka commits the next offset to consume, i.e. message offset + 1.
    assert (tp.topic, tp.partition, tp.offset) == (
        "MetadataChangeLog_Versioned_v1",
        3,
        101,
    )


def test_skip_entirely_advances_offset_async():
    # An EventTypeFilter that targets a non-MCL event type means the pipeline drops
    # all MCLs, so the source skips them entirely (_skip_mcl_entirely) before
    # deserialization. Those skipped MCLs must still advance the offset.
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
        {"filter": {ENTITY_CHANGE_EVENT_V1_TYPE: {"event": [{"category": "TAG"}]}}},
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])
    assert source._skip_mcl_entirely, "filter without MCL should skip all MCLs"

    assert list(source.handle_mcl(_rejected_msg())) == []

    source.consumer.store_offsets.assert_called_once()
    tp = source.consumer.store_offsets.call_args.kwargs["offsets"][0]
    assert tp.offset == 101


def test_rejected_message_commits_offset_sync_mode():
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(_EXEC_REQUEST_FILTER, ctx)
    source = _make_source(
        enable_mcl_pre_deserialization_filter=True, async_commit_enabled=False
    )
    source.set_filters([f])

    assert list(source.handle_mcl(_rejected_msg())) == []

    # Sync mode commits synchronously instead of storing for auto-commit.
    source.consumer.commit.assert_called_once()
    source.consumer.store_offsets.assert_not_called()


def test_matched_message_does_not_advance_offset():
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [{"entityType": "dataset"}]
                }
            }
        },
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])

    # A matched message flows to the pipeline (yielded) and is acked there, not here.
    assert len(list(source.handle_mcl(TestMessage(_MCL_MSG)))) == 1
    source.consumer.store_offsets.assert_not_called()
