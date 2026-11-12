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

from unittest.mock import patch

from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE
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
) -> KafkaEventSource:
    config = KafkaEventSourceConfig(
        enable_mcl_pre_deserialization_filter=enable_mcl_pre_deserialization_filter
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


def test_handle_mcl():
    source = _make_source()
    msg = TestMessage(_MCL_MSG)
    result = list(source.handle_mcl(msg))[0]
    assert result is not None
    assert result.event_type == "MetadataChangeLogEvent_v1"


def test_handle_entity_event():
    msg = TestMessage(
        {
            "name": "entityChangeEvent",
            "payload": {
                "contentType": "application/json",
                "value": b'{"entityUrn": "urn:li:dataset:abc","entityType": "dataset","category": "TAG","operation": "ADD","modifier": "urn:li:tag:PII","auditStamp": {"actor": "urn:li:corpuser:jdoe","time": 1649953100653},"version":0}',
            },
        }
    )
    source = _make_source()
    result = list(source.handle_pe(msg))[0]
    assert result is not None
    assert result.event_type == "EntityChangeEvent_v1"


# ── set_filters / pre-deserialization filter ──────────────────────────────────

_ctx = PipelineContext(pipeline_name="test", graph=None)


def _ece_only_filter() -> EventTypeFilter:
    return EventTypeFilter.create({"filter": {"EntityChangeEvent_v1": {}}}, _ctx)


def _mcl_filter_with_entity_type(entity_type: str) -> EventTypeFilter:
    return EventTypeFilter.create(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [{"entityType": entity_type}]
                }
            }
        },
        _ctx,
    )


def test_set_filters_noop_when_disabled():
    source = _make_source(enable_mcl_pre_deserialization_filter=False)
    source.set_filters([_ece_only_filter()])
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == []


def test_set_filters_noop_when_no_event_type_filter():
    """If enable_mcl_pre_deserialization_filter is True but there are no EventTypeFilters,
    MCL must not be skipped — no-op, pass everything through."""
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([])  # no filters at all
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == []


def test_set_filters_skip_mcl_entirely_when_no_mcl_type():
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([_ece_only_filter()])
    assert source._skip_mcl_entirely


def test_set_filters_extracts_scalar_early_criteria():
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([_mcl_filter_with_entity_type("dataset")])
    assert not source._skip_mcl_entirely
    assert source._early_mcl_criteria_list == [{"entityType": "dataset"}]


def test_set_filters_disabled_when_predicate_has_no_extractable_fields():
    """Predicates with only nested/dict fields have no extractable criteria.
    If ANY predicate has no extractable fields, optimization must be disabled
    (OR semantics: any message might match the unoptimizable predicate)."""
    ctx = PipelineContext(pipeline_name="test", graph=None)
    f = EventTypeFilter.create(
        {
            "filter": {
                METADATA_CHANGE_LOG_EVENT_V1_TYPE: {
                    "event": [{"aspect": {"value": {"executorId": "default"}}}]
                }
            }
        },
        ctx,
    )
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source.set_filters([f])
    assert not source._skip_mcl_entirely
    # Optimization disabled - predicate has no extractable fields
    assert source._early_mcl_criteria_list == []


def test_handle_mcl_skipped_when_skip_mcl_entirely():
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source._skip_mcl_entirely = True
    msg = TestMessage(_MCL_MSG)
    result = list(source.handle_mcl(msg))
    assert result == []


def test_handle_mcl_skipped_when_early_criteria_not_matched():
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source._early_mcl_criteria_list = [{"entityType": "schemaField"}]
    msg = TestMessage(_MCL_MSG)  # entityType == "dataset"
    result = list(source.handle_mcl(msg))
    assert result == []


def test_handle_mcl_passes_when_early_criteria_matched():
    source = _make_source(enable_mcl_pre_deserialization_filter=True)
    source._early_mcl_criteria_list = [{"entityType": "dataset"}]
    msg = TestMessage(_MCL_MSG)
    result = list(source.handle_mcl(msg))
    assert len(result) == 1
    assert result[0].event_type == METADATA_CHANGE_LOG_EVENT_V1_TYPE


def test_handle_pe_not_affected_by_mcl_pre_deserialization_filter():
    """EntityChangeEvent (PE) delivery must never be affected by the MCL
    pre-deserialization filter, regardless of the filter state."""
    pe_msg = TestMessage(
        {
            "name": "entityChangeEvent",
            "payload": {
                "contentType": "application/json",
                "value": b'{"entityUrn": "urn:li:dataset:abc","entityType": "dataset",'
                b'"category": "TAG","operation": "ADD","modifier": "urn:li:tag:PII",'
                b'"auditStamp": {"actor": "urn:li:corpuser:jdoe","time": 1649953100653},'
                b'"version":0}',
            },
        }
    )
    for skip_mcl, early_criteria_list in [
        (True, []),
        (False, [{"entityType": "schemaField"}]),
    ]:
        source = _make_source(enable_mcl_pre_deserialization_filter=True)
        source._skip_mcl_entirely = skip_mcl
        source._early_mcl_criteria_list = early_criteria_list

        result = list(source.handle_pe(pe_msg))
        assert len(result) == 1, (
            f"PE event must pass through regardless of skip_mcl={skip_mcl}, "
            f"early_criteria_list={early_criteria_list}"
        )
        assert result[0].event_type == "EntityChangeEvent_v1"
