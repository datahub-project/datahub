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

import pytest

from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)
from tests.unit.test_helpers import TestMessage


@pytest.fixture
def pipeline_context():
    """Create a minimal pipeline context for testing."""
    return PipelineContext(pipeline_name="test_pipeline", graph=None)


@pytest.fixture
def mock_schema_registry():
    """Mock the schema registry client to avoid network calls."""
    with patch(
        "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
    ):
        yield


@pytest.fixture
def mock_consumer():
    """Mock the Kafka consumer to avoid network calls."""
    with patch(
        "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
    ):
        yield


def create_event_source(
    early_filter: dict = None, pipeline_context=None, mock_consumer=None
) -> KafkaEventSource:
    """Helper to create KafkaEventSource with early_filter config."""
    if pipeline_context is None:
        pipeline_context = PipelineContext(pipeline_name="test_pipeline", graph=None)

    config_dict = {
        "connection": {
            "bootstrap": "localhost:9092",
            "schema_registry_url": "http://localhost:8081",
        }
    }
    if early_filter:
        config_dict["early_filter"] = early_filter

    config = KafkaEventSourceConfig.model_validate(config_dict)

    with (
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.SchemaRegistryClient"
        ),
        patch(
            "datahub_actions.plugin.source.kafka.kafka_event_source.confluent_kafka.DeserializingConsumer"
        ),
    ):
        return KafkaEventSource(config, pipeline_context)


def test_early_filter_rejects_on_entity_type_mismatch():
    """Test that early filter correctly rejects based on entityType mismatch."""
    source = create_event_source(
        early_filter={
            "entityType": "dataHubExecutionRequest",
            "aspectName": "dataHubExecutionRequestInput",
        }
    )

    # Event with different entityType should be rejected
    msg = TestMessage(
        {
            "entityType": "dataset",  # Doesn't match filter
            "aspectName": "dataHubExecutionRequestInput",
            "changeType": "UPSERT",
        }
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 0  # Rejected - no EventEnvelope


def test_early_filter_rejects_on_aspect_name_mismatch():
    """Test that early filter correctly rejects based on aspectName mismatch."""
    source = create_event_source(
        early_filter={
            "entityType": "dataHubExecutionRequest",
            "aspectName": "dataHubExecutionRequestInput",
        }
    )

    # Event with different aspectName should be rejected
    msg = TestMessage(
        {
            "entityType": "dataHubExecutionRequest",
            "aspectName": "someOtherAspect",  # Doesn't match filter
            "changeType": "UPSERT",
        }
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 0  # Rejected - no EventEnvelope


def test_early_filter_passes_then_full_deserialization():
    """Test that early filter passes simple checks, then full deserialization occurs."""
    source = create_event_source(
        early_filter={
            "entityType": "dataset",
            "aspectName": "dataPlatformInstance",
        }
    )

    # Event passes early filter
    msg = TestMessage(
        {
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
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 1  # Passed - EventEnvelope yielded
    assert result[0].event_type == "MetadataChangeLogEvent_v1"


def test_early_filter_list_matching():
    """Test that early filter supports list matching (any match passes)."""
    source = create_event_source(
        early_filter={
            "entityType": "dataHubExecutionRequest",
            "aspectName": [
                "dataHubExecutionRequestInput",
                "dataHubExecutionRequestSignal",
            ],
        }
    )

    # Event with aspectName matching first item in list
    msg1 = TestMessage(
        {
            "entityType": "dataHubExecutionRequest",
            "aspectName": "dataHubExecutionRequestInput",
            "changeType": "UPSERT",
            "created": (
                "com.linkedin.pegasus2avro.common.AuditStamp",
                {
                    "time": 1651593944068,
                    "actor": "urn:li:corpuser:UNKNOWN",
                    "impersonator": None,
                },
            ),
        }
    )

    result1 = list(source.handle_mcl(msg1))
    assert len(result1) == 1  # Passed - matches first item

    # Event with aspectName matching second item in list
    msg2 = TestMessage(
        {
            "entityType": "dataHubExecutionRequest",
            "aspectName": "dataHubExecutionRequestSignal",
            "changeType": "UPSERT",
            "created": (
                "com.linkedin.pegasus2avro.common.AuditStamp",
                {
                    "time": 1651593944068,
                    "actor": "urn:li:corpuser:UNKNOWN",
                    "impersonator": None,
                },
            ),
        }
    )

    result2 = list(source.handle_mcl(msg2))
    assert len(result2) == 1  # Passed - matches second item

    # Event with aspectName not in list
    msg3 = TestMessage(
        {
            "entityType": "dataHubExecutionRequest",
            "aspectName": "someOtherAspect",
            "changeType": "UPSERT",
        }
    )

    result3 = list(source.handle_mcl(msg3))
    assert len(result3) == 0  # Rejected - doesn't match any item


def test_early_filter_not_configured():
    """Test that events are processed normally when early_filter is not configured."""
    source = create_event_source()  # No early_filter

    msg = TestMessage(
        {
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
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 1  # Processed normally
    assert result[0].event_type == "MetadataChangeLogEvent_v1"


def test_early_filter_ignores_nested_fields():
    """Test that early filter ignores nested dict fields and logs debug warning."""
    source = create_event_source(
        early_filter={
            "entityType": "dataset",
            "aspect": {"value": {"executorId": "default"}},  # Nested dict - ignored
        }
    )

    # Should have only entityType in criteria (aspect ignored)
    assert "entityType" in source._early_filter_criteria
    assert "aspect" not in source._early_filter_criteria

    # Event should pass (only entityType checked)
    msg = TestMessage(
        {
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
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 1  # Passed - nested field ignored


def test_early_filter_error_handling():
    """Test that early filter gracefully falls back on errors."""
    source = create_event_source(
        early_filter={
            "entityType": "dataset",
        }
    )

    # Create a mock dict that raises exception on .get()
    class ErrorDict:
        def get(self, key):
            raise Exception("Test error")

    error_dict = ErrorDict()

    # Should gracefully fall back - return True (deserialize anyway)
    result = KafkaEventSource._should_deserialize(
        error_dict, source._early_filter_criteria, "test_pipeline"
    )
    assert result is True  # Graceful fallback


def test_early_filter_missing_field():
    """Test that early filter rejects when required field is missing."""
    source = create_event_source(
        early_filter={
            "entityType": "dataHubExecutionRequest",
        }
    )

    # Event without entityType key
    msg = TestMessage(
        {
            "aspectName": "dataHubExecutionRequestInput",
            "changeType": "UPSERT",
            # No entityType field
        }
    )

    result = list(source.handle_mcl(msg))
    assert len(result) == 0  # Rejected - missing field doesn't match


def test_early_filter_ignores_unsupported_fields():
    """Test that early filter ignores fields not in SIMPLE_FIELDS."""
    source = create_event_source(
        early_filter={
            "entityType": "dataset",
            "entityUrn": "urn:li:dataset:abc",  # Not in SIMPLE_FIELDS
            "customField": "value",  # Not in SIMPLE_FIELDS
        }
    )

    # Should only have entityType in criteria
    assert "entityType" in source._early_filter_criteria
    assert "entityUrn" not in source._early_filter_criteria
    assert "customField" not in source._early_filter_criteria
