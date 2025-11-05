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

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)
from tests.unit.test_helpers import TestMessage


def test_handle_mcl():
    inp = {
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
    msg = TestMessage(inp)
    result = list(KafkaEventSource.handle_mcl(msg))[0]
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
    result = list(KafkaEventSource.handle_pe(msg))[0]
    assert result is not None
    assert result.event_type == "EntityChangeEvent_v1"


# MSK IAM Authentication Tests


def test_kafka_consumer_connection_config_basic():
    """Test basic KafkaConsumerConnectionConfig without SASL."""
    config = KafkaConsumerConnectionConfig()
    assert config.sasl_mechanism is None
    assert config.aws_region is None
    assert config.consumer_config == {}


def test_kafka_consumer_connection_config_aws_msk_iam():
    """Test KafkaConsumerConnectionConfig with AWS_MSK_IAM SASL mechanism."""
    config = KafkaConsumerConnectionConfig(sasl_mechanism="AWS_MSK_IAM")

    assert config.sasl_mechanism == "AWS_MSK_IAM"
    assert "security.protocol" in config.consumer_config
    assert config.consumer_config["security.protocol"] == "SASL_SSL"
    assert config.consumer_config["sasl.mechanism"] == "AWS_MSK_IAM"
    # AWS_MSK_IAM doesn't require oauth_cb
    assert "oauth_cb" not in config.consumer_config


def test_kafka_consumer_connection_config_oauthbearer_missing_region():
    """Test that OAUTHBEARER without aws_region raises validation error."""
    with pytest.raises(
        ValueError, match="aws_region must be set when using OAUTHBEARER"
    ):
        KafkaConsumerConnectionConfig(sasl_mechanism="OAUTHBEARER")


def test_kafka_consumer_connection_config_oauthbearer_with_region():
    """Test KafkaConsumerConnectionConfig with OAUTHBEARER and aws_region."""
    # Mock the module import before creating the config
    import sys
    from unittest.mock import MagicMock

    mock_msk_module = MagicMock()
    mock_msk_module.MSKAuthTokenProvider = MagicMock()
    sys.modules["aws_msk_iam_sasl_signer"] = mock_msk_module

    try:
        config = KafkaConsumerConnectionConfig(
            sasl_mechanism="OAUTHBEARER", aws_region="us-east-1"
        )

        assert config.sasl_mechanism == "OAUTHBEARER"
        assert config.aws_region == "us-east-1"
        assert "security.protocol" in config.consumer_config
        assert config.consumer_config["security.protocol"] == "SASL_SSL"
        assert config.consumer_config["sasl.mechanism"] == "OAUTHBEARER"
        assert "oauth_cb" in config.consumer_config
        assert callable(config.consumer_config["oauth_cb"])
    finally:
        # Clean up the mock module
        if "aws_msk_iam_sasl_signer" in sys.modules:
            del sys.modules["aws_msk_iam_sasl_signer"]


def test_kafka_consumer_connection_config_oauthbearer_missing_library():
    """Test that OAUTHBEARER without aws_msk_iam_sasl_signer raises ImportError."""
    with patch(
        "builtins.__import__",
        side_effect=ImportError("No module named 'aws_msk_iam_sasl_signer'"),
    ):
        with pytest.raises(
            ImportError, match="aws_msk_iam_sasl_signer.*library is required"
        ):
            KafkaConsumerConnectionConfig(
                sasl_mechanism="OAUTHBEARER", aws_region="us-east-1"
            )


def test_kafka_consumer_connection_config_unsupported_sasl():
    """Test warning for unsupported SASL mechanism."""
    with patch("datahub.configuration.kafka.logger") as mock_logger:
        config = KafkaConsumerConnectionConfig(sasl_mechanism="UNSUPPORTED_MECHANISM")

        assert config.consumer_config["sasl.mechanism"] == "UNSUPPORTED_MECHANISM"
        mock_logger.warning.assert_called_once()
        assert "UNSUPPORTED_MECHANISM" in mock_logger.warning.call_args[0][0]


def test_kafka_consumer_connection_config_preserves_existing_config():
    """Test that existing consumer_config is preserved when adding SASL."""
    existing_config = {"enable.auto.commit": False, "group.id": "test-group"}
    config = KafkaConsumerConnectionConfig(
        consumer_config=existing_config, sasl_mechanism="AWS_MSK_IAM"
    )

    # Existing config should be preserved
    assert config.consumer_config["enable.auto.commit"] is False
    assert config.consumer_config["group.id"] == "test-group"

    # SASL config should be added
    assert config.consumer_config["security.protocol"] == "SASL_SSL"
    assert config.consumer_config["sasl.mechanism"] == "AWS_MSK_IAM"


@patch("confluent_kafka.DeserializingConsumer")
@patch("confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient")
def test_kafka_event_source_with_msk_iam(mock_schema_client, mock_consumer):
    """Test that KafkaEventSource properly uses MSK IAM configuration."""
    config = KafkaEventSourceConfig(
        connection=KafkaConsumerConnectionConfig(
            bootstrap="my-cluster:9092", sasl_mechanism="AWS_MSK_IAM"
        )
    )
    ctx = PipelineContext(pipeline_name="test-pipeline", graph=None)

    # Create the event source (this should not raise)
    _ = KafkaEventSource(config, ctx)

    # Verify the consumer was created with the right config
    mock_consumer.assert_called_once()
    consumer_config = mock_consumer.call_args[0][0]

    assert consumer_config["bootstrap.servers"] == "my-cluster:9092"
    assert consumer_config["security.protocol"] == "SASL_SSL"
    assert consumer_config["sasl.mechanism"] == "AWS_MSK_IAM"
    assert consumer_config["group.id"] == "test-pipeline"


@patch("confluent_kafka.DeserializingConsumer")
@patch("confluent_kafka.schema_registry.schema_registry_client.SchemaRegistryClient")
def test_kafka_event_source_with_oauthbearer(mock_schema_client, mock_consumer):
    """Test that KafkaEventSource properly uses OAUTHBEARER configuration."""
    # Mock the module import before creating the config
    import sys
    from unittest.mock import MagicMock

    mock_msk_module = MagicMock()
    mock_msk_module.MSKAuthTokenProvider = MagicMock()
    sys.modules["aws_msk_iam_sasl_signer"] = mock_msk_module

    try:
        config = KafkaEventSourceConfig(
            connection=KafkaConsumerConnectionConfig(
                bootstrap="my-cluster:9092",
                sasl_mechanism="OAUTHBEARER",
                aws_region="us-west-2",
            )
        )
        ctx = PipelineContext(pipeline_name="test-pipeline", graph=None)

        # Create the event source (this should not raise)
        _ = KafkaEventSource(config, ctx)

        # Verify the consumer was created with the right config
        mock_consumer.assert_called_once()
        consumer_config = mock_consumer.call_args[0][0]

        assert consumer_config["bootstrap.servers"] == "my-cluster:9092"
        assert consumer_config["security.protocol"] == "SASL_SSL"
        assert consumer_config["sasl.mechanism"] == "OAUTHBEARER"
        assert consumer_config["group.id"] == "test-pipeline"
        assert "oauth_cb" in consumer_config
        assert callable(consumer_config["oauth_cb"])
    finally:
        # Clean up the mock module
        if "aws_msk_iam_sasl_signer" in sys.modules:
            del sys.modules["aws_msk_iam_sasl_signer"]


def test_oauth_callback_function():
    """Test the oauth callback function works correctly."""
    # Mock the module import before creating the config
    import sys
    from unittest.mock import MagicMock

    mock_msk_module = MagicMock()
    mock_msk_provider = MagicMock()
    mock_msk_provider.generate_auth_token.return_value = (
        "test-token",
        3600000,
    )  # 1 hour in ms
    mock_msk_module.MSKAuthTokenProvider = mock_msk_provider
    sys.modules["aws_msk_iam_sasl_signer"] = mock_msk_module

    try:
        config = KafkaConsumerConnectionConfig(
            sasl_mechanism="OAUTHBEARER", aws_region="us-east-1"
        )

        oauth_cb = config.consumer_config["oauth_cb"]
        token, expiry = oauth_cb("dummy_config_str")

        assert token == "test-token"
        assert expiry == 3600.0  # Should be converted from ms to seconds
        mock_msk_provider.generate_auth_token.assert_called_once_with("us-east-1")
    finally:
        # Clean up the mock module
        if "aws_msk_iam_sasl_signer" in sys.modules:
            del sys.modules["aws_msk_iam_sasl_signer"]
