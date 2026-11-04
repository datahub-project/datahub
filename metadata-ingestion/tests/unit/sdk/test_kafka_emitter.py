import unittest
from unittest.mock import patch

import pydantic
import pytest

from datahub.emitter.kafka_emitter import (
    DEFAULT_MCE_KAFKA_TOPIC,
    DEFAULT_MCP_KAFKA_TOPIC,
    MCE_KEY,
    MCP_KEY,
    DatahubKafkaEmitter,
    KafkaEmitterConfig,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    OwnershipClass,
    StatusClass,
)


class KafkaEmitterTest(unittest.TestCase):
    def test_kafka_emitter_config(self):
        emitter_config = KafkaEmitterConfig.model_validate(
            {"connection": {"bootstrap": "foobar:9092"}}
        )
        assert emitter_config.topic_routes[MCE_KEY] == DEFAULT_MCE_KAFKA_TOPIC
        assert emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC

    def test_kafka_emitter_config_old_and_new(self):
        """Respecifying old and new topic config should barf."""
        with pytest.raises(pydantic.ValidationError):
            KafkaEmitterConfig.model_validate(
                {
                    "connection": {"bootstrap": "foobar:9092"},
                    "topic": "NewTopic",
                    "topic_routes": {MCE_KEY: "NewTopic"},
                }
            )

    def test_kafka_emitter_config_topic_upgrade(self):
        """Old topic config provided should get auto-upgraded to new topic_routes."""
        emitter_config = KafkaEmitterConfig.model_validate(
            {"connection": {"bootstrap": "foobar:9092"}, "topic": "NewTopic"}
        )
        assert emitter_config.topic_routes[MCE_KEY] == "NewTopic"  # MCE topic upgraded
        assert (
            emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC
        )  # No change to MCP

    def test_kafka_emitter_config_missing_mce_key(self):
        """Missing MCE_KEY in topic_routes should not raise an error."""
        emitter_config = KafkaEmitterConfig.parse_obj(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCP_KEY: "MCP_Topic"},
            }
        )
        assert MCE_KEY not in emitter_config.topic_routes
        assert emitter_config.topic_routes[MCP_KEY] == "MCP_Topic"

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_no_producer_for_mce_key(self, mock_producer):
        """No producer should be created for MCE_KEY if it's missing in topic_routes."""
        emitter_config = KafkaEmitterConfig.parse_obj(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCP_KEY: "MCP_Topic"},
            }
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)

        # Ensure no producer is created for MCE_KEY
        assert MCE_KEY not in emitter.producers
        assert MCP_KEY in emitter.producers

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_produces_mcp_per_aspect(self, mock_producer):
        """An MCE is published as one MCP per aspect to the MCP topic, even without an MCE route."""
        emitter_config = KafkaEmitterConfig.model_validate(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCP_KEY: "MCP_Topic"},
            }
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)
        status = StatusClass(removed=False)
        ownership = OwnershipClass(owners=[])
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123", aspects=[status, ownership]
            )
        )

        emitter.emit(mce, callback=lambda err, msg: None)

        produce_calls = mock_producer.return_value.produce.call_args_list
        assert [c.kwargs["topic"] for c in produce_calls] == ["MCP_Topic"] * 2
        values = [c.kwargs["value"] for c in produce_calls]
        assert all(isinstance(v, MetadataChangeProposalWrapper) for v in values)
        assert [v.aspect for v in values] == [status, ownership]
        assert {c.kwargs["key"] for c in produce_calls} == {"urn:li:dataset:123"}

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mcp(self, mock_producer):
        emitter_config = KafkaEmitterConfig.model_validate(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCE_KEY: "MCE_Topic", MCP_KEY: "MCP_Topic"},
            }
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)
        mcp = MetadataChangeProposal(
            entityUrn="urn:li:entity:123",
            entityType="dataset",
            changeType="UPSERT",
        )

        emitter.emit_mcp_async(mcp, callback=lambda err, msg: None)

        mock_producer.return_value.produce.assert_called_once()
        assert (
            mock_producer.return_value.produce.call_args.kwargs["topic"] == "MCP_Topic"
        )

    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    @patch(
        "datahub.emitter.kafka_emitter.SchemaRegistryClient", autospec=True
    )  # Mock schema registry
    def test_kafka_emitter_oauth_initialization_with_oauth(
        self, mock_schema_registry, mock_producer
    ):
        """Test that OAuth callback poll() is triggered during emitter initialization when OAuth is configured"""
        config = KafkaEmitterConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "producer_config": {
                        "security.protocol": "SASL_SSL",
                        "sasl.mechanism": "OAUTHBEARER",
                        "oauth_cb": "tests.integration.kafka.oauth:create_token",
                    },
                }
            }
        )

        mock_producer_instance = mock_producer.return_value
        DatahubKafkaEmitter(config)

        # Verify poll(0) was called during initialization for OAuth
        # Should be called twice (once for MCE producer, once for MCP producer)
        assert mock_producer_instance.poll.call_count == 2, (
            "poll() should be called twice for OAuth initialization (MCE + MCP producers)"
        )
        mock_producer_instance.poll.assert_called_with(
            0
        )  # Verify it's called with timeout=0

    @patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
    @patch("datahub.emitter.kafka_emitter.SchemaRegistryClient", autospec=True)
    def test_kafka_emitter_no_oauth_initialization_without_oauth(
        self, mock_schema_registry, mock_producer
    ):
        """Test that poll() is NOT called during initialization when OAuth is not configured (backwards compatibility)"""
        config = KafkaEmitterConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "producer_config": {
                        "security.protocol": "SASL_SSL",
                        "sasl.mechanism": "PLAIN",
                        "sasl.username": "user",
                        "sasl.password": "pass",
                    },
                }
            }
        )

        mock_producer_instance = mock_producer.return_value
        DatahubKafkaEmitter(config)

        # Verify poll() was NOT called during initialization (no OAuth)
        mock_producer_instance.poll.assert_not_called()
