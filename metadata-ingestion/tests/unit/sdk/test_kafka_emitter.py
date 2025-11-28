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


class KafkaEmitterTest(unittest.TestCase):
    def test_kafka_emitter_config(self):
        emitter_config = KafkaEmitterConfig.model_validate(
            {"connection": {"bootstrap": "foobar:9092"}}
        )
        assert emitter_config.topic_routes[MCE_KEY] == DEFAULT_MCE_KAFKA_TOPIC
        assert emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC

    """
    Respecifying old and new topic config should barf
    """

    def test_kafka_emitter_config_old_and_new(self):
        with pytest.raises(pydantic.ValidationError):
            KafkaEmitterConfig.model_validate(
                {
                    "connection": {"bootstrap": "foobar:9092"},
                    "topic": "NewTopic",
                    "topic_routes": {MCE_KEY: "NewTopic"},
                }
            )

    """
    Old topic config provided should get auto-upgraded to new topic_routes
    """

    def test_kafka_emitter_config_topic_upgrade(self):
        emitter_config = KafkaEmitterConfig.model_validate(
            {"connection": {"bootstrap": "foobar:9092"}, "topic": "NewTopic"}
        )
        assert emitter_config.topic_routes[MCE_KEY] == "NewTopic"  # MCE topic upgraded
        assert (
            emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC
        )  # No change to MCP

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
