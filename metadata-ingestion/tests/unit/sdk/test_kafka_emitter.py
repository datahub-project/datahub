import unittest

import pydantic
import pytest
from unittest.mock import (
    patch,
    MagicMock,
)
from datahub.emitter.kafka_emitter import DatahubKafkaEmitter
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

from datahub.emitter.kafka_emitter import (
    DEFAULT_MCE_KAFKA_TOPIC,
    DEFAULT_MCP_KAFKA_TOPIC,
    MCE_KEY,
    MCP_KEY,
    KafkaEmitterConfig,
)


class KafkaEmitterTest(unittest.TestCase):
    def test_kafka_emitter_config(self):
        emitter_config = KafkaEmitterConfig.parse_obj(
            {"connection": {"bootstrap": "foobar:9092"}}
        )
        assert emitter_config.topic_routes[MCE_KEY] == DEFAULT_MCE_KAFKA_TOPIC
        assert emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC

    """
    Respecifying old and new topic config should barf
    """

    def test_kafka_emitter_config_old_and_new(self):
        with pytest.raises(pydantic.ValidationError):
            KafkaEmitterConfig.parse_obj(
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
        emitter_config = KafkaEmitterConfig.parse_obj(
            {"connection": {"bootstrap": "foobar:9092"}, "topic": "NewTopic"}
        )
        assert emitter_config.topic_routes[MCE_KEY] == "NewTopic"  # MCE topic upgraded
        assert (
            emitter_config.topic_routes[MCP_KEY] == DEFAULT_MCP_KAFKA_TOPIC
        )  # No change to MCP

    """
    Missing MCE_KEY in topic_routes should not raise an error
    """
    def test_kafka_emitter_config_missing_mce_key(self):
        emitter_config = KafkaEmitterConfig.parse_obj(
            {"connection": {"bootstrap": "foobar:9092"}, "topic_routes": {MCP_KEY: "MCP_Topic"}}
        )
        assert MCE_KEY not in emitter_config.topic_routes
        assert emitter_config.topic_routes[MCP_KEY] == "MCP_Topic"

    """
    No producer should be created for MCE_KEY if it's missing in topic_routes
    """
    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_no_producer_for_mce_key(self, mock_producer):
        emitter_config = KafkaEmitterConfig.parse_obj(
            {"connection": {"bootstrap": "foobar:9092"}, "topic_routes": {MCP_KEY: "MCP_Topic"}}
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)

        # Ensure no producer is created for MCE_KEY
        assert MCE_KEY not in emitter.producers
        assert MCP_KEY in emitter.producers
    
    """
    Emitting an MCE when MCE_KEY is missing should be a no-op
    """
    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_without_mce_key(self, mock_producer):
        emitter_config = KafkaEmitterConfig.parse_obj(
            {"connection": {"bootstrap": "foobar:9092"}, "topic_routes": {MCP_KEY: "MCP_Topic"}}
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)

        # Attempt to emit an MCE
        mce = MetadataChangeEvent(proposedSnapshot=None)  # Mock MCE object
        emitter.emit_mce_async(mce, callback=None)

        mock_producer.return_value.produce.assert_not_called()  # Ensure produce is not called
    
    """
    Emitting both MCE and MCP when both keys are present
    """
    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_and_mcp(self, mock_producer):
        emitter_config = KafkaEmitterConfig.parse_obj(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCE_KEY: "MCE_Topic", MCP_KEY: "MCP_Topic"},
            }
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)

        # Ensure producers are created for both keys
        assert MCE_KEY in emitter.producers
        assert MCP_KEY in emitter.producers

        # Emit an MCE
        mce = MetadataChangeEvent(proposedSnapshot=MagicMock(urn="urn:li:dataset:123"))  # Mock MCE object
        emitter.emit_mce_async(mce, callback=None)
        mock_producer.return_value.produce.assert_called()

        # Emit an MCP
        mcp =MetadataChangeProposal(
            entityUrn="urn:li:entity:123",
            entityType="dataset",
            changeType="UPSERT",
        )  # Mock MCP object
        emitter.emit_mcp_async(mcp, callback=None)
        mock_producer.return_value.produce.assert_called()
