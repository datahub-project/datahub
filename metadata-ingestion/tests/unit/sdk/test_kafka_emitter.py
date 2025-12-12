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
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.metadata.schema_classes import DatasetSnapshotClass, StatusClass


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
    def test_emit_mce_without_mce_key(self, mock_producer):
        """Emitting an MCE when MCE_KEY is missing should report error via callback."""
        emitter_config = KafkaEmitterConfig.parse_obj(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCP_KEY: "MCP_Topic"},
            }
        )
        emitter = DatahubKafkaEmitter(config=emitter_config)

        # Track callback invocations
        callback_errors = []

        def test_callback(err, msg):
            if err:
                callback_errors.append((err, msg))

        # Attempt to emit an MCE should call callback with error
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123", aspects=[StatusClass(False)]
            )
        )
        emitter.emit_mce_async(mce, callback=test_callback)

        # Verify callback was called with the error
        assert len(callback_errors) == 1
        error, msg = callback_errors[0]
        assert isinstance(error, Exception)
        assert (
            f"Cannot emit MetadataChangeEvent: {MCE_KEY} topic not configured in topic_routes"
            == str(error)
        )
        assert msg == "MCE emission failed - topic not configured"

        # Verify produce was not called since MCE topic is not configured
        mock_producer.return_value.produce.assert_not_called()

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_and_mcp(self, mock_producer):
        """Emitting both MCE and MCP when both keys are present."""
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
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123", aspects=[StatusClass(False)]
            )
        )  # Mock MCE object
        emitter.emit_mce_async(mce, callback=lambda err, msg: None)
        mock_producer.return_value.produce.assert_called()

        # Emit an MCP
        mcp = MetadataChangeProposal(
            entityUrn="urn:li:entity:123",
            entityType="dataset",
            changeType="UPSERT",
        )  # Mock MCP object
        emitter.emit_mcp_async(mcp, callback=lambda err, msg: None)
        mock_producer.return_value.produce.assert_called()
