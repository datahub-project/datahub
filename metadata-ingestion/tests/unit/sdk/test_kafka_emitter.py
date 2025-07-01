import unittest

import pydantic
import pytest

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
