# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
