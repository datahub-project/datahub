import os
import unittest
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.error import SchemaRegistryError

from datahub.configuration.common import ConfigurationError, ConfigurationWarning
from datahub.emitter.kafka_emitter import (
    DEFAULT_MCP_KAFKA_TOPIC,
    MCE_KEY,
    MCP_KEY,
    DatahubKafkaEmitter,
    KafkaEmitterConfig,
    _warn_mce_deprecated,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import DataHubDeprecationWarning
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
    def setUp(self):
        _warn_mce_deprecated.cache_clear()
        self.addCleanup(_warn_mce_deprecated.cache_clear)

    def test_kafka_emitter_config(self):
        with self.assertNoLogs("datahub.emitter.kafka_emitter", "WARNING"):
            emitter_config = KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        assert emitter_config.topic_routes == {MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC}

    def test_kafka_emitter_config_mce_route_ignored(self):
        with self.assertLogs("datahub.emitter.kafka_emitter", "WARNING") as logs:
            KafkaEmitterConfig.model_validate(
                {
                    "connection": {"bootstrap": "foobar:9092"},
                    "topic_routes": {MCE_KEY: "MCE_Topic", MCP_KEY: "MCP_Topic"},
                }
            )
        assert any("MCP_Topic" in r.getMessage() for r in logs.records)

    def test_kafka_emitter_config_topic_removed(self):
        with pytest.warns(ConfigurationWarning):
            emitter_config = KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}, "topic": "NewTopic"}
            )
        assert emitter_config.topic_routes == {MCP_KEY: DEFAULT_MCP_KAFKA_TOPIC}

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_produces_mcp_per_aspect(self, mock_producer):
        """An MCE is published as one MCP per aspect to the MCP topic, even with an MCE route set."""
        emitter_config = KafkaEmitterConfig.model_validate(
            {
                "connection": {"bootstrap": "foobar:9092"},
                "topic_routes": {MCE_KEY: "MCE_Topic", MCP_KEY: "MCP_Topic"},
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

        results = []

        with pytest.warns(DataHubDeprecationWarning):
            emitter.emit(mce, callback=lambda err, msg: results.append(err))

        produce_calls = mock_producer.return_value.produce.call_args_list
        assert [c.kwargs["topic"] for c in produce_calls] == ["MCP_Topic"] * 2
        values = [c.kwargs["value"] for c in produce_calls]
        assert all(isinstance(v, MetadataChangeProposalWrapper) for v in values)
        assert [v.aspect for v in values] == [status, ownership]
        assert {c.kwargs["key"] for c in produce_calls} == {"urn:li:dataset:123"}

        # One callback per MCE, fired once every aspect is delivered.
        for c in produce_calls:
            assert results == []
            c.kwargs["on_delivery"](None, "msg")
        assert results == [None]

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_delivery_failure_reports_once(self, mock_producer):
        emitter = DatahubKafkaEmitter(
            config=KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        )
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123",
                aspects=[StatusClass(removed=False), OwnershipClass(owners=[])],
            )
        )
        results = []

        with pytest.warns(DataHubDeprecationWarning):
            emitter.emit(mce, callback=lambda err, msg: results.append(err))

        for c in mock_producer.return_value.produce.call_args_list:
            c.kwargs["on_delivery"]("delivery failed", "msg")
        assert results == ["delivery failed"]

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_produce_failure_raises_without_callback(self, mock_producer):
        emitter = DatahubKafkaEmitter(
            config=KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        )
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123",
                aspects=[StatusClass(removed=False), OwnershipClass(owners=[])],
            )
        )
        mock_producer.return_value.produce.side_effect = [None, RuntimeError("boom")]
        results = []

        with pytest.raises(RuntimeError), pytest.warns(DataHubDeprecationWarning):
            emitter.emit(mce, callback=lambda err, msg: results.append(err))

        # The first aspect's late delivery failure must not report the record a second time.
        first_call = mock_producer.return_value.produce.call_args_list[0]
        with self.assertLogs("datahub.emitter.kafka_emitter", "ERROR"):
            first_call.kwargs["on_delivery"]("delivery failed", "msg")
        assert results == []

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_without_aspects_still_calls_back(self, mock_producer):
        emitter = DatahubKafkaEmitter(
            config=KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        )
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(urn="urn:li:dataset:123", aspects=[])
        )
        results = []

        with pytest.warns(DataHubDeprecationWarning):
            emitter.emit(mce, callback=lambda err, msg: results.append(err))

        assert results == [None]
        mock_producer.return_value.produce.assert_not_called()

    @patch("datahub.emitter.kafka_emitter.SerializingProducer")
    def test_emit_mce_is_deprecated(self, mock_producer):
        emitter = DatahubKafkaEmitter(
            config=KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        )
        mce = MetadataChangeEvent(
            proposedSnapshot=DatasetSnapshotClass(
                urn="urn:li:dataset:123", aspects=[StatusClass(removed=False)]
            )
        )

        with (
            pytest.warns(DataHubDeprecationWarning) as warned,
            self.assertLogs("datahub.emitter.kafka_emitter", "WARNING") as logs,
        ):
            emitter.emit(mce, callback=lambda err, msg: None)
            emitter.emit(mce, callback=lambda err, msg: None)

        assert len([r for r in logs.records if "deprecated" in r.getMessage()]) == 1
        # Attributed to the caller, not to kafka_emitter.py.
        assert all(w.filename == __file__ for w in warned)

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

        # Verify poll(0) was called during initialization for OAuth, once for the MCP producer
        assert mock_producer_instance.poll.call_count == 1
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


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
@patch("datahub.emitter.kafka_emitter.AvroSerializer", autospec=True)
@patch("datahub.emitter.kafka_emitter.SchemaRegistryClient", autospec=True)
def test_auto_register_schemas_enabled_by_default(
    mock_sr_client: MagicMock, mock_serializer: MagicMock, mock_producer: MagicMock
) -> None:
    with patch.dict(os.environ, clear=True):
        DatahubKafkaEmitter(
            KafkaEmitterConfig.model_validate(
                {"connection": {"bootstrap": "foobar:9092"}}
            )
        )

    assert mock_serializer.call_count == 1
    for c in mock_serializer.call_args_list:
        assert c.kwargs["conf"] == {}
    mock_sr_client.return_value.lookup_schema.assert_not_called()


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
@patch("datahub.emitter.kafka_emitter.AvroSerializer", autospec=True)
@patch("datahub.emitter.kafka_emitter.SchemaRegistryClient", autospec=True)
def test_auto_register_schemas_disabled_verifies_routed_subjects(
    mock_sr_client: MagicMock, mock_serializer: MagicMock, mock_producer: MagicMock
) -> None:
    DatahubKafkaEmitter(
        KafkaEmitterConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "foobar:9092",
                    "auto_register_schemas": False,
                },
                "topic_routes": {MCP_KEY: "custom-mcp"},
            }
        )
    )

    assert mock_serializer.call_count == 1
    for c in mock_serializer.call_args_list:
        assert c.kwargs["conf"] == {"auto.register.schemas": False}
    # The MCE topic isn't routed, so only the MCP subject must be pre-registered.
    lookups = mock_sr_client.return_value.lookup_schema.call_args_list
    assert [c.args[0] for c in lookups] == ["custom-mcp-value"]


@patch("datahub.emitter.kafka_emitter.SerializingProducer", autospec=True)
@patch("datahub.emitter.kafka_emitter.SchemaRegistryClient", autospec=True)
def test_auto_register_schemas_disabled_missing_subject_fails_fast(
    mock_sr_client: MagicMock, mock_producer: MagicMock
) -> None:
    mock_sr_client.return_value.lookup_schema.side_effect = SchemaRegistryError(
        http_status_code=404, error_code=40401, error_message="Subject not found"
    )

    with pytest.raises(ConfigurationError, match=f"{DEFAULT_MCP_KAFKA_TOPIC}-value"):
        DatahubKafkaEmitter(
            KafkaEmitterConfig.model_validate(
                {
                    "connection": {
                        "bootstrap": "foobar:9092",
                        "auto_register_schemas": False,
                    }
                }
            )
        )
    mock_producer.assert_not_called()
