import json
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.ingestion.source.kafka.kafka_config import SchemaResolutionFallback
from datahub.ingestion.source.kafka.schema_resolution import KafkaSchemaResolver


class TestKafkaSchemaResolver:
    @pytest.fixture
    def mock_source_config(self):
        config = MagicMock()
        config.schema_resolution = SchemaResolutionFallback(enabled=False)
        config.topic_subject_map = {
            "test-topic-value": "com.example.TestRecord-value",
            "mapped-topic-key": "com.example.KeyRecord-key",
        }
        config.connection = MagicMock()
        config.connection.bootstrap = "localhost:9092"
        config.connection.consumer_config = {}
        return config

    @pytest.fixture
    def mock_schema_registry_client(self):
        return MagicMock()

    @pytest.fixture
    def known_subjects(self):
        return [
            "test-topic-value",
            "test-topic-key",
            "user-topic-value",
            "com.example.TestRecord-value",
            "com.example.User-value",
            "order-topic-com.example.Order-value",
        ]

    @pytest.fixture
    def schema_resolver(
        self, mock_source_config, mock_schema_registry_client, known_subjects
    ):
        return KafkaSchemaResolver(
            source_config=mock_source_config,
            schema_registry_client=mock_schema_registry_client,
            known_subjects=known_subjects,
            max_workers=2,
        )

    def test_topic_name_strategy_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_registered_schema = RegisteredSchema(
            schema_id="1",
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
            ),
            subject="test-topic-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        result = schema_resolver._try_topic_name_strategy(
            "test-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "topic_name_strategy"
        assert result.subject_name == "test-topic-value"
        mock_schema_registry_client.get_latest_version.assert_called_once_with(
            "test-topic-value"
        )

    def test_topic_name_strategy_not_found(
        self, schema_resolver, mock_schema_registry_client
    ):
        result = schema_resolver._try_topic_name_strategy(
            "unknown-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "topic_name_failed"
        mock_schema_registry_client.get_latest_version.assert_not_called()

    def test_topic_subject_map_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_registered_schema = RegisteredSchema(
            schema_id="2",
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "TestRecord"}',
                schema_type="AVRO",
            ),
            subject="com.example.TestRecord-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        result = schema_resolver._try_topic_subject_map(
            "test-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "topic_subject_map"
        assert result.subject_name == "com.example.TestRecord-value"
        mock_schema_registry_client.get_latest_version.assert_called_once_with(
            "com.example.TestRecord-value"
        )

    def test_topic_subject_map_not_configured(
        self, schema_resolver, mock_schema_registry_client
    ):
        result = schema_resolver._try_topic_subject_map(
            "unmapped-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "subject_map_failed"
        mock_schema_registry_client.get_latest_version.assert_not_called()

    def test_extract_record_name_from_schema_avro(self, schema_resolver):
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "User",
                "namespace": "com.example",
                "fields": [{"name": "id", "type": "int"}],
            }
        )
        result = schema_resolver._extract_record_name_from_schema(schema_str)

        assert result.record_name == "User"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.User"

    def test_extract_record_name_from_schema_no_namespace(self, schema_resolver):
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "SimpleRecord",
                "fields": [{"name": "value", "type": "string"}],
            }
        )
        result = schema_resolver._extract_record_name_from_schema(schema_str)

        assert result.record_name == "SimpleRecord"
        assert result.namespace is None
        assert result.full_name == "SimpleRecord"

    def test_extract_record_name_from_schema_invalid_json(self, schema_resolver):
        result = schema_resolver._extract_record_name_from_schema("invalid json")
        assert result.record_name is None

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_record_name_strategies_success(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
        schema_resolver.schema_inference = mock_inference

        mock_inference._sample_topic_messages.return_value = [
            b"\x00\x00\x00\x00\x01" + b"dummy_data"
        ]

        mock_schema = Schema(
            schema_str='{"type": "record", "name": "User", "namespace": "com.example"}',
            schema_type="AVRO",
        )
        mock_schema_registry_client.get_by_id.return_value = mock_schema
        mock_schema_registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id="3",
            guid=None,
            schema=mock_schema,
            subject="com.example.User-value",
            version=1,
        )

        result = schema_resolver._try_record_name_strategies(
            "user-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "record_name_strategy"
        assert result.subject_name == "com.example.User-value"
        assert result.record_name == "com.example.User"

    def test_record_name_strategies_no_inference(self, schema_resolver):
        schema_resolver.schema_inference = None

        result = schema_resolver._try_record_name_strategies(
            "test-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "no_inference_available"

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_resolve_schemas_batch(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
        schema_resolver.schema_inference = mock_inference

        mock_schema_registry_client.get_latest_version.return_value = RegisteredSchema(
            schema_id="1",
            guid=None,
            schema=Schema(
                schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
            ),
            subject="test-topic-value",
            version=1,
        )
        mock_inference.infer_schemas_batch.return_value = {
            "unknown-topic": [MagicMock()]
        }

        results = schema_resolver.resolve_schemas_batch(
            ["test-topic", "unknown-topic"], is_key_schema=False
        )

        assert len(results) == 2
        assert results["test-topic"].resolution_method == "topic_name_strategy"
        assert results["unknown-topic"].resolution_method == "schema_inference"

    def test_resolve_schemas_batch_empty_topics(self, schema_resolver):
        results = schema_resolver.resolve_schemas_batch([], is_key_schema=False)
        assert results == {}

    def test_extract_record_name_from_message_schema_registry_format(
        self, schema_resolver, mock_schema_registry_client
    ):
        mock_schema_registry_client.get_by_id.return_value = Schema(
            schema_str='{"type": "record", "name": "Order", "namespace": "com.example"}',
            schema_type="AVRO",
        )

        result = schema_resolver._extract_record_name_from_message(
            b"\x00\x00\x00\x00\x01" + b"dummy_avro_data"
        )

        assert result.record_name == "Order"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.Order"
        mock_schema_registry_client.get_by_id.assert_called_once_with(1)

    def test_extract_record_name_from_message_not_schema_registry(
        self, schema_resolver
    ):
        result = schema_resolver._extract_record_name_from_message(b"plain_json_data")
        assert result.record_name is None

    def test_extract_record_name_from_message_too_short(self, schema_resolver):
        result = schema_resolver._extract_record_name_from_message(b"123")
        assert result.record_name is None
