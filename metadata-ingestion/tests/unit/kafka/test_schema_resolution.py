"""Unit tests for KafkaSchemaResolver."""

import json
from typing import cast
from unittest.mock import MagicMock, Mock, patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.ingestion.source.kafka.kafka_config import SchemaResolutionFallback
from datahub.ingestion.source.kafka.schema_resolution import (
    KafkaSchemaResolver,
    RecordNameExtractionResult,
    SchemaResolutionResult,
)
from datahub.metadata.schema_classes import SchemaFieldClass


class TestKafkaSchemaResolver:
    """Test cases for KafkaSchemaResolver."""

    @pytest.fixture
    def mock_source_config(self):
        """Create a mock source config."""
        config = MagicMock()
        config.schema_resolution = SchemaResolutionFallback(enabled=True)
        config.schemaless_fallback = SchemaResolutionFallback(enabled=False)
        config.topic_subject_map = {
            "test-topic-value": "com.example.TestRecord-value",
            "mapped-topic-key": "com.example.KeyRecord-key",
        }
        # Create nested connection mock
        config.connection = MagicMock()
        config.connection.bootstrap = "localhost:9092"
        config.connection.consumer_config = {}
        return config

    @pytest.fixture
    def mock_schema_registry_client(self):
        """Create a mock schema registry client."""
        client = MagicMock()
        return client

    @pytest.fixture
    def known_subjects(self):
        """List of known subjects in the schema registry."""
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
        """Create a KafkaSchemaResolver instance."""
        return KafkaSchemaResolver(
            source_config=mock_source_config,
            schema_registry_client=mock_schema_registry_client,
            known_subjects=known_subjects,
            max_workers=2,
        )

    def test_topic_name_strategy_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        """Test successful TopicNameStrategy resolution."""
        # Mock registered schema
        mock_schema = Schema(
            schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
        )
        mock_registered_schema = RegisteredSchema(
            schema_id="1",
            guid=None,
            schema=mock_schema,
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
        """Test TopicNameStrategy when subject not found."""
        result = schema_resolver._try_topic_name_strategy(
            "unknown-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "topic_name_failed"
        mock_schema_registry_client.get_latest_version.assert_not_called()

    def test_topic_subject_map_success(
        self, schema_resolver, mock_schema_registry_client
    ):
        """Test successful TopicSubjectMap resolution."""
        # Mock registered schema
        mock_schema = Schema(
            schema_str='{"type": "record", "name": "TestRecord"}', schema_type="AVRO"
        )
        mock_registered_schema = RegisteredSchema(
            schema_id="2",
            guid=None,
            schema=mock_schema,
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
        """Test TopicSubjectMap when mapping not configured."""
        result = schema_resolver._try_topic_subject_map(
            "unmapped-topic", is_key_schema=False
        )

        assert result.schema is None
        assert result.resolution_method == "subject_map_failed"
        mock_schema_registry_client.get_latest_version.assert_not_called()

    def test_extract_record_name_from_schema_avro(self, schema_resolver):
        """Test record name extraction from Avro schema."""
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
        """Test record name extraction from Avro schema without namespace."""
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
        """Test record name extraction with invalid JSON."""
        result = schema_resolver._extract_record_name_from_schema("invalid json")

        assert result.record_name is None

    @patch("datahub.ingestion.source.kafka.schema_resolution.KafkaSchemaInference")
    def test_record_name_strategies_success(
        self, mock_inference_class, schema_resolver, mock_schema_registry_client
    ):
        """Test successful RecordNameStrategy resolution."""
        # Mock schema inference
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
        schema_resolver.schema_inference = mock_inference

        # Mock message sampling - return actual message values, not message objects
        schema_registry_message_value = (
            b"\x00\x00\x00\x00\x01" + b"dummy_data"
        )  # Schema registry format
        mock_inference._sample_topic_messages.return_value = [
            schema_registry_message_value
        ]

        # Mock schema registry calls
        mock_schema = Schema(
            schema_str='{"type": "record", "name": "User", "namespace": "com.example"}',
            schema_type="AVRO",
        )
        mock_schema_registry_client.get_by_id.return_value = mock_schema

        mock_registered_schema = RegisteredSchema(
            schema_id="3",
            guid=None,
            schema=mock_schema,
            subject="com.example.User-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        result = schema_resolver._try_record_name_strategies(
            "user-topic", is_key_schema=False
        )

        assert result.schema is not None
        assert result.resolution_method == "record_name_strategy"
        assert result.subject_name == "com.example.User-value"
        assert result.record_name == "com.example.User"

    def test_record_name_strategies_no_inference(self, schema_resolver):
        """Test RecordNameStrategy when schema inference is not available."""
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
        """Test batch schema resolution."""
        # Mock schema inference
        mock_inference = MagicMock()
        mock_inference_class.return_value = mock_inference
        schema_resolver.schema_inference = mock_inference

        # Mock successful TopicNameStrategy for first topic
        mock_schema = Schema(
            schema_str='{"type": "record", "name": "Test"}', schema_type="AVRO"
        )
        mock_registered_schema = RegisteredSchema(
            schema_id="1",
            guid=None,
            schema=mock_schema,
            subject="test-topic-value",
            version=1,
        )
        mock_schema_registry_client.get_latest_version.return_value = (
            mock_registered_schema
        )

        # Mock schema inference for second topic
        mock_inference.infer_schemas_batch.return_value = {
            "unknown-topic": [MagicMock()]  # Mock inferred fields
        }

        topics = ["test-topic", "unknown-topic"]
        results = schema_resolver.resolve_schemas_batch(topics, is_key_schema=False)

        assert len(results) == 2
        assert results["test-topic"].resolution_method == "topic_name_strategy"
        assert results["unknown-topic"].resolution_method == "schema_inference"

    def test_resolve_schemas_batch_empty_topics(self, schema_resolver):
        """Test batch schema resolution with empty topic list."""
        results = schema_resolver.resolve_schemas_batch([], is_key_schema=False)
        assert results == {}

    def test_extract_record_name_from_message_schema_registry_format(
        self, schema_resolver, mock_schema_registry_client
    ):
        """Test record name extraction from schema registry formatted message."""
        # Message with magic byte and schema ID
        message_value = b"\x00\x00\x00\x00\x01" + b"dummy_avro_data"

        # Mock schema retrieval
        mock_schema = Schema(
            schema_str='{"type": "record", "name": "Order", "namespace": "com.example"}',
            schema_type="AVRO",
        )
        mock_schema_registry_client.get_by_id.return_value = mock_schema

        result = schema_resolver._extract_record_name_from_message(message_value)

        assert result.record_name == "Order"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.Order"
        mock_schema_registry_client.get_by_id.assert_called_once_with(1)

    def test_extract_record_name_from_message_not_schema_registry(
        self, schema_resolver
    ):
        """Test record name extraction from non-schema registry message."""
        message_value = b"plain_json_data"

        result = schema_resolver._extract_record_name_from_message(message_value)

        assert result.record_name is None

    def test_extract_record_name_from_message_too_short(self, schema_resolver):
        """Test record name extraction from message that's too short."""
        message_value = b"123"  # Less than 5 bytes

        result = schema_resolver._extract_record_name_from_message(message_value)

        assert result.record_name is None


class TestSchemaResolutionResult:
    """Test cases for SchemaResolutionResult dataclass."""

    def test_schema_resolution_result_creation(self):
        """Test creating a SchemaResolutionResult."""
        mock_schema = Mock()
        # Create proper mock SchemaFieldClass objects
        mock_field1 = Mock(spec=SchemaFieldClass)
        mock_field2 = Mock(spec=SchemaFieldClass)
        # Cast to satisfy mypy
        mock_fields = cast(list[SchemaFieldClass], [mock_field1, mock_field2])

        result = SchemaResolutionResult(
            schema=mock_schema,
            fields=mock_fields,
            resolution_method="topic_name_strategy",
            subject_name="test-subject",
            record_name="TestRecord",
        )

        assert result.schema == mock_schema
        assert result.fields == mock_fields
        assert result.resolution_method == "topic_name_strategy"
        assert result.subject_name == "test-subject"
        assert result.record_name == "TestRecord"

    def test_schema_resolution_result_defaults(self):
        """Test SchemaResolutionResult with default values."""
        result = SchemaResolutionResult(
            schema=None,
            fields=[],
            resolution_method="none",
        )

        assert result.schema is None
        assert result.fields == []
        assert result.resolution_method == "none"
        assert result.subject_name is None
        assert result.record_name is None


class TestRecordNameExtractionResult:
    """Test cases for RecordNameExtractionResult dataclass."""

    def test_record_name_extraction_result_creation(self):
        """Test creating a RecordNameExtractionResult."""
        result = RecordNameExtractionResult(
            record_name="User",
            namespace="com.example",
            full_name="com.example.User",
        )

        assert result.record_name == "User"
        assert result.namespace == "com.example"
        assert result.full_name == "com.example.User"

    def test_record_name_extraction_result_defaults(self):
        """Test RecordNameExtractionResult with default values."""
        result = RecordNameExtractionResult(record_name="SimpleRecord")

        assert result.record_name == "SimpleRecord"
        assert result.namespace is None
        assert result.full_name is None
