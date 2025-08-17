"""Clean unit tests for Kafka schema inference functionality."""

from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import SchemalessFallback
from datahub.ingestion.source.kafka.kafka_schema_inference import (
    FieldAnalysis,
    KafkaSchemaInference,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
)


@pytest.fixture
def fallback_config():
    """Create a test fallback configuration."""
    return SchemalessFallback(
        enabled=True,
        sample_timeout_seconds=1.0,
        sample_strategy="hybrid",
    )


@pytest.fixture
def schema_inference(fallback_config):
    """Create a KafkaSchemaInference instance for testing."""
    return KafkaSchemaInference(
        bootstrap_servers="localhost:9092",
        consumer_config={"group.id": "test"},
        fallback_config=fallback_config,
        max_workers=2,  # Test with 2 workers
    )


class TestFieldAnalysis:
    """Test the FieldAnalysis dataclass."""

    def test_field_analysis_initialization(self):
        """Test that FieldAnalysis initializes correctly."""
        analysis = FieldAnalysis()
        assert analysis.types == set()
        assert analysis.sample_values == []

    def test_field_analysis_add_types_and_values(self):
        """Test adding types and sample values."""
        analysis = FieldAnalysis()
        analysis.types.add("string")
        analysis.types.add("long")
        analysis.sample_values.extend(["test", "123"])

        assert "string" in analysis.types
        assert "long" in analysis.types
        assert "test" in analysis.sample_values
        assert "123" in analysis.sample_values


class TestKafkaSchemaInference:
    """Test the KafkaSchemaInference class."""

    def test_initialization(self, schema_inference, fallback_config):
        """Test that KafkaSchemaInference initializes correctly."""
        assert schema_inference.bootstrap_servers == "localhost:9092"
        assert schema_inference.consumer_config == {"group.id": "test"}
        assert schema_inference.fallback_config == fallback_config

    def test_infer_schemas_batch_empty_topics(self, schema_inference):
        """Test that empty topic list returns empty results."""
        result = schema_inference.infer_schemas_batch([])
        assert result == {}

    def test_extract_fields_from_samples(self, schema_inference):
        """Test extracting schema fields from sample messages."""
        sample_messages = [
            {"field1": "string_value", "field2": 123, "field3": True},
            {"field1": "another_string", "field2": 456, "field4": 78.9},
            {"field1": None, "field2": 789, "field3": False},
        ]

        result = schema_inference._extract_fields_from_samples(
            "test-topic", sample_messages
        )

        # Verify we get SchemaField objects
        assert isinstance(result, list)
        assert all(isinstance(field, SchemaField) for field in result)

        # Check that we have fields for the expected field names
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths
        assert "field3" in field_paths
        assert "field4" in field_paths

    def test_extract_fields_from_samples_type_detection(self, schema_inference):
        """Test that type detection works correctly."""
        sample_messages = [
            {
                "string_field": "test",
                "int_field": 123,
                "bool_field": True,
                "float_field": 45.6,
            }
        ]

        result = schema_inference._extract_fields_from_samples(
            "test-topic", sample_messages
        )

        # Create a mapping of field paths to types for easier testing
        field_types = {field.fieldPath: field.type.type for field in result}

        # Verify type detection (note: int becomes "long" in the type detection logic)
        assert isinstance(field_types["string_field"], StringTypeClass)
        assert isinstance(
            field_types["int_field"], NumberTypeClass
        )  # int -> long -> NumberTypeClass
        assert isinstance(field_types["bool_field"], BooleanTypeClass)
        assert isinstance(
            field_types["float_field"], NumberTypeClass
        )  # float -> double -> NumberTypeClass

    def test_infer_schema_from_messages_success(self, schema_inference):
        """Test successful schema inference from messages."""
        # Mock the _sample_topic_messages method to return test data
        with patch.object(schema_inference, "_sample_topic_messages") as mock_sample:
            mock_sample.return_value = [
                {"field1": "value1", "field2": 123},
                {"field1": "value2", "field2": 456},
            ]

            result = schema_inference._infer_schema_from_messages("test-topic")

        # Verify we get SchemaField objects
        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(field, SchemaField) for field in result)

        # Verify we have the expected fields
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths

    def test_infer_schema_from_messages_no_samples(self, schema_inference):
        """Test schema inference when no samples are available."""
        with patch.object(schema_inference, "_sample_topic_messages") as mock_sample:
            mock_sample.return_value = []

            result = schema_inference._infer_schema_from_messages("empty-topic")

        # Should return empty list when no samples
        assert result == []

    def test_infer_schemas_batch_sequential(self, schema_inference):
        """Test batch schema inference in sequential mode."""
        # Set max_workers to 1 to force sequential processing
        schema_inference.max_workers = 1

        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = [
                [Mock(spec=SchemaField)],  # topic1 result
                [Mock(spec=SchemaField), Mock(spec=SchemaField)],  # topic2 result
            ]

            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        # Verify both topics were processed
        assert len(result) == 2
        assert "topic1" in result
        assert "topic2" in result
        assert len(result["topic1"]) == 1
        assert len(result["topic2"]) == 2

        # Verify sequential calls
        assert mock_infer.call_count == 2

    def test_infer_schemas_batch_parallel(self, schema_inference):
        """Test batch schema inference in parallel mode."""
        # Set max_workers > 1 to enable parallel processing
        schema_inference.max_workers = 2

        with patch.object(schema_inference, "_infer_schemas_parallel") as mock_parallel:
            mock_parallel.return_value = {
                "topic1": [Mock(spec=SchemaField)],
                "topic2": [Mock(spec=SchemaField), Mock(spec=SchemaField)],
            }

            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        # Verify parallel processing was used
        mock_parallel.assert_called_once_with(["topic1", "topic2"])

        # Verify results
        assert len(result) == 2
        assert "topic1" in result
        assert "topic2" in result

    def test_sample_topic_messages_hybrid_strategy(self, schema_inference):
        """Test hybrid sampling strategy (latest first, then earliest)."""
        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            # Mock latest returning empty, earliest returning data
            mock_sample.side_effect = [[], [{"field": "value"}]]

            result = schema_inference._sample_topic_messages("test-topic")

        # Verify both strategies were tried
        assert mock_sample.call_count == 2
        mock_sample.assert_any_call("test-topic", "latest")
        mock_sample.assert_any_call("test-topic", "earliest")

        # Verify result from earliest strategy
        assert result == [{"field": "value"}]

    def test_sample_topic_messages_latest_strategy(self, schema_inference):
        """Test latest-only sampling strategy."""
        schema_inference.fallback_config.sample_strategy = "latest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]

            result = schema_inference._sample_topic_messages("test-topic")

        # Verify only latest was tried
        mock_sample.assert_called_once_with("test-topic", "latest")
        assert result == [{"field": "value"}]

    def test_sample_topic_messages_earliest_strategy(self, schema_inference):
        """Test earliest-only sampling strategy."""
        schema_inference.fallback_config.sample_strategy = "earliest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]

            result = schema_inference._sample_topic_messages("test-topic")

        # Verify only earliest was tried
        mock_sample.assert_called_once_with("test-topic", "earliest")
        assert result == [{"field": "value"}]

    def test_flatten_for_schema_inference(self, schema_inference):
        """Test the type-preserving flattening function."""
        nested_obj = {
            "field1": "string_value",
            "nested": {"field2": 123, "field3": True},
            "array": [1, 2, 3],
        }

        result = schema_inference._flatten_for_schema_inference(nested_obj)

        # Verify flattening preserves types
        assert result["field1"] == "string_value"
        assert result["nested.field2"] == 123
        assert result["nested.field3"] is True
        assert result["array"] == [1, 2, 3]

        # Verify types are preserved (not converted to strings)
        assert isinstance(result["field1"], str)
        assert isinstance(result["nested.field2"], int)
        assert isinstance(result["nested.field3"], bool)
        assert isinstance(result["array"], list)
