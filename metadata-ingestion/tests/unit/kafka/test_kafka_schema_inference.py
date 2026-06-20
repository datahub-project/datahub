from unittest.mock import Mock, patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import SchemaResolutionFallback
from datahub.ingestion.source.kafka.kafka_schema_inference import (
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
    return SchemaResolutionFallback(
        enabled=True,
        sample_timeout_seconds=1.0,
        sample_strategy="hybrid",
    )


@pytest.fixture
def schema_inference(fallback_config):
    return KafkaSchemaInference(
        bootstrap_servers="localhost:9092",
        consumer_config={"group.id": "test"},
        fallback_config=fallback_config,
        max_workers=2,
    )


class TestKafkaSchemaInference:
    def test_initialization(self, schema_inference, fallback_config):
        assert schema_inference.bootstrap_servers == "localhost:9092"
        assert schema_inference.consumer_config == {"group.id": "test"}
        assert schema_inference.fallback_config == fallback_config

    def test_infer_schemas_batch_empty_topics(self, schema_inference):
        assert schema_inference.infer_schemas_batch([]) == {}

    def test_extract_fields_from_samples(self, schema_inference):
        sample_messages = [
            {"field1": "string_value", "field2": 123, "field3": True},
            {"field1": "another_string", "field2": 456, "field4": 78.9},
            {"field1": None, "field2": 789, "field3": False},
        ]
        result = schema_inference._extract_fields_from_samples(
            "test-topic", sample_messages
        )

        assert isinstance(result, list)
        assert all(isinstance(field, SchemaField) for field in result)
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths
        assert "field3" in field_paths
        assert "field4" in field_paths

    def test_extract_fields_from_samples_type_detection(self, schema_inference):
        result = schema_inference._extract_fields_from_samples(
            "test-topic",
            [
                {
                    "string_field": "test",
                    "int_field": 123,
                    "bool_field": True,
                    "float_field": 45.6,
                }
            ],
        )
        field_types = {field.fieldPath: field.type.type for field in result}

        assert isinstance(field_types["string_field"], StringTypeClass)
        assert isinstance(field_types["int_field"], NumberTypeClass)
        assert isinstance(field_types["bool_field"], BooleanTypeClass)
        assert isinstance(field_types["float_field"], NumberTypeClass)

    def test_infer_schema_from_messages_success(self, schema_inference):
        with patch.object(schema_inference, "_sample_topic_messages") as mock_sample:
            mock_sample.return_value = [
                {"field1": "value1", "field2": 123},
                {"field1": "value2", "field2": 456},
            ]
            result = schema_inference._infer_schema_from_messages("test-topic")

        assert isinstance(result, list)
        assert len(result) > 0
        assert all(isinstance(field, SchemaField) for field in result)
        field_paths = [field.fieldPath for field in result]
        assert "field1" in field_paths
        assert "field2" in field_paths

    def test_infer_schema_from_messages_no_samples(self, schema_inference):
        with patch.object(schema_inference, "_sample_topic_messages") as mock_sample:
            mock_sample.return_value = []
            result = schema_inference._infer_schema_from_messages("empty-topic")
        assert result == []

    def test_infer_schemas_batch_sequential(self, schema_inference):
        schema_inference.max_workers = 1

        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = [
                [Mock(spec=SchemaField)],
                [Mock(spec=SchemaField), Mock(spec=SchemaField)],
            ]
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        assert len(result) == 2
        assert len(result["topic1"]) == 1
        assert len(result["topic2"]) == 2
        assert mock_infer.call_count == 2

    def test_infer_schemas_batch_parallel(self, schema_inference):
        schema_inference.max_workers = 2

        with patch.object(schema_inference, "_infer_schemas_parallel") as mock_parallel:
            mock_parallel.return_value = {
                "topic1": [Mock(spec=SchemaField)],
                "topic2": [Mock(spec=SchemaField), Mock(spec=SchemaField)],
            }
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

        mock_parallel.assert_called_once_with(["topic1", "topic2"])
        assert len(result) == 2

    def test_sample_topic_messages_hybrid_strategy(self, schema_inference):
        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.side_effect = [[], [{"field": "value"}]]
            result = schema_inference._sample_topic_messages("test-topic")

        assert mock_sample.call_count == 2
        mock_sample.assert_any_call("test-topic", "latest")
        mock_sample.assert_any_call("test-topic", "earliest")
        assert result == [{"field": "value"}]

    def test_sample_topic_messages_latest_strategy(self, schema_inference):
        schema_inference.fallback_config.sample_strategy = "latest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]
            result = schema_inference._sample_topic_messages("test-topic")

        mock_sample.assert_called_once_with("test-topic", "latest")
        assert result == [{"field": "value"}]

    def test_sample_topic_messages_earliest_strategy(self, schema_inference):
        schema_inference.fallback_config.sample_strategy = "earliest"

        with patch.object(
            schema_inference, "_sample_messages_with_strategy"
        ) as mock_sample:
            mock_sample.return_value = [{"field": "value"}]
            result = schema_inference._sample_topic_messages("test-topic")

        mock_sample.assert_called_once_with("test-topic", "earliest")
        assert result == [{"field": "value"}]

    def test_flatten_for_schema_inference(self, schema_inference):
        nested_obj = {
            "field1": "string_value",
            "nested": {"field2": 123, "field3": True},
            "array": [1, 2, 3],
        }
        result = schema_inference._flatten_for_schema_inference(nested_obj)

        assert result["field1"] == "string_value"
        assert result["nested.field2"] == 123
        assert result["nested.field3"] is True
        assert result["array"] == [1, 2, 3]
        assert isinstance(result["nested.field2"], int)
        assert isinstance(result["nested.field3"], bool)
