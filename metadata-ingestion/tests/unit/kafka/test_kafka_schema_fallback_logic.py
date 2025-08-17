"""Integration tests for Kafka schema registry and schemaless fallback functionality."""

from typing import Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    SchemalessFallback,
)
from datahub.ingestion.source.kafka.kafka_schema_inference import KafkaSchemaInference
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class TestKafkaSchemaFallbackIntegration:
    """Test integration between schema registry and schemaless fallback."""

    def test_schema_inference_initialization_when_enabled(self):
        """Test that KafkaSchemaInference is initialized when fallback is enabled."""
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schemaless_fallback": {"enabled": True},
            }
        )

        # The KafkaSource would initialize schema inference
        fallback_config = config.schemaless_fallback
        assert fallback_config.enabled is True

        # Test that we can create a KafkaSchemaInference instance
        schema_inference = KafkaSchemaInference(
            bootstrap_servers=config.connection.bootstrap,
            consumer_config=config.connection.consumer_config,
            fallback_config=fallback_config,
        )

        assert schema_inference.bootstrap_servers == "localhost:9092"
        assert schema_inference.fallback_config.enabled is True

    def test_schema_inference_not_initialized_when_disabled(self):
        """Test that KafkaSchemaInference is not initialized when fallback is disabled."""
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schemaless_fallback": {"enabled": False},
            }
        )

        # The KafkaSource would NOT initialize schema inference
        fallback_config = config.schemaless_fallback
        assert fallback_config.enabled is False

    def test_topics_needing_fallback_identification(self):
        """Test logic for identifying topics that need schema inference fallback."""
        # Simulate schema registry results
        topic_value_schemas = {
            "topic-with-schema": (Mock(), [Mock()]),  # Has schema
            "topic-without-schema": (None, []),  # No schema
            "topic-with-fields-only": (None, [Mock()]),  # Has inferred fields
        }

        # Test the logic for identifying topics needing fallback
        topics_needing_fallback = [
            topic
            for topic in topic_value_schemas
            if topic_value_schemas.get(topic, (None, []))[0] is None
            and not topic_value_schemas.get(topic, (None, []))[1]
        ]

        # Only topic-without-schema should need fallback
        assert topics_needing_fallback == ["topic-without-schema"]

    def test_profiling_schema_info_check(self):
        """Test logic for determining if profiling should run based on schema availability."""
        test_cases = [
            # (value_schema, value_fields, key_schema, key_fields, expected_has_schema_info)
            (Mock(), [Mock()], None, [], True),  # Value schema available
            (None, [], Mock(), [Mock()], True),  # Key schema available
            (None, [Mock()], None, [], True),  # Value fields available (inferred)
            (None, [], None, [Mock()], True),  # Key fields available (inferred)
            (Mock(), [], Mock(), [], True),  # Both schemas available
            (None, [], None, [], False),  # No schema info at all
        ]

        for value_schema, value_fields, key_schema, key_fields, expected in test_cases:
            has_schema_info = (
                value_schema is not None
                or key_schema is not None
                or bool(value_fields)  # Convert list to boolean
                or bool(key_fields)  # Convert list to boolean
            )
            assert has_schema_info == expected, (
                f"Failed for case: {(value_schema, value_fields, key_schema, key_fields)}"
            )

    def test_schema_inference_batch_processing(self):
        """Test that schema inference processes topics in batch correctly."""
        fallback_config = SchemalessFallback(enabled=True)

        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=2,
        )

        # Mock the internal methods
        with patch.object(schema_inference, "_infer_schemas_parallel") as mock_parallel:
            mock_parallel.return_value = {
                "topic1": [Mock(spec=SchemaField)],
                "topic2": [Mock(spec=SchemaField)],
            }

            # Test parallel processing (max_workers > 1, multiple topics)
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

            mock_parallel.assert_called_once_with(["topic1", "topic2"])
            assert len(result) == 2
            assert "topic1" in result
            assert "topic2" in result

    def test_schema_inference_sequential_processing(self):
        """Test that schema inference falls back to sequential processing when appropriate."""
        fallback_config = SchemalessFallback(enabled=True)

        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,  # Force sequential
        )

        # Mock the internal methods
        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = [
                [Mock(spec=SchemaField)],  # topic1 result
                [Mock(spec=SchemaField)],  # topic2 result
            ]

            # Test sequential processing (max_workers = 1)
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

            assert mock_infer.call_count == 2
            assert len(result) == 2

    def test_schema_inference_error_handling(self):
        """Test that schema inference handles errors gracefully."""
        fallback_config = SchemalessFallback(enabled=True)

        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=fallback_config,
            max_workers=1,
        )

        # Mock the internal method to raise an exception
        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = Exception("Schema inference failed")

            # Should not raise exception, should return empty list for failed topic
            result = schema_inference.infer_schemas_batch(["failing-topic"])

            assert result == {"failing-topic": []}

    def test_configuration_combinations(self):
        """Test various configuration combinations work correctly."""
        # Test 1: Fallback enabled, profiling enabled
        config1 = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schemaless_fallback": {
                    "enabled": True,
                    "sample_strategy": "hybrid",
                },
                "profiling": {"enabled": True, "max_workers": 8},
            }
        )

        assert config1.schemaless_fallback.enabled is True
        assert config1.profiling.enabled is True
        assert config1.profiling.max_workers == 8

        # Test 2: Fallback disabled, profiling enabled
        config2 = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schemaless_fallback": {"enabled": False},
                "profiling": {"enabled": True},
            }
        )

        assert config2.schemaless_fallback.enabled is False
        assert config2.profiling.enabled is True

        # Test 3: Both disabled
        config3 = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schemaless_fallback": {"enabled": False},
                "profiling": {"enabled": False},
            }
        )

        assert config3.schemaless_fallback.enabled is False
        assert config3.profiling.enabled is False

    def test_fallback_results_integration_with_profiling_logic(self):
        """Test that fallback results are properly integrated with profiling decision logic."""
        # Simulate the workflow from get_workunits_internal

        # Initial schema registry results (no schema for test-topic)
        topic_value_schemas: Dict[str, Tuple[Optional[Mock], List[Mock]]] = {
            "test-topic": (None, [])
        }

        # Simulate fallback processing
        fallback_results = {"test-topic": [Mock(spec=SchemaField)]}

        # Update results with fallback schemas (as done in get_workunits_internal)
        for topic, inferred_fields in fallback_results.items():
            topic_value_schemas[topic] = (None, inferred_fields)

        # Now check if profiling should run
        topic = "test-topic"
        value_schema, value_fields = topic_value_schemas.get(topic, (None, []))
        key_schema: Optional[Mock] = None
        key_fields: List[Mock] = []  # Simulate no key schema

        has_schema_info = (
            value_schema is not None
            or key_schema is not None
            or bool(value_fields)  # Convert list to boolean
            or bool(key_fields)  # Convert list to boolean
        )

        # Should have schema info after fallback processing
        assert has_schema_info is True
        assert len(value_fields) > 0  # Should have inferred fields
