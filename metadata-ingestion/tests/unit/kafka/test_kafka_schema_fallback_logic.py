from unittest.mock import Mock, patch

from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    SchemaResolutionFallback,
)
from datahub.ingestion.source.kafka.kafka_schema_inference import KafkaSchemaInference
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaField


class TestKafkaSchemaFallbackIntegration:
    def test_schema_inference_initialization_when_enabled(self):
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schema_resolution": {"enabled": True},
            }
        )
        fallback_config = config.schema_resolution
        assert fallback_config.enabled is True

        schema_inference = KafkaSchemaInference(
            bootstrap_servers=config.connection.bootstrap,
            consumer_config=config.connection.consumer_config,
            fallback_config=fallback_config,
        )
        assert schema_inference.bootstrap_servers == "localhost:9092"
        assert schema_inference.fallback_config.enabled is True

    def test_schema_inference_not_initialized_when_disabled(self):
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schema_resolution": {"enabled": False},
            }
        )
        assert config.schema_resolution.enabled is False

    def test_schema_inference_batch_processing(self):
        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=SchemaResolutionFallback(enabled=True),
            max_workers=2,
        )

        with patch.object(schema_inference, "_infer_schemas_parallel") as mock_parallel:
            mock_parallel.return_value = {
                "topic1": [Mock(spec=SchemaField)],
                "topic2": [Mock(spec=SchemaField)],
            }
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

            mock_parallel.assert_called_once_with(["topic1", "topic2"])
            assert len(result) == 2

    def test_schema_inference_sequential_processing(self):
        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=SchemaResolutionFallback(enabled=True),
            max_workers=1,
        )

        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = [
                [Mock(spec=SchemaField)],
                [Mock(spec=SchemaField)],
            ]
            result = schema_inference.infer_schemas_batch(["topic1", "topic2"])

            assert mock_infer.call_count == 2
            assert len(result) == 2

    def test_schema_inference_error_handling(self):
        schema_inference = KafkaSchemaInference(
            bootstrap_servers="localhost:9092",
            consumer_config={"group.id": "test"},
            fallback_config=SchemaResolutionFallback(enabled=True),
            max_workers=1,
        )

        with patch.object(
            schema_inference, "_infer_schema_from_messages"
        ) as mock_infer:
            mock_infer.side_effect = Exception("Schema inference failed")
            result = schema_inference.infer_schemas_batch(["failing-topic"])
            assert result == {"failing-topic": []}

    def test_configuration_combinations(self):
        config1 = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schema_resolution": {"enabled": True, "sample_strategy": "hybrid"},
                "profiling": {"enabled": True, "max_workers": 8},
            }
        )
        assert config1.schema_resolution.enabled is True
        assert config1.profiling.enabled is True
        assert config1.profiling.max_workers == 8

        config2 = KafkaSourceConfig.parse_obj(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
                "schema_resolution": {"enabled": False},
                "profiling": {"enabled": False},
            }
        )
        assert config2.schema_resolution.enabled is False
        assert config2.profiling.enabled is False
