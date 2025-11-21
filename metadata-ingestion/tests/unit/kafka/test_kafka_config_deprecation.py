from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka.kafka import KafkaSource
from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    SchemaResolutionFallback,
)


def test_schemaless_fallback_deprecation_warning_when_only_old_config_enabled():
    """Test that deprecation warning is logged when only schemaless_fallback is enabled."""
    config = KafkaSourceConfig(
        schemaless_fallback=SchemaResolutionFallback(enabled=True),
        schema_resolution=SchemaResolutionFallback(enabled=False),
    )

    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None

    # Mock the Kafka consumer and admin client to avoid actual connection
    with (
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
        patch.object(KafkaSource, "init_kafka_admin_client"),
        patch.object(KafkaSource, "create_schema_registry"),
        patch("datahub.ingestion.source.kafka.kafka.KafkaSchemaInference"),
        patch("datahub.ingestion.source.kafka.kafka.logger") as mock_logger,
    ):
        # Initialize source (which should trigger the warning)
        KafkaSource(config=config, ctx=ctx)

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        warning_message = mock_logger.warning.call_args[0][0]
        assert "schemaless_fallback" in warning_message
        assert "deprecated" in warning_message.lower()
        assert "schema_resolution" in warning_message


def test_no_deprecation_warning_when_both_configs_enabled():
    """Test that no warning is logged when both configs are enabled (schema_resolution takes precedence)."""
    config = KafkaSourceConfig(
        schemaless_fallback=SchemaResolutionFallback(enabled=True),
        schema_resolution=SchemaResolutionFallback(enabled=True),
    )

    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None

    # Mock the Kafka consumer and admin client to avoid actual connection
    with (
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
        patch.object(KafkaSource, "init_kafka_admin_client"),
        patch.object(KafkaSource, "create_schema_registry"),
        patch("datahub.ingestion.source.kafka.kafka.KafkaSchemaInference"),
        patch("datahub.ingestion.source.kafka.kafka.logger") as mock_logger,
    ):
        # Initialize source (which should NOT trigger the warning)
        KafkaSource(config=config, ctx=ctx)

        # Verify warning was NOT logged
        mock_logger.warning.assert_not_called()


def test_no_deprecation_warning_when_both_configs_disabled():
    """Test that no warning is logged when both configs are disabled."""
    config = KafkaSourceConfig(
        schemaless_fallback=SchemaResolutionFallback(enabled=False),
        schema_resolution=SchemaResolutionFallback(enabled=False),
    )

    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None

    # Mock the Kafka consumer and admin client to avoid actual connection
    with (
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
        patch.object(KafkaSource, "init_kafka_admin_client"),
        patch.object(KafkaSource, "create_schema_registry"),
        patch("datahub.ingestion.source.kafka.kafka.logger") as mock_logger,
    ):
        # Initialize source (which should NOT trigger the warning)
        KafkaSource(config=config, ctx=ctx)

        # Verify warning was NOT logged
        mock_logger.warning.assert_not_called()


def test_no_deprecation_warning_when_only_new_config_enabled():
    """Test that no warning is logged when only schema_resolution is enabled."""
    config = KafkaSourceConfig(
        schemaless_fallback=SchemaResolutionFallback(enabled=False),
        schema_resolution=SchemaResolutionFallback(enabled=True),
    )

    ctx = MagicMock(spec=PipelineContext)
    ctx.graph = None

    # Mock the Kafka consumer and admin client to avoid actual connection
    with (
        patch("datahub.ingestion.source.kafka.kafka.get_kafka_consumer"),
        patch.object(KafkaSource, "init_kafka_admin_client"),
        patch.object(KafkaSource, "create_schema_registry"),
        patch("datahub.ingestion.source.kafka.kafka.logger") as mock_logger,
    ):
        # Initialize source (which should NOT trigger the warning)
        KafkaSource(config=config, ctx=ctx)

        # Verify warning was NOT logged
        mock_logger.warning.assert_not_called()
