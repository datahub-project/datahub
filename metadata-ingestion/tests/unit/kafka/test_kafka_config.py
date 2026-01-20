"""Tests for Kafka configuration, especially profiling config changes."""

import os

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    ProfilerConfig,
    SchemaResolutionFallback,
)
from datahub.ingestion.source_config.operation_config import OperationConfig


class TestKafkaProfilingConfig:
    """Test Kafka profiling configuration changes."""

    def test_profiler_config_inherits_from_ge_profiling_config(self):
        """Test that ProfilerConfig properly inherits from GEProfilingConfig."""
        config = ProfilerConfig()

        # Should be instance of GEProfilingConfig
        assert isinstance(config, GEProfilingConfig)

        # Should have all GE profiling fields
        assert hasattr(config, "enabled")
        assert hasattr(config, "operation_config")
        assert hasattr(config, "turn_off_expensive_profiling_metrics")
        assert hasattr(config, "field_sample_values_limit")
        assert hasattr(config, "max_number_of_fields_to_profile")
        assert hasattr(config, "report_dropped_profiles")
        assert hasattr(config, "catch_exceptions")
        assert hasattr(config, "tags_to_ignore_sampling")
        assert hasattr(config, "profile_nested_fields")

        # Should have Kafka-specific fields
        assert hasattr(config, "max_sample_time_seconds")
        assert hasattr(config, "sampling_strategy")
        # Should have inherited global nested field depth control
        assert hasattr(config, "nested_field_max_depth")

    def test_profiler_config_kafka_specific_defaults(self):
        """Test Kafka-specific default values."""
        config = ProfilerConfig()

        # Kafka-specific defaults
        assert config.sample_size == 1000  # Override from base class
        assert config.max_workers == 5 * (
            os.cpu_count() or 4
        )  # Override from base class
        assert config.max_sample_time_seconds == 60
        assert config.sampling_strategy == "latest"
        assert config.batch_size == 100
        # Test that global nested_field_max_depth is inherited
        assert (
            config.nested_field_max_depth == 10
        )  # Global default from GEProfilingConfig

    def test_profiler_config_inherited_defaults(self):
        """Test that inherited GE defaults work correctly."""
        config = ProfilerConfig()

        # Inherited GE defaults
        assert not config.enabled
        assert not config.turn_off_expensive_profiling_metrics
        assert config.field_sample_values_limit == 20
        assert config.max_number_of_fields_to_profile is None
        assert not config.report_dropped_profiles
        assert config.catch_exceptions
        assert config.tags_to_ignore_sampling is None
        assert not config.profile_nested_fields

    def test_profiler_config_operation_config_inheritance(self):
        """Test that operation_config is properly inherited."""
        config = ProfilerConfig()

        # Should have operation_config
        assert hasattr(config, "operation_config")

        # Should have operation_config fields
        op_config = config.operation_config
        assert hasattr(op_config, "lower_freq_profile_enabled")
        assert hasattr(op_config, "profile_day_of_week")
        assert hasattr(op_config, "profile_date_of_month")

    def test_kafka_source_config_is_profiling_enabled(self):
        """Test is_profiling_enabled method respects operation_config."""
        # Test with profiling disabled
        config = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=False),
        )
        assert not config.is_profiling_enabled()

        # Test with profiling enabled but no operation_config restrictions
        config = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=True),
        )
        assert config.is_profiling_enabled()

    def test_basic_operation_config_inheritance(self):
        """Test that operation_config is properly inherited and accessible."""
        config = ProfilerConfig(
            enabled=True,
            operation_config=OperationConfig(
                lower_freq_profile_enabled=True,
                profile_day_of_week=1,
                profile_date_of_month=15,
            ),
        )

        # Should have operation_config fields accessible
        assert config.operation_config.lower_freq_profile_enabled
        assert config.operation_config.profile_day_of_week == 1
        assert config.operation_config.profile_date_of_month == 15

    def test_profiler_config_validation(self):
        """Test configuration validation."""
        # Valid config
        config = ProfilerConfig(
            enabled=True,
            sample_size=500,
            max_sample_time_seconds=30,
            sampling_strategy="random",
        )
        assert config.enabled
        assert config.sample_size == 500
        assert config.max_sample_time_seconds == 30
        assert config.sampling_strategy == "random"

        # Invalid max_sample_time_seconds (must be positive)
        with pytest.raises(ValidationError):
            ProfilerConfig(max_sample_time_seconds=-10)

    def test_profiler_config_custom_values(self):
        """Test setting custom values for inherited fields."""
        config = ProfilerConfig(
            enabled=True,
            turn_off_expensive_profiling_metrics=True,
            field_sample_values_limit=50,
            max_number_of_fields_to_profile=100,
            report_dropped_profiles=True,
            profile_nested_fields=True,
            tags_to_ignore_sampling=["sensitive", "pii"],
        )

        assert config.enabled
        assert config.turn_off_expensive_profiling_metrics
        assert config.field_sample_values_limit == 50
        assert config.max_number_of_fields_to_profile == 100
        assert config.report_dropped_profiles
        assert config.profile_nested_fields
        assert config.tags_to_ignore_sampling == ["sensitive", "pii"]


class TestSchemaResolutionFallbackConfig:
    """Test SchemaResolutionFallback configuration."""

    def test_schema_resolution_defaults(self):
        """Test default values for SchemaResolutionFallback."""
        config = SchemaResolutionFallback()

        assert config.enabled is False  # Default to disabled (opt-in)
        assert config.sample_timeout_seconds == 2.0
        assert config.sample_strategy == "hybrid"
        assert config.max_messages_per_topic == 10

    def test_schema_resolution_custom_values(self):
        """Test custom values for SchemaResolutionFallback."""
        config = SchemaResolutionFallback(
            enabled=True,
            sample_timeout_seconds=5.0,
            sample_strategy="latest",
            max_messages_per_topic=20,
        )

        assert config.enabled is True
        assert config.sample_timeout_seconds == 5.0
        assert config.sample_strategy == "latest"
        assert config.max_messages_per_topic == 20

    def test_schema_resolution_validation(self):
        """Test validation of SchemaResolutionFallback fields."""
        # Valid strategies
        for strategy in ["earliest", "latest", "hybrid"]:
            config = SchemaResolutionFallback(sample_strategy=strategy)
            assert config.sample_strategy == strategy

        # Positive values
        config = SchemaResolutionFallback(sample_timeout_seconds=0.1)
        assert config.sample_timeout_seconds == 0.1

    def test_kafka_source_config_includes_schema_resolution(self):
        """Test that KafkaSourceConfig includes schema_resolution field."""
        # Create minimal config
        config_dict = {
            "connection": {
                "bootstrap": "localhost:9092",
                "schema_registry_url": "http://localhost:8081",
            }
        }

        config = KafkaSourceConfig.parse_obj(config_dict)

        # Should have schema_resolution field
        assert hasattr(config, "schema_resolution")
        assert isinstance(config.schema_resolution, SchemaResolutionFallback)

        # Should be disabled by default
        assert config.schema_resolution.enabled is False

    def test_kafka_source_config_with_custom_schema_resolution(self):
        """Test KafkaSourceConfig with custom schema_resolution settings."""
        config_dict = {
            "connection": {
                "bootstrap": "localhost:9092",
                "schema_registry_url": "http://localhost:8081",
            },
            "schema_resolution": {
                "enabled": True,
                "sample_timeout_seconds": 3.0,
                "sample_strategy": "earliest",
            },
        }

        config = KafkaSourceConfig.parse_obj(config_dict)

        assert config.schema_resolution.enabled is True
        assert config.schema_resolution.sample_timeout_seconds == 3.0
        assert config.schema_resolution.sample_strategy == "earliest"

    def test_profiler_config_default_profiling_values(self):
        """Test that profiling config has sensible defaults."""
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "profiling": {
                    "enabled": True,
                },
            }
        )

        # Check default profiling config values
        profiling = config.profiling

        assert profiling.enabled is True
        assert profiling.sample_size > 0
        assert profiling.sampling_strategy in ["latest", "random", "stratified", "full"]
        assert profiling.max_workers >= 1
