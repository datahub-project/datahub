"""Tests for Kafka configuration, especially profiling config changes."""

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.ge_profiling_config import GEProfilingConfig
from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    ProfilerConfig,
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
        assert hasattr(config, "flatten_max_depth")

    def test_profiler_config_kafka_specific_defaults(self):
        """Test Kafka-specific default values."""
        config = ProfilerConfig()

        # Kafka-specific defaults
        assert config.sample_size == 1000  # Override from base class
        assert config.max_workers == 1  # Override from base class
        assert config.max_sample_time_seconds == 60
        assert config.sampling_strategy == "latest"
        assert config.cache_sample_results
        assert config.cache_ttl_seconds == 3600
        assert config.batch_size == 100
        assert config.flatten_max_depth == 5

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
