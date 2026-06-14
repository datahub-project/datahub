import os

import pytest
from pydantic import ValidationError

from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    ProfilerConfig,
    SchemaResolutionFallback,
)
from datahub.ingestion.source_config.operation_config import OperationConfig


class TestKafkaProfilingConfig:
    def test_profiler_config_kafka_specific_defaults(self):
        config = ProfilerConfig()

        assert config.sample_size == 200
        assert config.max_workers == 5 * (os.cpu_count() or 4)
        assert config.max_sample_time_seconds == 60
        assert config.sampling_strategy == "latest"
        assert config.batch_size == 100
        assert config.nested_field_max_depth == 10

    def test_kafka_source_config_is_profiling_enabled(self):
        config_off = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=False),
        )
        assert not config_off.is_profiling_enabled()

        config_on = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=True),
        )
        assert config_on.is_profiling_enabled()

        # operation_config gates profiling even when enabled=True
        config_gated = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(
                enabled=True,
                operation_config=OperationConfig(
                    lower_freq_profile_enabled=False,
                    profile_day_of_week=1,
                ),
            ),
        )
        # Whether this is enabled depends on the current day, but the method should not raise
        assert isinstance(config_gated.is_profiling_enabled(), bool)


class TestSchemaResolutionFallbackConfig:
    def test_schema_resolution_defaults(self):
        config = SchemaResolutionFallback()

        assert config.enabled is False
        assert config.sample_timeout_seconds == 2.0
        assert config.sample_strategy == "hybrid"
        assert config.max_messages_per_topic == 10

    def test_kafka_source_config_with_custom_schema_resolution(self):
        config = KafkaSourceConfig.parse_obj(
            {
                "connection": {"bootstrap": "localhost:9092"},
                "schema_resolution": {
                    "enabled": True,
                    "sample_timeout_seconds": 3.0,
                    "sample_strategy": "earliest",
                },
            }
        )

        assert config.schema_resolution.enabled is True
        assert config.schema_resolution.sample_timeout_seconds == 3.0
        assert config.schema_resolution.sample_strategy == "earliest"


class TestConfigurationValidationEdgeCases:
    def test_schema_resolution_invalid_strategy(self):
        with pytest.raises(ValidationError):
            SchemaResolutionFallback(sample_strategy="lates")

    def test_schema_resolution_negative_timeout(self):
        with pytest.raises(ValidationError):
            SchemaResolutionFallback(sample_timeout_seconds=-1.0)

    def test_schema_resolution_zero_timeout(self):
        with pytest.raises(ValidationError):
            SchemaResolutionFallback(sample_timeout_seconds=0.0)

    def test_schema_resolution_zero_max_messages(self):
        with pytest.raises(ValidationError):
            SchemaResolutionFallback(max_messages_per_topic=0)

    def test_schema_resolution_negative_max_messages(self):
        with pytest.raises(ValidationError):
            SchemaResolutionFallback(max_messages_per_topic=-10)

    def test_profiler_config_invalid_sampling_strategy(self):
        with pytest.raises(ValidationError):
            ProfilerConfig(sampling_strategy="invalid")

    def test_profiler_config_negative_batch_size(self):
        with pytest.raises(ValidationError):
            ProfilerConfig(batch_size=-100)

    def test_profiler_config_zero_batch_size(self):
        with pytest.raises(ValidationError):
            ProfilerConfig(batch_size=0)

    def test_profiler_config_negative_sample_time(self):
        with pytest.raises(ValidationError):
            ProfilerConfig(max_sample_time_seconds=-5)

    def test_profiler_config_zero_sample_time(self):
        with pytest.raises(ValidationError):
            ProfilerConfig(max_sample_time_seconds=0)

    def test_all_valid_profiling_strategies(self):
        for strategy in ["latest", "random", "stratified", "full"]:
            config = ProfilerConfig(sampling_strategy=strategy)
            assert config.sampling_strategy == strategy

    def test_very_small_positive_values_accepted(self):
        config = SchemaResolutionFallback(
            sample_timeout_seconds=0.001,
            max_messages_per_topic=1,
        )
        assert config.sample_timeout_seconds == 0.001
        assert config.max_messages_per_topic == 1

    def test_very_large_values_accepted(self):
        config = ProfilerConfig(
            batch_size=10000,
            max_sample_time_seconds=3600,
        )
        assert config.batch_size == 10000
        assert config.max_sample_time_seconds == 3600
