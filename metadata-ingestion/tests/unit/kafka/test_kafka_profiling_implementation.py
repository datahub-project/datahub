"""Tests for Kafka profiling implementation focusing on actual functionality."""

from typing import List, Union
from unittest.mock import patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import (
    KafkaSourceConfig,
    ProfilerConfig,
)
from datahub.ingestion.source.kafka.kafka_profiler import KafkaProfiler, flatten_json
from datahub.ingestion.source_config.operation_config import (
    OperationConfig,
    is_profiling_enabled,
)
from datahub.metadata.schema_classes import (
    DatasetProfileClass,
    KafkaSchemaClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


@pytest.fixture
def sample_kafka_data():
    """Sample Kafka message data for testing."""
    return [
        {"id": 1, "name": "Alice", "score": 85.5, "active": True},
        {"id": 2, "name": "Bob", "score": 92.0, "active": False},
        {"id": 3, "name": "Charlie", "score": 78.3, "active": True},
        {"id": 4, "name": None, "score": 88.7, "active": True},
        {"id": 5, "name": "Eve", "score": 0, "active": False},
    ]


@pytest.fixture
def sample_schema_metadata():
    """Sample schema metadata for testing."""
    return SchemaMetadataClass(
        schemaName="test_topic",
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=KafkaSchemaClass(documentSchema="{}"),
        fields=[
            SchemaFieldClass(
                fieldPath="id",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="int",
            ),
            SchemaFieldClass(
                fieldPath="name",
                type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                nativeDataType="string",
            ),
        ],
    )


@pytest.fixture(autouse=True)
def clear_operation_config_cache():
    """Clear operation config cache before each test."""
    # The is_profiling_enabled function is cached, clear its cache
    if hasattr(is_profiling_enabled, "cache_clear"):
        is_profiling_enabled.cache_clear()
    yield
    if hasattr(is_profiling_enabled, "cache_clear"):
        is_profiling_enabled.cache_clear()


class TestKafkaProfilingImplementation:
    """Test actual Kafka profiling implementation functionality."""

    @patch("datahub.ingestion.source.kafka.kafka_config.is_profiling_enabled")
    def test_operation_config_integration(self, mock_is_profiling_enabled):
        """Test that operation_config is properly integrated."""
        # Test that KafkaSourceConfig uses is_profiling_enabled correctly
        config_disabled = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=False),
        )
        assert not config_disabled.is_profiling_enabled()

        # Test that enabled config calls is_profiling_enabled
        mock_is_profiling_enabled.return_value = True
        config_enabled = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(
                enabled=True,
                operation_config=OperationConfig(lower_freq_profile_enabled=False),
            ),
        )
        assert config_enabled.is_profiling_enabled()
        mock_is_profiling_enabled.assert_called_once_with(
            config_enabled.profiling.operation_config
        )

    def test_implemented_ge_profiling_flags(self):
        """Test GE profiling flags that are actually implemented."""
        config = ProfilerConfig(
            enabled=True,
            # Test implemented flags
            include_field_null_count=True,
            include_field_distinct_count=True,
            include_field_min_value=True,
            include_field_max_value=True,
            include_field_mean_value=True,
            include_field_median_value=True,
            include_field_stddev_value=True,
            include_field_quantiles=True,
            include_field_distinct_value_frequencies=True,
            include_field_histogram=True,
            include_field_sample_values=True,
            turn_off_expensive_profiling_metrics=False,
            field_sample_values_limit=10,
            max_number_of_fields_to_profile=5,
        )

        profiler = KafkaProfiler(config)

        # Test that the flags are properly read
        assert profiler.profiler_config.include_field_null_count
        assert profiler.profiler_config.include_field_distinct_count
        assert profiler.profiler_config.field_sample_values_limit == 10
        assert profiler.profiler_config.max_number_of_fields_to_profile == 5
        assert not profiler._expensive_profiling_disabled

    def test_expensive_profiling_limits_implementation(self):
        """Test that expensive profiling limits are actually implemented."""
        config = ProfilerConfig(
            enabled=True,
            turn_off_expensive_profiling_metrics=True,
            max_number_of_fields_to_profile=2,
        )

        profiler = KafkaProfiler(config)
        assert profiler._expensive_profiling_disabled

        # Test that field processing respects limits
        # This tests the actual implementation in _process_field_statistics
        field_values: List[Union[str, int, float, bool, None]] = [1, 2, 3, 4, 5]

        # First field should be processed normally
        stats1 = profiler._process_field_statistics("field1", field_values)
        assert stats1.field_path == "field1"

        # Second field should be processed normally
        stats2 = profiler._process_field_statistics("field2", field_values)
        assert stats2.field_path == "field2"

        # Third field should have minimal processing due to limit
        stats3 = profiler._process_field_statistics("field3", field_values)
        assert stats3.field_path == "field3"
        # Should have empty sample_values due to field limit
        assert len(stats3.sample_values) == 0

    def test_static_profile_topic_method(
        self, sample_kafka_data, sample_schema_metadata
    ):
        """Test the static profile_topic method that's actually used."""
        config = ProfilerConfig(enabled=True)

        result = KafkaProfiler.profile_topic(
            "test_topic", sample_kafka_data, sample_schema_metadata, config
        )

        assert result is not None
        assert isinstance(result, DatasetProfileClass)
        assert result.partitionSpec is not None
        assert "SAMPLE" in result.partitionSpec.partition
        assert "5 messages" in result.partitionSpec.partition

    def test_flatten_json_recursion_protection(self):
        """Test flatten_json recursion protection that's actually implemented."""
        # Test deeply nested structure
        from typing import Any, Dict

        nested_data: Dict[str, Any] = {
            "level1": {"level2": {"level3": {"level4": {"level5": "deep_value"}}}}
        }

        # Test with max_depth=3
        flattened = flatten_json(nested_data, max_depth=3)

        # Should flatten up to level 3, then truncate
        assert "level1.level2.level3" in flattened
        # Should not go deeper than max_depth
        assert "level1.level2.level3.level4" not in flattened

        # Test circular reference protection
        circular_data: Dict[str, Any] = {"a": {}}
        circular_data["a"]["b"] = circular_data["a"]  # Create circular reference

        # Should not crash and should handle circular reference
        result = flatten_json(circular_data, max_depth=5)
        assert isinstance(result, dict)

    def test_kafka_specific_config_fields(self):
        """Test Kafka-specific configuration fields that are actually used."""
        config = ProfilerConfig(
            enabled=True,
            max_sample_time_seconds=30,
            sampling_strategy="random",
            nested_field_max_depth=3,
            max_workers=4,
            sample_size=500,
        )

        # Test that Kafka-specific fields are properly set
        assert config.max_sample_time_seconds == 30
        assert config.sampling_strategy == "random"
        assert config.nested_field_max_depth == 3
        assert config.max_workers == 4
        assert config.sample_size == 500  # Kafka default override

    def test_field_statistics_with_config_flags(self):
        """Test that field statistics respect configuration flags."""
        config = ProfilerConfig(
            enabled=True,
            include_field_null_count=True,
            include_field_distinct_count=True,
            include_field_sample_values=True,
            field_sample_values_limit=3,
        )

        profiler = KafkaProfiler(config)

        # Test with mixed data including nulls
        values: List[Union[str, int, float, bool, None]] = [1, 2, 3, None, 2, 4]
        stats = profiler._process_field_statistics("test_field", values)

        # Should calculate null count
        assert stats.null_count == 1
        assert stats.null_proportion == 1 / 6

        # Should have sample values limited by config
        assert len(stats.sample_values) <= 3

    def test_unimplemented_ge_features_not_tested(self):
        """
        Document GE features that are NOT implemented in Kafka profiler.

        These features are inherited from GEProfilingConfig but not actually used:
        - profile_if_updated_since_days (SQL-specific)
        - profile_table_size_limit (SQL-specific)
        - profile_table_row_limit (SQL-specific)
        - profile_table_row_count_estimate_only (SQL-specific)
        - query_combiner_enabled (SQL-specific)
        - partition_profiling_enabled (SQL-specific)
        - partition_datetime (SQL-specific)
        - use_sampling (SQL-specific, Kafka has its own sampling)
        - profile_external_tables (SQL-specific)
        - profile_nested_fields (might be useful for Kafka but not implemented)
        - tags_to_ignore_sampling (inherited but not used in Kafka context)
        """
        config = ProfilerConfig(
            enabled=True,
            # These are inherited but not implemented in Kafka
            profile_if_updated_since_days=7.0,
            profile_table_size_limit=10,
            profile_table_row_limit=1000000,
            query_combiner_enabled=False,
            partition_profiling_enabled=False,
            use_sampling=False,  # Kafka has its own sampling mechanism
            profile_nested_fields=True,  # Could be useful but not implemented
        )

        # These should be set but won't affect Kafka profiling behavior
        assert config.profile_if_updated_since_days == 7.0
        assert config.profile_table_size_limit == 10
        assert not config.query_combiner_enabled

        # This is just documenting what's not implemented
        # We don't test these because they don't do anything in Kafka context
        pass
