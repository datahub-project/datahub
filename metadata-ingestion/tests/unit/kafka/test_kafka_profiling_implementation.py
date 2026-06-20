from typing import Any, Dict, List, Union
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
    return [
        {"id": 1, "name": "Alice", "score": 85.5, "active": True},
        {"id": 2, "name": "Bob", "score": 92.0, "active": False},
        {"id": 3, "name": "Charlie", "score": 78.3, "active": True},
        {"id": 4, "name": None, "score": 88.7, "active": True},
        {"id": 5, "name": "Eve", "score": 0, "active": False},
    ]


@pytest.fixture
def sample_schema_metadata():
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
    if hasattr(is_profiling_enabled, "cache_clear"):
        is_profiling_enabled.cache_clear()
    yield
    if hasattr(is_profiling_enabled, "cache_clear"):
        is_profiling_enabled.cache_clear()


class TestKafkaProfilingImplementation:
    @patch("datahub.ingestion.source.kafka.kafka_config.is_profiling_enabled")
    def test_operation_config_integration(self, mock_is_profiling_enabled):
        config_disabled = KafkaSourceConfig(
            connection={"bootstrap": "localhost:9092"},
            profiling=ProfilerConfig(enabled=False),
        )
        assert not config_disabled.is_profiling_enabled()

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

    def test_expensive_profiling_limits(self):
        config = ProfilerConfig(
            enabled=True,
            turn_off_expensive_profiling_metrics=True,
            max_number_of_fields_to_profile=2,
        )
        profiler = KafkaProfiler(config)
        assert profiler._expensive_profiling_disabled

        field_values: List[Union[str, int, float, bool, None]] = [1, 2, 3, 4, 5]
        stats1 = profiler._process_field_statistics("field1", field_values)
        assert stats1.field_path == "field1"
        stats2 = profiler._process_field_statistics("field2", field_values)
        assert stats2.field_path == "field2"

        # Third field exceeds the limit of 2 fields, so gets minimal processing
        stats3 = profiler._process_field_statistics("field3", field_values)
        assert stats3.field_path == "field3"
        assert len(stats3.sample_values) == 0

    def test_static_profile_topic_method(
        self, sample_kafka_data, sample_schema_metadata
    ):
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
        nested_data: Dict[str, Any] = {
            "level1": {"level2": {"level3": {"level4": {"level5": "deep_value"}}}}
        }
        flattened = flatten_json(nested_data, max_depth=3)

        assert "level1.level2.level3" in flattened
        assert "level1.level2.level3.level4" not in flattened

        circular_data: Dict[str, Any] = {"a": {}}
        circular_data["a"]["b"] = circular_data["a"]
        result = flatten_json(circular_data, max_depth=5)
        assert isinstance(result, dict)

    def test_kafka_specific_config_fields(self):
        config = ProfilerConfig(
            enabled=True,
            max_sample_time_seconds=30,
            sampling_strategy="random",
            nested_field_max_depth=3,
            max_workers=4,
            sample_size=500,
        )
        assert config.max_sample_time_seconds == 30
        assert config.sampling_strategy == "random"
        assert config.nested_field_max_depth == 3
        assert config.max_workers == 4
        assert config.sample_size == 500

    def test_field_statistics_with_config_flags(self):
        config = ProfilerConfig(
            enabled=True,
            include_field_null_count=True,
            include_field_distinct_count=True,
            include_field_sample_values=True,
            field_sample_values_limit=3,
        )
        profiler = KafkaProfiler(config)

        values: List[Union[str, int, float, bool, None]] = [1, 2, 3, None, 2, 4]
        stats = profiler._process_field_statistics("test_field", values)

        assert stats.null_count == 1
        assert stats.null_proportion == 1 / 6
        assert len(stats.sample_values) <= 3
