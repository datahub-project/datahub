from typing import Any, Dict, List, Union, cast
from unittest.mock import patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import ProfilerConfig
from datahub.ingestion.source.kafka.kafka_profiler import (
    KafkaFieldStatistics,
    KafkaProfiler,
    clean_field_path,
    is_special_value,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DatasetProfileClass,
    HistogramClass,
    KafkaSchemaClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)


@pytest.fixture
def basic_config():
    return ProfilerConfig()


@pytest.fixture
def profiler(basic_config):
    return KafkaProfiler(basic_config)


@pytest.fixture
def sample_data():
    return [
        {
            "id": 1,
            "name": "John",
            "active": True,
            "score": 85.5,
            "tags": ["user", "premium"],
        },
        {"id": 2, "name": "Jane", "active": True, "score": 92.0, "tags": ["user"]},
        {
            "id": 3,
            "name": "Bob",
            "active": False,
            "score": 78.3,
            "tags": ["user", "trial"],
        },
        {
            "id": 4,
            "name": "Alice",
            "active": True,
            "score": 88.7,
            "tags": ["user", "premium"],
        },
        {"id": 5, "name": None, "active": False, "score": 0, "tags": []},
    ]


@pytest.fixture
def schema_metadata():
    kafka_schema = KafkaSchemaClass(
        documentSchema='{"type":"record","name":"SampleRecord","fields":[]}'
    )

    return SchemaMetadataClass(
        schemaName="sample_topic",
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=kafka_schema,
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
            SchemaFieldClass(
                fieldPath="active",
                type=SchemaFieldDataTypeClass(type=BooleanTypeClass()),
                nativeDataType="boolean",
            ),
            SchemaFieldClass(
                fieldPath="score",
                type=SchemaFieldDataTypeClass(type=NumberTypeClass()),
                nativeDataType="float",
            ),
            SchemaFieldClass(
                fieldPath="tags",
                type=SchemaFieldDataTypeClass(type=ArrayTypeClass()),
                nativeDataType="array",
            ),
        ],
    )


def test_clean_field_path():
    assert (
        clean_field_path(
            "user.profile.name[type=string][version=1]", preserve_types=True
        )
        == "user.profile.name[type=string][version=1]"
    )

    path = "user.profile.name[type=string][version=1]"
    cleaned = clean_field_path(path, preserve_types=False)
    assert path != cleaned
    assert "name" in cleaned

    path = "id[key=True]"
    assert clean_field_path(path, preserve_types=False) == "id"


def test_is_special_value():
    assert not is_special_value(42)
    assert not is_special_value("test")
    assert not is_special_value(True)

    assert is_special_value(-1)
    assert is_special_value(2147483647)
    assert is_special_value(9.223372036854776e18)


def test_process_field_statistics(profiler, sample_data):
    score_values = [sample["score"] for sample in sample_data if "score" in sample]
    stats = profiler._process_field_statistics("score", score_values)

    assert stats.field_path == "score"
    assert (stats.data_type or "").upper() in ["NUMERIC", "NUMBER", "FLOAT"]
    assert stats.null_count == 0
    assert stats.min_value == 0
    assert stats.max_value == 92.0
    assert stats.mean_value is not None
    assert stats.median_value is not None

    name_values = [sample.get("name") for sample in sample_data if "name" in sample]
    stats = profiler._process_field_statistics("name", name_values)

    assert stats.field_path == "name"
    assert (stats.data_type or "").upper() in ["STRING", "TEXT", "VARCHAR"]
    assert stats.null_count == 1
    assert stats.null_proportion == 0.2

    active_values = [sample["active"] for sample in sample_data if "active" in sample]
    stats = profiler._process_field_statistics("active", active_values)
    assert stats.field_path == "active"
    assert stats.null_count == 0


def test_histogram_generation(profiler):
    profiler.profiler_config.include_field_histogram = True

    with patch.object(profiler, "_create_histogram") as mock_create_histogram:
        mock_create_histogram.return_value = HistogramClass(
            boundaries=["10.0", "30.0", "50.0", "70.0", "90.0"],
            heights=[0.2, 0.2, 0.2, 0.2, 0.2],
        )

        test_stats = KafkaFieldStatistics(
            field_path="test_numeric_field",
            sample_values=["10.0", "20.0", "30.0"],
            data_type="NUMERIC",
            distinct_value_frequencies={str(i * 10): 1 for i in range(1, 10)},
        )

        profiler.create_profile_data({"test_numeric_field": test_stats}, 9)
        mock_create_histogram.assert_called_once()


def test_profile_samples(profiler, sample_data, schema_metadata):
    result = profiler.profile_samples(
        cast(List[Dict[str, Any]], sample_data), schema_metadata
    )
    assert isinstance(result, DatasetProfileClass)
    assert result.fieldProfiles is not None


def test_value_frequencies(profiler, sample_data):
    active_values = [sample["active"] for sample in sample_data if "active" in sample]
    stats = profiler._process_field_statistics("active", active_values)

    assert stats.distinct_value_frequencies is not None
    assert len(stats.distinct_value_frequencies) == 2
    assert stats.distinct_value_frequencies.get("True") == 3
    assert stats.distinct_value_frequencies.get("False") == 2


@patch("random.sample")
def test_sample_values(mock_random_sample, profiler):
    mock_random_sample.return_value = [0, 2, 4]
    values: List[Union[str, int, float, bool, None]] = [
        "value1",
        "value2",
        "value3",
        "value4",
        "value5",
    ]
    samples = profiler._get_sample_values(values, max_samples=3)

    assert len(samples) == 3
    assert samples[0] == "value1"
    assert samples[1] == "value3"
    assert samples[2] == "value5"


def test_custom_profiler_config():
    custom_config = ProfilerConfig(
        sample_size=200,
        max_sample_time_seconds=120,
        sampling_strategy="random",
    )
    profiler = KafkaProfiler(custom_config)

    assert profiler.profiler_config.sample_size == 200
    assert profiler.profiler_config.max_sample_time_seconds == 120
    assert profiler.profiler_config.sampling_strategy == "random"


def test_profile_topic_static_method(sample_data, schema_metadata):
    config = ProfilerConfig(enabled=True)

    result = KafkaProfiler.profile_topic(
        "test_topic", sample_data, schema_metadata, config
    )

    assert result is not None
    assert isinstance(result, DatasetProfileClass)
    assert result.partitionSpec is not None
    assert "SAMPLE" in result.partitionSpec.partition
    assert str(len(sample_data)) in result.partitionSpec.partition

    assert (
        KafkaProfiler.profile_topic("empty_topic", [], schema_metadata, config) is None
    )


def test_profile_topic_recursion_error_handling():
    config = ProfilerConfig(enabled=True, nested_field_max_depth=1)
    deeply_nested_sample: Dict[str, Any] = {
        "level1": {"level2": {"level3": {"level4": "value"}}}
    }
    result = KafkaProfiler.profile_topic(
        "nested_topic", [deeply_nested_sample], None, config
    )
    assert isinstance(result, (DatasetProfileClass, type(None)))


def test_ge_profiling_config_inheritance():
    config = ProfilerConfig(
        enabled=True,
        turn_off_expensive_profiling_metrics=True,
        field_sample_values_limit=10,
        max_number_of_fields_to_profile=5,
        profile_nested_fields=True,
    )
    profiler = KafkaProfiler(config)

    assert profiler._expensive_profiling_disabled
    assert profiler.profiler_config.field_sample_values_limit == 10
    assert profiler.profiler_config.max_number_of_fields_to_profile == 5
    assert profiler.profiler_config.profile_nested_fields


def test_flatten_json_max_depth():
    from datahub.ingestion.source.kafka.kafka_profiler import flatten_json

    nested_data: Dict[str, Any] = {
        "level1": {"level2": {"level3": {"level4": "deep_value"}}}
    }
    flattened = flatten_json(nested_data, max_depth=2)

    assert "level1.level2" in flattened
    for key in flattened:
        assert key.count(".") <= 1


def test_expensive_profiling_limits():
    config = ProfilerConfig(
        enabled=True,
        turn_off_expensive_profiling_metrics=True,
        max_number_of_fields_to_profile=2,
    )
    profiler = KafkaProfiler(config)

    field_values: dict[str, List[Union[str, int, float, bool, None]]] = {
        "field1": [1, 2, 3],
        "field2": ["a", "b", "c"],
        "field3": [True, False, True],
        "field4": [1.1, 2.2, 3.3],
    }
    processed_fields = [
        profiler._process_field_statistics(name, vals)
        for name, vals in field_values.items()
    ]

    assert len(processed_fields) > 0
    assert len(processed_fields[-1].sample_values) == 0
