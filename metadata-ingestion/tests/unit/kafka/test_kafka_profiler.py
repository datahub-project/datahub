from typing import Any, Dict, List, Union, cast
from unittest.mock import patch

import pytest

from datahub.ingestion.source.kafka.kafka_config import ProfilerConfig
from datahub.ingestion.source.kafka.kafka_profiler import (
    KafkaProfiler,
    clean_field_path,
    is_overflow_value,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    DatasetProfileClass,
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
    path = "user.profile.name[type=string][version=1]"
    cleaned = clean_field_path(path)
    assert path != cleaned
    assert "name" in cleaned

    assert clean_field_path("id[key=True]") == "id"


def test_is_overflow_value():
    assert not is_overflow_value(42)
    assert not is_overflow_value("test")
    assert not is_overflow_value(True)

    # Legitimate sentinel-looking values must NOT be treated as special anymore.
    assert not is_overflow_value(-1)
    assert not is_overflow_value(2147483647)

    # Only values that would overflow float64 aggregation are excluded.
    assert is_overflow_value(9.223372036854776e18)


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
    # 10 evenly spaced values over [0, 9] into 5 buckets.
    histogram = profiler._create_histogram([float(i) for i in range(10)], max_buckets=5)

    assert histogram is not None
    # N buckets => N+1 boundaries and N heights.
    assert len(histogram.boundaries) == 6
    assert len(histogram.heights) == 5
    # Boundaries span the value range and increase monotonically.
    assert [float(b) for b in histogram.boundaries] == [0.0, 1.8, 3.6, 5.4, 7.2, 9.0]
    # Heights are normalized frequencies that sum to 1.0.
    assert sum(histogram.heights) == pytest.approx(1.0)


def test_histogram_generation_degenerate_inputs(profiler):
    # Too few values or a zero-width range yields no histogram.
    assert profiler._create_histogram([5.0]) is None
    assert profiler._create_histogram([5.0, 5.0, 5.0]) is None


def test_histogram_independent_of_distinct_count(sample_data, schema_metadata):
    # Histograms must render even when distinct-count is disabled, since both used to
    # share the same frequency map.
    config = ProfilerConfig(
        include_field_histogram=True,
        include_field_distinct_count=False,
    )
    profiler = KafkaProfiler(config)

    result = profiler.generate_dataset_profile(
        cast(List[Dict[str, Any]], sample_data), schema_metadata
    )

    assert result.fieldProfiles is not None
    score = next(p for p in result.fieldProfiles if "score" in (p.fieldPath or ""))
    assert score.histogram is not None
    assert score.uniqueCount is None


def test_profile_samples(profiler, sample_data, schema_metadata):
    result = profiler.generate_dataset_profile(
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
    # max_depth=1 must truncate the nesting into a profile rather than recursing
    # forever or crashing.
    result = KafkaProfiler.profile_topic(
        "nested_topic", [deeply_nested_sample], None, config
    )
    assert isinstance(result, DatasetProfileClass)
    assert result.fieldProfiles


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
    from datahub.ingestion.source.kafka.kafka_utils import flatten_json

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

    field_values: dict[str, List[Any]] = {
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
