from typing import Any, Dict, List, Set, Tuple, Union

import pytest

from datahub.ingestion.reporting.datahub_ingestion_run_summary_provider import (
    DatahubIngestionRunSummaryProvider,
    DatahubIngestionRunSummaryProviderConfig,
)
from datahub.ingestion.run.pipeline_config import PipelineConfig


@pytest.mark.parametrize(
    "pipeline_config, expected_key",
    [
        (
            {
                "source": {
                    "type": "snowflake",
                    "config": {"platform_instance": "my_instance"},
                },
                "sink": {"type": "console"},
                "pipeline_name": "urn:li:ingestionSource:12345",
            },
            {
                "type": "snowflake",
                "platform_instance": "my_instance",
                "pipeline_name": "urn:li:ingestionSource:12345",
            },
        ),
        (
            {"source": {"type": "snowflake"}, "sink": {"type": "console"}},
            {"type": "snowflake"},
        ),
        (
            {
                "source": {"type": "snowflake"},
                "sink": {"type": "console"},
                "pipeline_name": "foobar",
            },
            {"type": "snowflake", "pipeline_name": "foobar"},
        ),
    ],
    ids=["all_things", "minimal", "with_pipeline_name"],
)
def test_unique_key_gen(pipeline_config, expected_key):
    config = PipelineConfig.from_dict(pipeline_config)
    key = DatahubIngestionRunSummaryProvider.generate_unique_key(config)
    assert key == expected_key


def test_default_config():
    typed_config = DatahubIngestionRunSummaryProviderConfig.parse_obj({})
    assert typed_config.sink is None
    assert typed_config.report_recipe is True


def test_simple_set() -> None:
    """Test conversion of a simple set"""
    input_data: Set[int] = {1, 2, 3}
    expected: List[int] = [1, 2, 3]
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    assert sorted(result) == sorted(expected)
    assert isinstance(result, list)


def test_nested_dict_with_sets() -> None:
    """Test conversion of nested dictionary containing sets"""
    input_data: Dict[str, Union[Set[int], Dict[str, Set[str]]]] = {
        "set1": {1, 2, 3},
        "dict1": {"set2": {"a", "b"}},
    }
    expected = {
        "set1": [1, 2, 3],
        "dict1": {"set2": ["a", "b"]},
    }
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)

    def sort_nested_lists(d):
        return {
            k: (
                sorted(v)
                if isinstance(v, list)
                else (sort_nested_lists(v) if isinstance(v, dict) else v)
            )
            for k, v in d.items()
        }

    assert sort_nested_lists(result) == sort_nested_lists(expected)


def test_nested_lists_with_sets() -> None:
    """Test conversion of nested lists containing sets"""
    input_data = [{1, 2}, [{3, 4}, {5, 6}]]
    expected = [[1, 2], [[3, 4], [5, 6]]]
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    assert [
        sorted(x)
        if isinstance(x, list) and len(x) > 0 and not isinstance(x[0], list)
        else x
        for x in result
    ] == [
        sorted(x)
        if isinstance(x, list) and len(x) > 0 and not isinstance(x[0], list)
        else x
        for x in expected
    ]


def test_tuple_with_sets() -> None:
    """Test conversion of tuples containing sets"""
    input_data = (1, {2, 3}, 4)
    expected = (1, [2, 3], 4)
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    assert (result[0], sorted(result[1]), result[2]) == (
        expected[0],
        sorted(expected[1]),
        expected[2],
    )
    assert isinstance(result, tuple)


def test_mixed_nested_structure() -> None:
    """Test conversion of a complex nested structure"""
    input_data = {
        "simple_set": {1, 2, 3},
        "nested_dict": {
            "another_set": {"a", "b", "c"},
            "mixed_list": [1, {2, 3}, {"x", "y"}],
        },
        "tuple_with_set": (1, {4, 5}, 6),
        "list_of_sets": [{1, 2}, {3, 4}],
    }
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)

    # Verify structure types
    assert isinstance(result["simple_set"], list)
    assert isinstance(result["nested_dict"]["another_set"], list)
    assert isinstance(result["nested_dict"]["mixed_list"][1], list)
    assert isinstance(result["nested_dict"]["mixed_list"][2], list)
    assert isinstance(result["tuple_with_set"], tuple)
    assert isinstance(result["tuple_with_set"][1], list)
    assert isinstance(result["list_of_sets"][0], list)


def test_non_set_data() -> None:
    """Test that non-set data remains unchanged"""
    input_data = {
        "string": "hello",
        "int": 42,
        "float": 3.14,
        "bool": True,
        "none": None,
        "list": [1, 2, 3],
        "dict": {"a": 1, "b": 2},
    }
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    assert result == input_data


def test_empty_structures() -> None:
    """Test handling of empty structures"""
    input_data: Dict[
        str, Union[Set[Any], Dict[Any, Any], List[Any], Tuple[Any, ...]]
    ] = {"empty_set": set(), "empty_dict": {}, "empty_list": [], "empty_tuple": ()}
    expected: Dict[
        str, Union[List[Any], Dict[Any, Any], List[Any], Tuple[Any, ...]]
    ] = {"empty_set": [], "empty_dict": {}, "empty_list": [], "empty_tuple": ()}
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    assert result == expected


def test_json_serializable() -> None:
    """Test that the converted structure is JSON serializable"""
    import json

    input_data = {
        "set": {1, 2, 3},
        "nested": {"set": {"a", "b"}},
        "mixed": [1, {2, 3}, {"x"}],
    }
    result = DatahubIngestionRunSummaryProvider._convert_sets_to_lists(input_data)
    try:
        json.dumps(result)
        serializable = True
    except TypeError:
        serializable = False
    assert serializable
