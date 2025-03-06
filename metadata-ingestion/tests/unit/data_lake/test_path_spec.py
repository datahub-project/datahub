from typing import Optional

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec


@pytest.mark.parametrize(
    "include, s3_uri, expected",
    [
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/table/p1/test.csv",
            True,
        ),
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/table/p1/p2/test.csv",
            False,
        ),
    ],
)
def test_allowed_ignores_depth_mismatch(
    include: str, s3_uri: str, expected: bool
) -> None:
    # arrange
    path_spec = PathSpec(
        include=include,
        table_name="{table}",
    )

    # act, assert
    assert path_spec.allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri, expected",
    [
        ("s3://bucket/table-111/p1/test.csv", True),
        ("s3://bucket/table-222/p1/test.csv", False),
    ],
)
def test_allowed_tables_pattern_allow(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_pattern=AllowDenyPattern(allow=["t.*111"]),
    )

    # act, assert
    assert path_spec.allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri, expected",
    [
        ("s3://bucket/table-111/p1/", True),
        ("s3://bucket/table-222/p1/", False),
    ],
)
def test_dir_allowed_tables_pattern_allow(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_pattern=AllowDenyPattern(allow=["t.*111"]),
    )

    # act, assert
    assert path_spec.dir_allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "include, s3_uri, expected",
    [
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/table/p1/test.csv",
            "table",
        ),
        (
            "s3://bucket/data1/{partition0}/test.csv",
            "s3://bucket/data1/p1/test.csv",
            None,
        ),
    ],
)
def test_get_table_name(include: str, s3_uri: str, expected: Optional[str]) -> None:
    # arrange
    path_spec = PathSpec(
        include=include,
    )

    # act, assert
    assert path_spec._get_table_name(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri",
    [
        "s3://bucket/dir/",
        "s3://bucket/dir",
    ],
)
def test_get_table_name_raises_error_table_not_found(s3_uri: str) -> None:
    # arrange
    path_spec = PathSpec(include="s3://bucket/dir1/{table}/{partition0}/*.csv")

    # act, assert
    with pytest.raises(ValueError) as e:
        path_spec._get_table_name(s3_uri)
    assert str(e.value) == f"Table not found in path: {s3_uri}"
