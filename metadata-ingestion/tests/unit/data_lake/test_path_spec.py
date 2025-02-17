from typing import List, Optional

import pytest

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
    "s3_uri, allowed_tables, expected",
    [
        ("s3://bucket/table-1/p1/test.csv", None, True),
        ("s3://bucket/table-1/p1/test.csv", [], True),
        ("s3://bucket/table-1/p1/test.csv", ["table-1"], True),
        ("s3://bucket/table-2/p1/test.csv", ["table-1"], False),
    ],
)
def test_allowed_allowed_tables(
    s3_uri: str, allowed_tables: List[str], expected: bool
) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        allowed_tables=allowed_tables,
    )

    # act, assert
    assert path_spec.allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "s3_uri, allowed_tables, expected",
    [
        ("s3://bucket/table-1/p1/", None, True),
        ("s3://bucket/table-1/p1/", [], True),
        ("s3://bucket/table-1/p1/", ["table-1"], True),
        ("s3://bucket/table-2/p1/", ["table-1"], False),
    ],
)
def test_dir_allowed_allowed_tables(
    s3_uri: str, allowed_tables: List[str], expected: bool
) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        allowed_tables=allowed_tables,
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


@pytest.mark.parametrize(
    "table_name, allowed_tables, expected",
    [
        ("table", ["table"], True),
        ("table", ["table-123"], False),
        ("table", [], True),
        ("table", None, True),
    ],
)
def test_is_allowed_tables(
    table_name: str, allowed_tables: str, expected: bool
) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        allowed_tables=allowed_tables,
    )

    # act, assert
    assert path_spec.is_allowed_table(table_name) == expected
