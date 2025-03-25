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
def test_allowed_tables_filter_pattern(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_filter_pattern=AllowDenyPattern(allow=["t.*111"]),
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
def test_dir_allowed_tables_filter_pattern(s3_uri: str, expected: bool) -> None:
    # arrange
    path_spec = PathSpec(
        include="s3://bucket/{table}/{partition0}/*.csv",
        tables_filter_pattern=AllowDenyPattern(allow=["t.*111"]),
    )

    # act, assert
    assert path_spec.dir_allowed(s3_uri) == expected


@pytest.mark.parametrize(
    "include, parse_path, expected_table_name, expected_table_path",
    [
        (
            "s3://bucket/{table}/{partition0}/*.csv",
            "s3://bucket/user_log/p1/test.csv",
            "user_log",
            "s3://bucket/user_log",
        ),
        (
            "s3://bucket/user_log/p1/*.csv",
            "s3://bucket/user_log/p1/test.csv",
            "test.csv",
            "s3://bucket/user_log/p1/test.csv",
        ),
    ],
)
def test_extract_table_name_and_path(
    include, parse_path, expected_table_name, expected_table_path
):
    # arrange
    path_spec = PathSpec(include=include)

    # act
    table_name, table_path = path_spec.extract_table_name_and_path(parse_path)

    # assert
    assert table_name == expected_table_name
    assert table_path == expected_table_path
