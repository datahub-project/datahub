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
