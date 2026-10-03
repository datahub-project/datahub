import pytest

from datahub.ingestion.source.s3.source import listing_prefix, table_marker_prefix


@pytest.mark.parametrize(
    "include,expected",
    [
        ("s3://my-bucket/data/{table}/**", "s3://my-bucket/data/"),
        ("s3://my-bucket/{dept}/data/{table}/*.parquet", "s3://my-bucket/*/data/"),
        (
            "s3://my-bucket/*/logs/{table}/{partition[0]}/*.json",
            "s3://my-bucket/*/logs/",
        ),
    ],
)
def test_table_marker_prefix_is_what_gets_resolved_by_listing(
    include: str, expected: str
) -> None:
    assert table_marker_prefix(include) == expected


def test_listing_prefix_splits_at_the_first_wildcard() -> None:
    assert listing_prefix("s3://my-bucket/data/ev*/*.csv") == (
        "s3://my-bucket/data/",
        "ev",
    )
