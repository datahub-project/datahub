import pytest

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.source.aws.s3_util import make_s3_urn, make_s3_urn_for_lineage
from datahub.utilities.urns.dataset_urn import DatasetUrn


@pytest.mark.parametrize(
    "s3_uri, remove_extension, expected_name",
    [
        ("s3://my-bucket/data/file.parquet", True, "my-bucket/data/file_parquet"),
        ("s3://my-bucket/data/file.parquet", False, "my-bucket/data/file.parquet"),
        ("s3a://my-bucket/data/", False, "my-bucket/data"),
        ("s3://my-bucket/", True, "my-bucket"),
    ],
)
def test_no_reserved_chars_unchanged(
    s3_uri: str, remove_extension: bool, expected_name: str
) -> None:
    assert (
        make_s3_urn(s3_uri, "PROD", remove_extension=remove_extension)
        == f"urn:li:dataset:(urn:li:dataPlatform:s3,{expected_name},PROD)"
    )


@pytest.mark.parametrize(
    "s3_uri, remove_extension, expected_name",
    [
        (
            "s3://my-bucket/data/folder(1)/file.parquet",
            False,
            "my-bucket/data/folder%281%29/file.parquet",
        ),
        (
            "s3://my-bucket/data/folder(1)/file.parquet",
            True,
            "my-bucket/data/folder%281%29/file_parquet",
        ),
        # The extension itself can hold reserved characters.
        ("s3://my-bucket/data/file.par(1)", True, "my-bucket/data/file_par%281%29"),
        ("s3://my-bucket/data/a,b/", False, "my-bucket/data/a%2Cb"),
        ("s3://my-bucket/data/folder(1/", False, "my-bucket/data/folder%281"),
        (
            "s3://my-bucket/data/folder)/file.parquet",
            False,
            "my-bucket/data/folder%29/file.parquet",
        ),
        ("s3://my-bucket/data/a␟b", False, "my-bucket/data/a%E2%90%9Fb"),
    ],
)
def test_reserved_chars_encoded(
    s3_uri: str, remove_extension: bool, expected_name: str
) -> None:
    urn = make_s3_urn(s3_uri, "PROD", remove_extension=remove_extension)
    assert urn == f"urn:li:dataset:(urn:li:dataPlatform:s3,{expected_name},PROD)"
    assert DatasetUrn.from_string(urn).name == expected_name


def test_extension_removed_by_default() -> None:
    assert (
        make_s3_urn("s3://my-bucket/data/file.parquet", "PROD")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/data/file_parquet,PROD)"
    )


def test_lineage_urn_matches_s3_source_urn() -> None:
    path = "my-bucket/data/folder(1)/file.parquet"
    assert make_s3_urn_for_lineage(
        f"s3://{path}", "PROD"
    ) == make_dataset_urn_with_platform_instance("s3", path, None, "PROD")


def test_percent_passes_through() -> None:
    assert (
        make_s3_urn_for_lineage("s3://my-bucket/a%28b/", "PROD")
        == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/a%28b,PROD)"
    )


def test_env_casing_preserved() -> None:
    assert make_s3_urn_for_lineage("s3://my-bucket/k", "prod").endswith(",prod)")


def test_non_s3_uri_raises() -> None:
    with pytest.raises(ValueError):
        make_s3_urn("gs://my-bucket/k", "PROD")
