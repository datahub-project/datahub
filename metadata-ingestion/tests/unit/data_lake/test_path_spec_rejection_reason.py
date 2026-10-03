from typing import Optional

import pytest

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec


@pytest.mark.parametrize(
    "path,reason",
    [
        ("s3://my-bucket/data/events/part-0.parquet", None),
        ("s3://my-bucket/other/events/part-0.parquet", "include"),
        ("s3://my-bucket/data/_tmp/part-0.parquet", "include_hidden_folders"),
        ("s3://my-bucket/data/scratch/part-0.parquet", "exclude"),
        ("s3://my-bucket/data/events/part-0.txt", "file_types"),
        ("s3://my-bucket/data/events/part-0", "default_extension"),
    ],
)
def test_rejection_reason_names_the_rule_that_decided(
    path: str, reason: Optional[str]
) -> None:
    # "*" (not "*.*") as the last component, so an extensionless file still
    # matches the include and reaches the default_extension rule.
    spec = PathSpec(
        include="s3://my-bucket/data/*/*",
        exclude=["s3://my-bucket/data/scratch/**"],
        file_types=["parquet"],
    )
    assert spec.rejection_reason(path) == reason
    assert spec.allowed(path) is (reason is None)


def test_tables_filter_pattern_is_reported_by_name() -> None:
    spec = PathSpec(
        include="s3://my-bucket/data/{table}/*.parquet",
        tables_filter_pattern=AllowDenyPattern(deny=["^users$"]),
    )
    assert (
        spec.rejection_reason("s3://my-bucket/data/users/a.parquet")
        == "tables_filter_pattern"
    )


def test_ignore_ext_skips_the_extension_rules_only() -> None:
    spec = PathSpec(include="s3://my-bucket/data/*/*.*", file_types=["parquet"])
    assert spec.rejection_reason("s3://my-bucket/data/e/a.txt", ignore_ext=True) is None
    assert (
        spec.rejection_reason("s3://my-bucket/other/e/a.txt", ignore_ext=True)
        == "include"
    )


def test_folder_rejection_reason_mirrors_folder_allowed() -> None:
    spec = PathSpec(
        include="s3://my-bucket/media/*/",
        exclude=["s3://my-bucket/media/private"],
        emit_folders_only=True,
    )
    assert spec.folder_rejection_reason("s3://my-bucket/media/public") is None
    assert spec.folder_rejection_reason("s3://my-bucket/media/private") == "exclude"
    assert (
        spec.folder_rejection_reason("s3://my-bucket/media/.cache")
        == "include_hidden_folders"
    )
    assert spec.folder_allowed("s3://my-bucket/media/private") is False
