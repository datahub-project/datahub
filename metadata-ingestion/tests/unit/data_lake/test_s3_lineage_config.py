from datahub.ingestion.source.data_lake_common.config import (
    S3LineageProviderConfig,
    S3PathMode,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

ENV = "PROD"
PARTITIONED_FILE = "s3://my-bucket/events/year=2024/month=01/part-0001.csv"


def _config_with_spec() -> S3LineageProviderConfig:
    return S3LineageProviderConfig(
        path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")]
    )


class TestS3LineageProviderConfig:
    def test_no_path_specs_strips_filename(self) -> None:
        config = S3LineageProviderConfig()
        assert (
            config.get_s3_path(PARTITIONED_FILE)
            == "s3://my-bucket/events/year=2024/month=01"
        )

    def test_no_path_specs_strip_disabled_returns_path(self) -> None:
        config = S3LineageProviderConfig(strip_urls=False)
        assert config.get_s3_path(PARTITIONED_FILE) == PARTITIONED_FILE

    def test_path_spec_folds_to_table_prefix(self) -> None:
        config = _config_with_spec()
        assert config.get_s3_path(PARTITIONED_FILE) == "s3://my-bucket/events"

    def test_ignore_non_path_spec_path_drops_unmatched(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_s3_path("s3://other-bucket/random/file.csv") is None

    def test_unmatched_path_without_ignore_strips_filename(self) -> None:
        config = _config_with_spec()
        assert (
            config.get_s3_path("s3://other-bucket/random/file.parquet")
            == "s3://other-bucket/random"
        )

    def test_get_s3_urn_for_lineage_uses_folded_path(self) -> None:
        config = _config_with_spec()
        urn = config.get_s3_urn_for_lineage(PARTITIONED_FILE, ENV)
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/events,PROD)"

    def test_get_s3_urn_for_lineage_returns_none_when_dropped(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_s3_urn_for_lineage("s3://other/x.csv", ENV) is None

    def test_first_spec_skipped_second_matches(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[
                PathSpec(include="s3://other-bucket/{table}/*.csv"),
                PathSpec(include="s3://my-bucket/{table}/*/*/*.csv"),
            ]
        )
        assert config.get_s3_path(PARTITIONED_FILE) == "s3://my-bucket/events"


class TestS3LineageProviderConfigPrefixFolding:
    def test_nested_stage_prefix_folds_to_table(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_s3_path(
                "s3://my-bucket/events/year=2024/month=01/", mode=S3PathMode.DIRECTORY
            )
            == "s3://my-bucket/events"
        )

    def test_prefix_at_table_root_not_dropped_by_ignore(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_s3_path("s3://lake/orders/", mode=S3PathMode.DIRECTORY)
            == "s3://lake/orders"
        )

    def test_wildcard_prefix_spec_folds_nested_stage(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/*/{table}")],
        )
        assert (
            config.get_s3_path(
                "s3://lake/raw/orders/region=us/", mode=S3PathMode.DIRECTORY
            )
            == "s3://lake/raw/orders"
        )

    def test_foreign_prefix_dropped_by_ignore(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_s3_path("s3://other-bucket/x/", mode=S3PathMode.DIRECTORY)
            is None
        )

    def test_prefix_folding_not_applied_to_file_inputs(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}/*.csv")],
        )
        assert (
            config.get_s3_path(
                "s3://lake/events/part-0001.parquet", mode=S3PathMode.FILE
            )
            == "s3://lake/events"
        )

    def test_directory_mode_with_no_path_specs_returns_path(self) -> None:
        config = S3LineageProviderConfig()
        assert (
            config.get_s3_path(
                "s3://my-bucket/events/year=2024/", mode=S3PathMode.DIRECTORY
            )
            == "s3://my-bucket/events/year=2024/"
        )

    def test_directory_mode_unmatched_without_ignore_returns_path(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*.csv")],
        )
        # No ignore_non_path_spec_path; directory mode skips strip_urls fallback.
        assert (
            config.get_s3_path("s3://other-bucket/x/", mode=S3PathMode.DIRECTORY)
            == "s3://other-bucket/x/"
        )

    def test_shallow_prefix_not_folded(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/*/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
        # Path is shallower than {table} depth; cannot fold, gets dropped.
        assert config.get_s3_path("s3://lake/raw/", mode=S3PathMode.DIRECTORY) is None


class TestPathSpecFoldDirToTable:
    def test_returns_none_when_spec_has_no_table_token(self) -> None:
        spec = PathSpec(include="s3://bucket/static/file.csv")
        assert spec.fold_dir_to_table("s3://bucket/static/") is None

    def test_returns_none_when_prefix_shallower_than_table_depth(self) -> None:
        spec = PathSpec(include="s3://lake/*/{table}/*.csv")
        # table is at depth 4; path has depth 3.
        assert spec.fold_dir_to_table("s3://lake/raw/") is None

    def test_returns_none_when_glob_mismatch_at_table_depth(self) -> None:
        spec = PathSpec(include="s3://lake/raw/{table}/*.csv")
        # Wrong bucket; path is deep enough but doesn't match the spec.
        assert spec.fold_dir_to_table("s3://other/raw/orders/") is None

    def test_honours_exclude(self) -> None:
        spec = PathSpec(
            include="s3://lake/{table}/*.csv",
            exclude=["s3://lake/internal/**"],
        )
        assert spec.fold_dir_to_table("s3://lake/internal/year=2024/") is None
        assert (
            spec.fold_dir_to_table("s3://lake/orders/year=2024/") == "s3://lake/orders"
        )
