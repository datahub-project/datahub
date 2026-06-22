from datahub.ingestion.source.data_lake_common.config import (
    DataLakeLineageProviderConfig,
    PathMode,
)
from datahub.ingestion.source.data_lake_common.path_spec import PathSpec

ENV = "PROD"
PARTITIONED_FILE = "s3://my-bucket/events/year=2024/month=01/part-0001.csv"


def _config_with_spec() -> DataLakeLineageProviderConfig:
    return DataLakeLineageProviderConfig(
        path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")]
    )


class TestDataLakeLineageProviderConfig:
    def test_no_path_specs_strips_filename(self) -> None:
        config = DataLakeLineageProviderConfig()
        assert (
            config.get_path(PARTITIONED_FILE)
            == "s3://my-bucket/events/year=2024/month=01"
        )

    def test_no_path_specs_strip_disabled_returns_path(self) -> None:
        config = DataLakeLineageProviderConfig(strip_urls=False)
        assert config.get_path(PARTITIONED_FILE) == PARTITIONED_FILE

    def test_path_spec_folds_to_table_prefix(self) -> None:
        config = _config_with_spec()
        assert config.get_path(PARTITIONED_FILE) == "s3://my-bucket/events"

    def test_ignore_non_path_spec_path_drops_unmatched(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_path("s3://other-bucket/random/file.csv") is None

    def test_unmatched_path_without_ignore_strips_filename(self) -> None:
        config = _config_with_spec()
        assert (
            config.get_path("s3://other-bucket/random/file.parquet")
            == "s3://other-bucket/random"
        )

    def test_get_urn_for_lineage_uses_folded_path(self) -> None:
        config = _config_with_spec()
        urn = config.get_urn_for_lineage(PARTITIONED_FILE, ENV)
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:s3,my-bucket/events,PROD)"

    def test_get_urn_for_lineage_returns_none_when_dropped(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_urn_for_lineage("s3://other/x.csv", ENV) is None

    def test_first_spec_skipped_second_matches(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[
                PathSpec(include="s3://other-bucket/{table}/*.csv"),
                PathSpec(include="s3://my-bucket/{table}/*/*/*.csv"),
            ]
        )
        assert config.get_path(PARTITIONED_FILE) == "s3://my-bucket/events"


class TestDataLakeLineageProviderConfigPrefixFolding:
    def test_nested_stage_prefix_folds_to_table(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_path(
                "s3://my-bucket/events/year=2024/month=01/", mode=PathMode.DIRECTORY
            )
            == "s3://my-bucket/events"
        )

    def test_prefix_at_table_root_not_dropped_by_ignore(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_path("s3://lake/orders/", mode=PathMode.DIRECTORY)
            == "s3://lake/orders"
        )

    def test_wildcard_prefix_spec_folds_nested_stage(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/*/{table}")],
        )
        assert (
            config.get_path("s3://lake/raw/orders/region=us/", mode=PathMode.DIRECTORY)
            == "s3://lake/raw/orders"
        )

    def test_foreign_prefix_dropped_by_ignore(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_path("s3://other-bucket/x/", mode=PathMode.DIRECTORY) is None

    def test_prefix_folding_not_applied_to_file_inputs(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}/*.csv")],
        )
        assert (
            config.get_path("s3://lake/events/part-0001.parquet", mode=PathMode.FILE)
            == "s3://lake/events"
        )

    def test_directory_mode_no_path_specs_strips_trailing_slash(self) -> None:
        config = DataLakeLineageProviderConfig()
        assert (
            config.get_path("s3://my-bucket/events/year=2024/", mode=PathMode.DIRECTORY)
            == "s3://my-bucket/events/year=2024"
        )

    def test_directory_mode_unmatched_without_ignore_strips_trailing_slash(
        self,
    ) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*.csv")],
        )
        assert (
            config.get_path("s3://other-bucket/x/", mode=PathMode.DIRECTORY)
            == "s3://other-bucket/x"
        )

    def test_shallow_prefix_not_folded(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/*/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
        # Path is shallower than {table} depth; cannot fold, gets dropped.
        assert config.get_path("s3://lake/raw/", mode=PathMode.DIRECTORY) is None


class TestDataLakeLineageProviderConfigSchemeDispatch:
    def test_gcs_url_routes_to_gcs_platform(self) -> None:
        config = DataLakeLineageProviderConfig()
        urn = config.get_urn_for_lineage(
            "gcs://my-bucket/events/year=2024/", ENV, mode=PathMode.DIRECTORY
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/events/year=2024,PROD)"
        )

    def test_azure_url_routes_to_abs_platform(self) -> None:
        config = DataLakeLineageProviderConfig()
        urn = config.get_urn_for_lineage(
            "azure://account.blob.core.windows.net/container/events/",
            ENV,
            mode=PathMode.DIRECTORY,
        )
        assert urn == ("urn:li:dataset:(urn:li:dataPlatform:abs,container/events,PROD)")

    def test_unsupported_scheme_returns_none(self) -> None:
        config = DataLakeLineageProviderConfig()
        assert (
            config.get_urn_for_lineage(
                "file:///local/path", ENV, mode=PathMode.DIRECTORY
            )
            is None
        )

    def test_gcs_path_spec_folds_directory(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="gcs://lake/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
        urn = config.get_urn_for_lineage(
            "gcs://lake/events/year=2024/", ENV, mode=PathMode.DIRECTORY
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:gcs,lake/events,PROD)"


class TestPathSpecFoldDirToTable:
    def test_returns_none_when_spec_has_no_table_token(self) -> None:
        spec = PathSpec(include="s3://bucket/static/file.csv")
        assert spec.fold_dir_to_table("s3://bucket/static/") is None

    def test_honours_exclude(self) -> None:
        spec = PathSpec(
            include="s3://lake/{table}/*.csv",
            exclude=["s3://lake/internal/**"],
        )
        assert spec.fold_dir_to_table("s3://lake/internal/year=2024/") is None
        assert (
            spec.fold_dir_to_table("s3://lake/orders/year=2024/") == "s3://lake/orders"
        )
