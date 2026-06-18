from datahub.ingestion.source.data_lake_common.config import S3LineageProviderConfig
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


class TestS3LineageProviderConfigPrefixFolding:
    def test_nested_stage_prefix_folds_to_table(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://my-bucket/{table}/*/*/*.csv")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_s3_path(
                "s3://my-bucket/events/year=2024/month=01/", strip_filename=False
            )
            == "s3://my-bucket/events"
        )

    def test_prefix_at_table_root_not_dropped_by_ignore(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert (
            config.get_s3_path("s3://lake/orders/", strip_filename=False)
            == "s3://lake/orders"
        )

    def test_wildcard_prefix_spec_folds_nested_stage(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/*/{table}")],
        )
        assert (
            config.get_s3_path("s3://lake/raw/orders/region=us/", strip_filename=False)
            == "s3://lake/raw/orders"
        )

    def test_foreign_prefix_dropped_by_ignore(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}")],
            ignore_non_path_spec_path=True,
        )
        assert config.get_s3_path("s3://other-bucket/x/", strip_filename=False) is None

    def test_prefix_folding_not_applied_to_file_inputs(self) -> None:
        config = S3LineageProviderConfig(
            path_specs=[PathSpec(include="s3://lake/{table}/*.csv")],
        )
        assert (
            config.get_s3_path(
                "s3://lake/events/part-0001.parquet", strip_filename=True
            )
            == "s3://lake/events"
        )
