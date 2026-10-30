import pytest

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

    def test_gcs_path_spec_folds_directory(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="gcs://lake/{table}/*.csv")],
            ignore_non_path_spec_path=True,
        )
        urn = config.get_urn_for_lineage(
            "gcs://lake/events/year=2024/", ENV, mode=PathMode.DIRECTORY
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:gcs,lake/events,PROD)"

    def test_canonical_gs_url_routes_to_gcs_platform(self) -> None:
        config = DataLakeLineageProviderConfig()
        urn = config.get_urn_for_lineage(
            "gs://my-bucket/events/year=2024/", ENV, mode=PathMode.DIRECTORY
        )
        assert urn == (
            "urn:li:dataset:(urn:li:dataPlatform:gcs,my-bucket/events/year=2024,PROD)"
        )

    def test_abs_https_url_routes_to_abs_platform(self) -> None:
        config = DataLakeLineageProviderConfig()
        urn = config.get_urn_for_lineage(
            "https://account.blob.core.windows.net/container/events/",
            ENV,
            mode=PathMode.DIRECTORY,
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:abs,container/events,PROD)"

    def test_azure_path_spec_folds_directory(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[
                PathSpec(
                    include="azure://account.blob.core.windows.net/container/{table}/*.csv"
                )
            ],
            ignore_non_path_spec_path=True,
        )
        urn = config.get_urn_for_lineage(
            "azure://account.blob.core.windows.net/container/events/year=2024/",
            ENV,
            mode=PathMode.DIRECTORY,
        )
        assert urn == "urn:li:dataset:(urn:li:dataPlatform:abs,container/events,PROD)"


class TestPathSpecFoldDirToTable:
    def test_returns_none_when_spec_has_no_table_token(self) -> None:
        spec = PathSpec(include="s3://bucket/static/file.csv")
        result = spec.fold_dir_to_table("s3://bucket/static/")
        assert result.table_path is None
        assert result.denied is False

    def test_honours_exclude(self) -> None:
        spec = PathSpec(
            include="s3://lake/{table}/*.csv",
            exclude=["s3://lake/internal/**"],
        )
        assert spec.fold_dir_to_table("s3://lake/internal/year=2024/").denied is True
        # The root itself does not glob-match `s3://lake/internal/**`, so it has to be
        # checked against the exclude's `/**` suffix explicitly.
        assert spec.fold_dir_to_table("s3://lake/internal/").denied is True
        assert (
            spec.fold_dir_to_table("s3://lake/orders/year=2024/").table_path
            == "s3://lake/orders"
        )

    def test_tables_filter_pattern_denial_is_distinguished_from_no_match(self) -> None:
        spec = PathSpec(
            include="s3://lake/{table}/*.csv",
            tables_filter_pattern={"deny": ["secret"]},
        )
        assert spec.fold_dir_to_table("s3://lake/secret/y=2024/").denied is True
        # A prefix under a different bucket is simply not this spec's business.
        no_match = spec.fold_dir_to_table("s3://other/secret/y=2024/")
        assert no_match.table_path is None
        assert no_match.denied is False

    def test_hidden_folders_do_not_become_datasets(self) -> None:
        spec = PathSpec(include="s3://lake/{table}/*.csv")
        assert spec.fold_dir_to_table("s3://lake/_tmp/").table_path is None
        assert spec.fold_dir_to_table("s3://lake/.staging/").table_path is None
        assert spec.fold_dir_to_table("s3://lake/orders/").table_path == (
            "s3://lake/orders"
        )


class TestExplicitDenialIsNotOverridden:
    """An `exclude` or `tables_filter_pattern` denial must win over the fallback,
    including under the default `ignore_non_path_spec_path=False`."""

    def test_excluded_prefix_emits_nothing(self) -> None:
        """The bug: a denied prefix fell through to the raw-path fallback and was
        emitted *unfolded*, so it both ignored the exclude and failed to match the
        lake source's URN."""
        config = DataLakeLineageProviderConfig(
            path_specs=[
                PathSpec(
                    include="s3://lake/{table}/*.csv",
                    exclude=["s3://lake/internal/**"],
                )
            ],
        )
        assert config.ignore_non_path_spec_path is False
        assert (
            config.get_path("s3://lake/internal/y=2024/", mode=PathMode.DIRECTORY)
            is None
        )
        assert (
            config.get_urn_for_lineage(
                "s3://lake/internal/y=2024/", ENV, mode=PathMode.DIRECTORY
            )
            is None
        )

    def test_denied_table_emits_nothing(self) -> None:
        config = DataLakeLineageProviderConfig(
            path_specs=[
                PathSpec(
                    include="s3://lake/{table}/*.csv",
                    tables_filter_pattern={"deny": ["secret"]},
                )
            ],
        )
        assert (
            config.get_path("s3://lake/secret/y=2024/", mode=PathMode.DIRECTORY) is None
        )

    def test_another_spec_may_still_claim_an_excluded_prefix(self) -> None:
        """Only the *fallback* is blocked, not other specs. A second spec without the
        exclusion legitimately covers the prefix — same as file mode, where a later
        spec's `allowed()` can claim a file an earlier spec excluded."""
        specs = [
            PathSpec(
                include="s3://lake/{table}/*.csv",
                exclude=["s3://lake/internal/**"],
            ),
            PathSpec(include="s3://lake/{table}"),
        ]
        config = DataLakeLineageProviderConfig(path_specs=specs)
        assert (
            config.get_path("s3://lake/internal/y=2024/", mode=PathMode.DIRECTORY)
            == "s3://lake/internal"
        )
        # ...and file mode agrees, which is the property that matters.
        assert (
            config.get_path("s3://lake/internal/y=2024/f.csv", mode=PathMode.FILE)
            == "s3://lake/internal"
        )


class TestFileAndDirectoryModesAgree:
    """The whole point of folding is that a stage prefix resolves to the same URN
    the lake source emits for the table's files. Assert that directly."""

    @pytest.mark.parametrize(
        "include,file_path,dir_path",
        [
            # whole-segment {table}
            (
                "s3://bucket/{table}/*/*.csv",
                "s3://bucket/orders/y=2024/part-0.csv",
                "s3://bucket/orders/y=2024/",
            ),
            # {table} is only part of the segment — the folded path must drop `tbl_`
            (
                "s3://bucket/tbl_{table}/*/*.csv",
                "s3://bucket/tbl_orders/y=2024/part-0.csv",
                "s3://bucket/tbl_orders/y=2024/",
            ),
            # stage sitting exactly at the table root
            (
                "s3://bucket/{table}/*/*.csv",
                "s3://bucket/orders/y=2024/part-0.csv",
                "s3://bucket/orders/",
            ),
        ],
    )
    def test_same_table_same_urn(
        self, include: str, file_path: str, dir_path: str
    ) -> None:
        config = DataLakeLineageProviderConfig(path_specs=[PathSpec(include=include)])
        assert config.get_urn_for_lineage(
            dir_path, ENV, mode=PathMode.DIRECTORY
        ) == config.get_urn_for_lineage(file_path, ENV, mode=PathMode.FILE)

    def test_partial_segment_table_folds_to_the_parsed_name(self) -> None:
        """Regression: folding used the raw segment, yielding `bucket/tbl_orders`
        where file mode yields `bucket/orders`, so lineage never stitched."""
        config = DataLakeLineageProviderConfig(
            path_specs=[PathSpec(include="s3://bucket/tbl_{table}/*/*.csv")]
        )
        assert (
            config.get_urn_for_lineage(
                "s3://bucket/tbl_orders/y=2024/", ENV, mode=PathMode.DIRECTORY
            )
            == "urn:li:dataset:(urn:li:dataPlatform:s3,bucket/orders,PROD)"
        )

    @pytest.mark.parametrize(
        "spec,file_path,dir_path,expected_table_path",
        [
            # `{table}` is only part of the segment: filter must see `orders`, not
            # `tbl_orders`, and the folded path must drop the `tbl_` prefix.
            (
                PathSpec(
                    include="s3://bucket/tbl_{table}/*/*.csv",
                    tables_filter_pattern={"allow": ["orders"]},
                ),
                "s3://bucket/tbl_orders/y=2024/part-0.csv",
                "s3://bucket/tbl_orders/y=2024/",
                "s3://bucket/orders",
            ),
            # `table_name` composes several vars: filter must see the composed name.
            (
                PathSpec(
                    include="s3://bucket/{dept}/{table}/*.csv",
                    table_name="{dept}.{table}",
                    tables_filter_pattern={"allow": [r"sales\.orders"]},
                ),
                "s3://bucket/sales/orders/part-0.csv",
                "s3://bucket/sales/orders/y=2024/",
                "s3://bucket/sales/orders",
            ),
        ],
    )
    def test_tables_filter_pattern_matches_the_same_name_in_both_modes(
        self, spec: PathSpec, file_path: str, dir_path: str, expected_table_path: str
    ) -> None:
        """Regression: the filter was applied to the raw segment in directory mode
        and to the parsed/composed table name in file mode, so one spec could
        accept a table's files while dropping the same table's stage."""
        assert spec.allowed(file_path) is True
        assert spec.fold_dir_to_table(dir_path).table_path == expected_table_path


class TestDegenerateAndUnsupportedUrls:
    @pytest.mark.parametrize(
        "url",
        [
            "s3://",  # built `s3,,PROD` — a URN with an empty dataset name
            "gcs://",  # raised InvalidUrnError out of the URN builder
        ],
    )
    def test_scheme_without_object_path_is_dropped(self, url: str) -> None:
        """Regression: file mode had nothing to strip, so the bare scheme reached the
        URN builders."""
        assert (
            DataLakeLineageProviderConfig().get_urn_for_lineage(
                url, ENV, mode=PathMode.FILE
            )
            is None
        )

    @pytest.mark.parametrize(
        "url",
        [
            # ADLS Gen2 — a real Snowflake Azure stage form that make_abs_urn rejects
            "azure://acct.dfs.core.windows.net/container/events/",
            "azure://weird-host.example.com/container/events/",
        ],
    )
    def test_unsupported_host_is_dropped_not_raised(self, url: str) -> None:
        """Regression: the azure:// branch called make_abs_urn unguarded, so a
        non-blob host raised ValueError out of stage extraction."""
        assert (
            DataLakeLineageProviderConfig().get_urn_for_lineage(
                url, ENV, mode=PathMode.DIRECTORY
            )
            is None
        )
