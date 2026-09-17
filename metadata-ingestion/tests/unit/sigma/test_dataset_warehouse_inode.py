"""Sigma Dataset -> warehouse table via /datasets/{id}/sources.

Covers the route that replaced the SQL-name match Sigma's 2026-09-15 dataset
deprecation broke. The URN is built through the connection registry, so these
tests pin the path-shape handling and the failure buckets rather than the URN
construction itself (covered by the DM element warehouse tests).
"""

from typing import Dict, List, Optional
from unittest.mock import patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import (
    PlatformDetail,
    SigmaSourceConfig,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    ConnectionPath,
    DatasetUpstream,
    Element,
    SigmaDataset,
    Workbook,
)
from datahub.ingestion.source.sigma.sigma import (
    SigmaSource,
    _normalize_warehouse_identifier,
    _WarehouseTableRef,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI

_SNOWFLAKE_CONN_ID = "conn-snowflake-001"
_REDSHIFT_CONN_ID = "conn-redshift-001"


def _make_source(redshift_default_db: Optional[str] = None) -> SigmaSource:
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    ctx = PipelineContext(run_id="dataset-inode-unit")
    # Patch get_connections as well as the token: SigmaSource builds the
    # connection registry in __init__, which otherwise makes a real HTTP call
    # from a unit test. The registry is replaced below anyway.
    with patch.object(SigmaAPI, "_generate_token"):
        with patch.object(SigmaAPI, "get_connections", return_value=[]):
            source = SigmaSource(config=config, ctx=ctx)
    records = [
        SigmaConnectionRecord(
            connection_id=_SNOWFLAKE_CONN_ID,
            name="Snowflake",
            sigma_type="snowflake",
            datahub_platform="snowflake",
            is_mappable=True,
        ),
        SigmaConnectionRecord(
            connection_id=_REDSHIFT_CONN_ID,
            name="Redshift",
            sigma_type="redshift",
            datahub_platform="redshift",
            default_database=redshift_default_db,
            is_mappable=True,
        ),
    ]
    source.connection_registry = SigmaConnectionRegistry(
        by_id={r.connection_id: r for r in records}
    )
    return source


class TestConnectionPathShapes:
    def test_three_segment_path_maps_db_schema_table(self) -> None:
        source = _make_source()
        with patch.object(
            source.sigma_api,
            "get_connection_path",
            return_value=ConnectionPath(
                connection_id=_SNOWFLAKE_CONN_ID, path=["DB", "SCHEMA", "TABLE"]
            ),
        ):
            ref = source._resolve_inode_to_warehouse_ref("inode-1")
        assert ref is not None
        assert (ref.db, ref.schema, ref.table) == ("DB", "SCHEMA", "TABLE")

    def test_two_segment_path_takes_db_from_connection(self) -> None:
        # Platforms with no database layer in the path (e.g. Redshift) rely on
        # the connection's configured database to produce a 3-part URN.
        source = _make_source(redshift_default_db="DEV")
        with patch.object(
            source.sigma_api,
            "get_connection_path",
            return_value=ConnectionPath(
                connection_id=_REDSHIFT_CONN_ID, path=["public", "orders"]
            ),
        ):
            ref = source._resolve_inode_to_warehouse_ref("inode-1")
        assert ref is not None
        assert (ref.db, ref.schema, ref.table) == ("DEV", "public", "orders")

    def test_two_segment_path_without_default_db_warns_once(self) -> None:
        # Emits schema.table, which will not match a connector using
        # db.schema.table -- so the operator gets told, once per connection.
        source = _make_source(redshift_default_db=None)
        with patch.object(
            source.sigma_api,
            "get_connection_path",
            return_value=ConnectionPath(
                connection_id=_REDSHIFT_CONN_ID, path=["public", "orders"]
            ),
        ):
            # Count calls, not entries: identical warnings collapse into a
            # single StructuredLogs entry regardless of the dedup set.
            with patch.object(
                source.reporter, "warning", wraps=source.reporter.warning
            ) as spy:
                ref = source._resolve_inode_to_warehouse_ref("inode-1")
                source._connection_path_cache.clear()
                source._resolve_inode_to_warehouse_ref("inode-2")
                assert spy.call_count == 1
        assert ref is not None
        assert ref.db is None

    @pytest.mark.parametrize("path", [["TABLE"], ["A", "B", "C", "D"]])
    def test_unexpected_depth_is_skipped(self, path: List[str]) -> None:
        source = _make_source()
        with patch.object(
            source.sigma_api,
            "get_connection_path",
            return_value=ConnectionPath(connection_id=_SNOWFLAKE_CONN_ID, path=path),
        ):
            assert source._resolve_inode_to_warehouse_ref("inode-1") is None
        assert source.reporter.connection_path_lookup_failed == 1

    def test_lookup_failure_is_cached(self) -> None:
        source = _make_source()
        with patch.object(
            source.sigma_api, "get_connection_path", return_value=None
        ) as mocked:
            assert source._resolve_inode_to_warehouse_ref("inode-1") is None
            assert source._resolve_inode_to_warehouse_ref("inode-1") is None
            assert mocked.call_count == 1


class TestDatasetWarehouseRefs:
    def test_unlisted_dataset_is_reported_as_info_and_skipped(self) -> None:
        # Referenced by an element but absent from /v2/datasets, usually because
        # workspace_pattern excludes its workspace. Visible, but an info rather
        # than a warning: excluding a workspace is normally deliberate.
        source = _make_source()
        warnings_before = len(source.reporter.warnings)
        infos_before = len(source.reporter.infos)
        with patch.object(source.sigma_api, "get_dataset_sources") as mocked:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            mocked.assert_not_called()
        assert source.reporter.dataset_warehouse_unlisted_dataset == 1
        assert len(source.reporter.infos) == infos_before + 1
        assert len(source.reporter.warnings) == warnings_before

    @pytest.mark.parametrize(
        "entries",
        [
            [],
            [{"type": "dataset", "inodeId": "inode-1"}],
        ],
    )
    def test_no_table_sources_is_not_an_error(
        self, entries: List[Dict[str, str]]
    ) -> None:
        # CSV uploads, dataset-on-dataset and custom-SQL datasets land here.
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(
            source.sigma_api, "get_dataset_sources", return_value=entries
        ):
            assert source._get_dataset_warehouse_refs("url-1") == []
        assert source.reporter.dataset_warehouse_no_table_sources == 1
        assert source.reporter.dataset_sources_lookup_failed == 0

    @pytest.mark.parametrize(
        "entries",
        [
            [{"type": "table"}],  # named a table but gave no inodeId
            ["inode-1"],  # not an object at all
            [{"type": "table", "inodeId": "i-1"}, 3],  # one good, one junk
        ],
    )
    def test_unusable_entry_is_not_the_benign_case(self, entries: List[object]) -> None:
        # Sigma named a table but the entry was unusable. That is a malformed
        # payload, not a CSV/custom-SQL dataset, so it must not land in
        # dataset_warehouse_no_table_sources -- which the docs call benign.
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(
            source.sigma_api, "get_dataset_sources", return_value=entries
        ):
            with patch.object(
                source.sigma_api, "get_connection_path", return_value=None
            ):
                with patch.object(
                    source.reporter, "warning", wraps=source.reporter.warning
                ) as spy:
                    source._get_dataset_warehouse_refs("url-1")
                    assert spy.call_count == 1
        assert source.reporter.dataset_warehouse_no_table_sources == 0
        assert source.reporter.dataset_warehouse_table_entry_incomplete == 1

    def test_failed_lookup_is_not_counted_as_no_table_sources(self) -> None:
        # None means the lookup failed (already counted and warned inside
        # SigmaAPI); [] means the dataset genuinely has no warehouse table. The
        # docs call no_table_sources benign, so a failure must not land there --
        # otherwise a retired endpoint reports every dataset as "CSV or
        # custom SQL" instead of "endpoint gone".
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(source.sigma_api, "get_dataset_sources", return_value=None):
            assert source._get_dataset_warehouse_refs("url-1") == []
        assert source.reporter.dataset_warehouse_no_table_sources == 0

    def test_unmappable_connection_counted_once_for_two_tables(self) -> None:
        # Two unmappable tables on one dataset is one unresolved dataset.
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(
            source.sigma_api,
            "get_dataset_sources",
            return_value=[
                {"type": "table", "inodeId": "inode-1"},
                {"type": "table", "inodeId": "inode-2"},
            ],
        ):
            with patch.object(
                source.sigma_api,
                "get_connection_path",
                side_effect=[
                    ConnectionPath(connection_id="conn-absent", path=["DB", "S", "T1"]),
                    ConnectionPath(connection_id="conn-absent", path=["DB", "S", "T2"]),
                ],
            ):
                assert source._resolve_dataset_warehouse_upstreams("url-1") == []
        assert source.reporter.dataset_warehouse_unknown_connection == 1

    def test_refs_are_cached_per_dataset(self) -> None:
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(
            source.sigma_api,
            "get_dataset_sources",
            return_value=[{"type": "table", "inodeId": "inode-1"}],
        ) as mocked_sources:
            with patch.object(
                source.sigma_api,
                "get_connection_path",
                return_value=ConnectionPath(
                    connection_id=_SNOWFLAKE_CONN_ID, path=["DB", "SCHEMA", "TABLE"]
                ),
            ):
                first = source._get_dataset_warehouse_refs("url-1")
                second = source._get_dataset_warehouse_refs("url-1")
        # Assert the contents, not just that the two calls agree: [] == [] would
        # satisfy equality plus the call count even if nothing resolved, which
        # made this pass while the route produced no refs at all.
        expected = [
            _WarehouseTableRef(
                connection_id=_SNOWFLAKE_CONN_ID,
                db="DB",
                schema="SCHEMA",
                table="TABLE",
            )
        ]
        assert first == expected
        assert second == expected
        assert mocked_sources.call_count == 1

    def test_unmappable_connection_counts_once_per_dataset(self) -> None:
        source = _make_source()
        source.sigma_dataset_id_by_url_id["url-1"] = "ds-uuid-1"
        with patch.object(
            source.sigma_api,
            "get_dataset_sources",
            return_value=[{"type": "table", "inodeId": "inode-1"}],
        ):
            with patch.object(
                source.sigma_api,
                "get_connection_path",
                return_value=ConnectionPath(
                    connection_id="conn-not-in-registry",
                    path=["DB", "SCHEMA", "TABLE"],
                ),
            ):
                # Two elements reading the same dataset.
                assert source._resolve_dataset_warehouse_upstreams("url-1") == []
                assert source._resolve_dataset_warehouse_upstreams("url-1") == []
        assert source.reporter.dataset_warehouse_unknown_connection == 1


class TestPlatformMappingEnvWarning:
    """The mapping's env / platform_instance vs. the URN actually emitted.

    The comparison is against the *effective* env and instance, which come from
    a connection_to_platform_map entry when one exists. WarehouseConnectionConfig
    supplies its own env default, so an override that sets only
    default_database still moves the env -- that case must warn, and the
    opposite case must not.
    """

    def _source(self, recipe_env: str = "PROD", **cfg: object) -> SigmaSource:
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                "env": recipe_env,
                **cfg,
            }
        )
        ctx = PipelineContext(run_id="mapping-env-unit")
        with patch.object(SigmaAPI, "_generate_token"):
            with patch.object(SigmaAPI, "get_connections", return_value=[]):
                source = SigmaSource(config=config, ctx=ctx)
        source.connection_registry = SigmaConnectionRegistry(
            by_id={
                _SNOWFLAKE_CONN_ID: SigmaConnectionRecord(
                    connection_id=_SNOWFLAKE_CONN_ID,
                    name="Snowflake",
                    sigma_type="snowflake",
                    datahub_platform="snowflake",
                    is_mappable=True,
                )
            }
        )
        return source

    def _ref(self) -> _WarehouseTableRef:
        return _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID, db="DB", schema="S", table="T"
        )

    def _warn_calls(
        self, source: SigmaSource, detail: Optional[PlatformDetail], times: int = 1
    ) -> int:
        """reporter.warning CALLS, not entries: identical warnings collapse."""
        with patch.object(
            source.reporter, "warning", wraps=source.reporter.warning
        ) as spy:
            for _ in range(times):
                source._warn_if_platform_mapping_env_ignored(self._ref(), detail)
            return spy.call_count

    @staticmethod
    def _detail(platform: str = "snowflake", **kw: object) -> PlatformDetail:
        return PlatformDetail.model_validate({"data_source_platform": platform, **kw})

    def test_warns_when_default_database_only_override_moves_the_env(self) -> None:
        # Recipe DEV, mapping DEV, override sets only default_database. The URN
        # is emitted with the override's inherited PROD, so the SQL route's DEV
        # spelling is silently lost -- this must warn.
        source = self._source(
            recipe_env="DEV",
            connection_to_platform_map={_SNOWFLAKE_CONN_ID: {"default_database": "DB"}},
        )
        assert self._warn_calls(source, self._detail(env="DEV")) == 1

    def test_silent_when_the_mapping_already_matches_the_effective_env(self) -> None:
        # Recipe DEV, mapping env unset (so PROD), same override (so PROD). The
        # emitted URN matches what the SQL route produced; warning here would be
        # false and would misreport the recipe's env as the one in use.
        source = self._source(
            recipe_env="DEV",
            connection_to_platform_map={_SNOWFLAKE_CONN_ID: {"default_database": "DB"}},
        )
        assert self._warn_calls(source, self._detail()) == 0

    def test_warns_once_when_platform_instance_only_on_mapping(self) -> None:
        source = self._source()
        assert (
            self._warn_calls(source, self._detail(platform_instance="mi"), times=2) == 1
        )

    def test_platform_instance_still_reported_when_override_sets_only_env(
        self,
    ) -> None:
        # Per-field: an override that sets env says nothing about
        # platform_instance, so the mapping's instance is still being dropped.
        source = self._source(
            connection_to_platform_map={_SNOWFLAKE_CONN_ID: {"env": "PROD"}},
        )
        assert self._warn_calls(source, self._detail(platform_instance="mi")) == 1

    def test_silent_when_override_supplies_both(self) -> None:
        source = self._source(
            connection_to_platform_map={
                _SNOWFLAKE_CONN_ID: {"env": "DEV", "platform_instance": "mi"}
            },
        )
        assert (
            self._warn_calls(source, self._detail(env="DEV", platform_instance="mi"))
            == 0
        )

    def test_silent_for_a_mapping_on_another_platform(self) -> None:
        source = self._source()
        assert (
            self._warn_calls(source, self._detail("postgres", platform_instance="mi"))
            == 0
        )

    def test_silent_when_mapping_adds_nothing(self) -> None:
        source = self._source()
        assert self._warn_calls(source, self._detail()) == 0

    def test_silent_without_a_mapping_for_this_element(self) -> None:
        source = self._source()
        assert self._warn_calls(source, None) == 0


class TestFallbackGate:
    """When the inode fallback fires, relative to the element's SQL.

    The gate is "SQL named no warehouse tables", which is wider than "the
    element has no SQL": the parser only runs when a
    chart_sources_platform_mapping entry matches the element's path. Both cases
    are pinned here so the distinction cannot drift silently again.
    """

    def _handle(
        self, source: SigmaSource, *, sql_named_tables: bool, in_tables: List[str]
    ) -> Dict[str, List[str]]:
        dataset_inputs: Dict[str, List[str]] = {}
        source._handle_dataset_upstream(
            upstream=DatasetUpstream(name="PETS dataset"),
            node_id="inode-url-1",
            element=Element(elementId="el-1", name="chart", url="http://x"),
            workbook=Workbook(
                workbookId="wb-1",
                name="WB",
                ownerId="u",
                createdBy="u",
                updatedBy="u",
                createdAt="2024-01-01T00:00:00Z",
                updatedAt="2024-01-01T00:00:00Z",
                url="http://x",
                path="ws",
                latestVersion=1,
            ),
            dataset_inputs=dataset_inputs,
            sql_parser_in_tables=in_tables,
            sql_named_tables=sql_named_tables,
        )
        return dataset_inputs

    def test_fallback_does_not_fire_when_sql_named_tables(self) -> None:
        # The element has working SQL. Resolving the dataset here would add a
        # second path to a table the chart already reaches directly.
        source = _make_source()
        with patch.object(source, "_resolve_dataset_warehouse_upstreams") as resolver:
            self._handle(
                source,
                sql_named_tables=True,
                in_tables=[
                    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"
                ],
            )
            resolver.assert_not_called()

    def test_fallback_fires_when_sql_named_nothing(self) -> None:
        # Either the element has no SQL (post-deprecation) or no platform
        # mapping matched, so the parser never ran. Both land here.
        source = _make_source()
        warehouse_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"
        with patch.object(
            source,
            "_resolve_dataset_warehouse_upstreams",
            return_value=[warehouse_urn],
        ) as resolver:
            dataset_inputs = self._handle(source, sql_named_tables=False, in_tables=[])
            resolver.assert_called_once()
        assert list(dataset_inputs.values()) == [[warehouse_urn]]

    def test_unresolved_fallback_leaves_inputs_untouched(self) -> None:
        # Matches pre-deprecation behaviour: the Sigma Dataset entered
        # ChartInfo.inputs only when its warehouse table resolved.
        source = _make_source()
        with patch.object(
            source, "_resolve_dataset_warehouse_upstreams", return_value=[]
        ):
            assert self._handle(source, sql_named_tables=False, in_tables=[]) == {}


class TestMigrationStatusProperty:
    """Sigma's per-dataset migration state, surfaced as a custom property."""

    def _dataset(self, **extra: object) -> SigmaDataset:
        return SigmaDataset.model_validate(
            {
                "datasetId": "ds-uuid-1",
                "name": "PETS",
                "description": "",
                "createdBy": "u",
                "createdAt": "2024-01-01T00:00:00Z",
                "updatedAt": "2024-01-01T00:00:00Z",
                "url": "https://app.sigmacomputing.com/org/b/urlid1",
                **extra,
            }
        )

    def _custom_properties(self, dataset: SigmaDataset) -> Dict[str, str]:
        source = _make_source()
        wu = source._gen_dataset_properties("urn:li:dataset:(x,y,PROD)", dataset)
        return wu.metadata.aspect.customProperties  # type: ignore[union-attr]

    @pytest.mark.parametrize("status", ["not-migrated", "not-required", "migrated"])
    def test_status_is_passed_through_verbatim(self, status: str) -> None:
        # Passed through as a string, so a status Sigma adds later needs no code
        # change here.
        props = self._custom_properties(self._dataset(migrationStatus=status))
        assert props["migrationStatus"] == status

    def test_absent_status_omits_the_property(self) -> None:
        # Keeps datasetProperties byte-identical on tenants that predate the
        # field, so no golden churn for them.
        assert "migrationStatus" not in self._custom_properties(self._dataset())


class TestNullUpstreamName:
    """A null upstream name must not block the inode route."""

    def _handle(
        self, source: SigmaSource, *, sql_named_tables: bool
    ) -> Dict[str, List[str]]:
        dataset_inputs: Dict[str, List[str]] = {}
        source._handle_dataset_upstream(
            upstream=DatasetUpstream(name=None),
            node_id="inode-url-1",
            element=Element(elementId="el-1", name="chart", url="http://x"),
            workbook=Workbook(
                workbookId="wb-1",
                name="WB",
                ownerId="u",
                createdBy="u",
                updatedBy="u",
                createdAt="2024-01-01T00:00:00Z",
                updatedAt="2024-01-01T00:00:00Z",
                url="http://x",
                path="ws",
                latestVersion=1,
            ),
            dataset_inputs=dataset_inputs,
            sql_parser_in_tables=[],
            sql_named_tables=sql_named_tables,
        )
        return dataset_inputs

    def test_null_name_with_no_sql_still_resolves(self) -> None:
        # The headline fix: the name is only needed for the SQL substring match,
        # so a null name must not stop the inode route. Sigma does send nulls.
        source = _make_source()
        warehouse_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s.t,PROD)"
        with patch.object(
            source,
            "_resolve_dataset_warehouse_upstreams",
            return_value=[warehouse_urn],
        ) as resolver:
            dataset_inputs = self._handle(source, sql_named_tables=False)
            resolver.assert_called_once()
        assert list(dataset_inputs.values()) == [[warehouse_urn]]

    def test_null_name_with_no_sql_is_not_reported(self) -> None:
        # Nothing was lost, so warning here would be pure noise post-deprecation.
        source = _make_source()
        with patch.object(
            source, "_resolve_dataset_warehouse_upstreams", return_value=[]
        ):
            with patch.object(source.reporter, "warning") as spy:
                self._handle(source, sql_named_tables=False)
                spy.assert_not_called()
        assert source.reporter.chart_dataset_upstream_name_missing == 0

    def test_null_name_with_sql_is_reported(self) -> None:
        # Here the name genuinely cost the SQL correlation.
        source = _make_source()
        with patch.object(source.reporter, "warning") as spy:
            self._handle(source, sql_named_tables=True)
            spy.assert_called_once()
        assert source.reporter.chart_dataset_upstream_name_missing == 1


class TestDatasetListingFailure:
    def test_unlisted_reason_names_the_listing_failure(self) -> None:
        # When /v2/datasets itself failed, the cause is the (deprecated) endpoint,
        # not workspace_pattern. The info must not send operators to the filter.
        source = _make_source()
        source.reporter.datasets_listing_failed = 1
        with patch.object(source.reporter, "info") as spy:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            spy.assert_called_once()
            assert "listing failed" in spy.call_args.kwargs["title"]

    def test_one_dropped_dataset_does_not_flip_the_diagnosis(self) -> None:
        # datasets_dropped_missing_file_metadata is a per-dataset signal, so it
        # must not re-label every unlisted dataset as a listing problem. Which
        # of the two applies cannot be told apart here: dropped datasets are
        # keyed by datasetId, which is exactly what an unlisted url_id lacks.
        source = _make_source()
        source.reporter.datasets_dropped_missing_file_metadata = 1
        with patch.object(source.reporter, "info") as spy:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            assert "workspace_pattern" in spy.call_args.kwargs["message"]

    def test_unlisted_reason_names_workspace_pattern_otherwise(self) -> None:
        source = _make_source()
        with patch.object(source.reporter, "info") as spy:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            assert "workspace_pattern" in spy.call_args.kwargs["message"]


class TestIdentifierCasing:
    """convert_urns_to_lowercase, and the table/column mirror.

    The flag is a no-op outside _WAREHOUSE_LOWERCASE_PLATFORMS unless the
    operator sets it, which is the only way back to the spelling the
    pre-deprecation SQL route produced. Table and column identifiers must always
    agree, or a schemaField URN pairs a lower-cased table with a mixed-case
    column.
    """

    REF = _WarehouseTableRef(
        connection_id="c", db="Analytics", schema="Public", table="Orders"
    )

    @pytest.mark.parametrize(
        ("platform", "lowercase", "explicit", "expected"),
        [
            # Untouched defaults: Snowflake lower-cases, others do not.
            ("snowflake", True, False, "analytics.public.orders"),
            ("postgres", True, False, "Analytics.Public.Orders"),
            # Explicitly set: the operator's choice wins on any platform.
            ("postgres", True, True, "analytics.public.orders"),
            ("postgres", False, True, "Analytics.Public.Orders"),
            ("snowflake", False, True, "Analytics.Public.Orders"),
        ],
    )
    def test_table_casing(
        self, platform: str, lowercase: bool, explicit: bool, expected: str
    ) -> None:
        assert (
            self.REF.fq_name(platform, lowercase=lowercase, explicit=explicit)
            == expected
        )

    @pytest.mark.parametrize("platform", ["snowflake", "postgres", "redshift"])
    @pytest.mark.parametrize(
        ("lowercase", "explicit"), [(True, False), (True, True), (False, True)]
    )
    def test_column_casing_mirrors_table_casing(
        self, platform: str, lowercase: bool, explicit: bool
    ) -> None:
        table = self.REF.fq_name(platform, lowercase=lowercase, explicit=explicit)
        column = _normalize_warehouse_identifier(
            "Order_Id", platform, lowercase, explicit=explicit
        )
        assert table.islower() == column.islower()


class TestCasingIsScopedPerRoute:
    """Which routes honour an explicitly-set convert_urns_to_lowercase.

    The helper-level mirror test above passes regardless of scoping, which is
    how a column-side leak on the Data Model route survived a review round. This
    asserts at the route level instead: opting a route in must move both its
    table and its column, and a route that has not opted in must move neither.
    """

    PLATFORM = "redshift"  # not in _WAREHOUSE_LOWERCASE_PLATFORMS

    def _source(self) -> SigmaSource:
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                # Explicitly set, which is what enables the behaviour at all.
                "connection_to_platform_map": {
                    _REDSHIFT_CONN_ID: {"convert_urns_to_lowercase": True}
                },
            }
        )
        ctx = PipelineContext(run_id="casing-scope-unit")
        with patch.object(SigmaAPI, "_generate_token"):
            with patch.object(SigmaAPI, "get_connections", return_value=[]):
                source = SigmaSource(config=config, ctx=ctx)
        source.connection_registry = SigmaConnectionRegistry(
            by_id={
                _REDSHIFT_CONN_ID: SigmaConnectionRecord(
                    connection_id=_REDSHIFT_CONN_ID,
                    name="Redshift",
                    sigma_type="redshift",
                    datahub_platform=self.PLATFORM,
                    is_mappable=True,
                )
            }
        )
        return source

    REF = _WarehouseTableRef(
        connection_id=_REDSHIFT_CONN_ID, db="Analytics", schema="Public", table="Orders"
    )

    def test_sigma_dataset_route_honours_the_flag(self) -> None:
        urn = self._source()._warehouse_ref_to_urn(self.REF, allow_explicit_case=True)
        assert urn is not None and "analytics.public.orders" in urn

    def test_other_routes_ignore_the_flag(self) -> None:
        # Default call, i.e. the DM element and workbook warehouse routes. On
        # base the flag was a no-op outside Snowflake; honouring it here would
        # move URNs those routes already emit.
        urn = self._source()._warehouse_ref_to_urn(self.REF)
        assert urn is not None and "Analytics.Public.Orders" in urn

    def test_data_model_column_route_ignores_the_flag(self) -> None:
        # The column side of the same route. Folding it while the table above
        # keeps its case would pair a preserved table with a lower-cased column
        # in one schemaField URN.
        assert (
            _normalize_warehouse_identifier("CustomerId", self.PLATFORM, True)
            == "CustomerId"
        )

    @pytest.mark.parametrize("platform", ["bigquery", "db2"])
    def test_case_sensitive_platforms_are_never_folded(self, platform: str) -> None:
        # Their identifiers are case-sensitive, so folding dangles the edge --
        # and the SQL route this emulates excluded them for the same reason.
        ref = _WarehouseTableRef(
            connection_id=_REDSHIFT_CONN_ID,
            db="My-Project",
            schema="Sales",
            table="Orders",
        )
        assert ref.fq_name(platform, lowercase=True, explicit=True) == (
            "My-Project.Sales.Orders"
        )
        assert (
            _normalize_warehouse_identifier("OrderId", platform, True, explicit=True)
            == "OrderId"
        )
