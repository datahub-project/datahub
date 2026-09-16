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
from datahub.ingestion.source.sigma.config import SigmaSourceConfig
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
        assert first == second
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
    """env / platform_instance set only on the legacy mapping is now ignored."""

    def _source(
        self, mapping: Dict[str, Dict[str, object]], **cfg: object
    ) -> SigmaSource:
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                "chart_sources_platform_mapping": mapping,
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

    def _warn_calls(self, source: SigmaSource, times: int = 1) -> int:
        """Number of reporter.warning CALLS, not report entries.

        StructuredLogs keys entries on title+message, so repeated identical
        warnings collapse into one entry -- counting entries would pass even
        with the dedup set removed.
        """
        with patch.object(
            source.reporter, "warning", wraps=source.reporter.warning
        ) as spy:
            for _ in range(times):
                source._warn_if_platform_mapping_env_ignored(self._ref())
            return spy.call_count

    def test_warns_once_when_platform_instance_only_on_mapping(self) -> None:
        source = self._source(
            {"ws/wb": {"data_source_platform": "snowflake", "platform_instance": "mi"}}
        )
        assert self._warn_calls(source, times=2) == 1

    def test_warns_when_env_set_explicitly(self) -> None:
        source = self._source(
            {"ws/wb": {"data_source_platform": "snowflake", "env": "DEV"}}
        )
        assert self._warn_calls(source) == 1

    def test_warns_when_env_only_defaulted_but_recipe_env_differs(self) -> None:
        # PlatformDetail.env defaults to PROD, so the URN diverges even though
        # the mapping never mentions env. The message must not claim it was set.
        source = self._source(
            {"ws/wb": {"data_source_platform": "snowflake"}}, env="DEV"
        )
        before = len(source.reporter.warnings)
        source._warn_if_platform_mapping_env_ignored(self._ref())
        assert len(source.reporter.warnings) == before + 1
        assert "defaulted" in str(list(source.reporter.warnings)[-1])

    def test_silent_for_a_mapping_on_another_platform(self) -> None:
        # A postgres-only mapping says nothing about a Snowflake connection.
        source = self._source(
            {"ws/wb": {"data_source_platform": "postgres", "platform_instance": "mi"}}
        )
        assert self._warn_calls(source) == 0

    def test_silent_when_mapping_adds_nothing(self) -> None:
        source = self._source({"ws/wb": {"data_source_platform": "snowflake"}})
        assert self._warn_calls(source) == 0

    def test_warns_when_override_sets_only_default_database(self) -> None:
        # An override that supplies neither env nor platform_instance leaves
        # both unconfigured for this connection, so the mapping's values are
        # still being ignored. WarehouseConnectionConfig inherits an env
        # default, so presence of the entry is not enough to tell.
        source = self._source(
            {"ws/wb": {"data_source_platform": "snowflake", "platform_instance": "mi"}},
            connection_to_platform_map={_SNOWFLAKE_CONN_ID: {"default_database": "DB"}},
        )
        assert self._warn_calls(source) == 1

    def test_silent_when_connection_has_an_override(self) -> None:
        source = self._source(
            {"ws/wb": {"data_source_platform": "snowflake", "platform_instance": "mi"}},
            connection_to_platform_map={_SNOWFLAKE_CONN_ID: {"env": "DEV"}},
        )
        assert self._warn_calls(source) == 0


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

    def test_unlisted_reason_names_workspace_pattern_otherwise(self) -> None:
        source = _make_source()
        with patch.object(source.reporter, "info") as spy:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            assert "workspace_pattern" in spy.call_args.kwargs["message"]
