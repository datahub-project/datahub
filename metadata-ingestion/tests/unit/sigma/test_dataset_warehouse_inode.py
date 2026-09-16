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
from datahub.ingestion.source.sigma.data_classes import ConnectionPath
from datahub.ingestion.source.sigma.sigma import SigmaSource
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI

_SNOWFLAKE_CONN_ID = "conn-snowflake-001"
_REDSHIFT_CONN_ID = "conn-redshift-001"


def _make_source(redshift_default_db: Optional[str] = None) -> SigmaSource:
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    ctx = PipelineContext(run_id="dataset-inode-unit")
    with patch.object(SigmaAPI, "_generate_token"):
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
        warnings_before = len(source.reporter.warnings)
        with patch.object(
            source.sigma_api,
            "get_connection_path",
            return_value=ConnectionPath(
                connection_id=_REDSHIFT_CONN_ID, path=["public", "orders"]
            ),
        ):
            ref = source._resolve_inode_to_warehouse_ref("inode-1")
            source._connection_path_cache.clear()
            source._resolve_inode_to_warehouse_ref("inode-2")
        assert ref is not None
        assert ref.db is None
        assert len(source.reporter.warnings) == warnings_before + 1

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
    def test_unlisted_dataset_warns_and_skips(self) -> None:
        # The dataset is referenced by an element but absent from /v2/datasets,
        # usually because workspace_pattern excludes its workspace. The old SQL
        # route did not need the listing, so this is a new way to lose lineage
        # and deserves a warning rather than a debug line.
        source = _make_source()
        warnings_before = len(source.reporter.warnings)
        with patch.object(source.sigma_api, "get_dataset_sources") as mocked:
            assert source._get_dataset_warehouse_refs("unknown-url-id") == []
            mocked.assert_not_called()
        assert source.reporter.dataset_warehouse_unlisted_dataset == 1
        assert len(source.reporter.warnings) == warnings_before + 1

    @pytest.mark.parametrize(
        "entries",
        [
            [],
            [{"type": "dataset", "inodeId": "inode-1"}],
            [{"type": "table"}],  # no inodeId
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

    def _source_with_mapping(self, **platform_detail: object) -> SigmaSource:
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                "chart_sources_platform_mapping": {
                    "ws/wb": {"data_source_platform": "snowflake", **platform_detail}
                },
            }
        )
        ctx = PipelineContext(run_id="mapping-env-unit")
        with patch.object(SigmaAPI, "_generate_token"):
            source = SigmaSource(config=config, ctx=ctx)
        source.connection_registry = SigmaConnectionRegistry(by_id={})
        return source

    def test_warns_once_when_platform_instance_only_on_mapping(self) -> None:
        # Pre-deprecation these edges took env/platform_instance from the
        # mapping; the registry route does not, so the URN silently changes.
        source = self._source_with_mapping(platform_instance="myinst")
        before = len(source.reporter.warnings)
        source._warn_if_platform_mapping_env_ignored("conn-1")
        source._warn_if_platform_mapping_env_ignored("conn-1")
        assert len(source.reporter.warnings) == before + 1

    def test_warns_when_env_differs_from_recipe(self) -> None:
        source = self._source_with_mapping(env="DEV")
        before = len(source.reporter.warnings)
        source._warn_if_platform_mapping_env_ignored("conn-1")
        assert len(source.reporter.warnings) == before + 1

    def test_silent_when_mapping_adds_nothing(self) -> None:
        source = self._source_with_mapping()
        before = len(source.reporter.warnings)
        source._warn_if_platform_mapping_env_ignored("conn-1")
        assert len(source.reporter.warnings) == before

    def test_silent_when_connection_has_an_override(self) -> None:
        # connection_to_platform_map governs this route, so nothing is ignored.
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                "chart_sources_platform_mapping": {
                    "ws/wb": {
                        "data_source_platform": "snowflake",
                        "platform_instance": "myinst",
                    }
                },
                "connection_to_platform_map": {"conn-1": {"env": "DEV"}},
            }
        )
        ctx = PipelineContext(run_id="mapping-env-unit")
        with patch.object(SigmaAPI, "_generate_token"):
            source = SigmaSource(config=config, ctx=ctx)
        before = len(source.reporter.warnings)
        source._warn_if_platform_mapping_env_ignored("conn-1")
        assert len(source.reporter.warnings) == before
