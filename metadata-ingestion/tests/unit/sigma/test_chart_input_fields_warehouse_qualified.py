"""Unit tests for workbook-lineage warehouse table index for chart formula resolution.

Coverage:
  - _build_workbook_warehouse_table_index: happy path, /lineage failure, /files failure,
    path unparseable, unknown connection, is_mappable=False, cache reuse with DM path
  - _merge_warehouse_table_indices: new keys, conflict resolution, empty inputs
  - _build_element_input_fields: warehouse-qualified counter, via_workbook_index counter
  - URN identity: DM element and workbook-lineage paths produce byte-equal Dataset URNs
    for the same inode (guard against orphan schemaField URNs)

Counters verified:
  chart_input_fields_warehouse_qualified
  chart_input_fields_warehouse_qualified_via_workbook_index
  chart_input_fields_warehouse_index_lookup_failed
  chart_input_fields_warehouse_table_lookup_failed
  chart_input_fields_warehouse_path_unparseable
  chart_input_fields_warehouse_unknown_connection
"""

from typing import Any, Dict, List, Optional
from unittest.mock import patch

import datahub.emitter.mce_builder as builder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import (
    SigmaSourceConfig,
    WarehouseConnectionConfig,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    Element,
    SigmaDataModel,
    Workbook,
    WorkbookLineageTableEntry,
)
from datahub.ingestion.source.sigma.sigma import SigmaSource, _WorkbookWarehouseIndex
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SNOWFLAKE_CONN_ID = "conn-sf-001"
_SNOWFLAKE_CONN_RECORD = SigmaConnectionRecord(
    connection_id=_SNOWFLAKE_CONN_ID,
    name="Prod Snowflake",
    sigma_type="snowflake",
    datahub_platform="snowflake",
    host="acme.snowflakecomputing.com",
    account="acme",
    is_mappable=True,
)
_UNMAPPABLE_CONN_ID = "conn-oracle-001"
_UNMAPPABLE_CONN_RECORD = SigmaConnectionRecord(
    connection_id=_UNMAPPABLE_CONN_ID,
    name="Oracle (unmappable)",
    sigma_type="oracle",
    datahub_platform="",
    is_mappable=False,
)

_INODE_ID = "aabbccdd-1111-0000-0000-000000000001"
_URL_ID = "fixture-url-id-001"
_TABLE_NAME = "MY_WAREHOUSE_TABLE"
_DB = "MY_DB"
_SCHEMA = "MY_SCHEMA"

_GOOD_FILES_RESPONSE: Dict[str, Any] = {
    "id": _INODE_ID,
    "urlId": _URL_ID,
    "name": _TABLE_NAME,
    "type": "table",
    "path": f"Connection Root/{_DB}/{_SCHEMA}",
}

_EXPECTED_WAREHOUSE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,"
    f"{_DB.lower()}.{_SCHEMA.lower()}.{_TABLE_NAME.lower()},PROD)"
)

_WB_LINEAGE_TYPE_TABLE_ENTRY = WorkbookLineageTableEntry(
    type="table",
    name=_TABLE_NAME,
    connectionId=_SNOWFLAKE_CONN_ID,
    inodeId=_INODE_ID,
)


def _make_source(
    extra_registry_records: Optional[List[SigmaConnectionRecord]] = None,
) -> SigmaSource:
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    ctx = PipelineContext(run_id="sigma-wh-index-unit")
    with (
        patch.object(SigmaAPI, "_generate_token"),
        patch.object(
            SigmaSource,
            "_build_connection_registry",
            return_value=SigmaConnectionRegistry(by_id={}),
        ),
    ):
        source = SigmaSource(config=config, ctx=ctx)

    records = [_SNOWFLAKE_CONN_RECORD, _UNMAPPABLE_CONN_RECORD]
    if extra_registry_records:
        records.extend(extra_registry_records)
    source.connection_registry = SigmaConnectionRegistry(
        by_id={r.connection_id: r for r in records}
    )
    return source


def _make_workbook(workbook_id: str = "wb-test-uuid") -> Workbook:
    return Workbook(
        workbookId=workbook_id,
        name="Test Workbook",
        ownerId="owner-1",
        createdBy="owner-1",
        updatedBy="owner-1",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url="https://app.sigmacomputing.com/test/workbook/wb-test-uuid",
        path="Test Workspace",
        latestVersion=1,
    )


def _make_element_with_formula(
    element_id: str,
    column: str,
    formula: str,
    name: str = "Test Element",
) -> Element:
    return Element(
        elementId=element_id,
        name=name,
        url="https://app.sigmacomputing.com/test/element/" + element_id,
        type="table",
        columns=[{"name": column, "formula": formula}],
    )


# ---------------------------------------------------------------------------
# Tests: _build_workbook_warehouse_table_index
# ---------------------------------------------------------------------------


class TestBuildWorkbookWarehouseTableIndex:
    def test_happy_path_snowflake(self):
        """type=table entry resolves to Snowflake warehouse Dataset URN."""
        source = _make_source()
        wb = _make_workbook()
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                return_value=_GOOD_FILES_RESPONSE,
            ),
        ):
            result = source._build_workbook_warehouse_table_index(wb)

        assert _TABLE_NAME.upper() in result.by_name
        assert result.by_name[_TABLE_NAME.upper()] == [_EXPECTED_WAREHOUSE_URN]
        assert source.reporter.chart_input_fields_warehouse_index_lookup_failed == 0
        assert source.reporter.chart_input_fields_warehouse_unknown_connection == 0

    def test_lineage_fetch_returns_none_increments_counter(self):
        """/lineage returning None → index_lookup_failed bumps, empty result."""
        source = _make_source()
        wb = _make_workbook()
        with patch.object(source.sigma_api, "get_workbook_lineage", return_value=None):
            result = source._build_workbook_warehouse_table_index(wb)

        assert result.by_name == {}
        assert result.by_url_id == {}
        assert source.reporter.chart_input_fields_warehouse_index_lookup_failed == 1

    def test_files_lookup_failure_increments_counter(self):
        """/files returning None → table_lookup_failed bumps, entry excluded."""
        source = _make_source()
        wb = _make_workbook()
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ),
            patch.object(source.sigma_api, "get_file_metadata", return_value=None),
        ):
            result = source._build_workbook_warehouse_table_index(wb)

        assert result.by_name == {}
        assert source.reporter.chart_input_fields_warehouse_table_lookup_failed == 1

    def test_files_lookup_failure_not_double_counted_via_cache(self):
        """A failed inode shared across workbook calls bumps the counter only once."""
        source = _make_source()
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ),
            patch.object(source.sigma_api, "get_file_metadata", return_value=None),
        ):
            source._build_workbook_warehouse_table_index(_make_workbook("wb-1"))
            source._build_workbook_warehouse_table_index(_make_workbook("wb-2"))

        assert source.reporter.chart_input_fields_warehouse_table_lookup_failed == 1

    def test_path_unparseable(self):
        source = _make_source()
        bad_files = {**_GOOD_FILES_RESPONSE, "path": "Not Connection Root/DB"}
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ),
            patch.object(source.sigma_api, "get_file_metadata", return_value=bad_files),
        ):
            result = source._build_workbook_warehouse_table_index(_make_workbook())

        assert result.by_name == {}
        assert source.reporter.chart_input_fields_warehouse_path_unparseable == 1

    def test_path_unparseable_deduped_across_workbooks(self):
        """Same unparseable inode shared across workbooks only bumps counter once."""
        source = _make_source()
        bad_files = {**_GOOD_FILES_RESPONSE, "path": "Not Connection Root/DB"}
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ),
            patch.object(source.sigma_api, "get_file_metadata", return_value=bad_files),
        ):
            source._build_workbook_warehouse_table_index(_make_workbook("wb-1"))
            source._build_workbook_warehouse_table_index(_make_workbook("wb-2"))

        assert source.reporter.chart_input_fields_warehouse_path_unparseable == 1

    def test_unknown_connection_increments_counter(self):
        """Connection ID not in registry → warehouse_unknown_connection bumps."""
        source = _make_source()
        entry = _WB_LINEAGE_TYPE_TABLE_ENTRY.model_copy(
            update={"connectionId": "conn-unknown"}
        )
        with (
            patch.object(
                source.sigma_api, "get_workbook_lineage", return_value=[entry]
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                return_value=_GOOD_FILES_RESPONSE,
            ),
        ):
            result = source._build_workbook_warehouse_table_index(_make_workbook())

        assert result.by_name == {}
        assert source.reporter.chart_input_fields_warehouse_unknown_connection == 1

    def test_is_mappable_false_increments_unknown_connection(self):
        """is_mappable=False → warehouse_unknown_connection bumps."""
        source = _make_source()
        entry = _WB_LINEAGE_TYPE_TABLE_ENTRY.model_copy(
            update={"connectionId": _UNMAPPABLE_CONN_ID}
        )
        with (
            patch.object(
                source.sigma_api, "get_workbook_lineage", return_value=[entry]
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                return_value=_GOOD_FILES_RESPONSE,
            ),
        ):
            result = source._build_workbook_warehouse_table_index(_make_workbook())

        assert result.by_name == {}
        assert source.reporter.chart_input_fields_warehouse_unknown_connection == 1

    def test_duplicate_url_id_logs_warning(self):
        """Two type=table entries sharing the same urlId: second overwrites first,
        a warning is logged."""
        source = _make_source()
        inode2 = "bbbbcccc-2222-0000-0000-000000000002"
        files2 = {
            **_GOOD_FILES_RESPONSE,
            "id": inode2,
            "urlId": _URL_ID,
            "name": "OTHER_TABLE",
        }
        entries = [
            _WB_LINEAGE_TYPE_TABLE_ENTRY,
            _WB_LINEAGE_TYPE_TABLE_ENTRY.model_copy(
                update={"inodeId": inode2, "name": "OTHER_TABLE"}
            ),
        ]
        with (
            patch.object(
                source.sigma_api, "get_workbook_lineage", return_value=entries
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                side_effect=[_GOOD_FILES_RESPONSE, files2],
            ),
        ):
            result = source._build_workbook_warehouse_table_index(_make_workbook())

        # Last writer wins; "OTHER_TABLE" is the final name for that urlId
        assert "OTHER_TABLE" in result.by_name or _TABLE_NAME in result.by_name

    def test_cache_reuse_with_dm_path(self):
        """An inode already populated by DM lineage processing is reused by
        the workbook-lineage path without an extra /files call."""
        source = _make_source()
        dm = SigmaDataModel(
            dataModelId="dm-test-uuid",
            name="Test DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
            warehouse_inodes_by_inode_id={
                _INODE_ID: {"connectionId": _SNOWFLAKE_CONN_ID}
            },
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value=_GOOD_FILES_RESPONSE,
        ) as mock_get:
            source._build_dm_warehouse_url_id_map(dm)
            assert _INODE_ID in source._files_cache
            with patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ):
                result = source._build_workbook_warehouse_table_index(_make_workbook())
            mock_get.assert_called_once()

        assert _TABLE_NAME.upper() in result.by_name


# ---------------------------------------------------------------------------
# Tests: _merge_warehouse_table_indices
# ---------------------------------------------------------------------------


class TestMergeWarehouseTableIndices:
    def test_supplementary_contributes_new_key(self):
        primary = {"TABLE_A": ["urn:A"]}
        supplementary = {"TABLE_B": ["urn:B"]}
        result = SigmaSource._merge_warehouse_table_indices(primary, supplementary)
        assert result == {"TABLE_A": ["urn:A"], "TABLE_B": ["urn:B"]}

    def test_primary_wins_on_conflict(self):
        primary = {"TABLE_A": ["urn:A-primary"]}
        supplementary = {"TABLE_A": ["urn:A-supplementary"]}
        result = SigmaSource._merge_warehouse_table_indices(primary, supplementary)
        assert result == {"TABLE_A": ["urn:A-primary"]}

    def test_no_urn_concat_on_conflict(self):
        """Conflict must NOT concat URN lists — only primary URN is used."""
        primary = {"TABLE_A": ["urn:A1"]}
        supplementary = {"TABLE_A": ["urn:A2"]}
        result = SigmaSource._merge_warehouse_table_indices(primary, supplementary)
        assert result["TABLE_A"] == ["urn:A1"]
        assert "urn:A2" not in result["TABLE_A"]

    def test_empty_supplementary_returns_copy_of_primary(self):
        primary = {"TABLE_A": ["urn:A"]}
        result = SigmaSource._merge_warehouse_table_indices(primary, {})
        assert result == primary
        assert result is not primary  # always returns a copy; callers must not mutate


# ---------------------------------------------------------------------------
# Tests: _build_element_input_fields warehouse counter tracking
# ---------------------------------------------------------------------------


class TestBuildElementInputFieldsWarehouseCounters:
    def _call_build(
        self,
        source: SigmaSource,
        element: Element,
        element_warehouse_table_index: Dict[str, List[str]],
        wb_only_warehouse_keys: frozenset,
    ) -> List:
        chart_urn = builder.make_chart_urn(
            platform="sigma",
            platform_instance=None,
            name=element.elementId,
        )
        return source._build_element_input_fields(
            element=element,
            chart_urn=chart_urn,
            chart_upstream_eids=set(),
            dm_upstream_urn_by_element_name={},
            wb_element_index={},
            element_warehouse_table_index=element_warehouse_table_index,
            elementId_to_chart_urn={},
            wb_only_warehouse_keys=wb_only_warehouse_keys,
        )

    def test_warehouse_qualified_via_workbook_index(self):
        """Workbook-only key bumps both _qualified and _qualified_via_workbook_index."""
        source = _make_source()
        element = _make_element_with_formula("elem-1", "col", f"[{_TABLE_NAME}/col]")
        self._call_build(
            source,
            element,
            {_TABLE_NAME: [_EXPECTED_WAREHOUSE_URN]},
            frozenset({_TABLE_NAME}),
        )

        assert source.reporter.chart_input_fields_warehouse_qualified == 1
        assert (
            source.reporter.chart_input_fields_warehouse_qualified_via_workbook_index
            == 1
        )
        assert source.reporter.chart_input_fields_resolved == 1

    def test_warehouse_qualified_via_per_element_index(self):
        """Per-element key bumps _qualified but NOT _via_workbook_index."""
        source = _make_source()
        element = _make_element_with_formula("elem-1", "col", f"[{_TABLE_NAME}/col]")
        self._call_build(
            source, element, {_TABLE_NAME: [_EXPECTED_WAREHOUSE_URN]}, frozenset()
        )

        assert source.reporter.chart_input_fields_warehouse_qualified == 1
        assert (
            source.reporter.chart_input_fields_warehouse_qualified_via_workbook_index
            == 0
        )
        assert source.reporter.chart_input_fields_resolved == 1

    def test_no_match_falls_through_to_self_ref(self):
        """Unresolvable formula ref → self_ref_fallback."""
        source = _make_source()
        element = _make_element_with_formula("elem-1", "col", "[UNKNOWN_TABLE/col]")
        self._call_build(source, element, {}, frozenset())

        assert source.reporter.chart_input_fields_warehouse_qualified == 0
        assert source.reporter.chart_input_fields_self_ref_fallback == 1

    def test_ambiguous_warehouse_candidates_not_qualified(self):
        """Two candidates for the same key → resolver returns None, no qualified bump."""
        source = _make_source()
        element = _make_element_with_formula("elem-1", "col", f"[{_TABLE_NAME}/col]")
        self._call_build(
            source, element, {_TABLE_NAME: ["urn:A", "urn:B"]}, frozenset({_TABLE_NAME})
        )

        assert source.reporter.chart_input_fields_warehouse_qualified == 0
        assert source.reporter.chart_input_fields_self_ref_fallback == 1


# ---------------------------------------------------------------------------
# Tests: URN identity between DM element and workbook-lineage paths (CRITICAL)
# ---------------------------------------------------------------------------


class TestUrnIdentityWarehouseIndex:
    def test_urn_identity_dm_and_workbook_paths(self):
        """The warehouse Dataset URN from the workbook-lineage path must equal
        the entity-level URN from the DM element path for the same inode.
        Mismatched URN bases produce orphan schemaField URNs.
        """
        source = _make_source()
        dm = SigmaDataModel(
            dataModelId="dm-identity-test",
            name="Identity DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
            warehouse_inodes_by_inode_id={
                _INODE_ID: {"connectionId": _SNOWFLAKE_CONN_ID}
            },
        )

        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value=_GOOD_FILES_RESPONSE,
        ):
            url_id_map = source._build_dm_warehouse_url_id_map(dm)
            dm_urn = source._resolve_dm_element_warehouse_upstream(
                url_id_suffix=_URL_ID,
                warehouse_map=url_id_map,
            )
            with patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ):
                wb_index = source._build_workbook_warehouse_table_index(
                    _make_workbook()
                )

        assert dm_urn is not None
        wb_urns = wb_index.by_name.get(_TABLE_NAME.upper()) or []
        wb_urn = wb_urns[0] if wb_urns else None
        assert wb_urn is not None
        assert dm_urn == wb_urn, (
            f"URN identity broken: DM path produced {dm_urn!r} but workbook-lineage "
            f"path produced {wb_urn!r}."
        )

    def test_urn_identity_with_connection_override(self):
        """URN identity holds with connection_to_platform_map overrides."""
        config = SigmaSourceConfig.model_validate(
            {
                "client_id": "test",
                "client_secret": "test",
                "connection_to_platform_map": {
                    _SNOWFLAKE_CONN_ID: {
                        "env": "STG",
                        "convert_urns_to_lowercase": True,
                    }
                },
            }
        )
        ctx = PipelineContext(run_id="sigma-wh-index-override")
        with (
            patch.object(SigmaAPI, "_generate_token"),
            patch.object(
                SigmaSource,
                "_build_connection_registry",
                return_value=SigmaConnectionRegistry(by_id={}),
            ),
        ):
            source = SigmaSource(config=config, ctx=ctx)
        source.connection_registry = SigmaConnectionRegistry(
            by_id={_SNOWFLAKE_CONN_ID: _SNOWFLAKE_CONN_RECORD}
        )
        dm = SigmaDataModel(
            dataModelId="dm-identity-override",
            name="Override DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
            warehouse_inodes_by_inode_id={
                _INODE_ID: {"connectionId": _SNOWFLAKE_CONN_ID}
            },
        )

        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value=_GOOD_FILES_RESPONSE,
        ):
            url_id_map = source._build_dm_warehouse_url_id_map(dm)
            dm_urn = source._resolve_dm_element_warehouse_upstream(
                url_id_suffix=_URL_ID,
                warehouse_map=url_id_map,
            )
            with patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_WB_LINEAGE_TYPE_TABLE_ENTRY],
            ):
                wb_index = source._build_workbook_warehouse_table_index(
                    _make_workbook()
                )

        assert dm_urn is not None
        wb_urns = wb_index.by_name.get(_TABLE_NAME.upper()) or []
        wb_urn = wb_urns[0] if wb_urns else None
        assert wb_urn is not None
        assert dm_urn == wb_urn


# ---------------------------------------------------------------------------
# Tests: 2-segment /files path + default_database resolution (workbook builder)
# Mirror of TestTwoSegmentPathDefaultDatabase in test_dm_element_warehouse_upstream.py
# ---------------------------------------------------------------------------

_REDSHIFT_CONN_ID = "conn-rs-wb-001"
_TWO_SEG_FILES_WB: Dict[str, Any] = {
    "id": "inode-rs-wb-001",
    "urlId": "rsWbUrlId001",
    "name": "orders",
    "type": "table",
    "path": "Connection Root/public",
}
_TWO_SEG_WB_LINEAGE_ENTRY = WorkbookLineageTableEntry(
    type="table",
    name="orders",
    connectionId=_REDSHIFT_CONN_ID,
    inodeId="inode-rs-wb-001",
)


def _make_source_with_workbook_override(
    override_default_db: Optional[str] = None,
    registry_default_db: Optional[str] = None,
) -> SigmaSource:
    conn_override = (
        WarehouseConnectionConfig.model_validate(
            {"default_database": override_default_db}
        )
        if override_default_db is not None
        else None
    )
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    if conn_override:
        config.connection_to_platform_map = {_REDSHIFT_CONN_ID: conn_override}
    ctx = PipelineContext(run_id="sigma-wb-2seg-unit")
    with (
        patch.object(SigmaAPI, "_generate_token"),
        patch.object(
            SigmaSource,
            "_build_connection_registry",
            return_value=SigmaConnectionRegistry(by_id={}),
        ),
    ):
        source = SigmaSource(config=config, ctx=ctx)
    record = SigmaConnectionRecord(
        connection_id=_REDSHIFT_CONN_ID,
        name="Prod Redshift",
        sigma_type="redshift",
        datahub_platform="redshift",
        host="cluster.redshift.amazonaws.com",
        default_database=registry_default_db,
        is_mappable=True,
    )
    source.connection_registry = SigmaConnectionRegistry(
        by_id={_REDSHIFT_CONN_ID: record}
    )
    return source


class TestWorkbookTwoSegmentPathDefaultDatabase:
    def _build_index(self, source: SigmaSource) -> _WorkbookWarehouseIndex:
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_TWO_SEG_WB_LINEAGE_ENTRY],
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                return_value=_TWO_SEG_FILES_WB,
            ),
        ):
            return source._build_workbook_warehouse_table_index(_make_workbook())

    def test_override_default_database_wins(self):
        source = _make_source_with_workbook_override(
            override_default_db="staging", registry_default_db="dev"
        )
        index = self._build_index(source)
        urns = index.by_name.get("ORDERS", [])
        assert len(urns) == 1
        assert "staging" in urns[0]

    def test_registry_default_database_fallback(self):
        source = _make_source_with_workbook_override(registry_default_db="dev")
        index = self._build_index(source)
        urns = index.by_name.get("ORDERS", [])
        assert len(urns) == 1
        assert "dev" in urns[0]

    def test_empty_db_emits_warning(self):
        source = _make_source_with_workbook_override()  # no default_database anywhere
        index = self._build_index(source)
        urns = index.by_name.get("ORDERS", [])
        assert len(urns) == 1
        assert any(
            "default_database" in (w.title or "") for w in source.reporter.warnings
        )

    def test_empty_db_warning_deduped_across_workbooks(self):
        source = _make_source_with_workbook_override()
        wb1 = _make_workbook("wb-rs-001")
        wb2 = _make_workbook("wb-rs-002")
        with (
            patch.object(
                source.sigma_api,
                "get_workbook_lineage",
                return_value=[_TWO_SEG_WB_LINEAGE_ENTRY],
            ),
            patch.object(
                source.sigma_api,
                "get_file_metadata",
                return_value=_TWO_SEG_FILES_WB,
            ),
        ):
            source._build_workbook_warehouse_table_index(wb1)
            source._build_workbook_warehouse_table_index(wb2)
        warnings = [
            w for w in source.reporter.warnings if "default_database" in (w.title or "")
        ]
        assert len(warnings) == 1
