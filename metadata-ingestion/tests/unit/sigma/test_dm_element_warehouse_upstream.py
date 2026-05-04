"""Unit tests for DM element -> warehouse table UpstreamLineage resolution.

Coverage:
  - _build_dm_warehouse_url_id_map: happy path, /files failure, path unparseable
  - _resolve_dm_element_warehouse_upstream: success (Snowflake), unknown
    connection (including unmappable platform), ref not in map
  - _gen_data_model_element_upstream_lineage: warehouse-only upstream,
    SD upstream only, co-emit (both SD + warehouse), empty registry,
    unresolved counter not double-bumped when warehouse resolves

Counters verified:
  dm_element_warehouse_upstream_emitted
  dm_element_warehouse_unknown_connection
  dm_element_warehouse_table_lookup_failed
  dm_element_warehouse_path_unparseable
"""

from typing import Any, Dict, List, Optional
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.connection_registry import (
    SigmaConnectionRecord,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    SigmaDataModel,
    SigmaDataModelElement,
)
from datahub.ingestion.source.sigma.sigma import (
    SigmaSource,
    _WarehouseTableRef,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage

# ---------------------------------------------------------------------------
# Helpers
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
    name="Oracle (unsupported)",
    sigma_type="oracle",
    datahub_platform="",
    is_mappable=False,
)

_GOOD_FILES_RESPONSE: Dict[str, Any] = {
    "id": "f09fe362-828a-42e6-9f8f-3f0feeb2fb3e",
    "urlId": "7k3e6T4RK9oix71Nm2umE6",
    "name": "CUSTOMERS",
    "type": "table",
    "path": "Connection Root/WAREHOUSE_COFFEE_COMPANY/PUBLIC",
}


def _make_source(
    extra_registry_records: Optional[List[SigmaConnectionRecord]] = None,
) -> SigmaSource:
    config = SigmaSourceConfig.model_validate(
        {"client_id": "test", "client_secret": "test"}
    )
    ctx = PipelineContext(run_id="t4b-unit")
    with patch.object(SigmaAPI, "_generate_token"):
        source = SigmaSource(config=config, ctx=ctx)

    # Inject a controlled connection registry.
    records = [_SNOWFLAKE_CONN_RECORD, _UNMAPPABLE_CONN_RECORD]
    if extra_registry_records:
        records.extend(extra_registry_records)
    source.connection_registry = SigmaConnectionRegistry(
        by_id={r.connection_id: r for r in records}
    )
    return source


def _make_dm_with_inodes(inodes: Dict[str, Dict[str, str]]) -> SigmaDataModel:
    """Build a minimal SigmaDataModel pre-loaded with warehouse_inodes."""
    return SigmaDataModel(
        dataModelId="dm-test-uuid",
        name="Test DM",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        warehouse_inodes_by_inode_id=inodes,
    )


# ---------------------------------------------------------------------------
# Tests: _build_dm_warehouse_url_id_map
# ---------------------------------------------------------------------------


class TestBuildDmWarehouseUrlIdMap:
    def test_happy_path_snowflake(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {
                "f09fe362-828a-42e6-9f8f-3f0feeb2fb3e": {
                    "connectionId": _SNOWFLAKE_CONN_ID,
                    "name": "CUSTOMERS",
                }
            }
        )
        with patch.object(
            source.sigma_api, "get_file_metadata", return_value=_GOOD_FILES_RESPONSE
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert "7k3e6T4RK9oix71Nm2umE6" in result
        ref = result["7k3e6T4RK9oix71Nm2umE6"]
        assert ref.connection_id == _SNOWFLAKE_CONN_ID
        assert ref.db == "WAREHOUSE_COFFEE_COMPANY"
        assert ref.schema == "PUBLIC"
        assert ref.table == "CUSTOMERS"

    def test_cache_deduplicates_files_calls(self):
        source = _make_source()
        inode_id = "f09fe362-828a-42e6-9f8f-3f0feeb2fb3e"
        inodes = {inode_id: {"connectionId": _SNOWFLAKE_CONN_ID, "name": "CUSTOMERS"}}
        dm1 = _make_dm_with_inodes(inodes)
        dm2 = _make_dm_with_inodes(inodes)
        with patch.object(
            source.sigma_api, "get_file_metadata", return_value=_GOOD_FILES_RESPONSE
        ) as mock_get:
            source._build_dm_warehouse_url_id_map(dm1)
            source._build_dm_warehouse_url_id_map(dm2)
            mock_get.assert_called_once()

    def test_files_lookup_failure_increments_counter(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"bad-inode": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "TABLE"}}
        )
        with patch.object(source.sigma_api, "get_file_metadata", return_value=None):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_table_lookup_failed == 1

    def test_path_unparseable_fewer_than_3_segments(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"inode-1": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "data.csv"}}
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value={
                "id": "inode-1",
                "urlId": "url-1",
                "name": "data.csv",
                "path": "Acryl Workspace",
            },
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_path_unparseable == 1

    def test_unrecognised_path_root_counts_as_unparseable(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"inode-1": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "MY_TABLE"}}
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value={
                "id": "inode-1",
                "urlId": "url-1",
                "name": "MY_TABLE",
                "path": "Connexion racine/DB/SCHEMA",
            },
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_path_unparseable == 1

    def test_empty_url_id_counts_as_path_unparseable(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"inode-1": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "TABLE"}}
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value={
                "id": "inode-1",
                "urlId": "",
                "name": "TABLE",
                "path": "Connection Root/DB/SCHEMA",
            },
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_path_unparseable == 1

    def test_path_unparseable_more_than_3_segments(self):
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"inode-1": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "TABLE"}}
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value={
                "id": "inode-1",
                "urlId": "url-1",
                "name": "TABLE",
                "path": "Connection Root/DB/SCHEMA/SUBPATH",
            },
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_path_unparseable == 1

    def test_path_unparseable_empty_segment(self):
        # B2: "Connection Root//PUBLIC" passes len==3 but has an empty DB segment.
        source = _make_source()
        dm = _make_dm_with_inodes(
            {"inode-1": {"connectionId": _SNOWFLAKE_CONN_ID, "name": "TABLE"}}
        )
        with patch.object(
            source.sigma_api,
            "get_file_metadata",
            return_value={
                "id": "inode-1",
                "urlId": "url-1",
                "name": "TABLE",
                "path": "Connection Root//PUBLIC",
            },
        ):
            result = source._build_dm_warehouse_url_id_map(dm)

        assert result == {}
        assert source.reporter.dm_element_warehouse_path_unparseable == 1


# ---------------------------------------------------------------------------
# Tests: _resolve_dm_element_warehouse_upstream
# ---------------------------------------------------------------------------


class TestResolveDmElementWarehouseUpstream:
    def test_success_snowflake_lowercase(self):
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="WAREHOUSE_COFFEE_COMPANY",
            schema="PUBLIC",
            table="CUSTOMERS",
        )
        warehouse_map = {"7k3e6T4RK9oix71Nm2umE6": ref}

        urn = source._resolve_dm_element_warehouse_upstream(
            url_id_suffix="7k3e6T4RK9oix71Nm2umE6",
            warehouse_map=warehouse_map,
        )

        assert (
            urn
            == "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse_coffee_company.public.customers,PROD)"
        )
        # Counter is bumped by caller post-dedup; resolver itself does not bump.
        assert source.reporter.dm_element_warehouse_upstream_emitted == 0
        assert source.reporter.dm_element_warehouse_unknown_connection == 0

    def test_not_in_warehouse_map_returns_none(self):
        source = _make_source()

        urn = source._resolve_dm_element_warehouse_upstream(
            url_id_suffix="not-in-map",
            warehouse_map={},
        )

        assert urn is None

    def test_unknown_connection_id(self):
        # unknown_connection counter is bumped by the caller (post-dedup);
        # the resolver itself just returns None.
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id="conn-unknown",
            db="DB",
            schema="SCHEMA",
            table="TABLE",
        )

        urn = source._resolve_dm_element_warehouse_upstream(
            url_id_suffix="url-1",
            warehouse_map={"url-1": ref},
        )

        assert urn is None
        assert source.reporter.dm_element_warehouse_unknown_connection == 0

    def test_unmappable_platform_counts_as_unknown_connection(self):
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id=_UNMAPPABLE_CONN_ID,
            db="DB",
            schema="SCHEMA",
            table="TABLE",
        )

        urn = source._resolve_dm_element_warehouse_upstream(
            url_id_suffix="url-1",
            warehouse_map={"url-1": ref},
        )

        assert urn is None
        assert source.reporter.dm_element_warehouse_unknown_connection == 0


# ---------------------------------------------------------------------------
# Tests: _gen_data_model_element_upstream_lineage (end-to-end wiring)
# ---------------------------------------------------------------------------


def _make_minimal_element(
    element_id: str = "elem-1",
    source_ids: Optional[List[str]] = None,
) -> SigmaDataModelElement:
    return SigmaDataModelElement(
        elementId=element_id,
        name="My Element",
        source_ids=source_ids or [],
    )


class TestUpstreamLineageWarehouseWiring:
    def _run(
        self,
        source: SigmaSource,
        element: SigmaDataModelElement,
        dm: SigmaDataModel,
        warehouse_url_id_map: Optional[Dict] = None,
    ) -> Optional[UpstreamLineage]:
        element_urn = source._gen_data_model_element_urn(dm, element)
        return source._gen_data_model_element_upstream_lineage(
            element,
            dm,
            element_urn,
            {},
            {},
            warehouse_url_id_map or {},
        )

    def test_warehouse_only_upstream_emitted(self):
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="WAREHOUSE_COFFEE_COMPANY",
            schema="PUBLIC",
            table="CUSTOMERS",
        )
        warehouse_map = {"7k3e6T4RK9oix71Nm2umE6": ref}
        element = _make_minimal_element(source_ids=["inode-7k3e6T4RK9oix71Nm2umE6"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, warehouse_map)

        assert lineage is not None
        upstream_urns = [u.dataset for u in lineage.upstreams]
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,warehouse_coffee_company.public.customers,PROD)"
            in upstream_urns
        )
        assert source.reporter.dm_element_warehouse_upstream_emitted == 1
        assert source.reporter.data_model_element_upstreams_unresolved == 0

    def test_sd_only_upstream_no_warehouse_map(self):
        source = _make_source()
        # Simulate a Sigma Dataset url_id that resolves via the SD map.
        source.sigma_dataset_urn_by_url_id["some-sd-url-id"] = (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,some-sd-url-id,PROD)"
        )
        element = _make_minimal_element(source_ids=["inode-some-sd-url-id"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, {})

        assert lineage is not None
        upstream_urns = [u.dataset for u in lineage.upstreams]
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,some-sd-url-id,PROD)"
            in upstream_urns
        )
        assert source.reporter.dm_element_warehouse_upstream_emitted == 0
        assert source.reporter.data_model_element_external_upstreams == 1
        assert source.reporter.data_model_element_upstreams_unresolved == 0

    def test_co_emit_sd_and_warehouse_both_in_lineage(self):
        """When the same inode is in both the SD map and the warehouse map,
        both URNs are included in a single UpstreamLineage aspect."""
        source = _make_source()
        url_id = "shared-url-id"
        source.sigma_dataset_urn_by_url_id[url_id] = (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,shared-url-id,PROD)"
        )
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="DB",
            schema="SCH",
            table="TBL",
        )
        warehouse_map = {url_id: ref}
        element = _make_minimal_element(source_ids=[f"inode-{url_id}"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, warehouse_map)

        assert lineage is not None
        upstream_urns = [u.dataset for u in lineage.upstreams]
        assert len(upstream_urns) == 2
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:sigma,shared-url-id,PROD)"
            in upstream_urns
        )
        assert (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.tbl,PROD)"
            in upstream_urns
        )
        assert source.reporter.dm_element_warehouse_upstream_emitted == 1
        assert source.reporter.data_model_element_upstreams_unresolved == 0

    def test_unresolved_counter_not_bumped_when_warehouse_resolves(self):
        """If the SD resolver returns None but warehouse resolves, the
        unresolved counters must NOT increment."""
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="DB",
            schema="SCH",
            table="TBL",
        )
        warehouse_map = {"url-wh": ref}
        element = _make_minimal_element(source_ids=["inode-url-wh"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        self._run(source, element, dm, warehouse_map)

        assert source.reporter.data_model_element_upstreams_unresolved == 0
        assert source.reporter.data_model_element_upstreams_unresolved_external == 0

    def test_both_unresolved_when_neither_sd_nor_warehouse_match(self):
        source = _make_source()
        element = _make_minimal_element(source_ids=["inode-totally-unknown"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, {})

        assert lineage is None
        assert source.reporter.data_model_element_upstreams_unresolved == 1
        assert source.reporter.data_model_element_upstreams_unresolved_external == 1

    def test_empty_registry_degrades_silently(self):
        """An empty connection registry (e.g. registry fetch failed) must not
        crash; all warehouse upstreams degrade to unknown_connection counters."""
        source = _make_source()
        source.connection_registry = SigmaConnectionRegistry()  # empty
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="DB",
            schema="SCH",
            table="TBL",
        )
        warehouse_map = {"url-1": ref}
        element = _make_minimal_element(source_ids=["inode-url-1"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, warehouse_map)

        assert lineage is None
        assert source.reporter.dm_element_warehouse_unknown_connection == 1
        assert source.reporter.dm_element_warehouse_upstream_emitted == 0

    def test_diamond_source_ids_emit_once(self):
        """Two source_ids resolving to the same warehouse URN produce one
        upstream entry and bump dm_element_warehouse_upstream_emitted once."""
        source = _make_source()
        ref = _WarehouseTableRef(
            connection_id=_SNOWFLAKE_CONN_ID,
            db="DB",
            schema="SCH",
            table="TBL",
        )
        # Both url-a and url-b map to the same ref -> same resolved URN.
        warehouse_map = {"url-a": ref, "url-b": ref}
        element = _make_minimal_element(source_ids=["inode-url-a", "inode-url-b"])
        dm = SigmaDataModel(
            dataModelId="dm-1",
            name="DM",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )

        lineage = self._run(source, element, dm, warehouse_map)

        assert lineage is not None
        assert len(lineage.upstreams) == 1
        assert source.reporter.dm_element_warehouse_upstream_emitted == 1


# ---------------------------------------------------------------------------
# Tests: sigma_api.py _assemble_data_model entry guard (#5)
# ---------------------------------------------------------------------------


class TestWarehouseInodeEntryGuard:
    def test_missing_name_or_inode_counts_incomplete(self):
        config = SigmaSourceConfig.model_validate(
            {"client_id": "x", "client_secret": "y"}
        )
        report = SigmaSourceReport()
        with patch.object(SigmaAPI, "_generate_token"):
            api = SigmaAPI(config, report)

        dm = SigmaDataModel(
            dataModelId="dm-x",
            name="X",
            createdAt="2024-01-01T00:00:00Z",
            updatedAt="2024-01-01T00:00:00Z",
        )
        lineage_entries = [
            # empty name — should be rejected, counter bumped
            {
                "type": "table",
                "inodeId": "inode-1",
                "connectionId": "conn-1",
                "name": "",
            },
            # empty inodeId — should be rejected, counter bumped
            {"type": "table", "inodeId": "", "connectionId": "conn-1", "name": "TABLE"},
            # valid
            {
                "type": "table",
                "inodeId": "inode-2",
                "connectionId": "conn-1",
                "name": "GOOD",
            },
        ]
        with (
            patch.object(api, "_get_data_model_elements", return_value=[]),
            patch.object(api, "_get_data_model_columns", return_value=[]),
            patch.object(
                api, "_get_data_model_lineage_entries", return_value=lineage_entries
            ),
        ):
            api._assemble_data_model(dm, file_meta=None)

        assert report.dm_element_warehouse_table_entry_incomplete == 2
        assert list(dm.warehouse_inodes_by_inode_id.keys()) == ["inode-2"]


# ---------------------------------------------------------------------------
# Tests: _WarehouseTableRef.fq_name normalization
# ---------------------------------------------------------------------------


class TestWarehouseTableRefFqName:
    def test_snowflake_lowercased(self):
        ref = _WarehouseTableRef(
            connection_id="c",
            db="WAREHOUSE_COFFEE_COMPANY",
            schema="PUBLIC",
            table="CUSTOMERS",
        )
        assert ref.fq_name("snowflake") == "warehouse_coffee_company.public.customers"

    def test_bigquery_preserves_case(self):
        ref = _WarehouseTableRef(
            connection_id="c", db="myProject", schema="myDataset", table="myTable"
        )
        assert ref.fq_name("bigquery") == "myProject.myDataset.myTable"

    def test_unvalidated_platform_preserves_case(self):
        # Platforms not in _WAREHOUSE_LOWERCASE_PLATFORMS preserve the casing
        # Sigma reports until their DataHub source's URN convention is verified.
        ref = _WarehouseTableRef(
            connection_id="c", db="DEV", schema="PUBLIC", table="ORDERS"
        )
        assert ref.fq_name("redshift") == "DEV.PUBLIC.ORDERS"
