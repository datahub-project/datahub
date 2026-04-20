from typing import Any
from unittest.mock import patch

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.informatica.config import InformaticaSourceConfig
from datahub.ingestion.source.informatica.models import (
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    InformaticaLoginError,
    LineageTable,
    MappingLineageInfo,
)
from datahub.ingestion.source.informatica.source import InformaticaSource
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob


def _make_source(**config_overrides: Any) -> InformaticaSource:
    defaults = {
        "username": "svc",
        "password": "pw",
        "login_url": "https://dm-us.informaticacloud.com",
    }
    defaults.update(config_overrides)
    config = InformaticaSourceConfig.parse_obj(defaults)
    ctx = PipelineContext(run_id="informatica-test")
    return InformaticaSource(config, ctx)


class TestPathHelpers:
    def test_resolve_parent_project_from_standard_path(self):
        assert (
            InformaticaSource._resolve_parent_project_from_path(
                "/Explore/AnalyticsProject/Folder1"
            )
            == "AnalyticsProject"
        )

    def test_resolve_parent_project_empty_path(self):
        assert InformaticaSource._resolve_parent_project_from_path("") == ""

    def test_is_bundle_object_by_path(self):
        obj = IdmcObject(
            id="1", name="X", path="Add-On Bundles/Snowflake", object_type="Folder"
        )
        assert InformaticaSource._is_bundle_object(obj) is True

    def test_is_bundle_object_by_updated_by(self):
        obj = IdmcObject(
            id="1",
            name="X",
            path="/Explore/P",
            object_type="DTEMPLATE",
            updated_by="bundle-license-notifier",
        )
        assert InformaticaSource._is_bundle_object(obj) is True

    def test_project_flow_id_shared_between_mapping_and_lineage(self):
        assert InformaticaSource._project_flow_id("Sales") == "project:Sales"
        assert InformaticaSource._project_flow_id("") == "project:default"


class TestTagFilterAndIteration:
    def test_returns_none_sentinel_when_no_tags(self):
        source = _make_source()
        assert source._tag_filters_or_none() == [None]

    def test_returns_tags_when_configured(self):
        source = _make_source(tag_filter_names=["pii", "critical"])
        assert source._tag_filters_or_none() == ["pii", "critical"]

    def test_iter_with_tags_dedupes_by_id(self):
        source = _make_source(tag_filter_names=["a", "b"])
        obj1 = IdmcObject(id="1", name="x", path="/Explore/P", object_type="Project")
        obj2 = IdmcObject(id="2", name="y", path="/Explore/P", object_type="Project")
        with patch.object(source.client, "list_objects") as m:
            m.side_effect = lambda object_type, tag=None: (
                iter([obj1, obj2]) if tag == "a" else iter([obj1])
            )
            results = list(source._iter_with_tags("Project"))
        assert [o.id for o in results] == ["1", "2"]


class TestConnectionPlatformResolution:
    def test_override_takes_priority(self):
        source = _make_source(connection_type_overrides={"01A": "snowflake"})
        conn = IdmcConnection(id="01A", name="c", conn_type="Oracle", base_type="")
        assert source._resolve_connection_platform(conn) == "snowflake"

    def test_conn_type_maps_to_platform(self):
        source = _make_source()
        conn = IdmcConnection(
            id="x", name="c", conn_type="Snowflake_Cloud_Data_Warehouse"
        )
        assert source._resolve_connection_platform(conn) == "snowflake"

    def test_base_type_fallback(self):
        source = _make_source()
        conn = IdmcConnection(id="x", name="c", conn_type="", base_type="Oracle")
        assert source._resolve_connection_platform(conn) == "oracle"

    def test_unknown_returns_none(self):
        source = _make_source()
        conn = IdmcConnection(
            id="x", name="c", conn_type="DoesNotExist", base_type="AlsoMissing"
        )
        assert source._resolve_connection_platform(conn) is None


class TestTableUrnResolution:
    def test_urn_includes_database_schema_table(self):
        source = _make_source()
        conn = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            schema="PUBLIC",
        )
        source._connections_by_fed_id["fed"] = conn
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(
                table_name="ORDERS", schema_name="SALES", connection_federated_id="fed"
            )
        )
        assert urn is not None
        assert "snowflake" in urn
        assert "DWH.SALES.ORDERS" in urn

    def test_schema_falls_back_to_connection_schema(self):
        source = _make_source()
        conn = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Oracle",
            federated_id="fed",
            database="",
            schema="PUBLIC",
        )
        source._connections_by_fed_id["fed"] = conn
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(table_name="T", connection_federated_id="fed")
        )
        assert urn is not None
        assert "PUBLIC.T" in urn

    def test_unknown_connection_reports_and_returns_none(self):
        source = _make_source()
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(table_name="T", connection_federated_id="missing")
        )
        assert urn is None
        assert any("missing" in entry for entry in source.report.connections_unresolved)


class TestEntityEmission:
    def test_make_project_container(self):
        source = _make_source()
        proj = IdmcObject(
            id="p1",
            name="Sales",
            path="/Explore/Sales",
            object_type="Project",
            description="sales org",
            updated_by="alice",
        )
        container = source._make_project_container(proj)
        assert isinstance(container, Container)
        assert str(container.urn).startswith("urn:li:container:")

    def test_make_folder_container_has_parent_key(self):
        source = _make_source()
        folder = IdmcObject(
            id="f1",
            name="Mappings",
            path="/Explore/Sales/Mappings",
            object_type="Folder",
        )
        container = source._make_folder_container(folder)
        assert isinstance(container, Container)

    def test_make_taskflow_dataflow(self):
        source = _make_source(platform_instance="prod")
        tf = IdmcObject(
            id="tf-1",
            name="daily_refresh",
            path="/Explore/Sales/Taskflows",
            object_type="TASKFLOW",
            description="daily",
            updated_by="bob",
        )
        flow = source._make_taskflow(tf)
        assert isinstance(flow, DataFlow)
        assert "taskflow:tf-1" in str(flow.urn)

    def test_make_mapping_datajob_uses_shared_project_flow_id(self):
        source = _make_source()
        obj = IdmcObject(
            id="guid-1",
            name="map_1",
            path="/Explore/Sales/Mappings",
            object_type="DTEMPLATE",
        )
        v2 = IdmcMapping(
            v2_id="v2-1", name="map_1", asset_frs_guid="guid-1", valid=False
        )
        flow = source._project_flow("Sales")
        job = source._make_mapping_datajob(obj, flow, v2)
        assert isinstance(job, DataJob)
        assert str(job.urn).endswith("guid-1)")
        assert "project:Sales" in str(job.urn)

    def test_make_mapping_task_datajob(self):
        source = _make_source(platform_instance="prod")
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="m-1",
            mapping_name="m",
            updated_by="bob",
        )
        flow = source._mapping_task_flow(mt)
        job = source._make_mapping_task_datajob(mt, flow)
        assert isinstance(job, DataJob)
        assert "mttask_flow:mt-1" in str(job.urn)

    def test_extract_ownership_disabled_suppresses_owners(self):
        source = _make_source(extract_ownership=False)
        assert source._owner_list("alice") is None

    def test_empty_user_identifier_returns_none(self):
        source = _make_source()
        assert source._owner_list(None) is None
        assert source._owner_list("") is None


class TestLineageEmission:
    def test_emit_lineage_uses_threaded_mapping_id(self):
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            schema="PUBLIC",
        )
        source._mapping_project["guid-42"] = "Sales"
        lineage = MappingLineageInfo(
            mapping_id="guid-42",
            mapping_name="my_map",
            source_tables=[
                LineageTable(
                    table_name="SRC",
                    schema_name="S1",
                    connection_federated_id="fed",
                )
            ],
            target_tables=[
                LineageTable(
                    table_name="TGT",
                    schema_name="S2",
                    connection_federated_id="fed",
                )
            ],
        )
        wus = list(source._emit_lineage(lineage))
        mcps = []
        for wu in wus:
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            mcps.append(wu.metadata)
        aspect_names = [m.aspectName for m in mcps]
        assert "dataJobInputOutput" in aspect_names
        assert "upstreamLineage" in aspect_names
        io_mcp = next(m for m in mcps if m.aspectName == "dataJobInputOutput")
        assert io_mcp.entityUrn is not None
        assert "guid-42" in io_mcp.entityUrn
        assert "project:Sales" in io_mcp.entityUrn
        assert source.report.lineage_edges_emitted == 2

    def test_emit_lineage_skips_when_mapping_id_missing(self):
        source = _make_source()
        lineage = MappingLineageInfo(
            mapping_id="",
            mapping_name="my_map",
            source_tables=[LineageTable(table_name="T", connection_federated_id="fed")],
            target_tables=[],
        )
        wus = list(source._emit_lineage(lineage))
        assert wus == []
        assert len(source.report.warnings) > 0

    def test_emit_lineage_skips_when_no_resolvable_tables(self):
        source = _make_source()
        source._mapping_project["m-1"] = "P"
        lineage = MappingLineageInfo(
            mapping_id="m-1",
            mapping_name="my_map",
            source_tables=[
                LineageTable(table_name="T", connection_federated_id="missing")
            ],
            target_tables=[],
        )
        wus = list(source._emit_lineage(lineage))
        assert wus == []


class TestFilteringAndErrorHandling:
    def test_project_pattern_filters_out_non_matching(self):
        source = _make_source(project_pattern={"allow": ["Prod.*"]})
        projects = [
            IdmcObject(
                id="1", name="Prod_sales", path="/Explore", object_type="Project"
            ),
            IdmcObject(
                id="2", name="Dev_sales", path="/Explore", object_type="Project"
            ),
        ]
        with patch.object(source.client, "list_objects") as mock_list:
            mock_list.side_effect = lambda object_type, tag=None: (
                iter(projects) if object_type == "Project" else iter([])
            )
            list(source._extract_containers())
        assert source.report.projects_scanned == 1
        assert source.report.projects_filtered == 1

    def test_list_mapping_tasks_failure_reported(self):
        source = _make_source()
        with (
            patch.object(source.client, "list_mappings", return_value=[]),
            patch.object(source.client, "list_objects", return_value=iter([])),
            patch.object(
                source.client,
                "list_mapping_tasks",
                side_effect=RuntimeError("403 Forbidden"),
            ),
        ):
            list(source._extract_mappings_and_tasks())
        assert len(source.report.warnings) > 0

    def test_load_connections_failure_reported_as_failure(self):
        source = _make_source()
        with patch.object(
            source.client, "list_connections", side_effect=RuntimeError("500")
        ):
            source._load_connections()
        assert len(source.report.failures) > 0

    def test_load_connections_is_idempotent(self):
        source = _make_source()
        source._connections_by_id["c1"] = IdmcConnection(
            id="c1", name="n", conn_type=""
        )
        with patch.object(source.client, "list_connections") as m:
            source._load_connections()
            m.assert_not_called()


class TestSourceLifecycle:
    def test_login_failure_aborts_cleanly(self):
        source = _make_source()
        with patch.object(
            source.client, "login", side_effect=InformaticaLoginError("bad creds")
        ):
            wus = list(source.get_workunits_internal())
        assert wus == []
        assert len(source.report.failures) > 0

    def test_get_report_returns_report_instance(self):
        source = _make_source()
        assert source.get_report() is source.report
