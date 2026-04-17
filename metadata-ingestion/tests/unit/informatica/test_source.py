from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.informatica.config import InformaticaSourceConfig
from datahub.ingestion.source.informatica.models import (
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    LineageTable,
    MappingLineageInfo,
)
from datahub.ingestion.source.informatica.source import InformaticaSource


def _make_source(**config_overrides) -> InformaticaSource:
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

    def test_resolve_parent_project_without_explore_prefix(self):
        assert (
            InformaticaSource._resolve_parent_project_from_path("Project1/Sub")
            == "Project1"
        )

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

    def test_is_not_bundle_object(self):
        obj = IdmcObject(
            id="1",
            name="X",
            path="/Explore/P",
            object_type="DTEMPLATE",
            updated_by="alice@acme.com",
        )
        assert InformaticaSource._is_bundle_object(obj) is False

    def test_make_owner_urn_returns_none_for_empty(self):
        assert InformaticaSource._make_owner_urn("") is None

    def test_make_owner_urn_returns_corp_user_urn(self):
        urn = InformaticaSource._make_owner_urn("alice")
        assert urn == "urn:li:corpuser:alice"


class TestTagFilterFallback:
    def test_returns_none_sentinel_when_no_tags(self):
        source = _make_source()
        assert source._tag_filters_or_none() == [None]

    def test_returns_tags_when_configured(self):
        source = _make_source(tag_filter_names=["pii", "critical"])
        assert source._tag_filters_or_none() == ["pii", "critical"]


class TestConnectionPlatformResolution:
    def test_override_takes_priority(self):
        source = _make_source(connection_type_overrides={"01A": "snowflake"})
        conn = IdmcConnection(
            id="01A", name="c", conn_type="Oracle", base_type="ORACLE"
        )
        assert source._resolve_connection_platform(conn) == "snowflake"

    def test_conn_type_maps_to_platform(self):
        source = _make_source()
        conn = IdmcConnection(
            id="x",
            name="c",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            base_type="",
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
        source._connections["fed"] = conn
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
        source._connections["fed"] = conn
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

    def test_unmapped_connection_type_reports_and_returns_none(self):
        source = _make_source()
        conn = IdmcConnection(
            id="c",
            name="weird",
            conn_type="UnmappedType",
            federated_id="fed",
        )
        source._connections["fed"] = conn
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(table_name="T", connection_federated_id="fed")
        )
        assert urn is None
        assert any("weird" in entry for entry in source.report.connections_unresolved)


class TestWorkunitGeneration:
    def test_emit_taskflow_yields_expected_aspects(self):
        source = _make_source(platform_instance="prod")
        tf = IdmcObject(
            id="tf-1",
            name="daily_refresh",
            path="/Explore/Sales/Taskflows",
            object_type="TASKFLOW",
            description="desc",
            created_by="alice",
            updated_by="bob",
        )
        wus = list(source._emit_taskflow(tf))
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataFlowInfo" in aspect_names
        assert "status" in aspect_names
        assert "dataPlatformInstance" in aspect_names
        assert "ownership" in aspect_names

    def test_emit_taskflow_skips_owner_when_disabled(self):
        source = _make_source(extract_ownership=False)
        tf = IdmcObject(
            id="tf-1",
            name="t",
            path="/Explore/P",
            object_type="TASKFLOW",
            updated_by="bob",
        )
        wus = list(source._emit_taskflow(tf))
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "ownership" not in aspect_names

    def test_emit_taskflow_skips_platform_instance_when_not_set(self):
        source = _make_source()
        tf = IdmcObject(
            id="tf-1",
            name="t",
            path="/Explore/P",
            object_type="TASKFLOW",
        )
        wus = list(source._emit_taskflow(tf))
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataPlatformInstance" not in aspect_names

    def test_emit_mapping_attaches_v2_properties_when_available(self):
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
        wus = list(source._emit_mapping(obj, v2))
        info_wu = next(wu for wu in wus if wu.metadata.aspectName == "dataJobInfo")
        props = info_wu.metadata.aspect.customProperties
        assert props["v3Id"] == "guid-1"
        assert props["v2Id"] == "v2-1"
        assert props["valid"] == "False"
        subtype_wu = next(wu for wu in wus if wu.metadata.aspectName == "subTypes")
        assert subtype_wu.metadata.aspect.typeNames == ["Mapping"]

    def test_emit_mapping_task(self):
        source = _make_source()
        mt = IdmcMappingTask(
            v2_id="mt-1", name="nightly_task", mapping_id="m-1", mapping_name="m"
        )
        wus = list(source._emit_mapping_task(mt))
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert aspect_names == ["dataJobInfo", "status", "subTypes"]

    def test_emit_lineage_yields_input_output_and_upstream(self):
        source = _make_source()
        conn = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            schema="PUBLIC",
        )
        source._connections["fed"] = conn
        lineage = MappingLineageInfo(
            mapping_id="m-1",
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
        aspect_names = [wu.metadata.aspectName for wu in wus]
        assert "dataJobInputOutput" in aspect_names
        assert "upstreamLineage" in aspect_names
        assert source.report.lineage_edges_emitted == 2

    def test_emit_lineage_skips_when_no_resolvable_tables(self):
        source = _make_source()
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


class TestFilteringInExtraction:
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
        assert "1" in source._project_objects
        assert "2" not in source._project_objects

    def test_create_method_parses_config_dict(self):
        ctx = PipelineContext(run_id="r")
        source = InformaticaSource.create({"username": "u", "password": "p"}, ctx)
        assert source.config.username == "u"

    def test_get_report_returns_report_instance(self):
        source = _make_source()
        assert source.get_report() is source.report
