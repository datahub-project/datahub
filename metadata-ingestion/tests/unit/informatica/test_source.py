from typing import Any, Iterator, Optional
from unittest.mock import MagicMock, patch

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
from datahub.metadata.schema_classes import BrowsePathsV2Class
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
    config = InformaticaSourceConfig.model_validate(defaults)
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

    def test_browse_path_entries_with_folder(self):
        source = _make_source()
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "ETL"))
        entries = source._browse_path_entries("/Explore/Sales/ETL/my_mapping")
        assert len(entries) == 2
        # Project entry references the Sales project container URN.
        assert entries[0].id == entries[0].urn
        assert entries[0].urn is not None
        assert entries[0].urn.startswith("urn:li:container:")
        assert entries[0].urn == source._project_key("Sales").as_urn()
        # Folder entry references the ETL folder container URN under Sales.
        assert entries[1].id == entries[1].urn
        assert entries[1].urn == source._folder_key("Sales", "ETL").as_urn()

    def test_browse_path_entries_no_folder(self):
        source = _make_source()
        source._emitted_project_names.add("Sales")
        entries = source._browse_path_entries("/Explore/Sales/my_mapping")
        assert len(entries) == 1
        assert entries[0].urn == source._project_key("Sales").as_urn()

    def test_browse_path_entries_deep_path_extra_segments_plain(self):
        # Segments beyond Project/Folder are emitted as plain id entries.
        source = _make_source()
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "ETL"))
        entries = source._browse_path_entries(
            "/Explore/Sales/ETL/Sub/Deeper/my_mapping"
        )
        assert [e.urn is not None for e in entries] == [True, True, False, False]
        assert [e.id for e in entries[2:]] == ["Sub", "Deeper"]

    def test_browse_path_entries_unemitted_folder_falls_back_to_plain_name(self):
        # When a folder is filtered out (or absent from IDMC), we must not emit
        # a URN entry for it — the UI can't resolve it and ends up rendering
        # the raw URN string where the folder name should appear. Plain-name
        # fallback keeps the hierarchy readable.
        source = _make_source()
        source._emitted_project_names.add("Sales")
        # Note: ("Sales", "Mappings") deliberately NOT added to emitted folders.
        entries = source._browse_path_entries("/Explore/Sales/Mappings/my_mapping")
        assert len(entries) == 2
        assert entries[0].urn == source._project_key("Sales").as_urn()
        # Folder falls back to plain name; urn is None so the UI shows "Mappings".
        assert entries[1].urn is None
        assert entries[1].id == "Mappings"

    def test_browse_path_entries_unemitted_project_falls_back_to_plain_name(self):
        source = _make_source()
        entries = source._browse_path_entries("/Explore/Sales/my_mapping")
        assert len(entries) == 1
        assert entries[0].urn is None
        assert entries[0].id == "Sales"

    def test_browse_path_entries_empty_path_returns_empty(self):
        source = _make_source()
        assert source._browse_path_entries("") == []

    def test_is_bundle_object_by_path(self):
        obj = IdmcObject(
            id="1", name="X", path="Add-On Bundles/Snowflake", object_type="Folder"
        )
        assert InformaticaSource._is_bundle_object(obj) is True

    def test_is_bundle_object_by_name(self):
        # The Add-On Bundles root Project has name "Add-On Bundles" and a
        # path like "/Explore/Add-On Bundles" — the name check is what
        # keeps it from being emitted as a regular project.
        obj = IdmcObject(
            id="1",
            name="Add-On Bundles",
            path="/Explore/Add-On Bundles",
            object_type="Project",
        )
        assert InformaticaSource._is_bundle_object(obj) is True

    def test_is_bundle_object_by_path_with_explore_prefix(self):
        obj = IdmcObject(
            id="1",
            name="PluggableAdapter",
            path="/Explore/Add-On Bundles/SnowflakeBundle/PluggableAdapter",
            object_type="DTEMPLATE",
        )
        assert InformaticaSource._is_bundle_object(obj) is True

    def test_is_bundle_object_regular_project_not_filtered(self):
        obj = IdmcObject(
            id="1",
            name="Default",
            path="/Explore/Default",
            object_type="Project",
        )
        assert InformaticaSource._is_bundle_object(obj) is False

    def test_excluded_by_folder_filter_rejects_entity_in_filtered_folder(self):
        # Entities (mappings, taskflows, mapping tasks) living inside a folder
        # that folder_pattern rejects should NOT be emitted — users setting
        # folder_pattern expect the exclusion to cascade, not just skip the
        # Folder container while emitting every entity inside it with a
        # dangling browse path.
        source = _make_source(
            folder_pattern={"allow": ["^develop_test$"]},
        )
        # Entity at Default/develop_test_2/TestMapping1 — folder 'develop_test_2'
        # is rejected by the anchored pattern, so the entity is excluded.
        assert (
            source._excluded_by_folder_filter("Default/develop_test_2/TestMapping1")
            is True
        )

    def test_excluded_by_folder_filter_keeps_entity_in_allowed_folder(self):
        source = _make_source(
            folder_pattern={"allow": ["^develop_test$"]},
        )
        assert (
            source._excluded_by_folder_filter("Default/develop_test/Mapping4") is False
        )

    def test_excluded_by_folder_filter_ignores_project_root_entities(self):
        # Entities at the project root (no folder segment) are not subject to
        # folder-pattern filtering — treat path length < 3 as "no folder".
        source = _make_source(
            folder_pattern={"allow": ["^develop_test$"]},
        )
        assert source._excluded_by_folder_filter("Default/Mapping1") is False

    def test_is_project_masquerading_as_folder_skips_project_paths(self):
        # IDMC's `type=Folder` query has been observed returning entries at the
        # project root. Those would duplicate the real Project container in the
        # navigate tree, so _extract_containers filters them out here.
        obj = IdmcObject(
            id="1", name="Default", path="/Explore/Default", object_type="Folder"
        )
        assert InformaticaSource._is_project_masquerading_as_folder(obj) is True

    def test_is_project_masquerading_as_folder_keeps_real_folders(self):
        obj = IdmcObject(
            id="1",
            name="develop_test",
            path="/Explore/Default/develop_test",
            object_type="Folder",
        )
        assert InformaticaSource._is_project_masquerading_as_folder(obj) is False

    def test_is_bundle_object_by_updated_by(self):
        obj = IdmcObject(
            id="1",
            name="X",
            path="/Explore/P",
            object_type="DTEMPLATE",
            updated_by="bundle-license-notifier",
        )
        assert InformaticaSource._is_bundle_object(obj) is True


class TestTagFilterAndIteration:
    def test_returns_tags_when_configured(self):
        source = _make_source(tag_filter_names=["pii", "critical"])
        assert source._tag_filters_or_none("TASKFLOW") == ["pii", "critical"]

    def test_skips_tag_filter_for_container_types(self):
        # IDMC v3 rejects `tag==` on Project/Folder; ensure the code passes
        # through with no tag for those types even when tag_filter_names is set.
        source = _make_source(tag_filter_names=["pii"])
        assert source._tag_filters_or_none("Project") == [None]
        assert source._tag_filters_or_none("Folder") == [None]

    def test_iter_with_tags_dedupes_by_id(self):
        source = _make_source(tag_filter_names=["a", "b"])
        obj1 = IdmcObject(id="1", name="x", path="/Explore/P", object_type="TASKFLOW")
        obj2 = IdmcObject(id="2", name="y", path="/Explore/P", object_type="TASKFLOW")
        with patch.object(source.client, "list_objects") as m:
            m.side_effect = lambda object_type, tag=None: (
                iter([obj1, obj2]) if tag == "a" else iter([obj1])
            )
            results = list(source._iter_with_tags("TASKFLOW"))
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

    def test_user_type_map_extends_builtin(self):
        # Users can map unrecognized connector type strings without a code
        # change. Takes priority over the built-in map when both match.
        source = _make_source(
            connection_type_platform_map={
                "MyCustomConnector": "snowflake",
                "Oracle": "mysql",  # deliberate override of a built-in
            }
        )
        custom = IdmcConnection(id="x", name="c", conn_type="MyCustomConnector")
        assert source._resolve_connection_platform(custom) == "snowflake"
        overridden = IdmcConnection(id="y", name="c", conn_type="Oracle")
        assert source._resolve_connection_platform(overridden) == "mysql"

    def test_name_inference_matches_sample_snowflake_connection(self):
        # Real report from the user's pod showed "Sample Snowflake Connection"
        # unresolved because CONNECTION_TYPE_MAP had no plain "Snowflake" key
        # (only Snowflake_Cloud_Data_Warehouse and Snowflake_Data_Cloud — too
        # long to substring-match "Snowflake" alone). The short-name alias
        # keeps these Sample-style connections working.
        source = _make_source()
        conn = IdmcConnection(
            id="01DM180B000000000002",
            name="Sample Snowflake Connection",
            conn_type="",
            base_type="TOOLKIT",
        )
        assert source._resolve_connection_platform(conn) == "snowflake"

    def test_name_inference_when_conn_type_is_empty(self):
        # IDMC sometimes returns conn_type='' base_type='TOOLKIT' for custom /
        # marketplace connectors. The connection name is often the only hint
        # about the real platform — "Sample GoogleBigQuery Connection" tells
        # us it's a BigQuery connector. Without this fallback, the connection
        # is reported unresolved and lineage dataset URNs can't be built.
        source = _make_source()
        bigquery = IdmcConnection(
            id="01DM180B00000000000A",
            name="Sample GoogleBigQuery Connection",
            conn_type="",
            base_type="TOOLKIT",
        )
        assert source._resolve_connection_platform(bigquery) == "bigquery"

        s3 = IdmcConnection(
            id="01DM180B000000000008",
            name="Sample S3 Connection",
            conn_type="",
            base_type="TOOLKIT",
        )
        # "S3" is short; make sure the substring match still picks it up.
        assert source._resolve_connection_platform(s3) == "s3"

    def test_name_inference_prefers_longest_match(self):
        # When multiple keys match as substrings, the more specific one wins
        # so "Snowflake_Data_Cloud" doesn't get beaten by bare "Snowflake".
        source = _make_source()
        conn = IdmcConnection(
            id="x",
            name="Acme Snowflake Data Cloud Prod",
            conn_type="",
            base_type="TOOLKIT",
        )
        assert source._resolve_connection_platform(conn) == "snowflake"

    def test_name_inference_skipped_when_no_substring_matches(self):
        # Generic names like "test" should still fall through to None —
        # better an unresolved report than a wrong platform.
        source = _make_source()
        conn = IdmcConnection(id="x", name="test", conn_type="", base_type="TOOLKIT")
        assert source._resolve_connection_platform(conn) is None

    def test_name_inference_does_not_trigger_when_conn_type_resolves(self):
        # The fallback should only run when the direct maps fail. If
        # conn_type resolves, don't let a spurious name-substring override it.
        source = _make_source()
        conn = IdmcConnection(
            id="x",
            name="Generic Connection",  # name has no platform hint
            conn_type="Oracle",
            base_type="",
        )
        assert source._resolve_connection_platform(conn) == "oracle"

    def test_user_type_map_applies_to_base_type_fallback(self):
        source = _make_source(
            connection_type_platform_map={"CustomToolkitSubtype": "teradata"}
        )
        conn = IdmcConnection(
            id="x", name="c", conn_type="", base_type="CustomToolkitSubtype"
        )
        assert source._resolve_connection_platform(conn) == "teradata"


class TestTableUrnResolution:
    def test_urn_includes_database_schema_table(self):
        source = _make_source()
        conn = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            db_schema="PUBLIC",
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
            db_schema="PUBLIC",
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

    def test_make_taskflow_falls_back_to_id_when_name_empty(self):
        source = _make_source()
        tf = IdmcObject(
            id="tf-guid-1",
            name="",  # empty — as returned by some IDMC instances
            path="/Explore/Sales/tf-guid-1",
            object_type="TASKFLOW",
        )
        flow = source._make_taskflow(tf)
        assert isinstance(flow, DataFlow)
        # flow_id is the IDMC v3 GUID — opaque but free of UI hierarchy
        # separators, so DataHub's nav tree can't split it into phantom
        # tree nodes.
        assert "tf-guid-1" in str(flow.urn)

    def test_make_taskflow_dataflow(self):
        source = _make_source(platform_instance="prod")
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "Taskflows"))
        tf = IdmcObject(
            id="tf-1",
            name="daily_refresh",
            path="/Explore/Sales/Taskflows/daily_refresh",
            object_type="TASKFLOW",
            description="daily",
            updated_by="bob",
        )
        flow = source._make_taskflow(tf)
        assert isinstance(flow, DataFlow)
        # flow_id is the IDMC v3 GUID; display_name shows the friendly name.
        assert "tf-1" in str(flow.urn)
        assert flow.display_name == "daily_refresh"
        assert flow.custom_properties.get("idmcId") == "tf-1"
        # Browse path uses Project/Folder container URNs so the taskflow
        # nests under its IDMC project/folder containers.
        bp = flow._get_aspect(BrowsePathsV2Class)
        assert bp is not None
        project_urn = source._project_key("Sales").as_urn()
        folder_urn = source._folder_key("Sales", "Taskflows").as_urn()
        assert [(e.id, e.urn) for e in bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
        ]

    def test_emit_mapping_entities_emits_dataflow_and_datajob_pair(self):
        # Each mapping gets its own DataFlow (browsable under the IDMC folder)
        # plus an inner DataJob that carries lineage — replacing the old
        # per-project "Mappings in <project>" synthetic DataFlow.
        source = _make_source(platform_instance="prod")
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "Mappings"))
        obj = IdmcObject(
            id="guid-1",
            name="map_1",
            path="/Explore/Sales/Mappings/map_1",
            object_type="DTEMPLATE",
        )
        v2 = IdmcMapping(
            v2_id="v2-1", name="map_1", asset_frs_guid="guid-1", valid=False
        )
        entities = list(source._emit_mapping_entities(obj, v2, subtype="Mapping"))
        assert len(entities) == 2
        flow, job = entities
        assert isinstance(flow, DataFlow)
        assert isinstance(job, DataJob)
        # DataFlow URN uses the IDMC v3 GUID (opaque but separator-free so
        # DataHub's UI can't split it into phantom tree nodes). Users never
        # see the GUID — display_name + DataJob browse-path override keep
        # the UI readable.
        assert "guid-1" in str(flow.urn)
        # DataJob URN reuses the same constant id the lineage phase rebuilds.
        assert str(job.urn).endswith(",transform)")
        # v3Id custom property still carries the underlying GUID for audit.
        assert flow.custom_properties.get("v3Id") == "guid-1"
        assert flow.custom_properties.get("v2Id") == "v2-1"
        assert flow.custom_properties.get("valid") == "false"
        # DataJob display_name carries the parent mapping's name so the UI
        # shows "map_1.transform" instead of a bare "transform" shared by
        # every mapping in the project.
        assert job.display_name == "map_1.transform"
        # DataFlow gets the browse path.
        bp = flow._get_aspect(BrowsePathsV2Class)
        assert bp is not None
        project_urn = source._project_key("Sales").as_urn()
        folder_urn = source._folder_key("Sales", "Mappings").as_urn()
        assert [(e.id, e.urn) for e in bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
        ]
        # DataJob's browse path is explicitly overridden so the last entry
        # uses the display name as the label (instead of the URN flow_id
        # which DataHub's UI otherwise renders verbatim as a raw path string
        # in the navigate tree).
        job_bp = job._get_aspect(BrowsePathsV2Class)
        assert job_bp is not None
        assert [(e.id, e.urn) for e in job_bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
            ("map_1", str(flow.urn)),
        ]
        # Flow URN tracked for later lineage emission.
        assert source._mapping_flow_urns["guid-1"] == str(flow.urn)

    def test_emit_mapping_entities_mapplet_preserves_object_type(self):
        # Mapplets go through the same emitter but keep their real objectType
        # on both the DataFlow and DataJob custom properties.
        source = _make_source()
        obj = IdmcObject(
            id="mpl-1",
            name="reusable_lookup",
            path="/Explore/Sales/Mapplets/reusable_lookup",
            object_type="MAPPLET",
        )
        entities = list(source._emit_mapping_entities(obj, None, subtype="Mapplet"))
        flow, job = entities
        assert isinstance(flow, DataFlow)
        assert isinstance(job, DataJob)
        assert flow.custom_properties.get("objectType") == "MAPPLET"
        assert job.custom_properties.get("objectType") == "MAPPLET"

    def test_make_mapping_task_dataflow_with_path(self):
        # Mapping task is a DataFlow (mirrors taskflow); browse path on the flow.
        source = _make_source(platform_instance="prod")
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "ETL"))
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            path="/Explore/Sales/ETL/nightly_task",
            mapping_id="m-1",
            mapping_name="m",
            updated_by="bob",
        )
        flow = source._make_mapping_task(mt)
        assert isinstance(flow, DataFlow)
        # flow_id is the IDMC v2 id (mt-1) — separator-free so DataHub's UI
        # can't split it into phantom tree nodes. display_name shows
        # "nightly_task" to users.
        assert "mt-1" in str(flow.urn)
        assert flow.display_name == "nightly_task"
        # No mttask/ prefix in the URN.
        assert "mttask/" not in str(flow.urn)
        assert flow.custom_properties.get("v2Id") == "mt-1"
        # Path custom property mirrors mappings/taskflows.
        assert flow.custom_properties.get("path") == "/Explore/Sales/ETL/nightly_task"
        assert flow.custom_properties.get("idmcId") == "mt-1"
        bp = flow._get_aspect(BrowsePathsV2Class)
        assert bp is not None
        project_urn = source._project_key("Sales").as_urn()
        folder_urn = source._folder_key("Sales", "ETL").as_urn()
        assert [(e.id, e.urn) for e in bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
        ]

    def test_make_mapping_task_dataflow_no_path_falls_back_to_parent_mapping(self):
        source = _make_source()
        source._emitted_project_names.add("Sales")
        source._emitted_folder_keys.add(("Sales", "Folder1"))
        source._v2_id_to_path["m-1"] = "/Explore/Sales/Folder1/my_mapping"
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="m-1",
            mapping_name="my_mapping",
        )
        flow = source._make_mapping_task(mt)
        bp = flow._get_aspect(BrowsePathsV2Class)
        assert bp is not None
        project_urn = source._project_key("Sales").as_urn()
        folder_urn = source._folder_key("Sales", "Folder1").as_urn()
        assert [(e.id, e.urn) for e in bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
        ]

    def test_make_mapping_task_dataflow_no_browse_path_when_unknown(self):
        # No path and no parent mapping in cache: no project/folder entries.
        source = _make_source()
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="unknown-id",
            mapping_name="m",
        )
        flow = source._make_mapping_task(mt)
        bp = flow._get_aspect(BrowsePathsV2Class)
        # SDK may set a default path; just verify we didn't set our own hierarchy.
        assert bp is None or all("mttask" not in (e.id or "") for e in bp.path)

    def test_extract_ownership_disabled_suppresses_owners(self):
        source = _make_source(extract_ownership=False)
        assert source._owner_list("alice") is None

    def test_empty_user_identifier_returns_none(self):
        source = _make_source()
        assert source._owner_list(None) is None
        assert source._owner_list("") is None


class TestLineageEmission:
    def test_emit_lineage_targets_per_mapping_dataflow(self):
        # Lineage is emitted against the DataJob inside each mapping's own
        # DataFlow (no longer the old project-level synthetic flow). The
        # extraction phase populates `_mapping_flow_urns`; lineage reuses it
        # to reconstruct the DataJob URN without re-deriving anything from
        # the export payload.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            db_schema="PUBLIC",
        )
        source._mapping_flow_urns["guid-42"] = (
            "urn:li:dataFlow:(informatica,guid-42,PROD)"
        )
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
        mcps: list[MetadataChangeProposalWrapper] = []
        for wu in wus:
            assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
            mcps.append(wu.metadata)
        aspect_names = [m.aspectName for m in mcps]
        assert "dataJobInputOutput" in aspect_names
        assert "upstreamLineage" in aspect_names
        io_mcp = next(m for m in mcps if m.aspectName == "dataJobInputOutput")
        assert io_mcp.entityUrn is not None
        # DataJob URN uses the per-mapping DataFlow URN and the constant
        # "transform" id the emitter sets for the inner DataJob.
        assert io_mcp.entityUrn.endswith(
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,guid-42,PROD),transform)"
        )
        assert source.report.lineage_edges_emitted == 2

    def test_emit_lineage_skips_when_mapping_not_emitted(self):
        # If export returns a mapping id we never emitted (filtered out or
        # export raced ahead of extraction), skip it with a warning rather
        # than fabricate a dangling DataJob URN.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c", name="prod", conn_type="Oracle", federated_id="fed"
        )
        lineage = MappingLineageInfo(
            mapping_id="ghost-id",
            mapping_name="ghost",
            source_tables=[LineageTable(table_name="T", connection_federated_id="fed")],
            target_tables=[],
        )
        wus = list(source._emit_lineage(lineage))
        assert wus == []
        assert len(source.report.warnings) > 0

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
        source._mapping_flow_urns["m-1"] = "urn:li:dataFlow:(informatica,m-1,PROD)"
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


class TestSafeListHelper:
    def test_yields_items_on_success(self):
        source = _make_source()
        out = list(
            source._safe_list(
                lambda: iter([1, 2, 3]),
                title="t",
                message="m",
                context="c",
            )
        )
        assert out == [1, 2, 3]
        assert source.report.warnings == []

    def test_catches_call_time_exception(self):
        # Factory-style invocation means even non-generator failures (e.g.,
        # a client method that raises on HTTP error before returning an
        # iterator) are caught and reported, not propagated.
        source = _make_source()

        def boom():
            raise RuntimeError("list call failed")

        out = list(source._safe_list(boom, title="t", message="m", context="/api/test"))
        assert out == []
        assert len(source.report.warnings) == 1

    def test_catches_iteration_time_exception(self):
        source = _make_source()

        def gen():
            yield 1
            raise RuntimeError("mid-iteration failure")

        out = list(source._safe_list(gen, title="t", message="m", context="/api/test"))
        assert out == [1]
        assert len(source.report.warnings) == 1


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

    def test_single_bad_taskflow_does_not_stop_subsequent_ones(self):
        # A bad object (e.g. name="" with no path fallback) must not abort the
        # entire taskflow loop — remaining valid taskflows should still be emitted.
        source = _make_source()
        bad = IdmcObject(id="bad", name="", path="", object_type="TASKFLOW")
        good = IdmcObject(
            id="good",
            name="daily_refresh",
            path="/Explore/Sales/daily_refresh",
            object_type="TASKFLOW",
        )
        with patch.object(source.client, "list_objects") as mock_list:
            mock_list.return_value = iter([bad, good])
            # Force bad to raise even after path fallback by patching _make_taskflow
            original = source._make_taskflow

            def _make_taskflow_with_raise(tf: IdmcObject) -> DataFlow:
                if tf.id == "bad":
                    raise ValueError("simulated bad object")
                return original(tf)

            source._make_taskflow = _make_taskflow_with_raise  # type: ignore[method-assign]
            entities = list(source._extract_taskflows())

        assert len(entities) == 1
        flow = entities[0]
        assert isinstance(flow, DataFlow)
        # flow_id is the v3 id "good"; display_name is "daily_refresh".
        assert "good" in str(flow.urn)
        assert flow.display_name == "daily_refresh"
        assert len(source.report.warnings) > 0

    def test_v2_mapplet_fallback_runs_when_v3_returns_zero(self):
        # Some IDMC pods return 0 items from v3 ``type=='MAPPLET'`` even when
        # mapplets exist in the UI (observed on ``use4.dm-us``). The v2 fallback
        # fills the gap so mapplets don't silently disappear from the navigate
        # tree.
        source = _make_source()
        fallback_mapplet = IdmcObject(
            id="mpl-1",
            name="Mapplet1",
            path="Default/develop_test/Mapplet1",
            object_type="MAPPLET",
        )

        def _no_v3_mapplets(
            object_type: str, tag: Optional[str] = None
        ) -> "Iterator[IdmcObject]":
            return iter([])

        with (
            patch.object(source.client, "list_mappings", return_value=[]),
            patch.object(source.client, "list_objects", side_effect=_no_v3_mapplets),
            patch.object(
                source.client, "list_mapplets_v2", return_value=[fallback_mapplet]
            ),
            patch.object(source.client, "list_mapping_tasks", return_value=iter([])),
        ):
            entities = list(source._extract_mappings_and_tasks())

        # A Mapplet DataFlow + inner DataJob should have been emitted from the
        # v2 fallback. URNs carry the IDMC GUID; display_name carries the name.
        urns = [str(e.urn) for e in entities]
        assert any("mpl-1" in u for u in urns)
        display_names = [
            getattr(e, "display_name", None)
            for e in entities
            if hasattr(e, "display_name")
        ]
        assert "Mapplet1" in display_names

    def test_v2_mapplet_fallback_skipped_when_v3_returns_mapplets(self):
        # If v3 already returns mapplets, the v2 fallback MUST NOT fire — we
        # don't want to double-count or hit an unnecessary endpoint.
        source = _make_source()
        v3_mapplet = IdmcObject(
            id="mpl-1",
            name="Mapplet1",
            path="/Explore/P/F/Mapplet1",
            object_type="MAPPLET",
        )

        def _v3_list(
            object_type: str, tag: Optional[str] = None
        ) -> "Iterator[IdmcObject]":
            if object_type == "MAPPLET":
                return iter([v3_mapplet])
            return iter([])

        with (
            patch.object(source.client, "list_mappings", return_value=[]),
            patch.object(source.client, "list_objects", side_effect=_v3_list),
            patch.object(source.client, "list_mapplets_v2") as v2_mock,
            patch.object(source.client, "list_mapping_tasks", return_value=iter([])),
        ):
            list(source._extract_mappings_and_tasks())
            v2_mock.assert_not_called()

    def test_mapplet_returned_via_dtemplate_query_gets_mapplet_subtype(self):
        # Some IDMC pods ship mapplets back on the DTEMPLATE query, with the
        # discriminating ``documentType`` field set to "MAPPLET". Before the
        # subtype fix these were labeled "Mapping" in the UI; now the subtype
        # is taken from the response documentType rather than the query type.
        source = _make_source()
        dtemplate_results = [
            IdmcObject(
                id="m1",
                name="a_mapping",
                path="/Explore/P/F/a_mapping",
                object_type="DTEMPLATE",
            ),
            IdmcObject(
                id="mpl1",
                name="a_mapplet",
                path="/Explore/P/F/a_mapplet",
                object_type="MAPPLET",  # mapplet shipped via DTEMPLATE query
            ),
        ]

        def _mock_list_objects(
            object_type: str, tag: Optional[str] = None
        ) -> "Iterator[IdmcObject]":
            if object_type == "DTEMPLATE":
                return iter(dtemplate_results)
            return iter([])

        with (
            patch.object(source.client, "list_mappings", return_value=[]),
            patch.object(source.client, "list_objects", side_effect=_mock_list_objects),
            patch.object(source.client, "list_mapping_tasks", return_value=iter([])),
        ):
            entities = list(source._extract_mappings_and_tasks())

        subtypes = {
            e.custom_properties.get("objectType"): e
            for e in entities
            if hasattr(e, "custom_properties")
        }
        # The flow pair for the mapplet keeps the MAPPLET objectType on its
        # DataFlow, while the genuine mapping keeps DTEMPLATE.
        assert "MAPPLET" in subtypes
        assert "DTEMPLATE" in subtypes

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
    def test_stale_entity_removal_handler_registered(self):
        # Stateful ingestion hinges on StaleEntityRemovalHandler being in the
        # workunit processor chain. Verify the returned workunit_processor is
        # actually registered — a refactor that calls ``create`` but drops the
        # result would silently leave deleted IDMC entities live in DataHub.
        from datahub.ingestion.source.state import stale_entity_removal_handler

        sentinel = object()
        fake_handler = MagicMock()
        fake_handler.workunit_processor = sentinel

        source = _make_source()
        with patch.object(
            stale_entity_removal_handler.StaleEntityRemovalHandler,
            "create",
            return_value=fake_handler,
        ) as create_mock:
            processors = source.get_workunit_processors()
        create_mock.assert_called_once()
        assert sentinel in processors, (
            "StaleEntityRemovalHandler.workunit_processor must be in the "
            "processor chain returned by get_workunit_processors()"
        )

    def test_login_failure_aborts_cleanly(self):
        source = _make_source()
        with patch.object(
            source.client, "login", side_effect=InformaticaLoginError("bad creds")
        ):
            wus = list(source.get_workunits_internal())
        assert wus == []
        assert len(source.report.failures) > 0
