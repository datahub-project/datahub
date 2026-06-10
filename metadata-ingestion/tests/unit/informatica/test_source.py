import threading
from typing import Any, List
from unittest.mock import MagicMock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.informatica.config import InformaticaSourceConfig
from datahub.ingestion.source.informatica.models import (
    ExportJobState,
    IdmcConnection,
    IdmcMapping,
    IdmcMappingTask,
    IdmcObject,
    InformaticaApiError,
    InformaticaLoginError,
    LineageTable,
    MappingLineageInfo,
    TaskflowDefinition,
    TaskflowStep,
    V2Id,
    V3Guid,
)
from datahub.ingestion.source.informatica.source import (
    InformaticaSource,
    OrchestrateState,
)
from datahub.ingestion.source.state import stale_entity_removal_handler
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DataJobInputOutputClass,
    OwnershipTypeClass,
)
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
    def test_tag_filter_single_pass(self):
        # list_objects must be called exactly once regardless of how many tags
        # are configured — tag matching is done client-side.
        source = _make_source(tag_filter_names=["pii", "critical"])
        obj_match = IdmcObject(
            id="1", name="x", path="/Explore/P", object_type="TASKFLOW", tags=["pii"]
        )
        obj_no_match = IdmcObject(
            id="2", name="y", path="/Explore/P", object_type="TASKFLOW", tags=["other"]
        )
        with patch.object(
            source.client, "list_objects", return_value=iter([obj_match, obj_no_match])
        ) as m:
            results = list(source._iter_with_tags("TASKFLOW"))
        m.assert_called_once_with("TASKFLOW")
        assert [o.id for o in results] == ["1"]

    def test_skips_tag_filter_for_container_types(self):
        # tag_filter_names must not drop Projects/Folders — IDMC v3 rejects
        # tag== on those types and all containers should always be fetched.
        source = _make_source(tag_filter_names=["pii"])
        obj = IdmcObject(id="1", name="P", path="/Explore/P", object_type="Project")
        with patch.object(source.client, "list_objects", return_value=iter([obj])):
            results = list(source._iter_with_tags("Project"))
        assert [o.id for o in results] == ["1"]


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
    def test_urn_matches_snowflake_default_lowercase(self):
        # Default convert_urns_to_lowercase=True aligns emitted URNs with the
        # Snowflake/Postgres/BigQuery connectors' default lowercasing, so
        # lineage edges actually land on real dataset URNs.
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
        assert "dwh.sales.orders" in urn

    def test_urn_preserves_case_when_lowercasing_disabled(self):
        source = _make_source(convert_urns_to_lowercase=False)
        conn = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Oracle",
            federated_id="fed",
            database="OLTP",
            db_schema="APPS",
        )
        source._connections_by_fed_id["fed"] = conn
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(
                table_name="ORDERS", schema_name="PUBLIC", connection_federated_id="fed"
            )
        )
        assert urn is not None
        assert "OLTP.PUBLIC.ORDERS" in urn

    def test_urn_threads_connection_to_platform_instance(self):
        # When the user declares that connection "c1" maps to the Snowflake
        # source ingested with platform_instance=prod_sf, the emitted URN must
        # include that instance or the lineage edge dangles.
        source = _make_source(
            connection_to_platform_instance={"c1": "prod_sf"},
        )
        conn = IdmcConnection(
            id="c1",
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
        assert "prod_sf" in urn

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
        assert "public.t" in urn

    def test_empty_table_name_returns_none(self):
        # Malformed IDMC transformations can carry an empty table name;
        # building a URN would yield ``snowflake,DWH.SALES.,PROD`` which
        # dangles. Guard returns None + reports unresolved.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Snowflake_Cloud_Data_Warehouse",
            federated_id="fed",
            database="DWH",
            db_schema="SALES",
        )
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(table_name="", connection_federated_id="fed")
        )
        assert urn is None
        assert any(
            "Empty table name" in entry
            for entry in source.report.connections_unresolved
        )

    def test_unknown_connection_reports_and_returns_none(self):
        source = _make_source()
        urn = source._resolve_table_to_dataset_urn(
            LineageTable(table_name="T", connection_federated_id="missing")
        )
        assert urn is None
        assert any("missing" in entry for entry in source.report.connections_unresolved)


class TestEntityEmission:
    def test_make_folder_container_parent_set_when_project_emitted(self):
        source = _make_source()
        source._emitted_project_names.add("Sales")
        folder = IdmcObject(
            id="f1",
            name="Mappings",
            path="/Explore/Sales/Mappings",
            object_type="Folder",
        )
        container = source._make_folder_container(folder)
        assert str(container.parent_container) == source._project_key("Sales").as_urn()

    def test_make_folder_container_parent_dropped_when_project_not_emitted(self):
        # Parent project filtered out upstream — folder must not dangle with a
        # ghost parent_container URN.
        source = _make_source()
        folder = IdmcObject(
            id="f1",
            name="Mappings",
            path="/Explore/Sales/Mappings",
            object_type="Folder",
        )
        container = source._make_folder_container(folder)
        assert container.parent_container is None

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
        # flow_id uses the human-readable name so DataHub's Navigate tree
        # renders "daily_refresh" rather than the opaque v3 GUID. The
        # v3 GUID is retained in customProperties for IDMC cross-reference.
        assert "daily_refresh" in str(flow.urn)
        assert "tf-1" not in str(flow.urn)
        assert flow.display_name == "daily_refresh"
        assert flow.custom_properties.get("v3Id") == "tf-1"
        assert flow.custom_properties.get("objectType") == "TASKFLOW"
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

    def test_emit_mapping_task_with_path(self):
        # Mapping task emits DataFlow + inner DataJob; browse path lands on both.
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
        entities = list(source._emit_mapping_task(mt))
        assert len(entities) == 2
        flow, job = entities
        assert isinstance(flow, DataFlow)
        assert isinstance(job, DataJob)
        # flow_id is the v2_id (unique per MT) so same-named MTs in different
        # folders get distinct URNs. The human-readable name is display_name.
        assert "mt-1" in str(flow.urn)
        assert "nightly_task" not in str(flow.urn)
        assert flow.display_name == "nightly_task"
        assert "mttask/" not in str(flow.urn)
        assert flow.custom_properties.get("v3Id") == "mt-1"
        assert flow.custom_properties.get("objectType") == "MTT"
        assert flow.custom_properties.get("path") == "/Explore/Sales/ETL/nightly_task"
        bp = flow._get_aspect(BrowsePathsV2Class)
        assert bp is not None
        project_urn = source._project_key("Sales").as_urn()
        folder_urn = source._folder_key("Sales", "ETL").as_urn()
        assert [(e.id, e.urn) for e in bp.path] == [
            (project_urn, project_urn),
            (folder_urn, folder_urn),
        ]
        assert job.name == "transform"

    def test_emit_mapping_task_carries_mapping_reference_in_custom_properties(self):
        # Mapping reference (v3 GUID + friendly name + v2 id) is surfaced in
        # the MT's customProperties so users can find the underlying mapping
        # without leaving DataHub.
        source = _make_source()
        source._v2_mappings_by_v2_id[V2Id("m-1")] = IdmcMapping(
            v2_id="m-1", name="my_mapping", asset_frs_guid="guid-1"
        )
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="m-1",
            mapping_name="my_mapping",
        )
        flow, _ = list(source._emit_mapping_task(mt))
        assert isinstance(flow, DataFlow)
        assert flow.custom_properties.get("mappingV3Id") == "guid-1"
        assert flow.custom_properties.get("mappingName") == "my_mapping"
        assert flow.custom_properties.get("mappingId") == "m-1"

    def test_emit_mapping_task_registers_for_lineage_fanout(self):
        # The MT emitter populates ``_mapping_v3_to_mt_job_urns`` so the
        # lineage phase can fan out one export result to every MT DataJob
        # that references that Mapping. It also appends the v3 GUID to
        # ``_mapping_ids`` (deduplicated) for the export batch.
        source = _make_source()
        source._v2_mappings_by_v2_id[V2Id("m-1")] = IdmcMapping(
            v2_id="m-1", name="my_mapping", asset_frs_guid="guid-1"
        )
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="m-1",
            mapping_name="my_mapping",
        )
        _, job = list(source._emit_mapping_task(mt))
        assert isinstance(job, DataJob)
        assert source._mapping_v3_to_mt_job_urns[V3Guid("guid-1")] == [str(job.urn)]
        assert source._mapping_ids == [V3Guid("guid-1")]

    def test_emit_mapping_task_skips_lineage_fanout_when_mapping_unknown(self):
        # Orphaned MT (v2 enrichment didn't find the mapping): no fan-out
        # registration, no entry in the export batch, no dangling lineage.
        source = _make_source()
        mt = IdmcMappingTask(
            v2_id="mt-1",
            name="nightly_task",
            mapping_id="unknown-id",
            mapping_name="m",
        )
        _, job = list(source._emit_mapping_task(mt))
        assert isinstance(job, DataJob)
        assert source._mapping_v3_to_mt_job_urns == {}
        assert source._mapping_ids == []
        # No dataJobInputOutput aspect is emitted at MT-emit time — that's
        # the lineage phase's job (inputDatasets/outputDatasets only).
        assert job._get_aspect(DataJobInputOutputClass) is None

    def test_two_mts_pointing_at_same_mapping_share_lineage_fanout(self):
        # One Mapping with two MTs referencing it: both MT DataJob URNs land
        # in the fan-out list so the lineage phase emits lineage to both.
        source = _make_source()
        source._v2_mappings_by_v2_id[V2Id("m-1")] = IdmcMapping(
            v2_id="m-1", name="shared", asset_frs_guid="guid-1"
        )
        for mt_name in ("nightly", "hourly"):
            mt = IdmcMappingTask(
                v2_id=f"mt-{mt_name}",
                name=mt_name,
                mapping_id="m-1",
                mapping_name="shared",
            )
            list(source._emit_mapping_task(mt))
        urns = source._mapping_v3_to_mt_job_urns[V3Guid("guid-1")]
        assert len(urns) == 2
        # Export batch deduped to one mapping despite two MTs.
        assert source._mapping_ids == [V3Guid("guid-1")]

    def test_extract_ownership_disabled_suppresses_owners(self):
        source = _make_source(extract_ownership=False)
        assert source._owner_list("alice", "bob") is None

    def test_empty_user_identifier_returns_none(self):
        source = _make_source()
        assert source._owner_list(None, None) is None
        assert source._owner_list("", "") is None

    def test_owner_list_emits_creator_as_dataowner_and_updater_as_technical(self):
        source = _make_source()
        owners = source._owner_list("alice", "bob")
        assert owners is not None
        assert len(owners) == 2
        assert owners[0][1] == OwnershipTypeClass.DATAOWNER
        assert owners[1][1] == OwnershipTypeClass.TECHNICAL_OWNER

    def test_owner_list_dedupes_when_creator_and_updater_match(self):
        source = _make_source()
        owners = source._owner_list("alice", "alice")
        assert owners is not None
        assert len(owners) == 1

    def test_owner_urn_keeps_email_domain_by_default(self):
        source = _make_source()
        owners = source._owner_list("alice@acme.com", None)
        assert owners is not None
        assert str(owners[0][0]) == "urn:li:corpuser:alice@acme.com"

    def test_owner_urn_strips_email_domain_when_configured(self):
        source = _make_source(strip_user_email_domain=True)
        owners = source._owner_list("alice@acme.com", None)
        assert owners is not None
        assert str(owners[0][0]) == "urn:li:corpuser:alice"


class TestTestConnection:
    BASE_CONFIG = {
        "username": "svc",
        "password": "pw",
        "login_url": "https://dm-us.informaticacloud.com",
    }

    def test_success_reports_basic_connectivity(self):
        with patch(
            "datahub.ingestion.source.informatica.source.InformaticaClient"
        ) as MockClient:
            MockClient.return_value.login.return_value = "session"
            result = InformaticaSource.test_connection(dict(self.BASE_CONFIG))
        assert result.basic_connectivity is not None
        assert result.basic_connectivity.capable is True
        MockClient.return_value.close.assert_called_once()

    def test_login_failure_reports_incapable(self):
        with patch(
            "datahub.ingestion.source.informatica.source.InformaticaClient"
        ) as MockClient:
            MockClient.return_value.login.side_effect = InformaticaLoginError(
                "bad credentials"
            )
            result = InformaticaSource.test_connection(dict(self.BASE_CONFIG))
        assert result.basic_connectivity is not None
        assert result.basic_connectivity.capable is False
        assert "bad credentials" in (result.basic_connectivity.failure_reason or "")

    def test_bad_config_reports_internal_failure(self):
        result = InformaticaSource.test_connection({"bogus": "config"})
        assert result.internal_failure is True
        assert "parse config" in (result.internal_failure_reason or "")


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
        mt_job_urn = (
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        )
        source._mapping_v3_to_mt_job_urns[V3Guid("guid-42")] = [mt_job_urn]
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
        # Both aspects are needed — backend derivation of dataset upstreams from
        # DataJobInputOutput isn't reliable across all UI views.
        assert "upstreamLineage" in aspect_names
        io_mcp = next(m for m in mcps if m.aspectName == "dataJobInputOutput")
        # dataJobInputOutput lands on the MT DataJob (not the Mapping's DataJob),
        # because Mappings aren't emitted — MTs are the runnable entities.
        assert io_mcp.entityUrn == mt_job_urn
        assert source.report.lineage_edges_emitted == 2

    def test_emit_lineage_fans_out_to_every_mt_referencing_mapping(self):
        # Two MTs running the same Mapping both receive dataJobInputOutput
        # from a single export result.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Oracle",
            federated_id="fed",
            database="OLTP",
            db_schema="APPS",
        )
        mt_urns = [
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-a,PROD),transform)",
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-b,PROD),transform)",
        ]
        source._mapping_v3_to_mt_job_urns[V3Guid("guid-42")] = list(mt_urns)
        lineage = MappingLineageInfo(
            mapping_id="guid-42",
            mapping_name="shared",
            source_tables=[LineageTable(table_name="T", connection_federated_id="fed")],
            target_tables=[],
        )
        wus = list(source._emit_lineage(lineage))
        io_mcps = [
            wu.metadata
            for wu in wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.aspectName == "dataJobInputOutput"
        ]
        emitted = sorted(m.entityUrn or "" for m in io_mcps)
        assert emitted == sorted(mt_urns)

    def test_emit_lineage_skips_when_no_mt_references_mapping(self):
        # Export returns a mapping id that no MT referenced (e.g. orphaned or
        # MT filtered out). Warn and skip — don't fabricate dangling URNs.
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
        source._mapping_v3_to_mt_job_urns[V3Guid("m-1")] = [
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        ]
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

    def test_emit_lineage_stubs_each_external_urn_with_status_exactly_once(self):
        # External URNs need a ``Status`` stub so ``searchAcrossLineage``
        # finds them (otherwise the MT's left-chevron expansion is empty).
        # Dedup via ``_stubbed_external_dataset_urns`` — a second mapping
        # referencing the same URN must not re-emit the stub.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c",
            name="prod",
            conn_type="Oracle",
            federated_id="fed",
            database="OLTP",
            db_schema="APPS",
        )
        mt_urn = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt,PROD),transform)"
        source._mapping_v3_to_mt_job_urns[V3Guid("guid-1")] = [mt_urn]
        source._mapping_v3_to_mt_job_urns[V3Guid("guid-2")] = [mt_urn]

        shared_src = LineageTable(
            table_name="SHARED_SRC", connection_federated_id="fed"
        )
        shared_tgt = LineageTable(
            table_name="SHARED_TGT", connection_federated_id="fed"
        )
        first = MappingLineageInfo(
            mapping_id="guid-1",
            mapping_name="map_1",
            source_tables=[shared_src],
            target_tables=[shared_tgt],
        )
        second = MappingLineageInfo(
            mapping_id="guid-2",
            mapping_name="map_2",
            source_tables=[shared_src],
            target_tables=[shared_tgt],
        )
        first_wus = list(source._emit_lineage(first))
        second_wus = list(source._emit_lineage(second))

        def _status_urns(wus: List[MetadataWorkUnit]) -> List[str]:
            return [
                wu.metadata.entityUrn or ""
                for wu in wus
                if isinstance(wu.metadata, MetadataChangeProposalWrapper)
                and wu.metadata.aspectName == "status"
            ]

        # First lineage emission stubs both URNs exactly once.
        first_status_urns = _status_urns(first_wus)
        assert sorted(first_status_urns) == [
            "urn:li:dataset:(urn:li:dataPlatform:oracle,oltp.apps.shared_src,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:oracle,oltp.apps.shared_tgt,PROD)",
        ]
        # Second emission references the same URNs — dedup guard must skip
        # both Status aspects.
        assert _status_urns(second_wus) == []

    def test_emit_lineage_sets_empty_inputdatajobs_for_first_mt_in_chain(self):
        # Entry-point contract: an MT with no Taskflow predecessor emits
        # ``inputDatajobs=[]`` — not omitted, not stale.
        source = _make_source()
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c", name="prod", conn_type="Oracle", federated_id="fed"
        )
        first_mt = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        source._mapping_v3_to_mt_job_urns[V3Guid("guid-1")] = [first_mt]
        # Deliberately leave ``_mt_predecessors`` empty.
        lineage = MappingLineageInfo(
            mapping_id="guid-1",
            mapping_name="first_step",
            source_tables=[LineageTable(table_name="T", connection_federated_id="fed")],
            target_tables=[],
        )
        wus = list(source._emit_lineage(lineage))
        io_mcp = next(
            wu.metadata
            for wu in wus
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
            and wu.metadata.aspectName == "dataJobInputOutput"
        )
        aspect = io_mcp.aspect
        assert isinstance(aspect, DataJobInputOutputClass)
        assert aspect.inputDatajobs == []


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
            raise InformaticaApiError("list call failed (HTTP 403)")

        out = list(source._safe_list(boom, title="t", message="m", context="/api/test"))
        assert out == []
        assert len(source.report.warnings) == 1

    def test_catches_iteration_time_exception(self):
        source = _make_source()

        def gen():
            yield 1
            raise InformaticaApiError("mid-iteration HTTP 500")

        out = list(source._safe_list(gen, title="t", message="m", context="/api/test"))
        assert out == [1]
        assert len(source.report.warnings) == 1

    def test_programmer_errors_propagate_from_safe_list(self):
        # A programmer error (AttributeError, TypeError, etc.) must NOT be
        # silently converted into a warning — that would mask real bugs
        # behind the connector's "graceful degradation" mechanic. Only
        # typed IDMC/network errors are catchable.
        source = _make_source()

        def gen():
            yield 1
            raise AttributeError("genuine bug")

        with pytest.raises(AttributeError):
            list(source._safe_list(gen, title="t", message="m", context="c"))


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
        with (
            patch.object(source.client, "list_objects") as mock_list,
            patch.object(source.client, "prefetch_taskflow_definitions"),
        ):
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
        # flow_id now uses the human-readable name, not the v3 GUID.
        assert "daily_refresh" in str(flow.urn)
        assert flow.display_name == "daily_refresh"
        assert len(source.report.warnings) > 0

    def test_extract_taskflows_batches_prefetch_into_single_call(self):
        # All filtered Taskflows' v3 GUIDs should land in ONE
        # prefetch_taskflow_definitions call, not N separate ones.
        source = _make_source()
        taskflows = [
            IdmcObject(
                id=f"tf-{i}",
                name=f"T{i}",
                path=f"/Explore/P/T{i}",
                object_type="TASKFLOW",
            )
            for i in range(3)
        ]
        with (
            patch.object(source.client, "list_objects") as mock_list,
            patch.object(source.client, "prefetch_taskflow_definitions") as prefetch,
        ):
            mock_list.return_value = iter(taskflows)
            list(source._extract_taskflows())
        prefetch.assert_called_once()
        # Verify all 3 GUIDs passed in the single call, in order.
        (arg,) = prefetch.call_args.args
        assert list(arg) == ["tf-0", "tf-1", "tf-2"]

    def test_list_mapping_tasks_failure_reported(self):
        source = _make_source()
        with (
            patch.object(source.client, "list_mappings", return_value=[]),
            patch.object(source.client, "list_objects", return_value=iter([])),
            patch.object(
                source.client,
                "list_mapping_tasks",
                side_effect=InformaticaApiError("403 Forbidden"),
            ),
        ):
            list(source._extract_mappings_and_tasks())
        assert len(source.report.warnings) > 0

    def test_load_connections_failure_reported_as_failure(self):
        source = _make_source()
        with patch.object(
            source.client, "list_connections", side_effect=InformaticaApiError("500")
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

    def test_extract_lineage_slices_mapping_ids_into_configured_batches(self):
        # 5 mapping ids + batch_size=2 → 3 export submissions with the
        # expected slices [ids[0:2], ids[2:4], ids[4:5]]. Ensures the
        # batching math doesn't drop or duplicate a mapping at the edge.
        source = _make_source(export_batch_size=2)
        source._connections_by_id["c"] = IdmcConnection(id="c", name="n", conn_type="")
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c", name="n", conn_type="", federated_id="fed"
        )
        source._mapping_ids = [V3Guid(f"guid-{i}") for i in range(5)]
        submitted_batches: List[List[str]] = []

        def _submit(batch: List[str]) -> str:
            submitted_batches.append(list(batch))
            return f"job-{len(submitted_batches)}"

        with (
            patch.object(source.client, "submit_export_job", side_effect=_submit),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(state=ExportJobState.SUCCESSFUL, message=""),
            ),
            patch.object(
                source.client, "download_and_parse_export", return_value=iter([])
            ),
        ):
            list(source._extract_lineage())

        assert submitted_batches == [
            ["guid-0", "guid-1"],
            ["guid-2", "guid-3"],
            ["guid-4"],
        ]

    def test_extract_lineage_continues_after_batch_submit_failure(self):
        # A submit failure on batch 1 must not prevent batches 2+ from
        # running; the failed batch is recorded in
        # ``report.export_jobs_failed`` so operators can see which slice
        # didn't make it through.
        source = _make_source(export_batch_size=1)
        source._connections_by_id["c"] = IdmcConnection(id="c", name="n", conn_type="")
        source._connections_by_fed_id["fed"] = IdmcConnection(
            id="c", name="n", conn_type="", federated_id="fed"
        )
        source._mapping_ids = [V3Guid("guid-a"), V3Guid("guid-b"), V3Guid("guid-c")]
        submitted: List[List[str]] = []

        def _submit(batch: List[str]) -> str:
            submitted.append(list(batch))
            if batch == ["guid-b"]:
                raise InformaticaApiError("simulated 500 on batch 2")
            return "job-ok"

        with (
            patch.object(source.client, "submit_export_job", side_effect=_submit),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(state=ExportJobState.SUCCESSFUL, message=""),
            ),
            patch.object(
                source.client, "download_and_parse_export", return_value=iter([])
            ),
        ):
            list(source._extract_lineage())

        assert submitted == [["guid-a"], ["guid-b"], ["guid-c"]]
        assert len(source.report.export_jobs_failed) == 1
        assert "guid-b" not in str(source.report.export_jobs_failed[0])
        # ``batch@N`` identifies which mapping_id slice failed.
        assert "batch@1" in source.report.export_jobs_failed[0]

    def test_extract_lineage_submits_batches_concurrently(self):
        # All submit_export_job calls must be in-flight before any
        # wait_for_export returns — i.e. the ThreadPoolExecutor is actually
        # used for concurrency, not sequentially one-at-a-time.
        source = _make_source(export_batch_size=1, max_concurrent_export_jobs=4)
        source._connections_by_id["c"] = IdmcConnection(id="c", name="n", conn_type="")
        source._mapping_ids = [V3Guid("g1"), V3Guid("g2"), V3Guid("g3")]

        submit_barrier = threading.Barrier(3, timeout=5)
        submitted: List[str] = []

        def _submit(batch: List[str]) -> str:
            submitted.append(batch[0])
            submit_barrier.wait()  # blocks until all 3 submits have been called
            return f"job-{batch[0]}"

        with (
            patch.object(source.client, "submit_export_job", side_effect=_submit),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(state=ExportJobState.SUCCESSFUL, message=""),
            ),
            patch.object(
                source.client, "download_and_parse_export", return_value=iter([])
            ),
        ):
            list(source._extract_lineage())

        assert sorted(submitted) == ["g1", "g2", "g3"]

    def test_extract_lineage_one_failure_does_not_cancel_other_batches(self):
        # A failure in one concurrent future must not prevent the others
        # from completing — error isolation across threads.
        source = _make_source(export_batch_size=1, max_concurrent_export_jobs=4)
        source._connections_by_id["c"] = IdmcConnection(id="c", name="n", conn_type="")
        source._mapping_ids = [V3Guid("ok-1"), V3Guid("fail"), V3Guid("ok-2")]

        def _submit(batch: List[str]) -> str:
            if batch == ["fail"]:
                raise InformaticaApiError("boom")
            return f"job-{batch[0]}"

        with (
            patch.object(source.client, "submit_export_job", side_effect=_submit),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(state=ExportJobState.SUCCESSFUL, message=""),
            ),
            patch.object(
                source.client, "download_and_parse_export", return_value=iter([])
            ),
        ):
            list(source._extract_lineage())

        assert len(source.report.export_jobs_failed) == 1

    def test_submit_and_wait_timeout_does_not_duplicate_warning(self):
        # ``wait_for_export`` already reports TIMEOUT; ``_submit_and_wait_export``
        # must NOT emit a second warning. FAILED/UNKNOWN still get a warning.
        source = _make_source()
        with (
            patch.object(source.client, "submit_export_job", return_value="job-x"),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(
                    state=ExportJobState.TIMEOUT, message="polling gave up"
                ),
            ),
        ):
            warnings_before = len(source.report.warnings)
            source._submit_and_wait_export([V3Guid("guid-x")], batch_start=0)
            warnings_after = len(source.report.warnings)
        assert warnings_after == warnings_before

    def test_submit_and_wait_failed_state_includes_status_message(self):
        # FAILED/UNKNOWN states carry IDMC's ``status.message`` into the warning.
        source = _make_source()
        with (
            patch.object(source.client, "submit_export_job", return_value="job-y"),
            patch.object(
                source.client,
                "wait_for_export",
                return_value=MagicMock(
                    state=ExportJobState.FAILED,
                    message="Asset - export privilege required",
                ),
            ),
        ):
            source._submit_and_wait_export([V3Guid("guid-y")], batch_start=7)
        assert len(source.report.warnings) >= 1
        last_warning_str = "".join(
            f"{w.title} {w.message}" for w in source.report.warnings
        ) + str(source.report.warnings)
        assert "Asset - export" in last_warning_str or "export" in last_warning_str


class TestTaskflowStepEmission:
    TF_OBJ = IdmcObject(
        id="tf-1",
        name="daily_refresh",
        path="/Explore/Sales/Taskflows/daily_refresh",
        object_type="TASKFLOW",
    )

    def _prepared_source(self):
        """Build a source with the Taskflow's DataFlow + MT URN caches seeded."""
        source = _make_source()
        # Simulate earlier phases populating the MT job URN caches so Taskflow
        # steps can resolve their task references.
        source._mt_v2_id_to_job_urn[V2Id("01DM-1")] = (
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        )
        source._mt_v2_id_to_job_urn[V2Id("mt-2")] = (
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-2,PROD),transform)"
        )
        return source

    def test_skips_emission_when_definition_unavailable(self):
        source = self._prepared_source()
        flow = source._make_taskflow(self.TF_OBJ)
        with patch.object(source.client, "get_taskflow_definition", return_value=None):
            entities = list(source._emit_taskflow_steps(self.TF_OBJ, flow))
        assert entities == []

    def test_chains_mts_and_orchestrate_sits_at_end(self):
        # Taskflow step order (s1 → s2) is surfaced as: MT2.inputDatajobs
        # picks up MT1 as a predecessor; orchestrate's inputDatajobs is just
        # the *last* MT in the chain so the Taskflow Lineage view reads
        # ``input_ds → MT1 → MT2 → orchestrate → out_ds``.
        source = self._prepared_source()
        flow = source._make_taskflow(self.TF_OBJ)
        mt1 = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        mt2 = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-2,PROD),transform)"
        definition = TaskflowDefinition(
            taskflow_id="tf-1",
            taskflow_name="daily_refresh",
            steps=[
                TaskflowStep(
                    step_id="s1",
                    step_name="Copy orders",
                    step_type="data",
                    task_ref_id="01DM-1",
                    task_ref_name="nightly_copy_orders",
                ),
                TaskflowStep(
                    step_id="s2",
                    step_name="Enrich customers",
                    step_type="data",
                    task_ref_id="mt-2",
                    predecessor_step_ids=["s1"],
                ),
            ],
        )
        with patch.object(
            source.client, "get_taskflow_definition", return_value=definition
        ):
            [job] = list(source._emit_taskflow_steps(self.TF_OBJ, flow))

        dio = job._get_aspect(DataJobInputOutputClass)
        assert dio is not None
        # Orchestrate points at the last MT only (not all MTs in parallel).
        assert dio.inputDatajobs == [mt2]
        # MT2 picks up MT1 as a predecessor; MT1 has no predecessor in this TF.
        assert source._mt_predecessors == {mt2: [mt1]}
        # Reverse index for the lineage-phase output-dataset bubble-up.
        assert source._orchestrate_by_last_mt[mt2] == [str(job.urn)]
        # Step metadata still preserved on orchestrate for auditing.
        assert job.custom_properties.get("stepOrder") == "s1 → s2"
        assert job.custom_properties.get("mappingTaskCount") == "2"

    def test_dedupes_self_chain_when_taskflow_runs_same_mt_twice(self):
        # If the same MT appears in two consecutive steps, we don't add it
        # as its own predecessor.
        source = self._prepared_source()
        flow = source._make_taskflow(self.TF_OBJ)
        definition = TaskflowDefinition(
            taskflow_id="tf-1",
            taskflow_name="dup",
            steps=[
                TaskflowStep(
                    step_id="s1",
                    step_name="Run copy",
                    step_type="data",
                    task_ref_id="01DM-1",
                ),
                TaskflowStep(
                    step_id="s2",
                    step_name="Run copy again",
                    step_type="data",
                    task_ref_id="01DM-1",  # same MT
                    predecessor_step_ids=["s1"],
                ),
            ],
        )
        with patch.object(
            source.client, "get_taskflow_definition", return_value=definition
        ):
            list(source._emit_taskflow_steps(self.TF_OBJ, flow))
        mt1 = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)"
        # No self-loop predecessor added.
        assert source._mt_predecessors.get(mt1, []) == []

    def test_orchestrate_mirrors_last_mt_outputs(self):
        # End-of-lineage pass emits the orchestrate's DataJobInputOutput with
        # the *last* MT's output datasets mirrored. ``inputDatajobs`` stays
        # as the last MT only, so the graph tail reads
        # ``last_MT → orchestrate → output_ds``.
        source = self._prepared_source()
        orch_urn = (
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,tf-1,PROD),orchestrate)"
        )
        last_mt = "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-2,PROD),transform)"
        source._orchestrate_state[orch_urn] = OrchestrateState(
            last_mt_urn=last_mt,
            mt_urns_in_order=[
                "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt-1,PROD),transform)",
                last_mt,
            ],
            output_datasets={
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.orders,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.customers,PROD)",
            },
        )
        workunits = list(source._emit_orchestrate_aggregated_lineage())
        assert len(workunits) == 1
        aspect = workunits[0].metadata.aspect
        assert aspect is not None
        assert aspect.inputDatajobs == [last_mt]
        assert aspect.inputDatasets == []
        assert aspect.outputDatasets == [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.customers,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,dwh.orders,PROD)",
        ]

    def test_orchestrate_skips_aggregation_when_last_mt_has_no_outputs(self):
        # No outputs resolved (export failure etc.) — keep the base emission
        # with inputDatajobs=[last_mt] and no dataset edges.
        source = self._prepared_source()
        source._orchestrate_state[
            "urn:li:dataJob:(urn:li:dataFlow:(informatica,tf-1,PROD),orchestrate)"
        ] = OrchestrateState(
            last_mt_urn=(
                "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt,PROD),transform)"
            ),
            mt_urns_in_order=[
                "urn:li:dataJob:(urn:li:dataFlow:(informatica,mt,PROD),transform)"
            ],
        )
        assert list(source._emit_orchestrate_aggregated_lineage()) == []

    def test_skips_emission_when_no_resolvable_mts(self):
        # Taskflow with only command / decision steps — no MT references to
        # surface, so no orchestrate DataJob is emitted (keeps the catalog
        # free of empty wrappers).
        source = self._prepared_source()
        flow = source._make_taskflow(self.TF_OBJ)
        definition = TaskflowDefinition(
            taskflow_id="tf-1",
            taskflow_name="admin_only",
            steps=[
                TaskflowStep(
                    step_id="cmd",
                    step_name="Shell step",
                    step_type="command",
                ),
            ],
        )
        with patch.object(
            source.client, "get_taskflow_definition", return_value=definition
        ):
            entities = list(source._emit_taskflow_steps(self.TF_OBJ, flow))
        assert entities == []


class TestSourceLifecycle:
    def test_stale_entity_removal_handler_registered(self):
        # Stateful ingestion hinges on StaleEntityRemovalHandler being in the
        # workunit processor chain. Verify the returned workunit_processor is
        # actually registered — a refactor that calls ``create`` but drops the
        # result would silently leave deleted IDMC entities live in DataHub.
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

    def test_summarize_taskflow_steps_warns_on_partial_coverage(self):
        # Missing step DAGs usually mean missing ``Asset - export``;
        # surface on the report so operators see it without log diving.
        source = _make_source()
        source.report.taskflows_scanned = 3
        source.report.taskflows_with_steps = 1
        source._summarize_taskflow_steps()
        assert len(source.report.warnings) == 1
        w = source.report.warnings[0]
        assert w.title == "Taskflow step DAG missing for some Taskflows"
        assert "Asset - export" in "".join(w.message or [])
        assert "1/3" in "".join(w.message or [])

    def test_summarize_taskflow_steps_silent_on_full_coverage(self):
        source = _make_source()
        source.report.taskflows_scanned = 5
        source.report.taskflows_with_steps = 5
        source._summarize_taskflow_steps()
        assert source.report.warnings == []

    def test_summarize_taskflow_steps_silent_when_no_taskflows_scanned(self):
        source = _make_source()
        source.report.taskflows_scanned = 0
        source.report.taskflows_with_steps = 0
        source._summarize_taskflow_steps()
        assert source.report.warnings == []
