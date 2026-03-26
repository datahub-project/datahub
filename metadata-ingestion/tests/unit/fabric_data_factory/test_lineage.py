"""Unit tests for Fabric Data Factory lineage extractors.

Tests CopyActivityLineageExtractor and InvokePipelineLineageExtractor,
covering connection resolution, platform mapping, OneLake URN generation,
table name extraction, and cross-pipeline lineage.
"""

from typing import Any, Dict, List, Optional

from datahub.ingestion.source.fabric.common.models import FabricConnection
from datahub.ingestion.source.fabric.data_factory.lineage import (
    CopyActivityLineageExtractor,
    InvokePipelineLineageExtractor,
)
from datahub.ingestion.source.fabric.data_factory.models import (
    ActivityDependency,
    PipelineActivity,
)
from datahub.ingestion.source.fabric.data_factory.report import (
    FabricDataFactorySourceReport,
)

WS_ID = "ws-11111111-1111-1111-1111-111111111111"
ARTIFACT_ID = "lh-00000000-0000-0000-0000-000000000001"
CHILD_PIPELINE_ID = "pl-child-1111-1111-1111-111111111111"


def _make_activity(
    name: str = "TestActivity",
    activity_type: str = "Copy",
    type_properties: Optional[Dict[str, Any]] = None,
) -> PipelineActivity:
    return PipelineActivity(
        name=name,
        type=activity_type,
        type_properties=type_properties or {},
    )


def _make_connection(
    conn_id: str,
    display_name: str,
    connection_type: str,
) -> FabricConnection:
    return FabricConnection(
        id=conn_id,
        display_name=display_name,
        connection_type=connection_type,
    )


class TestResolvePlatform:
    """Tests for _resolve_platform method."""

    def setup_method(self) -> None:
        self.extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )

    def test_fabric_connection_type(self) -> None:
        assert self.extractor._resolve_platform("Snowflake") == "snowflake"

    def test_fabric_onelake_type(self) -> None:
        assert self.extractor._resolve_platform("Lakehouse") == "fabric-onelake"

    def test_adf_linked_service_fallback(self) -> None:
        # AzureBlobStorage is in ADF_LINKED_SERVICE_PLATFORM_MAP, not FABRIC_CONNECTION_PLATFORM_MAP
        result = self.extractor._resolve_platform("AzureBlobStorage")
        assert result == "abs"

    def test_unmapped_returns_connection_type(self) -> None:
        assert self.extractor._resolve_platform("UnknownDB") == "UnknownDB"


class TestResolvePlatformInstance:
    def setup_method(self) -> None:
        self.extractor = CopyActivityLineageExtractor(
            connections_cache={},
            report=FabricDataFactorySourceReport(),
            env="PROD",
            platform_instance="global-instance",
            platform_instance_map={"My Snowflake": "snowflake-prod"},
        )

    def test_connection_in_map(self) -> None:
        assert (
            self.extractor._resolve_platform_instance("My Snowflake")
            == "snowflake-prod"
        )

    def test_connection_not_in_map_falls_back_to_global(self) -> None:
        assert (
            self.extractor._resolve_platform_instance("Unknown Conn")
            == "global-instance"
        )

    def test_none_connection_falls_back_to_global(self) -> None:
        assert self.extractor._resolve_platform_instance(None) == "global-instance"

    def test_no_map_no_global(self) -> None:
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        assert extractor._resolve_platform_instance("anything") is None


class TestResolveConnectionAndType:
    """Tests for _resolve_connection_and_type - the multi-fallback resolution chain."""

    def test_external_references_cache_hit(self) -> None:
        """Path 1: externalReferences.connection → cache lookup."""
        conn = _make_connection("conn-1", "My Snowflake", "Snowflake")
        extractor = CopyActivityLineageExtractor(
            connections_cache={"conn-1": conn},
            report=FabricDataFactorySourceReport(),
            env="PROD",
        )
        name, ctype = extractor._resolve_connection_and_type(
            {"externalReferences": {"connection": "conn-1"}}
        )
        assert name == "My Snowflake"
        assert ctype == "Snowflake"

    def test_connection_settings_external_references_cache_hit(self) -> None:
        """Path 2a: connectionSettings.properties.externalReferences → cache."""
        conn = _make_connection("conn-2", "My Postgres", "PostgreSQL")
        extractor = CopyActivityLineageExtractor(
            connections_cache={"conn-2": conn},
            report=FabricDataFactorySourceReport(),
            env="PROD",
        )
        name, ctype = extractor._resolve_connection_and_type(
            {
                "connectionSettings": {
                    "properties": {"externalReferences": {"connection": "conn-2"}}
                }
            }
        )
        assert name == "My Postgres"
        assert ctype == "PostgreSQL"

    def test_connection_settings_inline_type(self) -> None:
        """Path 2b: connectionSettings inline type."""
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        name, ctype = extractor._resolve_connection_and_type(
            {
                "connectionSettings": {
                    "name": "LH Connection",
                    "properties": {"type": "Lakehouse"},
                }
            }
        )
        assert name == "LH Connection"
        assert ctype == "Lakehouse"

    def test_linked_service_inline_type(self) -> None:
        """Path 3: linkedService."""
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        name, ctype = extractor._resolve_connection_and_type(
            {
                "linkedService": {
                    "name": "AzureBlob_LS",
                    "properties": {"type": "AzureBlobStorage"},
                }
            }
        )
        assert name == "AzureBlob_LS"
        assert ctype == "AzureBlobStorage"

    def test_no_connection_returns_none(self) -> None:
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        name, ctype = extractor._resolve_connection_and_type({})
        assert name is None
        assert ctype is None

    def test_cache_miss_falls_through(self) -> None:
        """externalReferences with unknown ID falls through to next resolution path."""
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        name, ctype = extractor._resolve_connection_and_type(
            {
                "externalReferences": {"connection": "unknown-id"},
                "connectionSettings": {
                    "name": "Fallback",
                    "properties": {"type": "SQL"},
                },
            }
        )
        assert name == "Fallback"
        assert ctype == "SQL"


class TestExtractTableName:
    def test_schema_and_table(self) -> None:
        result = CopyActivityLineageExtractor._extract_table_name(
            {"schema": "public", "table": "customers"}
        )
        assert result == "public.customers"

    def test_table_only(self) -> None:
        result = CopyActivityLineageExtractor._extract_table_name(
            {"table": "customers"}
        )
        assert result == "customers"

    def test_file_path_from_location(self) -> None:
        result = CopyActivityLineageExtractor._extract_table_name(
            {
                "location": {
                    "container": "raw",
                    "folderPath": "data/",
                    "fileName": "out.csv",
                }
            }
        )
        assert result == "raw/data/out.csv"

    def test_empty_returns_none(self) -> None:
        assert CopyActivityLineageExtractor._extract_table_name({}) is None


class TestExtractFilePath:
    def test_container_folder_file(self) -> None:
        result = CopyActivityLineageExtractor._extract_file_path(
            {"container": "raw", "folderPath": "data/2024/", "fileName": "output.csv"}
        )
        assert result == "raw/data/2024/output.csv"

    def test_filesystem_only(self) -> None:
        result = CopyActivityLineageExtractor._extract_file_path(
            {"fileSystem": "adls-fs"}
        )
        assert result == "adls-fs"

    def test_bucket_name(self) -> None:
        result = CopyActivityLineageExtractor._extract_file_path(
            {"bucketName": "my-bucket", "folderPath": "/prefix/"}
        )
        assert result == "my-bucket/prefix"

    def test_folder_strips_slashes(self) -> None:
        result = CopyActivityLineageExtractor._extract_file_path(
            {"container": "c", "folderPath": "/path/to/data/"}
        )
        assert result == "c/path/to/data"

    def test_empty_location_returns_none(self) -> None:
        assert CopyActivityLineageExtractor._extract_file_path({}) is None


class TestResolveOnelakeUrn:
    def setup_method(self) -> None:
        self.extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )

    def test_structured_with_schema_and_table(self) -> None:
        ds = {
            "connectionSettings": {
                "properties": {
                    "typeProperties": {"artifactId": ARTIFACT_ID, "workspaceId": WS_ID},
                }
            },
            "typeProperties": {"table": "customers", "schema": "dbo"},
        }
        activity = _make_activity()
        result = self.extractor._resolve_onelake_urn(ds, activity, WS_ID)
        assert result is not None
        assert "fabric-onelake" in result
        assert f"{WS_ID}.{ARTIFACT_ID}.dbo.customers" in result

    def test_no_artifact_id_returns_none(self) -> None:
        ds: Dict[str, Any] = {
            "connectionSettings": {"properties": {}},
            "typeProperties": {},
        }
        result = self.extractor._resolve_onelake_urn(ds, _make_activity(), WS_ID)
        assert result is None

    def test_no_table_returns_none(self) -> None:
        """File-based datasets (no table) currently return None."""
        ds = {
            "connectionSettings": {
                "properties": {
                    "typeProperties": {"artifactId": ARTIFACT_ID},
                }
            },
            "typeProperties": {"location": {"container": "Files"}},
        }
        result = self.extractor._resolve_onelake_urn(ds, _make_activity(), WS_ID)
        assert result is None

    def test_workspace_id_fallback_to_pipeline_workspace(self) -> None:
        """When workspaceId is absent, falls back to pipeline's workspace."""
        pipeline_ws = "ws-pipeline-workspace"
        ds = {
            "connectionSettings": {
                "properties": {
                    "typeProperties": {"artifactId": ARTIFACT_ID},
                }
            },
            "typeProperties": {"table": "t1", "schema": "s1"},
        }
        result = self.extractor._resolve_onelake_urn(ds, _make_activity(), pipeline_ws)
        assert result is not None
        assert pipeline_ws in result


class TestCopyExtractLineage:
    """End-to-end tests for CopyActivityLineageExtractor.extract_lineage."""

    def test_external_source_and_onelake_sink(self) -> None:
        conn = _make_connection("conn-sf", "My Snowflake", "Snowflake")
        extractor = CopyActivityLineageExtractor(
            connections_cache={"conn-sf": conn},
            report=FabricDataFactorySourceReport(),
            env="PROD",
        )
        activity = _make_activity(
            type_properties={
                "source": {
                    "datasetSettings": {
                        "externalReferences": {"connection": "conn-sf"},
                        "typeProperties": {"schema": "public", "table": "customers"},
                    }
                },
                "sink": {
                    "datasetSettings": {
                        "connectionSettings": {
                            "properties": {
                                "type": "Lakehouse",
                                "typeProperties": {
                                    "artifactId": ARTIFACT_ID,
                                    "workspaceId": WS_ID,
                                },
                            },
                        },
                        "typeProperties": {
                            "artifactId": ARTIFACT_ID,
                            "workspaceId": WS_ID,
                            "table": "customers",
                            "schema": "dbo",
                        },
                    }
                },
            }
        )
        inputs, outputs = extractor.extract_lineage(activity, WS_ID)
        assert len(inputs) == 1
        assert len(outputs) == 1
        assert "snowflake" in inputs[0]
        assert "public.customers" in inputs[0]
        assert "fabric-onelake" in outputs[0]

    def test_empty_source_and_sink(self) -> None:
        extractor = CopyActivityLineageExtractor(
            connections_cache={}, report=FabricDataFactorySourceReport(), env="PROD"
        )
        activity = _make_activity(type_properties={})
        inputs, outputs = extractor.extract_lineage(activity, WS_ID)
        assert inputs == []
        assert outputs == []

    def test_destination_key_alias_for_sink(self) -> None:
        """The 'destination' key is used as fallback when 'sink' is absent."""
        conn = _make_connection("conn-1", "Dest Conn", "SQL")
        extractor = CopyActivityLineageExtractor(
            connections_cache={"conn-1": conn},
            report=FabricDataFactorySourceReport(),
            env="PROD",
        )
        activity = _make_activity(
            type_properties={
                "source": {
                    "datasetSettings": {
                        "externalReferences": {"connection": "conn-1"},
                        "typeProperties": {"table": "src_table"},
                    }
                },
                "destination": {
                    "datasetSettings": {
                        "externalReferences": {"connection": "conn-1"},
                        "typeProperties": {"table": "dest_table"},
                    }
                },
            }
        )
        inputs, outputs = extractor.extract_lineage(activity, WS_ID)
        assert len(inputs) == 1
        assert len(outputs) == 1


class TestFindRootActivity:
    def test_single_root(self) -> None:
        activities = [
            PipelineActivity(
                name="Root", type="Lookup", type_properties={}, depends_on=[]
            ),
            PipelineActivity(
                name="Step2",
                type="Copy",
                type_properties={},
                depends_on=[
                    ActivityDependency(
                        activity="Root", dependency_conditions=["Succeeded"]
                    )
                ],
            ),
        ]
        result = InvokePipelineLineageExtractor._find_root_activity(activities)
        assert result == "Root"

    def test_empty_list_returns_none(self) -> None:
        assert InvokePipelineLineageExtractor._find_root_activity([]) is None


class TestInvokePipelineExtractLineage:
    def _make_extractor(
        self,
        cache: Optional[Dict] = None,
    ) -> InvokePipelineLineageExtractor:
        return InvokePipelineLineageExtractor(
            pipeline_activities_cache=cache or {},
            report=FabricDataFactorySourceReport(),
            platform="fabric-data-factory",
            env="PROD",
        )

    def test_unsupported_operation_type_returns_none(self) -> None:
        extractor = self._make_extractor()
        activity = _make_activity(
            activity_type="InvokePipeline",
            type_properties={"operationType": "InvokeADFPipeline"},
        )
        assert extractor.extract_lineage(activity, WS_ID) is None

    def test_no_operation_type_returns_none(self) -> None:
        extractor = self._make_extractor()
        activity = _make_activity(
            activity_type="InvokePipeline",
            type_properties={},
        )
        assert extractor.extract_lineage(activity, WS_ID) is None

    def test_no_pipeline_id_returns_none(self) -> None:
        extractor = self._make_extractor()
        activity = _make_activity(
            activity_type="InvokePipeline",
            type_properties={"operationType": "InvokeFabricPipeline"},
        )
        assert extractor.extract_lineage(activity, WS_ID) is None

    def test_child_pipeline_not_in_cache(self) -> None:
        extractor = self._make_extractor()
        activity = _make_activity(
            activity_type="InvokePipeline",
            type_properties={
                "operationType": "InvokeFabricPipeline",
                "pipelineId": CHILD_PIPELINE_ID,
            },
        )
        result = extractor.extract_lineage(activity, WS_ID)
        assert result is not None
        # child_datajob_urn is None because the child pipeline isn't cached
        assert result.child_datajob_urn is None
        assert result.custom_properties["calls_pipeline_id"] == CHILD_PIPELINE_ID

    def test_child_pipeline_resolved(self) -> None:
        child_activities: List[PipelineActivity] = [
            PipelineActivity(
                name="ChildRoot", type="Lookup", type_properties={}, depends_on=[]
            ),
            PipelineActivity(name="ChildStep2", type="Copy", type_properties={}),
        ]
        cache: dict = {(WS_ID, CHILD_PIPELINE_ID): child_activities}
        extractor = self._make_extractor(cache=cache)

        activity = _make_activity(
            name="CallChild",
            activity_type="InvokePipeline",
            type_properties={
                "operationType": "InvokeFabricPipeline",
                "pipelineId": CHILD_PIPELINE_ID,
            },
        )
        result = extractor.extract_lineage(activity, WS_ID)
        assert result is not None
        assert result.child_datajob_urn is not None
        assert "ChildRoot" in result.child_datajob_urn
        assert result.custom_properties["child_root_activity"] == "ChildRoot"
        assert result.custom_properties["operation_type"] == "InvokeFabricPipeline"

    def test_cross_workspace_child(self) -> None:
        """When workspaceId is specified, uses that instead of parent workspace."""
        other_ws = "ws-other-workspace"
        child_activities: List[PipelineActivity] = [
            PipelineActivity(
                name="Step1", type="Copy", type_properties={}, depends_on=[]
            ),
        ]
        cache: dict = {(other_ws, CHILD_PIPELINE_ID): child_activities}
        extractor = self._make_extractor(cache=cache)

        activity = _make_activity(
            activity_type="InvokePipeline",
            type_properties={
                "operationType": "InvokeFabricPipeline",
                "pipelineId": CHILD_PIPELINE_ID,
                "workspaceId": other_ws,
            },
        )
        result = extractor.extract_lineage(activity, WS_ID)
        assert result is not None
        assert result.child_datajob_urn is not None
        assert result.custom_properties["calls_workspace_id"] == other_ws
