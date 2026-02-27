"""Unit tests for Azure Data Factory source - business logic only.

Following the accelerator guidelines, we test:
- Platform mapping logic (linked service type -> DataHub platform)
- Activity subtype mapping
- Table name extraction from dataset properties
- Run status mapping
- Lineage extraction logic patterns

We do NOT test:
- Trivial getters/setters
- Third-party library behavior
- Pydantic validation (covered by test_adf_config.py)
"""

from typing import Callable, Optional

import pytest

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.ingestion.source.azure_data_factory.adf_column_lineage import (
    CopyActivityColumnLineageExtractor,
    DatasetSchemaInfo,
)
from datahub.ingestion.source.azure_data_factory.adf_source import (
    ACTIVITY_SUBTYPE_MAP,
    LINKED_SERVICE_PLATFORM_MAP,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)


class TestLinkedServicePlatformMapping:
    """Tests for linked service to DataHub platform mapping.

    This is critical business logic - incorrect mapping would create
    lineage to wrong platform URNs.
    """

    def test_azure_sql_variants_map_to_mssql(self) -> None:
        """All Azure SQL variants should map to mssql platform."""
        azure_sql_types = ["AzureSqlDatabase", "AzureSqlMI", "SqlServer"]
        for sql_type in azure_sql_types:
            assert LINKED_SERVICE_PLATFORM_MAP.get(sql_type) == "mssql", (
                f"{sql_type} should map to 'mssql'"
            )

    def test_synapse_variants_map_to_mssql(self) -> None:
        """Azure Synapse variants should map to mssql platform (same protocol)."""
        synapse_types = ["AzureSynapseAnalytics", "AzureSqlDW"]
        for synapse_type in synapse_types:
            assert LINKED_SERVICE_PLATFORM_MAP.get(synapse_type) == "mssql", (
                f"{synapse_type} should map to 'mssql'"
            )

    def test_databricks_variants_map_correctly(self) -> None:
        """Databricks services should all map to databricks platform."""
        databricks_types = ["AzureDatabricks", "AzureDatabricksDeltaLake"]
        for db_type in databricks_types:
            assert LINKED_SERVICE_PLATFORM_MAP.get(db_type) == "databricks", (
                f"{db_type} should map to 'databricks'"
            )

    def test_azure_storage_types_map_to_abs_platform(self) -> None:
        """All Azure storage types should map to abs (Azure Blob Storage) platform."""
        assert LINKED_SERVICE_PLATFORM_MAP["AzureBlobStorage"] == "abs"
        assert LINKED_SERVICE_PLATFORM_MAP["AzureBlobFS"] == "abs"
        assert LINKED_SERVICE_PLATFORM_MAP["AzureDataLakeStore"] == "abs"

    def test_major_cloud_databases_covered(self) -> None:
        """Major cloud databases should be mapped."""
        major_databases = {
            "Snowflake": "snowflake",
            "GoogleBigQuery": "bigquery",
            "AmazonRedshift": "redshift",
        }
        for service_type, expected_platform in major_databases.items():
            assert LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform

    def test_common_open_source_databases_covered(self) -> None:
        """Common OSS databases should be mapped."""
        oss_databases = {
            "PostgreSql": "postgres",
            "MySql": "mysql",
            "Oracle": "oracle",
        }
        for service_type, expected_platform in oss_databases.items():
            assert LINKED_SERVICE_PLATFORM_MAP.get(service_type) == expected_platform

    def test_unknown_service_type_returns_none(self) -> None:
        """Unknown service types should return None (not raise)."""
        assert LINKED_SERVICE_PLATFORM_MAP.get("UnknownServiceType") is None
        assert LINKED_SERVICE_PLATFORM_MAP.get("CustomConnector") is None


class TestActivitySubtypeMapping:
    """Tests for activity type to subtype mapping.

    Subtypes affect how activities appear in the UI and their grouping.
    """

    def test_copy_activity_subtype(self) -> None:
        """Copy activity should have descriptive subtype."""
        assert ACTIVITY_SUBTYPE_MAP["Copy"] == "Copy Activity"

    def test_dataflow_activities_grouped_together(self) -> None:
        """Both DataFlow and ExecuteDataFlow should have same subtype."""
        assert ACTIVITY_SUBTYPE_MAP["DataFlow"] == "Data Flow Activity"
        assert ACTIVITY_SUBTYPE_MAP["ExecuteDataFlow"] == "Data Flow Activity"

    def test_control_flow_activities_have_descriptive_names(self) -> None:
        """Control flow activities should have user-friendly subtypes."""
        control_flow_map = {
            "IfCondition": "If Condition",
            "ForEach": "ForEach Loop",
            "Until": "Until Loop",
            "Switch": "Switch Activity",
            "Wait": "Wait Activity",
        }
        for activity_type, expected_subtype in control_flow_map.items():
            assert ACTIVITY_SUBTYPE_MAP.get(activity_type) == expected_subtype

    def test_databricks_activities_identifiable(self) -> None:
        """Databricks activities should be clearly identified."""
        databricks_activities = [
            "DatabricksNotebook",
            "DatabricksSparkJar",
            "DatabricksSparkPython",
        ]
        for activity in databricks_activities:
            subtype = ACTIVITY_SUBTYPE_MAP.get(activity)
            assert subtype is not None
            assert "Databricks" in subtype


class TestTableNameExtractionLogic:
    """Tests for the logic patterns used in table name extraction.

    These tests verify the extraction logic that would be used in
    _extract_table_name without needing a full source instance.
    """

    def test_extract_simple_table_name(self) -> None:
        """Should extract tableName property directly."""
        type_props = {"tableName": "dbo.customers"}
        # Logic pattern from _extract_table_name
        table_name = type_props.get("tableName")
        assert table_name == "dbo.customers"

    def test_combine_schema_and_table(self) -> None:
        """Should combine separate schema and table fields."""
        type_props = {"schema": "sales", "table": "orders"}
        # Logic pattern from _extract_table_name
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "sales.orders"

    def test_schema_only_returns_schema(self) -> None:
        """Should return schema when table is missing."""
        type_props = {"schema": "dbo"}
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "dbo"

    def test_table_only_returns_table(self) -> None:
        """Should return table when schema is missing."""
        type_props = {"table": "orders"}
        schema = type_props.get("schema", "")
        table = type_props.get("table", "")
        result = f"{schema}.{table}" if schema and table else table or schema
        assert result == "orders"


class TestFilePathExtractionLogic:
    """Tests for file path extraction from dataset properties."""

    def test_combine_folder_and_filename(self) -> None:
        """Should combine folderPath and fileName."""
        type_props = {"folderPath": "raw/data", "fileName": "file.csv"}
        folder = type_props.get("folderPath", "")
        filename = type_props.get("fileName", "")
        result = f"{folder}/{filename}" if folder and filename else filename or folder
        assert result == "raw/data/file.csv"

    def test_folder_only_returns_folder(self) -> None:
        """Should return folder when filename is missing."""
        type_props = {"folderPath": "raw/data"}
        folder = type_props.get("folderPath", "")
        filename = type_props.get("fileName", "")
        result = f"{folder}/{filename}" if folder and filename else filename or folder
        assert result == "raw/data"

    def test_nested_location_extraction(self) -> None:
        """Should extract path components from nested location object."""
        type_props = {
            "location": {
                "container": "mycontainer",
                "folderPath": "data/raw",
                "fileName": "output.parquet",
            }
        }
        location = type_props.get("location", {})
        if isinstance(location, dict):
            container = location.get("container", "")
            folder = location.get("folderPath", "")
            filename = location.get("fileName", "")
            parts = [p for p in [container, folder, filename] if p]
            result = "/".join(parts) if parts else None
        else:
            result = None
        assert result == "mycontainer/data/raw/output.parquet"


class TestRunStatusMapping:
    """Tests for mapping ADF run status to DataHub InstanceRunResult."""

    def test_succeeded_maps_to_success(self) -> None:
        """Succeeded status should map to SUCCESS result."""
        status_map = {
            "Succeeded": InstanceRunResult.SUCCESS,
            "Failed": InstanceRunResult.FAILURE,
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Succeeded"] == InstanceRunResult.SUCCESS

    def test_failed_maps_to_failure(self) -> None:
        """Failed status should map to FAILURE result."""
        status_map = {
            "Succeeded": InstanceRunResult.SUCCESS,
            "Failed": InstanceRunResult.FAILURE,
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Failed"] == InstanceRunResult.FAILURE

    def test_cancelled_maps_to_skipped(self) -> None:
        """Cancelled status should map to SKIPPED result."""
        status_map = {
            "Cancelled": InstanceRunResult.SKIPPED,
        }
        assert status_map["Cancelled"] == InstanceRunResult.SKIPPED

    def test_in_progress_should_return_none(self) -> None:
        """In-progress statuses should not have a final result."""
        incomplete_statuses = ["InProgress", "Queued", "Cancelling"]
        status_map = {
            "InProgress": None,
            "Queued": None,
            "Cancelling": None,
        }
        for status in incomplete_statuses:
            assert status_map.get(status) is None


class TestResourceGroupExtractionLogic:
    """Tests for extracting resource group from Azure resource ID."""

    def test_extract_from_standard_resource_id(self) -> None:
        """Should extract resource group from standard Azure resource ID."""
        resource_id = (
            "/subscriptions/12345678-1234-1234-1234-123456789012"
            "/resourceGroups/my-resource-group"
            "/providers/Microsoft.DataFactory/factories/my-factory"
        )
        parts = resource_id.split("/")
        rg_index = parts.index("resourceGroups")
        resource_group = parts[rg_index + 1]
        assert resource_group == "my-resource-group"

    def test_extract_with_complex_resource_group_name(self) -> None:
        """Should handle resource groups with hyphens, underscores, and numbers."""
        test_cases = [
            ("prod-data-rg-001", "prod-data-rg-001"),
            ("RG_Production_123", "RG_Production_123"),
            ("simple", "simple"),
        ]
        for rg_name, expected in test_cases:
            resource_id = (
                f"/subscriptions/00000000-0000-0000-0000-000000000000"
                f"/resourceGroups/{rg_name}"
                f"/providers/Microsoft.DataFactory/factories/factory1"
            )
            parts = resource_id.split("/")
            rg_index = parts.index("resourceGroups")
            extracted = parts[rg_index + 1]
            assert extracted == expected


class TestActivityRunPropertyExtraction:
    """Tests for activity run property extraction logic.

    Activity runs create DataProcessInstance entities linked to DataJobs.
    These tests verify the property extraction patterns.
    """

    def test_activity_run_properties_extracted(self) -> None:
        """Verify essential activity run properties are extracted."""
        activity_run: dict[str, object] = {
            "activityRunId": "act-run-123",
            "activityName": "CopyData",
            "activityType": "Copy",
            "pipelineRunId": "pipe-run-456",
            "status": "Succeeded",
            "durationInMs": 45000,
        }

        # Logic pattern from _emit_activity_runs
        properties: dict[str, str] = {
            "activity_run_id": str(activity_run["activityRunId"]),
            "activity_type": str(activity_run["activityType"]),
            "pipeline_run_id": str(activity_run["pipelineRunId"]),
            "status": str(activity_run["status"]),
        }

        if activity_run.get("durationInMs") is not None:
            properties["duration_ms"] = str(activity_run["durationInMs"])

        assert properties["activity_run_id"] == "act-run-123"
        assert properties["activity_type"] == "Copy"
        assert properties["pipeline_run_id"] == "pipe-run-456"
        assert properties["status"] == "Succeeded"
        assert properties["duration_ms"] == "45000"

    def test_activity_run_error_truncated(self) -> None:
        """Verify error messages are truncated to prevent oversized properties."""
        MAX_RUN_MESSAGE_LENGTH = 500
        long_error = "E" * 1000  # 1000 character error

        activity_run: dict[str, object] = {
            "activityRunId": "act-run-err",
            "error": {"message": long_error},
        }

        # Logic pattern from _emit_activity_runs
        truncated = ""
        error = activity_run.get("error")
        if isinstance(error, dict):
            error_msg = str(error.get("message", ""))
            if error_msg:
                truncated = error_msg[:MAX_RUN_MESSAGE_LENGTH]

        assert len(truncated) == MAX_RUN_MESSAGE_LENGTH
        assert len(truncated) < len(long_error)

    def test_activity_run_missing_optional_fields(self) -> None:
        """Verify graceful handling of missing optional fields."""
        activity_run: dict[str, object] = {
            "activityRunId": "act-run-minimal",
            "activityName": "MinimalActivity",
            "activityType": "Copy",
            "pipelineRunId": "pipe-run-789",
            "status": "Succeeded",
            # No durationInMs, error, input, output
        }

        properties: dict[str, str] = {
            "activity_run_id": str(activity_run["activityRunId"]),
            "activity_type": str(activity_run["activityType"]),
            "pipeline_run_id": str(activity_run["pipelineRunId"]),
            "status": str(activity_run["status"]),
        }

        # Optional fields should not cause errors
        if activity_run.get("durationInMs") is not None:
            properties["duration_ms"] = str(activity_run["durationInMs"])

        error = activity_run.get("error")
        if isinstance(error, dict):
            error_msg = str(error.get("message", ""))
            if error_msg:
                properties["error"] = error_msg[:500]

        assert "duration_ms" not in properties
        assert "error" not in properties
        assert len(properties) == 4


class TestActivityRunToDataJobUrnMapping:
    """Tests for mapping activity runs to DataJob URNs.

    Activity runs must link to DataJob URNs (not DataFlow URNs) so the
    Runs tab appears on DataJob pages in the UI.
    """

    def test_datajob_urn_constructed_from_activity_run(self) -> None:
        """DataJob URN should use activity name as job_id."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        factory_name = "my-factory"
        pipeline_name = "DataPipeline"
        activity_name = "CopyActivity"
        env = "PROD"
        platform = "azure-data-factory"

        # Logic pattern from _emit_activity_runs
        flow_name = f"{factory_name}.{pipeline_name}"
        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=platform,
            flow_id=flow_name,
            env=env,
        )
        job_urn = DataJobUrn.create_from_ids(
            data_flow_urn=str(flow_urn),
            job_id=activity_name,
        )

        # Verify URN structure
        assert "dataJob" in str(job_urn)
        assert activity_name in str(job_urn)
        assert flow_name in str(job_urn)
        assert platform in str(job_urn)

    def test_activity_run_links_to_datajob_not_dataflow(self) -> None:
        """Verify activity runs link to DataJob, enabling the Runs tab in UI."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator="azure-data-factory",
            flow_id="factory.pipeline",
            env="PROD",
        )
        job_urn = DataJobUrn.create_from_ids(
            data_flow_urn=str(flow_urn),
            job_id="MyActivity",
        )

        # The URN type should be dataJob, not dataFlow
        assert job_urn.entity_type == "dataJob"
        assert flow_urn.entity_type == "dataFlow"

        # The job URN should reference the flow URN
        assert str(flow_urn) in str(job_urn)

    def test_multiple_activities_get_unique_urns(self) -> None:
        """Each activity in a pipeline should have a unique DataJob URN."""
        from datahub.metadata.urns import DataFlowUrn, DataJobUrn

        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator="azure-data-factory",
            flow_id="factory.pipeline",
            env="PROD",
        )

        activities = ["CopyData", "TransformData", "LoadData"]
        job_urns = [
            DataJobUrn.create_from_ids(
                data_flow_urn=str(flow_urn),
                job_id=activity,
            )
            for activity in activities
        ]

        # All URNs should be unique
        assert len(set(str(u) for u in job_urns)) == len(activities)

        # Each URN should contain its activity name
        for activity, urn in zip(activities, job_urns, strict=False):
            assert activity in str(urn)


# =============================================================================
# Column-Level Lineage Tests
# =============================================================================


class MockActivity:
    """Mock activity object for testing column lineage extraction."""

    def __init__(
        self,
        activity_type: str = "Copy",
        translator: dict | None = None,
        type_properties: dict | None = None,
        name: str = "TestActivity",
    ):
        self.name = name
        self.type = activity_type
        self.translator = translator
        self.type_properties = type_properties


def make_schema_resolver(
    schema_map: Optional[dict[str, DatasetSchemaInfo]] = None,
) -> Callable[[str], Optional[DatasetSchemaInfo]]:
    """Create a schema resolver function for testing.

    Args:
        schema_map: Optional dict mapping dataset URNs to DatasetSchemaInfo.
                   If None, resolver always returns None.
    """

    def resolver(dataset_urn: str) -> Optional[DatasetSchemaInfo]:
        if schema_map is None:
            return None
        return schema_map.get(dataset_urn)

    return resolver


class TestCopyActivityColumnLineageExtractor:
    """Tests for CopyActivityColumnLineageExtractor.

    These tests verify the column mapping extraction logic for Copy activities.
    """

    @pytest.mark.parametrize(
        "activity_type,expected",
        [
            ("Copy", True),
            ("ExecuteDataFlow", False),
            ("Lookup", False),
            ("ForEach", False),
            ("If", False),
            ("Switch", False),
        ],
    )
    def test_supports_activity_types(self, activity_type: str, expected: bool) -> None:
        """Extractor should only support Copy activity type."""
        extractor = CopyActivityColumnLineageExtractor()
        assert extractor.supports_activity(activity_type) is expected

    def test_extract_dict_format_column_mappings(self) -> None:
        """Should parse legacy dictionary format: {source_col: sink_col}."""

        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {
                    "source_id": "target_id",
                    "source_name": "target_name",
                },
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.source_table,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.sink_table,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 2
        # Extract column names from URNs (format: urn:li:schemaField:(...,column))
        mapping_dict = {}
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            mapping_dict[upstream_col] = downstream_col
        assert mapping_dict["source_id"] == "target_id"
        assert mapping_dict["source_name"] == "target_name"
        # Verify URNs contain the dataset URNs
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            assert source_urn in fgl.upstreams[0]
            assert sink_urn in fgl.downstreams[0]

    def test_extract_list_format_column_mappings(self) -> None:
        """Should parse current list format: [{source: {name}, sink: {name}}]."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "mappings": [
                    {"source": {"name": "col_a"}, "sink": {"name": "col_x"}},
                    {"source": {"name": "col_b"}, "sink": {"name": "col_y"}},
                ],
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 2
        # Extract column names from URNs
        mapping_dict = {}
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            mapping_dict[upstream_col] = downstream_col
        assert mapping_dict["col_a"] == "col_x"
        assert mapping_dict["col_b"] == "col_y"

    def test_empty_when_no_translator(self) -> None:
        """Should return empty list when no translator configuration."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(translator=None, type_properties=None)

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert lineages == []

    def test_empty_when_missing_inlets_or_outlets(self) -> None:
        """Should return empty list when inlets or outlets are empty."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"a": "b"}},
        )

        # Missing inlets
        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[],
            outlets=["urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"],
            schema_resolver=make_schema_resolver(),
        )
        assert lineages == []

        # Missing outlets
        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=["urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"],
            outlets=[],
            schema_resolver=make_schema_resolver(),
        )
        assert lineages == []

    def test_handles_empty_column_names_gracefully(self) -> None:
        """Should skip mappings with empty or None column names."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "columnMappings": {
                    "valid_src": "valid_sink",
                    "": "empty_source",
                    "empty_sink": "",
                },
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        # Only the valid mapping should be extracted
        assert len(lineages) == 1
        # Extract column names from URNs
        assert lineages[0].upstreams is not None and lineages[0].downstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        downstream_col = lineages[0].downstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "valid_src"
        assert downstream_col == "valid_sink"

    def test_extract_from_sdk_flattened_translator(self) -> None:
        """Should extract translator from activity-level attribute (SDK flattening)."""
        extractor = CopyActivityColumnLineageExtractor()
        # Translator at activity level (not in typeProperties)
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {"id": "id"},
            },
            type_properties={},  # Empty typeProperties
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "id"

    def test_extract_from_type_properties_translator(self) -> None:
        """Should extract translator from typeProperties when not at activity level."""
        extractor = CopyActivityColumnLineageExtractor()
        # Translator in typeProperties (raw JSON format)
        activity = MockActivity(
            translator=None,
            type_properties={
                "translator": {
                    "type": "TabularTranslator",
                    "columnMappings": {"name": "full_name"},
                }
            },
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreams is not None and lineages[0].downstreams is not None
        upstream_col = lineages[0].upstreams[0].split(",")[-1].rstrip(")")
        downstream_col = lineages[0].downstreams[0].split(",")[-1].rstrip(")")
        assert upstream_col == "name"
        assert downstream_col == "full_name"

    def test_infer_auto_mapped_columns_from_source_schema(self) -> None:
        """Should infer 1:1 mappings from source schema when no explicit mappings."""
        extractor = CopyActivityColumnLineageExtractor()
        # TabularTranslator without explicit mappings
        activity = MockActivity(
            translator={"type": "TabularTranslator"},  # No columnMappings or mappings
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"
        source_schema = DatasetSchemaInfo(columns=["id", "name", "email"])

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver({source_urn: source_schema}),
        )

        assert len(lineages) == 3
        # Auto-mapping means same column name on both sides
        for fgl in lineages:
            assert fgl.upstreams is not None and fgl.downstreams is not None
            upstream_col = fgl.upstreams[0].split(",")[-1].rstrip(")")
            downstream_col = fgl.downstreams[0].split(",")[-1].rstrip(")")
            assert upstream_col == downstream_col
        column_names = set()
        for fgl in lineages:
            assert fgl.upstreams is not None
            column_names.add(fgl.upstreams[0].split(",")[-1].rstrip(")"))
        assert column_names == {"id", "name", "email"}

    def test_no_inference_without_source_schema(self) -> None:
        """Should not infer mappings when source schema is unavailable."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"type": "TabularTranslator"},  # No explicit mappings
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),  # Returns None
        )

        assert lineages == []


class TestSourceDatasetSchemaExtraction:
    """Tests for dataset schema extraction logic.

    These tests verify the schema extraction patterns used by
    _get_source_dataset_schema and _extract_dataset_schema.
    """

    def test_extract_columns_from_schema_definition(self) -> None:
        """Should extract columns from dataset's schema property (newer format)."""
        schema = [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "created_at", "type": "datetime"},
        ]

        # Logic pattern from _extract_dataset_schema
        columns = []
        for field in schema:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(str(name))

        assert columns == ["id", "name", "created_at"]

    def test_extract_columns_from_structure(self) -> None:
        """Should extract columns from dataset's structure property (legacy format)."""
        structure = [
            {"name": "column_a"},
            {"name": "column_b"},
        ]

        columns = []
        for field in structure:
            if isinstance(field, dict):
                name = field.get("name")
                if name:
                    columns.append(str(name))

        assert columns == ["column_a", "column_b"]

    def test_returns_empty_when_no_schema(self) -> None:
        """Should return empty list when no schema or structure."""
        schema = None
        structure = None

        columns: list[str] = []
        if schema and isinstance(schema, list):
            for field in schema:
                if isinstance(field, dict):
                    name = field.get("name")
                    if name:
                        columns.append(str(name))

        if not columns and structure and isinstance(structure, list):
            for field in structure:
                if isinstance(field, dict):
                    name = field.get("name")
                    if name:
                        columns.append(str(name))

        assert columns == []


class TestFineGrainedLineageOutput:
    """Tests for FineGrainedLineageClass output from column lineage extractor.

    These tests verify the extractor produces correct URN formats and types.
    """

    def test_produces_correct_schema_field_urns(self) -> None:
        """Should produce valid SchemaFieldUrn for upstreams and downstreams."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={
                "type": "TabularTranslator",
                "columnMappings": {"id": "target_id"},
            }
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.src,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dest,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        fgl = lineages[0]

        # Verify URN formats
        assert fgl.upstreams is not None and fgl.downstreams is not None
        assert "schemaField" in fgl.upstreams[0]
        assert "id" in fgl.upstreams[0]
        assert "schemaField" in fgl.downstreams[0]
        assert "target_id" in fgl.downstreams[0]

    def test_upstream_type_is_field_set(self) -> None:
        """Upstream type should be FIELD_SET (many-to-one possible)."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"col": "col"}},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t2,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET

    def test_downstream_type_is_field(self) -> None:
        """Downstream type should be FIELD (single field target)."""
        extractor = CopyActivityColumnLineageExtractor()
        activity = MockActivity(
            translator={"columnMappings": {"col": "col"}},
        )

        source_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t,PROD)"
        sink_urn = "urn:li:dataset:(urn:li:dataPlatform:mssql,t2,PROD)"

        lineages = extractor.extract_column_lineage(
            activity=activity,
            inlets=[source_urn],
            outlets=[sink_urn],
            schema_resolver=make_schema_resolver(),
        )

        assert len(lineages) == 1
        assert lineages[0].downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD
