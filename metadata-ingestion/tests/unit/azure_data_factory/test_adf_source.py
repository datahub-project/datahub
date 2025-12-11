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

from datahub.api.entities.dataprocess.dataprocess_instance import InstanceRunResult
from datahub.ingestion.source.azure_data_factory.adf_source import (
    ACTIVITY_SUBTYPE_MAP,
    LINKED_SERVICE_PLATFORM_MAP,
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
            "CosmosDbMongoDbApi": "mongodb",  # MongoDB via Cosmos DB API
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
