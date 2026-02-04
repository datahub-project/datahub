"""Integration tests for Dataplex source with mocked API and golden file validation.

This tests the Dataplex source using the Entries API (Universal Catalog) which is
the only supported API for metadata extraction.
"""

import datetime
from pathlib import Path
from typing import Any, Dict
from unittest.mock import Mock, patch

from freezegun import freeze_time
from google.cloud import dataplex_v1
from google.protobuf import struct_pb2

from datahub.testing import mce_helpers
from tests.test_helpers.state_helpers import run_and_get_pipeline

FROZEN_TIME = "2024-01-15 12:00:00"


def dataplex_entries_recipe(mcp_output_path: str) -> Dict[str, Any]:
    """Create a test recipe for Dataplex ingestion using Entries API."""
    return {
        "source": {
            "type": "dataplex",
            "config": {
                "project_ids": ["test-project"],
                "entries_location": "us",
                "include_lineage": False,  # Disable lineage for simpler test
                "include_schema": True,
            },
        },
        "sink": {"type": "file", "config": {"filename": mcp_output_path}},
    }


def create_mock_entry_group(
    project_id: str,
    location: str,
    entry_group_id: str,
) -> Mock:
    """Create a mock Dataplex Entry Group."""
    entry_group = Mock(spec=dataplex_v1.EntryGroup)
    entry_group.name = (
        f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}"
    )
    entry_group.display_name = f"{entry_group_id} Display"
    entry_group.description = f"Description for {entry_group_id}"
    entry_group.create_time = datetime.datetime(
        2024, 1, 1, tzinfo=datetime.timezone.utc
    )
    entry_group.update_time = datetime.datetime(
        2024, 1, 10, tzinfo=datetime.timezone.utc
    )
    return entry_group


def create_mock_entry(
    project_id: str,
    location: str,
    entry_group_id: str,
    entry_id: str,
    fqn: str,
    entry_type: str = "projects/test-project/locations/us/entryTypes/bigquery-table",
    description: str = "",
) -> Mock:
    """Create a mock Dataplex Entry from Universal Catalog.

    Args:
        project_id: GCP project ID
        location: GCP location
        entry_group_id: Entry group ID
        entry_id: Entry ID (typically the table/object name)
        fqn: Fully qualified name (e.g., "bigquery:project.dataset.table")
        entry_type: Entry type identifier
        description: Entry description
    """
    entry = Mock(spec=dataplex_v1.Entry)
    entry.name = f"projects/{project_id}/locations/{location}/entryGroups/{entry_group_id}/entries/{entry_id}"
    entry.fully_qualified_name = fqn
    entry.entry_type = entry_type
    entry.parent_entry = None  # Explicitly set to None to avoid Mock object issues

    # Create entry_source with timestamps and description
    entry_source = Mock()
    entry_source.create_time = datetime.datetime(
        2024, 1, 4, tzinfo=datetime.timezone.utc
    )
    entry_source.update_time = datetime.datetime(
        2024, 1, 13, tzinfo=datetime.timezone.utc
    )
    entry_source.description = description
    # Explicitly set optional fields to None to avoid Mock object issues
    entry_source.resource = None
    entry_source.system = None
    entry_source.platform = None
    entry.entry_source = entry_source

    # Create empty aspects (tests can populate these as needed)
    entry.aspects = {}

    return entry


def create_mock_entry_with_schema(
    project_id: str,
    location: str,
    entry_group_id: str,
    entry_id: str,
    fqn: str,
    columns: list[tuple[str, str, str]],  # List of (name, type, description)
    entry_type: str = "projects/test-project/locations/us/entryTypes/bigquery-table",
    description: str = "",
) -> Mock:
    """Create a mock Dataplex Entry with schema from Universal Catalog.

    Args:
        project_id: GCP project ID
        location: GCP location
        entry_group_id: Entry group ID
        entry_id: Entry ID
        fqn: Fully qualified name
        columns: List of (column_name, column_type, column_description)
        entry_type: Entry type identifier
        description: Entry description
    """
    entry = create_mock_entry(
        project_id, location, entry_group_id, entry_id, fqn, entry_type, description
    )

    # Create schema aspect with columns
    schema_struct = struct_pb2.Struct()

    # Build columns list for the schema
    columns_list = []
    for col_name, col_type, col_desc in columns:
        col_struct = {
            "column": col_name,
            "dataType": col_type,
            "description": col_desc,
            "mode": "NULLABLE",
        }
        columns_list.append(col_struct)

    schema_struct.update({"columns": columns_list})

    # Create aspect with schema
    schema_aspect = Mock()
    schema_aspect.data = schema_struct

    entry.aspects = {"schema": schema_aspect}

    return entry


@freeze_time(FROZEN_TIME)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog.lineage_v1.LineageClient")
def test_dataplex_entries_integration(
    mock_lineage_client_class,
    mock_catalog_client_class,
    mock_google_auth,
    pytestconfig,
    tmp_path,
):
    """Test Dataplex source with Entries API (Universal Catalog) and golden file validation."""

    # Mock Google Application Default Credentials
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")

    # Setup mock clients
    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client

    mock_lineage_client = Mock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # Create mock entry group (system entry group like @bigquery)
    mock_entry_group = create_mock_entry_group("test-project", "us", "@bigquery")

    # Create mock entries
    mock_entry1 = create_mock_entry_with_schema(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id="customers",
        fqn="bigquery:test-project.analytics.customers",
        columns=[
            ("id", "INT64", "Customer ID"),
            ("name", "STRING", "Customer name"),
            ("email", "STRING", "Customer email address"),
            ("created_at", "TIMESTAMP", "Account creation timestamp"),
        ],
        description="Customer master data table",
    )

    mock_entry2 = create_mock_entry_with_schema(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id="orders",
        fqn="bigquery:test-project.analytics.orders",
        columns=[
            ("order_id", "INT64", "Order ID"),
            ("customer_id", "INT64", "Customer ID reference"),
            ("total_amount", "FLOAT64", "Order total amount"),
            ("order_date", "DATE", "Order date"),
        ],
        description="Orders transaction table",
    )

    # Configure mock responses
    mock_catalog_client.list_entry_groups.return_value = [mock_entry_group]
    mock_catalog_client.list_entries.return_value = [mock_entry1, mock_entry2]
    mock_catalog_client.get_entry.side_effect = lambda request: (
        mock_entry1 if "customers" in request.name else mock_entry2
    )

    # Setup paths
    mcp_output_path = tmp_path / "dataplex_entries_mces.json"
    mcp_golden_path = Path(__file__).parent / "golden" / "dataplex_entries_golden.json"

    # Run pipeline
    pipeline_config = dataplex_entries_recipe(str(mcp_output_path))
    run_and_get_pipeline(pipeline_config)

    # Validate against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(mcp_output_path),
        golden_path=str(mcp_golden_path),
    )


@freeze_time(FROZEN_TIME)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog.lineage_v1.LineageClient")
def test_dataplex_multiple_entry_groups(
    mock_lineage_client_class,
    mock_catalog_client_class,
    mock_google_auth,
    pytestconfig,
    tmp_path,
):
    """Test Dataplex source with multiple entry groups (BigQuery and custom)."""

    # Mock Google Application Default Credentials
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")

    # Setup mock clients
    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client

    mock_lineage_client = Mock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # Create mock entry groups
    mock_bigquery_group = create_mock_entry_group("test-project", "us", "@bigquery")
    mock_custom_group = create_mock_entry_group("test-project", "us", "custom-catalog")

    # Create mock entries for BigQuery group
    mock_bq_entry = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="@bigquery",
        entry_id="sales_data",
        fqn="bigquery:test-project.warehouse.sales_data",
        description="Sales data table",
    )

    # Create mock entry for custom group (GCS)
    mock_gcs_entry = create_mock_entry(
        project_id="test-project",
        location="us",
        entry_group_id="custom-catalog",
        entry_id="data_export",
        fqn="gcs:my-bucket/exports/data_export.parquet",
        entry_type="projects/test-project/locations/us/entryTypes/gcs-fileset",
        description="Exported data file",
    )

    # Configure mock responses
    mock_catalog_client.list_entry_groups.return_value = [
        mock_bigquery_group,
        mock_custom_group,
    ]

    def list_entries_side_effect(request):
        if "@bigquery" in request.parent:
            return [mock_bq_entry]
        elif "custom-catalog" in request.parent:
            return [mock_gcs_entry]
        return []

    mock_catalog_client.list_entries.side_effect = list_entries_side_effect
    mock_catalog_client.get_entry.side_effect = lambda request: (
        mock_bq_entry if "sales_data" in request.name else mock_gcs_entry
    )

    # Setup paths
    mcp_output_path = tmp_path / "dataplex_multi_group_mces.json"
    mcp_golden_path = (
        Path(__file__).parent / "golden" / "dataplex_multi_entry_groups_golden.json"
    )

    # Run pipeline
    pipeline_config = dataplex_entries_recipe(str(mcp_output_path))
    run_and_get_pipeline(pipeline_config)

    # Validate against golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=str(mcp_output_path),
        golden_path=str(mcp_golden_path),
    )


@freeze_time(FROZEN_TIME)
@patch("google.auth.default")
@patch("google.cloud.dataplex_v1.CatalogServiceClient")
@patch("google.cloud.datacatalog.lineage_v1.LineageClient")
def test_dataplex_empty_catalog(
    mock_lineage_client_class,
    mock_catalog_client_class,
    mock_google_auth,
    tmp_path,
):
    """Test Dataplex source with empty catalog (no entry groups)."""

    # Mock Google Application Default Credentials
    mock_credentials = Mock()
    mock_google_auth.return_value = (mock_credentials, "test-project")

    # Setup mock clients
    mock_catalog_client = Mock()
    mock_catalog_client_class.return_value = mock_catalog_client

    mock_lineage_client = Mock()
    mock_lineage_client_class.return_value = mock_lineage_client

    # Configure mock responses - empty entry groups
    mock_catalog_client.list_entry_groups.return_value = []

    # Setup paths
    mcp_output_path = tmp_path / "dataplex_empty_mces.json"

    # Run pipeline
    pipeline_config = dataplex_entries_recipe(str(mcp_output_path))
    pipeline = run_and_get_pipeline(pipeline_config)

    # Verify pipeline completed without errors
    assert pipeline.source.get_report().failures == []

    # Output file should exist but be empty or contain minimal data
    assert mcp_output_path.exists()
