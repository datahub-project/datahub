"""
Comprehensive integration tests for Google Sheets connector.

These tests use mock API responses stored in the setup/ directory to simulate
real Google Drive and Sheets API interactions.
"""

import json
import pathlib
from typing import Any, Dict
from unittest import mock

import pytest
from freezegun import freeze_time

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

FROZEN_TIME = "2024-03-20 12:00:00"

test_resources_dir = pathlib.Path(__file__).parent


def read_mock_response(file_name: str) -> Dict[str, Any]:
    """Read a mock API response from the setup directory."""
    response_path = test_resources_dir / "setup" / file_name
    with open(response_path) as file:
        return json.load(file)


def create_mock_drive_service():
    """Create a mock Google Drive service with realistic API responses."""
    mock_service = mock.MagicMock()

    # Mock files().list() for My Drive
    def mock_files_list(**kwargs):
        mock_request = mock.MagicMock()

        # Determine which mock response to return based on drive ID
        drive_id = kwargs.get("driveId")

        if drive_id == "drive_engineering_team":
            mock_request.execute.return_value = read_mock_response(
                "drive_files_engineering_drive.json"
            )
        else:
            mock_request.execute.return_value = read_mock_response(
                "drive_files_mydrive.json"
            )

        return mock_request

    mock_service.files.return_value.list.side_effect = mock_files_list

    # Mock files().get() for individual file metadata
    def mock_files_get(**kwargs):
        mock_request = mock.MagicMock()
        file_id = kwargs.get("fileId")
        fields = kwargs.get("fields", "")

        # Return appropriate mock data based on file ID
        if "permissions" in fields:
            # Return file with permissions
            mydrive_files = read_mock_response("drive_files_mydrive.json")
            for file_data in mydrive_files.get("files", []):
                if file_data["id"] == file_id:
                    mock_request.execute.return_value = file_data
                    return mock_request

        # Default: return basic file metadata
        mock_request.execute.return_value = {
            "id": file_id,
            "name": f"File {file_id}",
            "parents": ["root"],
        }
        return mock_request

    mock_service.files.return_value.get.side_effect = mock_files_get

    # Mock drives().list() for shared drives
    def mock_drives_list(**kwargs):
        mock_request = mock.MagicMock()
        mock_request.execute.return_value = read_mock_response(
            "drive_shared_drives_list.json"
        )
        return mock_request

    mock_service.drives.return_value.list.side_effect = mock_drives_list

    return mock_service


def create_mock_sheets_service():
    """Create a mock Google Sheets service with realistic API responses."""
    mock_service = mock.MagicMock()

    # Map spreadsheet IDs to mock files
    spreadsheet_mocks = {
        "1abc_sales_report_2024": "spreadsheet_basic.json",
        "2def_product_catalog": "spreadsheet_with_formulas.json",
        "3ghi_engineering_metrics": "spreadsheet_with_jdbc.json",
        "4jkl_budget_template": "spreadsheet_with_named_ranges.json",
        "5mno_customer_data": "spreadsheet_header_detection.json",
    }

    # Mock spreadsheets().get()
    def mock_spreadsheets_get(**kwargs):
        mock_request = mock.MagicMock()
        spreadsheet_id = kwargs.get("spreadsheetId", "")

        # Return appropriate mock data
        mock_file = spreadsheet_mocks.get(spreadsheet_id, "spreadsheet_basic.json")
        mock_request.execute.return_value = read_mock_response(mock_file)

        return mock_request

    mock_service.spreadsheets.return_value.get.side_effect = mock_spreadsheets_get

    # Mock spreadsheets().values().get() for value ranges
    def mock_values_get(**kwargs):
        mock_request = mock.MagicMock()
        spreadsheet_id = kwargs.get("spreadsheetId", "")
        range_notation = kwargs.get("range", "")

        # Return sample values based on the spreadsheet
        if spreadsheet_id == "1abc_sales_report_2024":
            mock_request.execute.return_value = {
                "range": range_notation,
                "majorDimension": "ROWS",
                "values": [
                    ["Date", "Product", "Quantity", "Revenue", "Region"],
                    ["2024-01-01", "Widget A", "150", "15000", "North"],
                    ["2024-01-02", "Widget B", "200", "30000", "South"],
                ],
            }
        else:
            mock_request.execute.return_value = {
                "range": range_notation,
                "majorDimension": "ROWS",
                "values": [],
            }

        return mock_request

    mock_service.spreadsheets.return_value.values.return_value.get.side_effect = (
        mock_values_get
    )

    return mock_service


def create_mock_drive_activity_service():
    """Create a mock Google Drive Activity service for usage statistics."""
    mock_service = mock.MagicMock()

    # Mock activity query
    def mock_query_activity(**kwargs):
        mock_request = mock.MagicMock()
        mock_request.execute.return_value = read_mock_response(
            "drive_activity_usage.json"
        )
        return mock_request

    mock_service.activity.return_value.query.side_effect = mock_query_activity

    return mock_service


@pytest.fixture
def mock_google_apis():
    """Mock all Google API services."""
    with mock.patch("googleapiclient.discovery.build") as mock_build:
        # Create mock services
        mock_drive = create_mock_drive_service()
        mock_sheets = create_mock_sheets_service()
        mock_drive_activity = create_mock_drive_activity_service()

        # Configure build to return appropriate service
        def build_service(service_name, version, **kwargs):
            if service_name == "drive":
                return mock_drive
            elif service_name == "sheets":
                return mock_sheets
            elif service_name == "driveactivity":
                return mock_drive_activity
            else:
                raise ValueError(f"Unknown service: {service_name}")

        mock_build.side_effect = build_service

        yield {
            "drive": mock_drive,
            "sheets": mock_sheets,
            "driveactivity": mock_drive_activity,
        }


@pytest.fixture
def mock_credentials():
    """Mock Google service account credentials."""
    with mock.patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"
    ) as mock_creds:
        mock_creds.return_value = mock.MagicMock()
        yield mock_creds


@freeze_time(FROZEN_TIME)
def test_basic_ingestion(mock_google_apis, mock_credentials, pytestconfig, tmp_path):
    """Test basic ingestion without lineage or profiling."""

    output_path = tmp_path / "google_sheets_basic_mces.json"
    golden_path = test_resources_dir / "google_sheets_basic_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "google-sheets",
                "config": {
                    "credentials": "dummy_credentials.json",
                    "sheets_as_datasets": False,
                    "extract_usage_stats": False,
                    "extract_lineage_from_formulas": False,
                    "profiling": {"enabled": False},
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_lineage_extraction(mock_google_apis, mock_credentials, pytestconfig, tmp_path):
    """Test lineage extraction from IMPORTRANGE and QUERY formulas."""

    output_path = tmp_path / "google_sheets_lineage_mces.json"
    golden_path = test_resources_dir / "google_sheets_lineage_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "google-sheets",
                "config": {
                    "credentials": "dummy_credentials.json",
                    "sheets_as_datasets": False,
                    "extract_lineage_from_formulas": True,
                    "parse_sql_for_lineage": True,
                    "enable_cross_platform_lineage": True,
                    "extract_column_level_lineage": True,
                    "convert_lineage_urns_to_lowercase": True,
                    "database_hostname_to_platform_instance_map": {
                        "bigquery.googleapis.com": "prod",
                        "acme.snowflakecomputing.com": "prod",
                    },
                    "profiling": {"enabled": False},
                    "extract_usage_stats": False,
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_shared_drives_filtering(
    mock_google_apis, mock_credentials, pytestconfig, tmp_path
):
    """Test shared drive scanning with name-based filtering."""

    output_path = tmp_path / "google_sheets_shared_drives_mces.json"
    golden_path = test_resources_dir / "google_sheets_shared_drives_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "google-sheets",
                "config": {
                    "credentials": "dummy_credentials.json",
                    "scan_shared_drives": True,
                    "shared_drive_patterns": {
                        "allow": ["Engineering*", "Product Team"],
                        "deny": ["*Archive*", "*Old*"],
                    },
                    "sheets_as_datasets": False,
                    "extract_usage_stats": False,
                    "extract_lineage_from_formulas": False,
                    "profiling": {"enabled": False},
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_header_auto_detection(
    mock_google_apis, mock_credentials, pytestconfig, tmp_path
):
    """Test automatic header row detection with empty leading rows."""

    output_path = tmp_path / "google_sheets_header_detection_mces.json"
    golden_path = test_resources_dir / "google_sheets_header_detection_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "google-sheets",
                "config": {
                    "credentials": "dummy_credentials.json",
                    "header_detection_mode": "auto_detect",
                    "skip_empty_leading_rows": True,
                    "sheets_as_datasets": False,
                    "extract_usage_stats": False,
                    "extract_lineage_from_formulas": False,
                    "profiling": {"enabled": False},
                    "max_retries": 3,
                    "retry_delay": 1,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


@freeze_time(FROZEN_TIME)
def test_profiling_and_usage(
    mock_google_apis, mock_credentials, pytestconfig, tmp_path
):
    """Test full feature set: profiling, usage stats, tags, named ranges."""

    output_path = tmp_path / "google_sheets_profiling_usage_mces.json"
    golden_path = test_resources_dir / "google_sheets_profiling_usage_golden.json"

    pipeline = Pipeline.create(
        {
            "source": {
                "type": "google-sheets",
                "config": {
                    "credentials": "dummy_credentials.json",
                    "sheets_as_datasets": True,
                    "extract_usage_stats": True,
                    "usage_stats_lookback_days": 30,
                    "extract_tags": True,
                    "extract_named_ranges": True,
                    "extract_lineage_from_formulas": True,
                    "parse_sql_for_lineage": True,
                    "enable_cross_platform_lineage": True,
                    "extract_column_level_lineage": True,
                    "header_detection_mode": "first_row",
                    "skip_empty_leading_rows": True,
                    "profiling": {
                        "enabled": True,
                        "max_rows": 10000,
                        "include_field_null_count": True,
                        "include_field_min_value": True,
                        "include_field_max_value": True,
                        "include_field_mean_value": True,
                    },
                    "enable_incremental_ingestion": True,
                    "max_retries": 3,
                    "retry_delay": 1,
                    "requests_per_second": 10,
                },
            },
            "sink": {
                "type": "file",
                "config": {
                    "filename": str(output_path),
                },
            },
        }
    )

    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )
