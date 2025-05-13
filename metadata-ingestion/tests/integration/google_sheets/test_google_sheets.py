import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.google_sheets import (
    GoogleSheetsSource,
    GoogleSheetsSourceConfig,
)

FROZEN_TIME = "2023-05-15 07:00:00"

# Test configurations for parametrized tests
TEST_CONFIGS = [
    {
        "name": "basic_test",
        "config": {
            "credentials": "test_creds.json",
            "sheets_as_datasets": False,
            "extract_usage_stats": True,
            "profiling": {"enabled": True},
        },
        "golden_file": "google_sheets_mces_golden.json",
        "mock_fixture": "mock_basic_sheet_data",
    },
    {
        "name": "lineage_test",
        "config": {
            "credentials": "test_creds.json",
            "sheets_as_datasets": False,
            "extract_lineage_from_formulas": True,
            "extract_column_level_lineage": True,
        },
        "golden_file": "google_sheets_lineage_golden.json",
        "mock_fixture": "mock_lineage_sheet_data",
    },
]


@pytest.fixture(scope="module")
def test_resources_dir(pytestconfig):
    return pytestconfig.rootpath / "tests/integration/google_sheets"


@pytest.fixture
def mock_google_services():
    """Mock the Google API services for testing."""
    with patch("googleapiclient.discovery.build") as mock_build:
        # Set up mock services
        mock_drive_service = MagicMock()
        mock_sheets_service = MagicMock()
        mock_drive_activity_service = MagicMock()

        # Configure the mock build function to return our mock services
        mock_build.side_effect = lambda service, version, credentials: {
            "drive": mock_drive_service,
            "sheets": mock_sheets_service,
            "driveactivity": mock_drive_activity_service,
        }[service]

        yield {
            "drive_service": mock_drive_service,
            "sheets_service": mock_sheets_service,
            "drive_activity_service": mock_drive_activity_service,
        }


@pytest.fixture
def mock_basic_sheet_data(mock_google_services):
    """Set up mocks for basic sheet tests."""
    # Mock files list
    mock_files_list = MagicMock()
    mock_google_services[
        "drive_service"
    ].files.return_value.list.return_value = mock_files_list

    # Mock response for files.list
    mock_files_list.execute.return_value = {
        "files": [
            {
                "id": "test_sheet_id_1",
                "name": "Test Sheet 1",
                "description": "Test sheet for integration tests",
                "owners": [{"emailAddress": "test@example.com"}],
                "createdTime": "2023-01-01T00:00:00.000Z",
                "modifiedTime": "2023-01-02T00:00:00.000Z",
                "webViewLink": "https://docs.google.com/spreadsheets/d/test_sheet_id_1",
                "shared": True,
            }
        ],
        "nextPageToken": None,
    }

    # Mock response for files.get for path resolution
    def mock_files_get(**kwargs):
        file_id = kwargs.get("fileId")
        mock_response = MagicMock()
        if file_id == "test_sheet_id_1":
            mock_response.execute.return_value = {
                "name": "Test Sheet 1",
                "parents": ["test_folder_id"],
            }
        elif file_id == "test_folder_id":
            mock_response.execute.return_value = {
                "name": "Test Folder",
                "parents": None,
            }
        return mock_response

    mock_google_services["drive_service"].files.return_value.get = mock_files_get

    # Mock response for spreadsheets.get
    mock_spreadsheet = MagicMock()
    mock_google_services[
        "sheets_service"
    ].spreadsheets.return_value.get.return_value = mock_spreadsheet

    mock_spreadsheet.execute.return_value = {
        "properties": {"title": "Test Sheet 1"},
        "sheets": [
            {
                "properties": {"title": "Sheet1"},
            }
        ],
    }

    # Mock response for spreadsheets.values.get
    mock_values = MagicMock()
    mock_google_services[
        "sheets_service"
    ].spreadsheets.return_value.values.return_value.get.return_value = mock_values

    mock_values.execute.return_value = {
        "values": [
            ["Name", "Age", "City"],  # Headers
            ["John Doe", "30", "New York"],
            ["Jane Smith", "25", "San Francisco"],
            ["Bob Johnson", "35", "Chicago"],
        ]
    }

    return mock_google_services


@pytest.fixture
def mock_lineage_sheet_data(mock_google_services):
    """Set up mocks for lineage tests between sheets."""
    # Mock files list
    mock_files_list = MagicMock()
    mock_google_services[
        "drive_service"
    ].files.return_value.list.return_value = mock_files_list

    # Mock response for files.list - include both source and target sheets
    mock_files_list.execute.return_value = {
        "files": [
            {
                "id": "test_sheet_id_1",
                "name": "Test Sheet 1",
                "description": "Source sheet",
                "owners": [{"emailAddress": "test@example.com"}],
                "createdTime": "2023-01-01T00:00:00.000Z",
                "modifiedTime": "2023-01-02T00:00:00.000Z",
                "webViewLink": "https://docs.google.com/spreadsheets/d/test_sheet_id_1",
                "shared": True,
            },
            {
                "id": "test_sheet_id_2",
                "name": "Test Sheet 2",
                "description": "Destination sheet with IMPORTRANGE formula",
                "owners": [{"emailAddress": "test@example.com"}],
                "createdTime": "2023-01-01T00:00:00.000Z",
                "modifiedTime": "2023-01-02T00:00:00.000Z",
                "webViewLink": "https://docs.google.com/spreadsheets/d/test_sheet_id_2",
                "shared": True,
            },
        ],
        "nextPageToken": None,
    }

    # Mock response for files.get for path resolution
    def mock_files_get(**kwargs):
        file_id = kwargs.get("fileId")
        mock_response = MagicMock()
        if file_id in ["test_sheet_id_1", "test_sheet_id_2"]:
            mock_response.execute.return_value = {
                "name": f"Test Sheet {file_id[-1]}",
                "parents": ["test_folder_id"],
            }
        elif file_id == "test_folder_id":
            mock_response.execute.return_value = {
                "name": "Test Folder",
                "parents": None,
            }
        return mock_response

    mock_google_services["drive_service"].files.return_value.get = mock_files_get

    # Mock responses for spreadsheets.get
    def mock_spreadsheets_get(**kwargs):
        sheet_id = kwargs.get("spreadsheetId")
        include_grid_data = kwargs.get("includeGridData", False)

        mock_response = MagicMock()

        if sheet_id == "test_sheet_id_1":
            mock_response.execute.return_value = {
                "properties": {"title": "Test Sheet 1"},
                "sheets": [
                    {
                        "properties": {"title": "Sheet1"},
                    }
                ],
            }
        elif sheet_id == "test_sheet_id_2":
            # Sheet with formulas
            if include_grid_data:
                mock_response.execute.return_value = {
                    "sheets": [
                        {
                            "properties": {"title": "Sheet1"},
                            "data": [
                                {
                                    "rowData": [
                                        {
                                            "values": [
                                                {"formattedValue": "Name"},
                                                {"formattedValue": "Age"},
                                                {"formattedValue": "City"},
                                            ]
                                        },
                                        {
                                            "values": [
                                                {
                                                    "userEnteredValue": {
                                                        "formulaValue": '=IMPORTRANGE("https://docs.google.com/spreadsheets/d/test_sheet_id_1", "Sheet1!A2:A4")'
                                                    }
                                                }
                                            ]
                                        },
                                    ]
                                }
                            ],
                        }
                    ]
                }
            else:
                mock_response.execute.return_value = {
                    "properties": {"title": "Test Sheet 2"},
                    "sheets": [
                        {
                            "properties": {"title": "Sheet1"},
                        }
                    ],
                }

        return mock_response

    mock_google_services[
        "sheets_service"
    ].spreadsheets.return_value.get = mock_spreadsheets_get

    # Mock response for spreadsheets.values.get
    def mock_values_get(**kwargs):
        sheet_id = kwargs.get("spreadsheetId")
        mock_response = MagicMock()

        if sheet_id == "test_sheet_id_1":
            mock_response.execute.return_value = {
                "values": [
                    ["Name", "Age", "City"],  # Headers
                    ["John Doe", "30", "New York"],
                    ["Jane Smith", "25", "San Francisco"],
                    ["Bob Johnson", "35", "Chicago"],
                ]
            }
        elif sheet_id == "test_sheet_id_2":
            mock_response.execute.return_value = {
                "values": [
                    ["Name", "Age", "City"],  # Headers
                    [
                        '=IMPORTRANGE("https://docs.google.com/spreadsheets/d/test_sheet_id_1", "Sheet1!A2:A4")',
                        "30",
                        "New York",
                    ],
                ]
            }

        return mock_response

    mock_google_services[
        "sheets_service"
    ].spreadsheets.return_value.values.return_value.get = mock_values_get

    return mock_google_services


def create_mock_source(config, ctx, mock_services):
    """
    Create a GoogleSheetsSource instance with mocked services.
    This helper function injects our mock services into the real Source class.
    """
    source = GoogleSheetsSource(config, ctx)

    # Replace the real services with mocks
    source.drive_service = mock_services["drive_service"]
    source.sheets_service = mock_services["sheets_service"]
    source.drive_activity_service = mock_services["drive_activity_service"]

    return source


def process_workunit_for_golden_file(wu):
    """Process a workunit into a format suitable for golden files."""
    # Start with basic metadata
    result = {"type": wu.__class__.__name__, "id": wu.id}

    try:
        # Extract the content of MetadataChangeEvent-based workunits
        if hasattr(wu, "mce") and wu.mce:
            # Extract the full MCE content
            if hasattr(wu.mce, "proposedSnapshot") and wu.mce.proposedSnapshot:
                # Get the snapshot
                snapshot = wu.mce.proposedSnapshot
                snapshot_dict = snapshot.to_obj()
                result["proposedSnapshot"] = snapshot_dict

                # Extract specific aspects based on the snapshot type
                if hasattr(snapshot, "aspects") and snapshot.aspects:
                    result["aspects"] = [aspect.to_obj() for aspect in snapshot.aspects]

            # Get system metadata if present
            if hasattr(wu.mce, "systemMetadata") and wu.mce.systemMetadata:
                result["systemMetadata"] = wu.mce.systemMetadata.to_obj()

        # Extract content of MetadataChangeProposal-based workunits
        elif hasattr(wu, "mcp") and wu.mcp:
            # Get the entity URN
            if hasattr(wu.mcp, "entityUrn"):
                result["entityUrn"] = str(wu.mcp.entityUrn)

            # Get the change type
            if hasattr(wu.mcp, "changeType"):
                result["changeType"] = wu.mcp.changeType

            # Get the aspect name
            if hasattr(wu.mcp, "aspectName"):
                result["aspectName"] = wu.mcp.aspectName

            # Get the aspect content
            if hasattr(wu.mcp, "aspect") and wu.mcp.aspect:
                try:
                    aspect_dict = wu.mcp.aspect.to_obj()
                    result["aspect"] = aspect_dict
                except Exception as aspect_error:
                    result["aspectError"] = str(aspect_error)

            # Get system metadata if present
            if hasattr(wu.mcp, "systemMetadata") and wu.mcp.systemMetadata:
                result["systemMetadata"] = wu.mcp.systemMetadata.to_obj()

        # For other workunit types, try to find any available data
        # This is a fallback mechanism for other workunit types
        else:
            # Look for common attributes that might contain useful data
            for attr_name in dir(wu):
                # Skip private attributes and methods
                if attr_name.startswith("_") or callable(getattr(wu, attr_name)):
                    continue

                # Try to get the attribute
                try:
                    attr_value = getattr(wu, attr_name)

                    # Skip the attributes we already handled
                    if attr_name in ["id", "mce", "mcp"]:
                        continue

                    # Try to convert to a serializable form if it has a to_obj method
                    if hasattr(attr_value, "to_obj") and callable(attr_value.to_obj):
                        result[attr_name] = attr_value.to_obj()
                    # For simple types, just store directly
                    elif (
                        isinstance(attr_value, (str, int, float, bool, list, dict))
                        or attr_value is None
                    ):
                        result[attr_name] = attr_value
                except Exception as attr_error:
                    result[f"{attr_name}_error"] = str(attr_error)

    except Exception as e:
        # If we encounter any error processing the workunit, include an error message
        result["error_extracting_content"] = str(e)
        logging.warning(f"Error extracting content from workunit {wu.id}: {e}")

    return result


@pytest.mark.integration
@pytest.mark.parametrize("test_case", TEST_CONFIGS)
def test_generate_golden_file(
    test_case,
    test_resources_dir,
    pytestconfig,
    tmp_path,
    mock_basic_sheet_data,
    mock_lineage_sheet_data,
):
    """Test generating golden files for Google Sheets metadata."""

    # Select the appropriate mock data based on the test case
    if test_case["mock_fixture"] == "mock_basic_sheet_data":
        mock_test_data = mock_basic_sheet_data
    else:
        mock_test_data = mock_lineage_sheet_data

    # Mock the credentials class
    credentials_patch = patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"
    )
    mock_credentials = credentials_patch.start()
    mock_credentials.return_value = MagicMock()

    try:
        # Create a temporary credentials file
        test_credentials_file = tmp_path / "test_creds.json"
        test_credentials_file.write_text("{}")

        test_config = test_case["config"].copy()
        test_config["credentials"] = str(test_credentials_file)

        # Set up a deterministic run ID
        ctx = PipelineContext(run_id=f"google-sheets-{test_case['name']}")

        # Create the source directly instead of using Pipeline.create
        source = GoogleSheetsSource(
            config=GoogleSheetsSourceConfig.parse_obj(test_config), ctx=ctx
        )

        # Inject mocked services
        source.drive_service = mock_test_data["drive_service"]
        source.sheets_service = mock_test_data["sheets_service"]
        source.drive_activity_service = mock_test_data["drive_activity_service"]

        # Get workunits
        workunits = list(source.get_workunits())
        print(f"\nGenerated {len(workunits)} workunits for test: {test_case['name']}")

        # Process workunits for the golden file
        workunit_data = []
        for i, wu in enumerate(workunits):
            print(
                f"  Processing workunit {i + 1}/{len(workunits)}: {wu.__class__.__name__} - {wu.id}"
            )
            workunit_data.append(process_workunit_for_golden_file(wu))

        # Update the golden file directly
        golden_file_path = test_resources_dir / test_case["golden_file"]
        with open(golden_file_path, "w") as f:
            json.dump(workunit_data, f, indent=2)
        print(f"Updated golden file: {golden_file_path}")

    finally:
        credentials_patch.stop()


@pytest.mark.parametrize(
    "config_dict, is_success, error_message",
    [
        (
            {
                "credentials": "test_creds.json",
            },
            True,
            None,
        ),
        (
            {
                "credentials": "nonexistent_file.json",
            },
            False,
            "Credentials file nonexistent_file.json does not exist",
        ),
    ],
)
@pytest.mark.integration
def test_google_sheets_validation(config_dict, is_success, error_message, tmp_path):
    """Test Google Sheets config validation."""
    # Mock the validator method to avoid actually checking file existence
    with patch("os.path.exists") as mock_exists:
        mock_exists.side_effect = lambda path: path != "nonexistent_file.json"

        if is_success:
            # Create a mock credentials file
            test_creds_file = tmp_path / "test_creds.json"
            test_creds_file.write_text("{}")
            config_dict["credentials"] = str(test_creds_file)

            # This should not raise an exception
            GoogleSheetsSourceConfig.parse_obj(config_dict)
        else:
            with pytest.raises(ValueError, match=error_message):
                GoogleSheetsSourceConfig.parse_obj(config_dict)
