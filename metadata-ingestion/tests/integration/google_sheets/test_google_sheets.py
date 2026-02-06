import json
import pathlib
from typing import Any, Dict
from unittest import mock

import pytest
import yaml

from datahub.ingestion.run.pipeline import Pipeline
from datahub.testing import mce_helpers

test_resources_dir = pathlib.Path(__file__).parent


def read_mock_response(filename: str) -> Dict[str, Any]:
    """Read a mock API response from the setup directory."""
    response_path = test_resources_dir / "setup" / filename
    with open(response_path) as f:
        return json.load(f)


def load_recipe_config(recipe_name: str) -> Dict[str, Any]:
    """Load a recipe YAML file and prepare it for testing."""
    recipe_path = test_resources_dir / "recipes" / recipe_name
    with open(recipe_path) as f:
        config = yaml.safe_load(f)

    # Replace relative credentials path with absolute path
    credentials_path = str(test_resources_dir / "setup" / "dummy_credentials.json")
    config["source"]["config"]["credentials"] = credentials_path

    return config


def create_mock_drive_service():
    """Create a mock Google Drive service with realistic API responses.

    Uses simple objects instead of Mock to avoid call-tracking issues.
    """

    class MockExecute:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class MockFiles:
        def list(self, **kwargs):
            return MockExecute(read_mock_response("drive_files_mydrive.json"))

        def get(self, **kwargs):
            file_id = kwargs.get("fileId", "test")
            # Return folder data without parents to terminate the loop
            if file_id in ["root", "0ALrfhxL-W8y1Uk9PVA"]:  # root folder IDs
                return MockExecute(
                    {
                        "id": file_id,
                        "name": "My Drive" if file_id == "root" else "root",
                        "parents": [],  # No parents to stop the loop
                    }
                )
            # Return file data with parent
            return MockExecute(
                {
                    "id": file_id,
                    "name": f"File {file_id}",
                    "parents": ["root"],  # Will terminate on next iteration
                }
            )

    class MockDrives:
        def list(self, **kwargs):
            return MockExecute(read_mock_response("drive_shared_drives_list.json"))

    class MockService:
        def files(self):
            return MockFiles()

        def drives(self):
            return MockDrives()

    return MockService()


def create_mock_sheets_service():
    """Create a mock Google Sheets service with realistic API responses."""

    class MockExecute:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class MockValues:
        def get(self, **kwargs):
            return MockExecute(
                {
                    "range": kwargs.get("range", ""),
                    "majorDimension": "ROWS",
                    "values": [
                        ["Date", "Product", "Quantity", "Revenue", "Region"],
                        ["2024-01-01", "Widget A", "150", "15000", "North"],
                    ],
                }
            )

    class MockSpreadsheets:
        def get(self, **kwargs):
            return MockExecute(read_mock_response("spreadsheet_basic.json"))

        def values(self):
            return MockValues()

    class MockService:
        def spreadsheets(self):
            return MockSpreadsheets()

    return MockService()


def create_mock_drive_activity_service():
    """Create a mock Google Drive Activity service for usage statistics."""

    class MockExecute:
        def __init__(self, data):
            self.data = data

        def execute(self):
            return self.data

    class MockActivity:
        def query(self, **kwargs):
            return MockExecute(read_mock_response("drive_activity_usage.json"))

    class MockService:
        def activity(self):
            return MockActivity()

    return MockService()


@pytest.fixture
def mock_google_apis():
    """Mock all Google API services.

    Patch at the source module level like Tableau does with Server.
    """
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

    with mock.patch(
        "datahub.ingestion.source.google_sheets.google_sheets_source.build",
        side_effect=build_service,
    ):
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


@pytest.fixture
def mock_sleep():
    """Mock time.sleep to prevent hangs during API rate limiting."""
    with mock.patch("time.sleep", return_value=None):
        yield


def test_basic_ingestion(
    mock_google_apis, mock_credentials, mock_sleep, pytestconfig, tmp_path
):
    """Test basic ingestion without lineage or profiling."""
    output_path = tmp_path / "google_sheets_basic_mces.json"
    golden_path = test_resources_dir / "google_sheets_basic_golden.json"

    # Load recipe and configure output
    config = load_recipe_config("recipe_basic_ingestion.yml")
    config["sink"]["config"]["filename"] = str(output_path)

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


def test_lineage_extraction(
    mock_google_apis, mock_credentials, mock_sleep, pytestconfig, tmp_path
):
    """Test lineage extraction from IMPORTRANGE and QUERY formulas."""
    output_path = tmp_path / "google_sheets_lineage_mces.json"
    golden_path = test_resources_dir / "google_sheets_lineage_golden.json"

    # Load recipe and configure output
    config = load_recipe_config("recipe_lineage_extraction.yml")
    config["sink"]["config"]["filename"] = str(output_path)

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


def test_header_detection(
    mock_google_apis, mock_credentials, mock_sleep, pytestconfig, tmp_path
):
    """Test automatic header row detection."""
    output_path = tmp_path / "google_sheets_header_mces.json"
    golden_path = test_resources_dir / "google_sheets_header_detection_golden.json"

    # Load recipe and configure output
    config = load_recipe_config("recipe_header_detection.yml")
    config["sink"]["config"]["filename"] = str(output_path)

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


def test_profiling_and_usage(
    mock_google_apis, mock_credentials, mock_sleep, pytestconfig, tmp_path
):
    """Test profiling and usage statistics extraction."""
    output_path = tmp_path / "google_sheets_profiling_mces.json"
    golden_path = test_resources_dir / "google_sheets_profiling_usage_golden.json"

    # Load recipe and configure output
    config = load_recipe_config("recipe_profiling_usage.yml")
    config["sink"]["config"]["filename"] = str(output_path)

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )


def test_shared_drives(
    mock_google_apis, mock_credentials, mock_sleep, pytestconfig, tmp_path
):
    """Test ingestion from Google Shared Drives."""
    output_path = tmp_path / "google_sheets_shared_drives_mces.json"
    golden_path = test_resources_dir / "google_sheets_shared_drives_golden.json"

    # Load recipe and configure output
    config = load_recipe_config("recipe_shared_drives.yml")
    config["sink"]["config"]["filename"] = str(output_path)

    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

    # Verify output matches golden file
    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=golden_path,
    )
