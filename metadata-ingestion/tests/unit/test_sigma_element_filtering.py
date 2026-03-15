"""Unit tests for Sigma connector element type filtering."""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import Page, Workbook
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


@pytest.fixture
def sigma_config():
    """Create a basic Sigma config for testing."""
    return SigmaSourceConfig(
        client_id="test_client_id",
        client_secret="test_client_secret",
        api_url="https://aws-api.sigmacomputing.com/v2",
    )


@pytest.fixture
def sigma_report():
    """Create a basic Sigma report for testing."""
    return SigmaSourceReport()


@pytest.fixture
def sigma_api(sigma_config, sigma_report):
    """Create a SigmaAPI instance for testing."""
    # Mock the _generate_token method to avoid real API calls
    with patch.object(SigmaAPI, "_generate_token", return_value=None):
        return SigmaAPI(sigma_config, sigma_report)


@pytest.fixture
def sample_workbook():
    """Create a sample workbook for testing."""
    return Workbook(
        workbookId="test-workbook-id",
        name="Test Workbook",
        createdBy="test-user",
        updatedBy="test-user",
        createdAt="2024-01-01T00:00:00Z",
        updatedAt="2024-01-01T00:00:00Z",
        url="https://app.sigmacomputing.com/test/workbook/test-id",
        path="Test Path",
        latestVersion=1,
    )


@pytest.fixture
def sample_page():
    """Create a sample page for testing."""
    return Page(pageId="test-page-id", name="Test Page")


def test_get_page_elements_filters_non_data_types(
    sigma_api, sample_workbook, sample_page
):
    """
    Test that get_page_elements only processes table and visualization element types.

    This test verifies the fix for the bug where the connector was attempting to fetch
    lineage for ALL element types, including UI components like text boxes and lines.
    According to Sigma's documentation, lineage is only available for data elements.
    """
    # Mock API response with mixed element types
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {
                "elementId": "elem-1",
                "name": "Data Table",
                "type": "table",
                "columns": ["col1", "col2"],
            },
            {
                "elementId": "elem-2",
                "name": "Text Box",
                "type": "text",  # Non-data element type
            },
            {
                "elementId": "elem-3",
                "name": "Bar Chart",
                "type": "visualization",
                "columns": ["metric"],
            },
            {
                "elementId": "elem-4",
                "name": "Decorative Line",
                "type": "line",  # Non-data element type
            },
            {
                "elementId": "elem-5",
                "name": "Logo Image",
                "type": "image",  # Non-data element type
            },
        ]
    }

    # Mock the _get_api_call method
    with patch.object(sigma_api, "_get_api_call", return_value=mock_response):
        # Call get_page_elements
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    # Verify results
    # Should only return table and visualization elements (elem-1 and elem-3)
    assert len(elements) == 2, (
        "Should only process data element types (table, visualization)"
    )

    # Verify the correct elements were processed
    element_ids = [elem.elementId for elem in elements]
    assert "elem-1" in element_ids, "Table element should be processed"
    assert "elem-3" in element_ids, "Visualization element should be processed"
    assert "elem-2" not in element_ids, "Text element should be filtered out"
    assert "elem-4" not in element_ids, "Line element should be filtered out"
    assert "elem-5" not in element_ids, "Image element should be filtered out"

    # Verify element details
    table_elem = next(e for e in elements if e.elementId == "elem-1")
    assert table_elem.name == "Data Table"
    assert table_elem.type == "table"

    viz_elem = next(e for e in elements if e.elementId == "elem-3")
    assert viz_elem.name == "Bar Chart"
    assert viz_elem.type == "visualization"


def test_get_page_elements_all_non_data_types(sigma_api, sample_workbook, sample_page):
    """
    Test that get_page_elements returns empty list when all elements are non-data types.
    """
    # Mock API response with only non-data element types
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {
                "elementId": "elem-1",
                "name": "Text Box 1",
                "type": "text",
            },
            {
                "elementId": "elem-2",
                "name": "Text Box 2",
                "type": "text",
            },
            {
                "elementId": "elem-3",
                "name": "Line",
                "type": "line",
            },
        ]
    }

    with patch.object(sigma_api, "_get_api_call", return_value=mock_response):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    # Should return empty list since no data elements
    assert len(elements) == 0, "Should return empty list when no data elements present"


def test_get_page_elements_all_data_types(sigma_api, sample_workbook, sample_page):
    """
    Test that get_page_elements processes all elements when they are all data types.
    """
    # Mock API response with only data element types
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {
                "elementId": "elem-1",
                "name": "Table 1",
                "type": "table",
            },
            {
                "elementId": "elem-2",
                "name": "Chart 1",
                "type": "visualization",
            },
            {
                "elementId": "elem-3",
                "name": "Table 2",
                "type": "table",
            },
        ]
    }

    with patch.object(sigma_api, "_get_api_call", return_value=mock_response):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    # Should return all elements
    assert len(elements) == 3, "Should process all data element types"
    assert all(elem.type in ["table", "visualization"] for elem in elements), (
        "All elements should be data types"
    )


def test_get_page_elements_missing_type_field(sigma_api, sample_workbook, sample_page):
    """
    Test that get_page_elements handles elements without a type field gracefully.
    """
    # Mock API response with element missing type field
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {
                "elementId": "elem-1",
                "name": "Element Without Type",
                # type field is missing
            },
            {
                "elementId": "elem-2",
                "name": "Table",
                "type": "table",
            },
        ]
    }

    with patch.object(sigma_api, "_get_api_call", return_value=mock_response):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    # Should only process the table element (elem-1 without type should be filtered)
    assert len(elements) == 1, "Should filter out elements without type field"
    assert elements[0].elementId == "elem-2"
