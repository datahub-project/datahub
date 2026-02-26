"""Unit tests for Sigma connector element type filtering.

Verifies that all element types are returned by get_page_elements(),
but only table and visualization elements get lineage and SQL extraction.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.sigma.config import SigmaSourceConfig, SigmaSourceReport
from datahub.ingestion.source.sigma.data_classes import Page, Workbook
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI


@pytest.fixture
def sigma_config():
    return SigmaSourceConfig(
        client_id="test_client_id",
        client_secret="test_client_secret",
        api_url="https://aws-api.sigmacomputing.com/v2",
    )


@pytest.fixture
def sigma_report():
    return SigmaSourceReport()


@pytest.fixture
def sigma_api(sigma_config, sigma_report):
    with patch.object(SigmaAPI, "_generate_token", return_value=None):
        return SigmaAPI(sigma_config, sigma_report)


@pytest.fixture
def sample_workbook():
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
    return Page(pageId="test-page-id", name="Test Page")


def test_all_element_types_returned_but_only_data_types_get_lineage(
    sigma_api, sample_workbook, sample_page
):
    """All elements are returned, but lineage is only extracted for table/visualization."""
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
                "type": "text",
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
                "type": "line",
            },
            {
                "elementId": "elem-5",
                "name": "Logo Image",
                "type": "image",
            },
        ]
    }

    # Enable lineage extraction
    sigma_api.config.extract_lineage = True

    with (
        patch.object(sigma_api, "_get_api_call", return_value=mock_response),
        patch.object(
            sigma_api, "_get_element_upstream_sources", return_value={}
        ) as mock_lineage,
        patch.object(
            sigma_api, "_get_element_sql_query", return_value=None
        ) as mock_sql,
    ):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    # All 5 elements should be returned
    assert len(elements) == 5
    element_ids = [e.elementId for e in elements]
    assert element_ids == ["elem-1", "elem-2", "elem-3", "elem-4", "elem-5"]

    # Lineage/SQL should only be called for table and visualization (elem-1, elem-3)
    assert mock_lineage.call_count == 2
    lineage_element_ids = [c.args[0].elementId for c in mock_lineage.call_args_list]
    assert lineage_element_ids == ["elem-1", "elem-3"]

    assert mock_sql.call_count == 2
    sql_element_ids = [c.args[0].elementId for c in mock_sql.call_args_list]
    assert sql_element_ids == ["elem-1", "elem-3"]


def test_non_data_types_returned_without_lineage(
    sigma_api, sample_workbook, sample_page
):
    """Non-data elements are returned but never get lineage extraction."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {"elementId": "elem-1", "name": "Text Box 1", "type": "text"},
            {"elementId": "elem-2", "name": "Text Box 2", "type": "text"},
            {"elementId": "elem-3", "name": "Line", "type": "line"},
        ]
    }

    sigma_api.config.extract_lineage = True

    with (
        patch.object(sigma_api, "_get_api_call", return_value=mock_response),
        patch.object(sigma_api, "_get_element_upstream_sources") as mock_lineage,
        patch.object(sigma_api, "_get_element_sql_query") as mock_sql,
    ):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    assert len(elements) == 3
    mock_lineage.assert_not_called()
    mock_sql.assert_not_called()


def test_all_data_types_get_lineage(sigma_api, sample_workbook, sample_page):
    """All table/visualization elements get lineage extraction."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {"elementId": "elem-1", "name": "Table 1", "type": "table"},
            {"elementId": "elem-2", "name": "Chart 1", "type": "visualization"},
            {"elementId": "elem-3", "name": "Table 2", "type": "table"},
        ]
    }

    sigma_api.config.extract_lineage = True

    with (
        patch.object(sigma_api, "_get_api_call", return_value=mock_response),
        patch.object(
            sigma_api, "_get_element_upstream_sources", return_value={}
        ) as mock_lineage,
        patch.object(
            sigma_api, "_get_element_sql_query", return_value=None
        ) as mock_sql,
    ):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    assert len(elements) == 3
    assert mock_lineage.call_count == 3
    assert mock_sql.call_count == 3


def test_missing_type_field_returned_without_lineage(
    sigma_api, sample_workbook, sample_page
):
    """Elements without a type field are returned but don't get lineage extraction."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [
            {"elementId": "elem-1", "name": "No Type Element"},
            {"elementId": "elem-2", "name": "Table", "type": "table"},
        ]
    }

    sigma_api.config.extract_lineage = True

    with (
        patch.object(sigma_api, "_get_api_call", return_value=mock_response),
        patch.object(
            sigma_api, "_get_element_upstream_sources", return_value={}
        ) as mock_lineage,
        patch.object(sigma_api, "_get_element_sql_query", return_value=None),
    ):
        elements = sigma_api.get_page_elements(sample_workbook, sample_page)

    assert len(elements) == 2
    # Only the table element should get lineage
    assert mock_lineage.call_count == 1
    assert mock_lineage.call_args_list[0].args[0].elementId == "elem-2"
