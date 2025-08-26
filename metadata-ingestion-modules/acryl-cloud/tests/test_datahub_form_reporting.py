from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
    DataHubFormReportingData,
)
from datahub.ingestion.graph.client import DataHubGraph


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


def test_get_form_existence_or_filters_with_allowed_forms(
    mock_graph: MagicMock,
) -> None:
    # Test with specific allowed forms
    allowed_forms = ["form1", "form2"]
    form_data = DataHubFormReportingData(mock_graph, allowed_forms=allowed_forms)

    filters = form_data.get_form_existence_or_filters()

    # Verify the structure of the filters
    assert len(filters) == 2  # Should have two OR conditions

    # Check completedForms filter
    completed_forms_filter = filters[0]
    assert "and" in completed_forms_filter
    assert len(completed_forms_filter["and"]) == 1
    assert completed_forms_filter["and"][0]["field"] == "completedForms"
    assert completed_forms_filter["and"][0]["condition"] == "EQUAL"
    assert completed_forms_filter["and"][0]["values"] == allowed_forms

    # Check incompleteForms filter
    incomplete_forms_filter = filters[1]
    assert "and" in incomplete_forms_filter
    assert len(incomplete_forms_filter["and"]) == 1
    assert incomplete_forms_filter["and"][0]["field"] == "incompleteForms"
    assert incomplete_forms_filter["and"][0]["condition"] == "EQUAL"
    assert incomplete_forms_filter["and"][0]["values"] == allowed_forms


def test_get_form_existence_or_filters_without_allowed_forms(
    mock_graph: MagicMock,
) -> None:
    # Test without allowed forms (should check for existence)
    form_data = DataHubFormReportingData(mock_graph, allowed_forms=None)

    filters = form_data.get_form_existence_or_filters()

    # Verify the structure of the filters
    assert len(filters) == 2  # Should have two OR conditions

    # Check completedForms filter
    completed_forms_filter = filters[0]
    assert "and" in completed_forms_filter
    assert len(completed_forms_filter["and"]) == 1
    assert completed_forms_filter["and"][0]["field"] == "completedForms"
    assert completed_forms_filter["and"][0]["condition"] == "EXISTS"
    assert "values" not in completed_forms_filter["and"][0]

    # Check incompleteForms filter
    incomplete_forms_filter = filters[1]
    assert "and" in incomplete_forms_filter
    assert len(incomplete_forms_filter["and"]) == 1
    assert incomplete_forms_filter["and"][0]["field"] == "incompleteForms"
    assert incomplete_forms_filter["and"][0]["condition"] == "EXISTS"
    assert "values" not in incomplete_forms_filter["and"][0]
