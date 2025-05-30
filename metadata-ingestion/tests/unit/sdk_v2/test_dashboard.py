import pathlib
import re
from datetime import datetime
from unittest import mock

import pytest

from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import (
    ChartUrn,
    CorpUserUrn,
    DashboardUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.dashboard import Dashboard
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "dashboard_golden"


def test_dashboard_basic(pytestconfig: pytest.Config) -> None:
    d = Dashboard(
        platform="looker",
        name="example_dashboard",
    )

    # Check urn setup.
    assert Dashboard.get_urn_type() == DashboardUrn
    assert isinstance(d.urn, DashboardUrn)
    assert str(d.urn) == "urn:li:dashboard:(looker,example_dashboard)"
    assert str(d.urn) in repr(d)

    # Check most attributes.
    assert d.platform is not None
    assert d.platform.platform_name == "looker"
    assert d.platform_instance is None
    assert d.tags is None
    assert d.terms is None
    assert d.last_modified is None
    assert d.description == ""
    assert d.custom_properties == {}
    assert d.domain is None

    with pytest.raises(AttributeError):
        assert d.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        d.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        d.owners = []  # type: ignore

    assert_entity_golden(d, _GOLDEN_DIR / "test_dashboard_basic_golden.json")


def test_dashboard_complex() -> None:
    updated = datetime(2025, 1, 9, 12, 4, 6)

    d = Dashboard(
        platform="looker",
        platform_instance="my_instance",
        name="example_dashboard",
        display_name="Example Dashboard",
        last_modified=updated,
        last_refreshed=updated,
        custom_properties={
            "key1": "value1",
            "key2": "value2",
        },
        description="Test dashboard",
        external_url="https://example.com",
        dashboard_url="https://looker.example.com/dashboards/123",
        owners=[
            CorpUserUrn("admin@datahubproject.io"),
        ],
        links=[
            "https://example.com/doc1",
            ("https://example.com/doc2", "Documentation 2"),
        ],
        tags=[
            TagUrn("tag1"),
            TagUrn("tag2"),
        ],
        terms=[
            GlossaryTermUrn("Dashboard"),
        ],
        domain=DomainUrn("Analytics"),
    )

    assert d.platform is not None
    assert d.platform.platform_name == "looker"
    assert d.platform_instance is not None
    assert (
        str(d.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:looker,my_instance)"
    )

    # Properties.
    assert d.description == "Test dashboard"
    assert d.display_name == "Example Dashboard"
    assert d.external_url == "https://example.com"
    assert d.dashboard_url == "https://looker.example.com/dashboards/123"
    assert d.last_modified == updated
    assert d.last_refreshed == updated
    assert d.custom_properties == {"key1": "value1", "key2": "value2"}

    # Check standard aspects.
    assert d.owners is not None and len(d.owners) == 1
    assert d.links is not None and len(d.links) == 2
    assert d.tags is not None and len(d.tags) == 2
    assert d.terms is not None and len(d.terms) == 1
    assert d.domain == DomainUrn("Analytics")

    # Add assertions for links
    assert d.links is not None
    assert len(d.links) == 2
    assert d.links[0].url == "https://example.com/doc1"
    assert d.links[1].url == "https://example.com/doc2"

    # Test chart and dataset operations
    d.add_chart("urn:li:chart:(looker,chart1)")
    d.add_chart("urn:li:chart:(looker,chart2)")
    d.add_input_dataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)")
    d.add_input_dataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table2,PROD)")

    assert d.charts == [
        ChartUrn("looker", "chart1"),
        ChartUrn("looker", "chart2"),
    ]
    assert d.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table", env="PROD"),
        DatasetUrn(platform="snowflake", name="my_table2", env="PROD"),
    ]

    d.remove_chart("urn:li:chart:(looker,chart1)")
    d.remove_input_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
    )

    assert d.charts == [ChartUrn("looker", "chart2")]
    assert d.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table2", env="PROD")
    ]

    assert_entity_golden(d, _GOLDEN_DIR / "test_dashboard_complex_golden.json")


def test_client_get_dashboard() -> None:
    """Test retrieving Dashboards using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Basic retrieval
    dashboard_urn = DashboardUrn("looker", "test_dashboard")
    expected_dashboard = Dashboard(
        platform="looker",
        name="test_dashboard",
        description="A test dashboard",
    )
    mock_entities.get.return_value = expected_dashboard

    result = mock_client.entities.get(dashboard_urn)
    assert result == expected_dashboard
    mock_entities.get.assert_called_once_with(dashboard_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = "urn:li:dashboard:(looker,string_dashboard,prod)"
    mock_entities.get.return_value = Dashboard(
        platform="looker", name="string_dashboard"
    )
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex dashboard with properties
    test_date = datetime(2023, 1, 1, 12, 0, 0)
    complex_dashboard = Dashboard(
        platform="looker",
        name="complex_dashboard",
        description="Complex test dashboard",
        display_name="My Complex Dashboard",
        external_url="https://example.com/dashboard",
        dashboard_url="https://looker.example.com/dashboards/456",
        last_modified=test_date,
        last_refreshed=datetime(2023, 1, 1, 12, 0, 0),
        custom_properties={"env": "production", "owner_team": "analytics"},
    )

    # Set relationships and tags
    complex_dashboard.set_tags([TagUrn("important"), TagUrn("analytics")])
    complex_dashboard.set_domain(DomainUrn("Analytics"))
    complex_dashboard.set_owners([CorpUserUrn("john@example.com")])
    complex_dashboard.add_chart("urn:li:chart:(looker,chart1)")
    complex_dashboard.add_input_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
    )

    dashboard_urn = DashboardUrn("looker", "complex_dashboard")
    mock_entities.get.return_value = complex_dashboard

    result = mock_client.entities.get(dashboard_urn)
    assert result.name == "complex_dashboard"
    assert result.display_name == "My Complex Dashboard"
    assert result.last_modified == test_date
    assert result.description == "Complex test dashboard"
    assert result.dashboard_url == "https://looker.example.com/dashboards/456"
    assert result.last_refreshed == datetime(2023, 1, 1, 12, 0, 0)
    assert result.tags is not None
    assert result.domain is not None
    assert result.owners is not None
    assert result.charts == [ChartUrn("looker", "chart1")]
    assert result.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table", env="PROD")
    ]
    mock_entities.get.assert_called_once_with(dashboard_urn)
    mock_entities.get.reset_mock()

    # Not found case
    error_message = f"Entity {dashboard_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(dashboard_urn)
