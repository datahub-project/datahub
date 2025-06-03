import pathlib
import re
from datetime import datetime
from unittest import mock

import pytest

import datahub.metadata.schema_classes as models
from datahub.errors import ItemNotFoundError
from datahub.metadata.urns import (
    ChartUrn,
    CorpUserUrn,
    DatasetUrn,
    DomainUrn,
    GlossaryTermUrn,
    TagUrn,
)
from datahub.sdk.chart import Chart
from datahub.testing.sdk_v2_helpers import assert_entity_golden

GOLDEN_DIR = pathlib.Path(__file__).parent / "chart_golden"
GOLDEN_DIR.mkdir(exist_ok=True)


def test_chart_basic(pytestconfig: pytest.Config) -> None:
    c = Chart(
        platform="looker",
        name="example_chart",
    )

    # Check urn setup.
    assert Chart.get_urn_type() == ChartUrn
    assert isinstance(c.urn, ChartUrn)
    assert str(c.urn) == "urn:li:chart:(looker,example_chart)"
    assert str(c.urn) in repr(c)

    # Check most attributes.
    assert c.platform is not None
    assert c.platform.platform_name == "looker"
    assert c.platform_instance is None
    assert c.tags is None
    assert c.terms is None
    assert c.last_modified is None
    assert c.description == ""
    assert c.custom_properties == {}
    assert c.domain is None
    assert c.chart_type is None
    assert c.access is None

    with pytest.raises(AttributeError):
        assert c.extra_attribute  # type: ignore
    with pytest.raises(AttributeError):
        c.extra_attribute = "slots should reject extra fields"  # type: ignore
    with pytest.raises(AttributeError):
        # This should fail. Eventually we should make it suggest calling set_owners instead.
        c.owners = []  # type: ignore

    assert_entity_golden(c, GOLDEN_DIR / "test_chart_basic_golden.json")


def test_chart_complex() -> None:
    updated = datetime(2025, 1, 9, 3, 4, 6)

    c = Chart(
        platform="looker",
        platform_instance="my_instance",
        name="example_chart",
        display_name="Example Chart",
        last_modified=updated,
        last_refreshed=updated,
        custom_properties={
            "key1": "value1",
            "key2": "value2",
        },
        description="Test chart",
        external_url="https://example.com",
        chart_url="https://looker.example.com/charts/123",
        chart_type=models.ChartTypeClass.BAR,
        access=models.AccessLevelClass.PUBLIC,
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
            GlossaryTermUrn("Chart"),
        ],
        domain=DomainUrn("Analytics"),
    )

    assert c.platform is not None
    assert c.platform.platform_name == "looker"
    assert c.platform_instance is not None
    assert (
        str(c.platform_instance)
        == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:looker,my_instance)"
    )

    # Properties.
    assert c.description == "Test chart"
    assert c.display_name == "Example Chart"
    assert c.external_url == "https://example.com"
    assert c.chart_url == "https://looker.example.com/charts/123"
    assert c.last_modified == updated
    assert c.last_refreshed == updated
    assert c.custom_properties == {"key1": "value1", "key2": "value2"}
    assert c.chart_type == models.ChartTypeClass.BAR
    assert c.access == models.AccessLevelClass.PUBLIC

    # Check standard aspects.
    assert c.owners is not None and len(c.owners) == 1
    assert c.links is not None and len(c.links) == 2
    assert c.tags is not None and len(c.tags) == 2
    assert c.terms is not None and len(c.terms) == 1
    assert c.domain == DomainUrn("Analytics")

    # Test input operations
    c.add_input_dataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)")
    c.add_input_dataset("urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table2,PROD)")

    assert c.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table", env="PROD"),
        DatasetUrn(platform="snowflake", name="my_table2", env="PROD"),
    ]

    c.remove_input_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
    )

    assert c.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table2", env="PROD")
    ]

    assert_entity_golden(c, GOLDEN_DIR / "test_chart_complex_golden.json")


def test_client_get_chart() -> None:
    """Test retrieving Charts using client.entities.get()."""
    # Set up mock
    mock_client = mock.MagicMock()
    mock_entities = mock.MagicMock()
    mock_client.entities = mock_entities

    # Basic retrieval
    chart_urn = ChartUrn("looker", "test_chart")
    expected_chart = Chart(
        platform="looker",
        name="test_chart",
        description="A test chart",
    )
    mock_entities.get.return_value = expected_chart

    result = mock_client.entities.get(chart_urn)
    assert result == expected_chart
    mock_entities.get.assert_called_once_with(chart_urn)
    mock_entities.get.reset_mock()

    # String URN
    urn_str = "urn:li:chart:(looker,string_chart)"
    mock_entities.get.return_value = Chart(platform="looker", name="string_chart")
    result = mock_client.entities.get(urn_str)
    mock_entities.get.assert_called_once_with(urn_str)
    mock_entities.get.reset_mock()

    # Complex chart with properties
    test_date = datetime(2023, 1, 1, 12, 0, 0)
    complex_chart = Chart(
        platform="looker",
        name="complex_chart",
        description="Complex test chart",
        display_name="My Complex Chart",
        external_url="https://example.com/chart",
        chart_url="https://looker.example.com/charts/456",
        last_modified=test_date,
        last_refreshed=datetime(2024, 1, 9, 3, 4, 6),
        chart_type=models.ChartTypeClass.LINE,
        access=models.AccessLevelClass.PRIVATE,
        custom_properties={"env": "production", "owner_team": "analytics"},
    )

    # Set relationships and tags
    complex_chart.set_tags([TagUrn("important"), TagUrn("analytics")])
    complex_chart.set_domain(DomainUrn("Analytics"))
    complex_chart.set_owners([CorpUserUrn("john@example.com")])
    complex_chart.add_input_dataset(
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_table,PROD)"
    )

    chart_urn = ChartUrn("looker", "complex_chart")
    mock_entities.get.return_value = complex_chart

    result = mock_client.entities.get(chart_urn)
    assert result.name == "complex_chart"
    assert result.display_name == "My Complex Chart"
    assert result.last_modified == test_date
    assert result.description == "Complex test chart"
    assert result.chart_url == "https://looker.example.com/charts/456"
    assert result.last_refreshed == datetime(2024, 1, 9, 3, 4, 6)
    assert result.chart_type == models.ChartTypeClass.LINE
    assert result.access == models.AccessLevelClass.PRIVATE
    assert result.tags is not None
    assert result.domain is not None
    assert result.owners is not None
    assert result.input_datasets == [
        DatasetUrn(platform="snowflake", name="my_table", env="PROD")
    ]
    mock_entities.get.assert_called_once_with(chart_urn)
    mock_entities.get.reset_mock()

    # Not found case
    error_message = f"Entity {chart_urn} not found"
    mock_entities.get.side_effect = ItemNotFoundError(error_message)
    with pytest.raises(ItemNotFoundError, match=re.escape(error_message)):
        mock_client.entities.get(chart_urn)


def test_chart_set_chart_type() -> None:
    c = Chart(
        platform="looker",
        name="example_chart",
    )
    c.set_chart_type(models.ChartTypeClass.BAR)
    assert c.chart_type == models.ChartTypeClass.BAR
    with pytest.raises(AssertionError):
        c.set_chart_type("invalid_type")
