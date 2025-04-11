from unittest.mock import Mock

import pytest
import yaml

from datahub.api.entities.structuredproperties.structuredproperties import (
    AllowedValue,
    StructuredProperties,
    TypeQualifierAllowedTypes,
)
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)


@pytest.fixture
def sample_yaml_content():
    return """
- id: test_property
  type: string
  description: Test description
  display_name: Test Property
  entity_types:
    - dataset
  cardinality: SINGLE
  allowed_values:
    - value: test_value
      description: Test value description
"""


@pytest.fixture
def sample_yaml_file(tmp_path, sample_yaml_content):
    yaml_file = tmp_path / "test_properties.yaml"
    yaml_file.write_text(sample_yaml_content)
    return str(yaml_file)


@pytest.fixture
def mock_graph():
    return Mock(spec=DataHubGraph)


def test_structured_properties_basic_creation():
    props = StructuredProperties(
        id="test_prop", type="string", description="Test description"
    )
    assert props.id == "test_prop"
    assert props.type == "urn:li:dataType:datahub.string"
    assert props.description == "Test description"
    assert props.urn == "urn:li:structuredProperty:test_prop"


def test_structured_properties_validate_type():
    # Test valid types
    props = StructuredProperties(id="test", type="string")
    assert props.type == "urn:li:dataType:datahub.string"

    # Test invalid type
    with pytest.raises(ValueError, match="Type .* is not allowed"):
        StructuredProperties(id="test", type="invalid_type")


def test_structured_properties_validate_entity_types():
    # Test valid entity type
    props = StructuredProperties(id="test", type="string", entity_types=["dataset"])
    assert props.entity_types
    assert "urn:li:entityType:datahub.dataset" in props.entity_types

    # Test invalid entity type
    with pytest.raises(ValueError, match="not a valid entity type"):
        StructuredProperties(id="test", type="string", entity_types=["invalid_entity"])


def test_structured_properties_from_yaml(sample_yaml_file):
    props = StructuredProperties.from_yaml(sample_yaml_file)
    assert len(props) == 1
    assert props[0].id == "test_property"
    assert props[0].type == "urn:li:dataType:datahub.string"
    assert props[0].description == "Test description"
    assert props[0].display_name
    assert props[0].display_name == "Test Property"
    assert props[0].allowed_values
    assert len(props[0].allowed_values) == 1
    assert props[0].allowed_values[0].value == "test_value"


def test_structured_properties_generate_mcps():
    props = StructuredProperties(
        id="test_prop",
        type="string",
        description="Test description",
        display_name="Test Property",
        entity_types=["dataset"],
        allowed_values=[
            AllowedValue(value="test_value", description="Test value description")
        ],
    )

    mcps = props.generate_mcps()
    assert len(mcps) == 1
    mcp = mcps[0]

    assert mcp.entityUrn == "urn:li:structuredProperty:test_prop"
    assert isinstance(mcp.aspect, StructuredPropertyDefinitionClass)
    assert mcp.aspect.valueType == "urn:li:dataType:datahub.string"
    assert mcp.aspect.description == "Test description"
    assert mcp.aspect.allowedValues
    assert len(mcp.aspect.allowedValues) == 1
    assert mcp.aspect.allowedValues[0].value == "test_value"


def test_structured_properties_from_datahub(mock_graph):
    mock_aspect = StructuredPropertyDefinitionClass(
        qualifiedName="test_prop",
        valueType="urn:li:dataType:datahub.string",
        displayName="Test Property",
        description="Test description",
        entityTypes=["urn:li:entityType:datahub.dataset"],
        cardinality="SINGLE",
        allowedValues=[
            PropertyValueClass(value="test_value", description="Test description")
        ],
    )

    mock_graph.get_aspect.return_value = mock_aspect

    props = StructuredProperties.from_datahub(
        mock_graph, "urn:li:structuredProperty:test_prop"
    )

    assert props.qualified_name == "test_prop"
    assert props.type == "urn:li:dataType:datahub.string"
    assert props.display_name == "Test Property"
    assert props.allowed_values
    assert len(props.allowed_values) == 1
    assert props.allowed_values[0].value == "test_value"


def test_structured_properties_to_yaml(tmp_path):
    props = StructuredProperties(
        id="test_prop",
        type="string",
        description="Test description",
        allowed_values=[
            AllowedValue(value="test_value", description="Test value description")
        ],
    )

    yaml_file = tmp_path / "output.yaml"
    props.to_yaml(yaml_file)

    # Verify the yaml file was created and contains expected content
    assert yaml_file.exists()
    with open(yaml_file) as f:
        content = yaml.safe_load(f)
        assert content["id"] == "test_prop"
        assert content["type"] == "urn:li:dataType:datahub.string"
        assert content["description"] == "Test description"


@pytest.mark.parametrize(
    "input_type,expected_type",
    [
        ("string", "urn:li:dataType:datahub.string"),
        ("STRING", "urn:li:dataType:datahub.string"),
        ("number", "urn:li:dataType:datahub.number"),
        ("date", "urn:li:dataType:datahub.date"),
    ],
)
def test_structured_properties_type_normalization(input_type, expected_type):
    props = StructuredProperties(id="test_prop", type=input_type)
    assert props.type == expected_type


def test_structured_properties_type_qualifier():
    props = StructuredProperties(
        id="test_prop",
        type="urn",
        type_qualifier=TypeQualifierAllowedTypes(allowed_types=["dataset"]),
    )

    mcps = props.generate_mcps()
    assert mcps[0].aspect
    assert mcps[0].aspect.typeQualifier["allowedTypes"] == [  # type: ignore
        "urn:li:entityType:datahub.dataset"
    ]


def test_structured_properties_list(mock_graph):
    mock_graph.get_urns_by_filter.return_value = [
        "urn:li:structuredProperty:prop1",
        "urn:li:structuredProperty:prop2",
    ]

    mock_aspect = StructuredPropertyDefinitionClass(
        qualifiedName="test_prop",
        valueType="urn:li:dataType:string",
        entityTypes=["urn:li:entityType:datahub.dataset"],
    )
    mock_graph.get_aspect.return_value = mock_aspect

    props = list(StructuredProperties.list(mock_graph))

    # Verify get_urns_by_filter was called with correct arguments
    mock_graph.get_urns_by_filter.assert_called_once_with(
        entity_types=["structuredProperty"]
    )

    assert len(props) == 2
    assert all(isinstance(prop, StructuredProperties) for prop in props)
