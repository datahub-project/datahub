"""Tests for structured property management tools."""

from unittest.mock import MagicMock, patch

import pytest

from datahub_integrations.mcp.tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)


@pytest.fixture
def mock_datahub_client():
    """Create a mock DataHub client."""
    mock_client = MagicMock()
    mock_client._graph = MagicMock()
    mock_client._graph.execute_graphql = MagicMock()
    return mock_client


# Add structured properties tests


def test_add_structured_properties_string_type(mock_datahub_client):
    """Test adding string-typed structured properties to entities."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    # Mock property validation response (string type)
    mock_datahub_client._graph.execute_graphql.side_effect = [
        # Property validation
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        # Upsert mutation
        {
            "upsertStructuredProperties": {
                "properties": [
                    {
                        "structuredProperty": {
                            "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality"
                        }
                    }
                ]
            }
        },
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "1 structured propert(ies)" in result["message"]
    assert "1 entit(ies)" in result["message"]

    # Verify mutation was called with correct parameters
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "upsertStructuredProperties"
    input_params = mutation_call.kwargs["variables"]["input"]
    assert input_params["assetUrn"] == entity_urns[0]
    assert len(input_params["structuredPropertyInputParams"]) == 1
    assert input_params["structuredPropertyInputParams"][0]["values"][0] == {
        "stringValue": "HIGH"
    }


def test_add_structured_properties_numeric_type(mock_datahub_client):
    """Test adding numeric-typed structured properties to entities."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold": [0.95]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.verified,PROD)"
    ]

    # Mock property validation response (number type)
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.dataQuality.scoreThreshold",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.number",
                        "info": {"qualifiedName": "number"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True

    # Verify numeric value was used
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["variables"]["input"]["structuredPropertyInputParams"][
        0
    ]["values"][0] == {"numberValue": 0.95}


def test_add_structured_properties_multiple_values(mock_datahub_client):
    """Test adding multiple values for a multi-valued property."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.dataClassification": [
            "PII",
            "SENSITIVE",
        ]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.dataClassification",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.dataClassification",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "MULTIPLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True

    # Verify multiple values were sent
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    values = mutation_call.kwargs["variables"]["input"][
        "structuredPropertyInputParams"
    ][0]["values"]
    assert len(values) == 2
    assert values[0] == {"stringValue": "PII"}
    assert values[1] == {"stringValue": "SENSITIVE"}


def test_add_structured_properties_multiple_properties(mock_datahub_client):
    """Test adding multiple structured properties at once."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime": ["90"],
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"],
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
    ]

    # Mock validation for both properties
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "2 structured propert(ies)" in result["message"]

    # Verify both properties were sent
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[2]
    params = mutation_call.kwargs["variables"]["input"]["structuredPropertyInputParams"]
    assert len(params) == 2


def test_add_structured_properties_multiple_entities(mock_datahub_client):
    """Test adding structured properties to multiple entities."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "2 entit(ies)" in result["message"]


# Remove structured properties tests


def test_remove_structured_properties_single_property(mock_datahub_client):
    """Test removing a single structured property from entities."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_structured_properties(
            property_urns=property_urns, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "1 structured propert(ies)" in result["message"]
    assert "1 entit(ies)" in result["message"]

    # Verify mutation was called correctly
    mutation_call = mock_datahub_client._graph.execute_graphql.call_args_list[1]
    assert mutation_call.kwargs["operation_name"] == "removeStructuredProperties"
    assert mutation_call.kwargs["variables"]["input"]["assetUrn"] == entity_urns[0]
    assert (
        mutation_call.kwargs["variables"]["input"]["structuredPropertyUrns"]
        == property_urns
    )


def test_remove_structured_properties_multiple_properties(mock_datahub_client):
    """Test removing multiple structured properties at once."""
    property_urns = [
        "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
        "urn:li:structuredProperty:io.acryl.common.businessCriticality",
    ]
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp,PROD)"]

    # Mock validation for both properties
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_structured_properties(
            property_urns=property_urns, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "2 structured propert(ies)" in result["message"]


def test_remove_structured_properties_multiple_entities(mock_datahub_client):
    """Test removing structured properties from multiple entities."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old2,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},
        {"removeStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = remove_structured_properties(
            property_urns=property_urns, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "2 entit(ies)" in result["message"]


# Validation tests


def test_add_structured_properties_empty_property_values(mock_datahub_client):
    """Test that empty property_values raises ValueError."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="property_values cannot be empty"):
            add_structured_properties(property_values={}, entity_urns=entity_urns)


def test_add_structured_properties_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            add_structured_properties(property_values=property_values, entity_urns=[])


def test_remove_structured_properties_empty_property_urns(mock_datahub_client):
    """Test that empty property_urns raises ValueError."""
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="property_urns cannot be empty"):
            remove_structured_properties(property_urns=[], entity_urns=entity_urns)


def test_remove_structured_properties_empty_entity_urns(mock_datahub_client):
    """Test that empty entity_urns raises ValueError."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="entity_urns cannot be empty"):
            remove_structured_properties(property_urns=property_urns, entity_urns=[])


def test_add_structured_properties_nonexistent_property(mock_datahub_client):
    """Test that nonexistent property URN raises ValueError."""
    property_values = {"urn:li:structuredProperty:io.acryl.nonexistent": ["value"]}
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    # Mock validation returning None (property doesn't exist)
    mock_datahub_client._graph.execute_graphql.return_value = {"entity": None}

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Structured property URN does not exist"):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_invalid_property_type(mock_datahub_client):
    """Test that URN with wrong entity type raises ValueError."""
    property_values = {"urn:li:tag:not-a-property": ["value"]}
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    # Mock validation returning wrong type
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entity": {"urn": "urn:li:tag:not-a-property", "type": "TAG"}
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="not a structured property entity"):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_type_mismatch(mock_datahub_client):
    """Test that value type mismatch raises ValueError."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold": [
            "not-a-number"
        ]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)"]

    # Mock property validation response (number type)
    mock_datahub_client._graph.execute_graphql.return_value = {
        "entity": {
            "urn": "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold",
            "type": "STRUCTURED_PROPERTY",
            "definition": {
                "qualifiedName": "io.acryl.dataQuality.scoreThreshold",
                "valueType": {
                    "urn": "urn:li:dataType:datahub.number",
                    "info": {"qualifiedName": "number"},
                },
                "cardinality": "SINGLE",
                "entityTypes": [
                    {
                        "urn": "urn:li:entityType:datahub.dataset",
                        "type": "DATASET",
                        "info": {"type": "DATASET"},
                    }
                ],
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="Value validation failed"):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_mixed_entity_types(mock_datahub_client):
    """Test applying property to multiple entity types when allowed."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
        "urn:li:dashboard:(urn:li:dataPlatform:looker,my_dashboard)",
    ]

    # Mock property validation response (allows both DATASET and DASHBOARD)
    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        },
                        {
                            "urn": "urn:li:entityType:datahub.dashboard",
                            "type": "DASHBOARD",
                            "info": {"type": "DASHBOARD"},
                        },
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True
    assert "2 entit(ies)" in result["message"]


def test_add_structured_properties_urn_type(mock_datahub_client):
    """Test adding URN-typed structured property."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.relatedDataset": [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.related,PROD)"
        ]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.main,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.relatedDataset",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.relatedDataset",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.urn",
                        "info": {"qualifiedName": "datahub.urn"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True


def test_add_structured_properties_invalid_urn_type(mock_datahub_client):
    """Test that invalid URN format fails validation for URN-typed property."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.relatedDataset": ["not-a-valid-urn"]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.main,PROD)"]

    mock_datahub_client._graph.execute_graphql.return_value = {
        "entity": {
            "urn": "urn:li:structuredProperty:io.acryl.common.relatedDataset",
            "type": "STRUCTURED_PROPERTY",
            "definition": {
                "qualifiedName": "io.acryl.common.relatedDataset",
                "valueType": {
                    "urn": "urn:li:dataType:datahub.urn",
                    "info": {"qualifiedName": "datahub.urn"},
                },
                "cardinality": "SINGLE",
                "entityTypes": [
                    {
                        "urn": "urn:li:entityType:datahub.dataset",
                        "type": "DATASET",
                        "info": {"type": "DATASET"},
                    }
                ],
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="invalid URN"):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_date_type(mock_datahub_client):
    """Test adding date-typed structured property."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.expirationDate": ["2024-12-31"]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.expirationDate",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.expirationDate",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.date",
                        "info": {"qualifiedName": "datahub.date"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True


def test_add_structured_properties_date_with_time(mock_datahub_client):
    """Test adding date-typed property with timestamp."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.createdAt": ["2024-12-22T10:30:00Z"]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.new,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.createdAt",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.createdAt",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.date",
                        "info": {"qualifiedName": "datahub.date"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True


def test_add_structured_properties_invalid_date(mock_datahub_client):
    """Test that invalid date format fails validation."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.expirationDate": ["invalid-date"]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp,PROD)"]

    mock_datahub_client._graph.execute_graphql.return_value = {
        "entity": {
            "urn": "urn:li:structuredProperty:io.acryl.common.expirationDate",
            "type": "STRUCTURED_PROPERTY",
            "definition": {
                "qualifiedName": "io.acryl.common.expirationDate",
                "valueType": {
                    "urn": "urn:li:dataType:datahub.date",
                    "info": {"qualifiedName": "datahub.date"},
                },
                "cardinality": "SINGLE",
                "entityTypes": [
                    {
                        "urn": "urn:li:entityType:datahub.dataset",
                        "type": "DATASET",
                        "info": {"type": "DATASET"},
                    }
                ],
            },
        }
    }

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(ValueError, match="ISO 8601"):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_rich_text_type(mock_datahub_client):
    """Test adding rich text-typed structured property."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.documentation": [
            "# Header\n\nThis is **markdown** content"
        ]
    }
    entity_urns = ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.data,PROD)"]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.documentation",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.documentation",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.rich_text",
                        "info": {"qualifiedName": "datahub.rich_text"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        result = add_structured_properties(
            property_values=property_values, entity_urns=entity_urns
        )

    assert result["success"] is True


# Error handling tests


def test_add_structured_properties_mutation_failure(mock_datahub_client):
    """Test handling when mutation fails for some entities."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
        Exception("Mutation failed"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError,
            match="Failed to add structured properties to 1 entit\\(ies\\)",
        ):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_remove_structured_properties_mutation_failure(mock_datahub_client):
    """Test handling when remove mutation fails for some entities."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},
        Exception("Mutation failed"),
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError,
            match="Failed to remove structured properties from 1 entit\\(ies\\)",
        ):
            remove_structured_properties(
                property_urns=property_urns, entity_urns=entity_urns
            )


def test_add_structured_properties_empty_mutation_result(mock_datahub_client):
    """Test handling when mutation returns empty/None result."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},
        {},  # Empty result for second entity
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="operation returned false or empty result"
        ):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_add_structured_properties_none_mutation_result(mock_datahub_client):
    """Test handling when mutation returns None (upsertStructuredProperties key missing)."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"someOtherKey": "value"},  # Result without upsertStructuredProperties key
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="operation returned false or empty result"
        ):
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )


def test_remove_structured_properties_empty_mutation_result(mock_datahub_client):
    """Test handling when remove mutation returns empty/None result."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},
        {},  # Empty result for second entity
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="operation returned false or empty result"
        ):
            remove_structured_properties(
                property_urns=property_urns, entity_urns=entity_urns
            )


def test_remove_structured_properties_none_mutation_result(mock_datahub_client):
    """Test handling when remove mutation returns None (removeStructuredProperties key missing)."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"someOtherKey": "value"},  # Result without removeStructuredProperties key
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(
            RuntimeError, match="operation returned false or empty result"
        ):
            remove_structured_properties(
                property_urns=property_urns, entity_urns=entity_urns
            )


def test_add_structured_properties_partial_success(mock_datahub_client):
    """Test handling when some entities succeed and some fail."""
    property_values = {
        "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
    }
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test3,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.common.businessCriticality",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.common.businessCriticality",
                    "valueType": {
                        "urn": "urn:li:dataType:datahub.string",
                        "info": {"qualifiedName": "string"},
                    },
                    "cardinality": "SINGLE",
                    "entityTypes": [
                        {
                            "urn": "urn:li:entityType:datahub.dataset",
                            "type": "DATASET",
                            "info": {"type": "DATASET"},
                        }
                    ],
                },
            }
        },
        {"upsertStructuredProperties": {"properties": []}},  # Success for first entity
        {},  # Empty result for second entity
        Exception("Network error"),  # Exception for third entity
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError) as exc_info:
            add_structured_properties(
                property_values=property_values, entity_urns=entity_urns
            )

        # Should contain both types of errors
        assert "Failed to add structured properties to 2 entit(ies)" in str(
            exc_info.value
        )
        assert "operation returned false or empty result" in str(exc_info.value)
        assert "Network error" in str(exc_info.value)


def test_remove_structured_properties_partial_success(mock_datahub_client):
    """Test handling when some entities succeed and some fail during removal."""
    property_urns = ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
    entity_urns = [
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test1,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test2,PROD)",
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test3,PROD)",
    ]

    mock_datahub_client._graph.execute_graphql.side_effect = [
        {
            "entity": {
                "urn": "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "type": "STRUCTURED_PROPERTY",
                "definition": {
                    "qualifiedName": "io.acryl.privacy.retentionTime",
                    "entityTypes": [],
                },
            }
        },
        {"removeStructuredProperties": {"properties": []}},  # Success for first entity
        {},  # Empty result for second entity
        Exception("Permission denied"),  # Exception for third entity
    ]

    with patch(
        "datahub_integrations.mcp.mcp_server.get_datahub_client",
        return_value=mock_datahub_client,
    ):
        with pytest.raises(RuntimeError) as exc_info:
            remove_structured_properties(
                property_urns=property_urns, entity_urns=entity_urns
            )

        # Should contain both types of errors
        assert "Failed to remove structured properties from 2 entit(ies)" in str(
            exc_info.value
        )
        assert "operation returned false or empty result" in str(exc_info.value)
        assert "Permission denied" in str(exc_info.value)
