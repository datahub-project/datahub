"""Structured property management tools for DataHub MCP server."""

import logging
from datetime import datetime
from typing import Dict, List, Union

from datahub.utilities.urns._urn_base import Urn
from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _validate_and_fetch_structured_property(property_urn: str) -> Dict:
    """
    Validate that the structured property exists and fetch its definition.

    Returns:
        Dictionary with property definition including valueType and entityTypes

    Raises:
        ValueError: If the property URN does not exist or is invalid
    """
    graph = get_graph()
    query = """
        query getStructuredProperty($urn: String!) {
            entity(urn: $urn) {
                urn
                type
                ... on StructuredPropertyEntity {
                    definition {
                        qualifiedName
                        entityTypes {
                            urn
                            type
                            info {
                                type
                            }
                        }
                        valueType {
                            urn
                            info {
                                qualifiedName
                            }
                        }
                        cardinality
                    }
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={"urn": property_urn},
            operation_name="getStructuredProperty",
        )

        entity = result.get("entity")

        if entity is None:
            raise ValueError(
                f"Structured property URN does not exist in DataHub: {property_urn}. "
                f"Please use the search tool to find existing structured properties, "
                f"or create the property first before assigning it."
            )

        if entity.get("type") != "STRUCTURED_PROPERTY":
            raise ValueError(
                f"The URN is not a structured property entity: {property_urn} (type: {entity.get('type')})"
            )

        return entity.get("definition", {})

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate structured property URN: {str(e)}") from e


def _validate_property_value(
    property_definition: Dict, value: Union[str, float, int]
) -> Dict:
    """
    Validate and convert a property value to the appropriate GraphQL format.

    Supports 5 data types:
    - datahub.string: Plain text strings
    - datahub.number: Numeric values (int, float, double, long)
    - datahub.urn: DataHub URN references
    - datahub.date: ISO 8601 date strings
    - datahub.rich_text: Rich text/markdown content

    Args:
        property_definition: The property definition containing valueType info
        value: The value to validate and convert

    Returns:
        Dictionary with either stringValue or numberValue key

    Raises:
        ValueError: If the value type doesn't match the property's valueType
    """
    value_type_info = property_definition.get("valueType", {}).get("info", {})
    qualified_name = value_type_info.get("qualifiedName", "").lower()

    # Determine the data type
    is_numeric_type = any(
        numeric_type in qualified_name
        for numeric_type in ["number", "int", "float", "double", "long"]
    )
    is_urn_type = "urn" in qualified_name and "datahub.urn" in qualified_name
    is_date_type = "date" in qualified_name
    is_rich_text_type = "rich_text" in qualified_name or "richtext" in qualified_name

    if is_numeric_type:
        # Value should be numeric
        if isinstance(value, (int, float)):
            return {"numberValue": float(value)}
        elif isinstance(value, str):
            try:
                return {"numberValue": float(value)}
            except ValueError as e:
                raise ValueError(
                    f"Property expects numeric type ({qualified_name}), but got non-numeric string: {value}"
                ) from e
        else:
            raise ValueError(
                f"Property expects numeric type ({qualified_name}), got {type(value).__name__}"
            )

    elif is_urn_type:
        # Value should be a valid DataHub URN
        if not isinstance(value, str):
            value = str(value)

        try:
            # Validate URN format
            Urn.from_string(value)
            return {"stringValue": value}
        except Exception as e:
            raise ValueError(
                f"Property expects URN type ({qualified_name}), but got invalid URN: {value}. "
                f"URNs must be in format 'urn:li:entityType:...' Error: {str(e)}"
            ) from e

    elif is_date_type:
        # Value should be an ISO 8601 date string
        if not isinstance(value, str):
            value = str(value)

        # Try to parse as ISO 8601 date
        try:
            # Support various ISO 8601 formats
            # Examples: 2024-12-22, 2024-12-22T10:30:00, 2024-12-22T10:30:00Z, 2024-12-22T10:30:00+00:00
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return {"stringValue": value}
        except ValueError as e:
            raise ValueError(
                f"Property expects date type ({qualified_name}), but got invalid date format: {value}. "
                f"Dates must be in ISO 8601 format (e.g., '2024-12-22', '2024-12-22T10:30:00Z')"
            ) from e

    elif is_rich_text_type:
        # Value should be string (can contain markdown/HTML)
        if isinstance(value, str):
            return {"stringValue": value}
        else:
            # Convert to string for non-string types
            return {"stringValue": str(value)}

    else:
        # Default to string type (datahub.string or unknown types)
        if isinstance(value, str):
            return {"stringValue": value}
        else:
            # Convert to string for non-string types
            return {"stringValue": str(value)}

    raise ValueError(
        f"Value type mismatch: property expects {qualified_name}, got {type(value).__name__}"
    )


def add_structured_properties(
    property_values: Dict[str, List[Union[str, float, int]]],
    entity_urns: List[str],
) -> dict:
    """Add structured properties with values to multiple DataHub entities.

    This tool allows you to assign structured properties to multiple entities in a single operation.
    Structured properties are schema-defined metadata fields that can store typed values (strings, numbers, etc.).

    Args:
        property_values: Dictionary mapping structured property URNs to lists of values.
                        Example: {
                            "urn:li:structuredProperty:io.acryl.privacy.retentionTime": ["90"],
                            "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
                        }
        entity_urns: List of entity URNs to assign properties to (e.g., dataset URNs, dashboard URNs)

    Examples:
        # Add retention time and criticality to datasets
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.privacy.retentionTime": ["90"],
                "urn:li:structuredProperty:io.acryl.common.businessCriticality": ["HIGH"]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add numeric property
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.dataQuality.scoreThreshold": [0.95]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.verified_data,PROD)"
            ]
        )

        # Add multiple values for a multi-valued property
        add_structured_properties(
            property_values={
                "urn:li:structuredProperty:io.acryl.common.dataClassification": ["PII", "SENSITIVE"]
            },
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ]
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = add_structured_properties(
                property_values={"urn:li:structuredProperty:...": ["value"]},
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    graph = get_graph()
    if not property_values:
        raise ValueError("property_values cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate all structured properties and fetch their definitions
    property_definitions = {}
    for property_urn in property_values:
        property_definitions[property_urn] = _validate_and_fetch_structured_property(
            property_urn
        )

    # Build structured property input params with type validation
    structured_property_params = []
    for property_urn, values in property_values.items():
        property_def = property_definitions[property_urn]

        # Validate and convert each value
        converted_values = []
        for value in values:
            try:
                converted_value = _validate_property_value(property_def, value)
                converted_values.append(converted_value)
            except ValueError as e:
                raise ValueError(
                    f"Value validation failed for {property_urn}: {str(e)}"
                ) from e

        structured_property_params.append(
            {"structuredPropertyUrn": property_urn, "values": converted_values}
        )

    # Execute upsert for each entity
    mutation = """
        mutation upsertStructuredProperties($input: UpsertStructuredPropertiesInput!) {
            upsertStructuredProperties(input: $input) {
                properties {
                    structuredProperty {
                        urn
                    }
                }
            }
        }
    """

    success_count = 0
    failed_urns = []
    error_messages = []

    for entity_urn in entity_urns:
        variables = {
            "input": {
                "assetUrn": entity_urn,
                "structuredPropertyInputParams": structured_property_params,
            }
        }

        try:
            result = execute_graphql(
                graph,
                query=mutation,
                variables=variables,
                operation_name="upsertStructuredProperties",
            )

            if result.get("upsertStructuredProperties"):
                success_count += 1
            else:
                failed_urns.append(entity_urn)
                error_messages.append(
                    f"{entity_urn}: operation returned false or empty result"
                )

        except Exception as e:
            failed_urns.append(entity_urn)
            error_messages.append(f"{entity_urn}: {str(e)}")

    if failed_urns:
        error_details = "; ".join(error_messages[:3])
        if len(error_messages) > 3:
            error_details += f"; and {len(error_messages) - 3} more error(s)"
        raise RuntimeError(
            f"Failed to add structured properties to {len(failed_urns)} entit(ies). Errors: {error_details}"
        )

    return {
        "success": True,
        "message": f"Successfully added {len(property_values)} structured propert(ies) to {success_count} entit(ies)",
    }


def remove_structured_properties(
    property_urns: List[str],
    entity_urns: List[str],
) -> dict:
    """Remove structured properties from multiple DataHub entities.

    This tool allows you to remove structured property assignments from multiple entities in a single operation.

    Args:
        property_urns: List of structured property URNs to remove
                      Example: ["urn:li:structuredProperty:io.acryl.privacy.retentionTime"]
        entity_urns: List of entity URNs to remove properties from (e.g., dataset URNs, dashboard URNs)

    Examples:
        # Remove retention time property from datasets
        remove_structured_properties(
            property_urns=["urn:li:structuredProperty:io.acryl.privacy.retentionTime"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_data,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.archived,PROD)"
            ]
        )

        # Remove multiple properties at once
        remove_structured_properties(
            property_urns=[
                "urn:li:structuredProperty:io.acryl.privacy.retentionTime",
                "urn:li:structuredProperty:io.acryl.common.businessCriticality"
            ],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp_table,PROD)"
            ]
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = remove_structured_properties(
                property_urns=["urn:li:structuredProperty:..."],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    graph = get_graph()
    if not property_urns:
        raise ValueError("property_urns cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate all structured properties exist
    for property_urn in property_urns:
        _validate_and_fetch_structured_property(property_urn)

    # Execute remove for each entity
    mutation = """
        mutation removeStructuredProperties($input: RemoveStructuredPropertiesInput!) {
            removeStructuredProperties(input: $input) {
                properties {
                    structuredProperty {
                        urn
                    }
                }
            }
        }
    """

    success_count = 0
    failed_urns = []
    error_messages = []

    for entity_urn in entity_urns:
        variables = {
            "input": {
                "assetUrn": entity_urn,
                "structuredPropertyUrns": property_urns,
            }
        }

        try:
            result = execute_graphql(
                graph,
                query=mutation,
                variables=variables,
                operation_name="removeStructuredProperties",
            )

            if result.get("removeStructuredProperties"):
                success_count += 1
            else:
                failed_urns.append(entity_urn)
                error_messages.append(
                    f"{entity_urn}: operation returned false or empty result"
                )

        except Exception as e:
            failed_urns.append(entity_urn)
            error_messages.append(f"{entity_urn}: {str(e)}")

    if failed_urns:
        error_details = "; ".join(error_messages[:3])
        if len(error_messages) > 3:
            error_details += f"; and {len(error_messages) - 3} more error(s)"
        raise RuntimeError(
            f"Failed to remove structured properties from {len(failed_urns)} entit(ies). Errors: {error_details}"
        )

    return {
        "success": True,
        "message": f"Successfully removed {len(property_urns)} structured propert(ies) from {success_count} entit(ies)",
    }
