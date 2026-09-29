"""Description management tools for DataHub MCP server."""

import logging
from typing import Literal, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _get_existing_description(entity_urn: str, column_path: Optional[str]) -> str:
    """Fetch existing description for entity or column."""
    graph = get_graph()
    query = """
        query getEntity($urn: String!) {
            entity(urn: $urn) {
                ... on Dataset {
                    editableProperties {
                        description
                    }
                    schemaMetadata {
                        fields {
                            fieldPath
                            description
                        }
                    }
                }
                ... on Container {
                    editableProperties {
                        description
                    }
                }
                ... on Chart {
                    editableProperties {
                        description
                    }
                }
                ... on Dashboard {
                    editableProperties {
                        description
                    }
                }
                ... on DataFlow {
                    editableProperties {
                        description
                    }
                }
                ... on DataJob {
                    editableProperties {
                        description
                    }
                }
                ... on MLModel {
                    editableProperties {
                        description
                    }
                }
                ... on MLModelGroup {
                    editableProperties {
                        description
                    }
                }
                ... on MLFeatureTable {
                    editableProperties {
                        description
                    }
                }
                ... on MLPrimaryKey {
                    editableProperties {
                        description
                    }
                }
                ... on Tag {
                    properties {
                        description
                    }
                }
                ... on GlossaryTerm {
                    properties {
                        description
                    }
                }
                ... on GlossaryNode {
                    properties {
                        description
                    }
                }
                ... on Domain {
                    properties {
                        description
                    }
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={"urn": entity_urn},
            operation_name="getEntity",
        )

        entity_data = result.get("entity", {})
        if column_path:
            # Get column description
            schema_metadata = entity_data.get("schemaMetadata", {})
            fields = schema_metadata.get("fields", [])
            for field in fields:
                if field.get("fieldPath") == column_path:
                    return field.get("description", "")
            return ""
        else:
            # Get entity description
            # Try editableProperties first (for Dataset, Container, etc.)
            editable_props = entity_data.get("editableProperties", {})
            existing_description = editable_props.get("description", "")

            # If not found, try properties (for Tag, GlossaryTerm, etc.)
            if not existing_description:
                properties = entity_data.get("properties", {})
                existing_description = properties.get("description", "")

            return existing_description

    except Exception as e:
        logger.warning(
            f"Failed to fetch existing description for {entity_urn}: {e}. Will treat as empty."
        )
        return ""


def update_description(
    entity_urn: str,
    operation: Literal["replace", "append", "remove"] = "replace",
    description: Optional[str] = None,
    column_path: Optional[str] = None,
) -> dict:
    """Update description for a DataHub entity or its column (e.g., schema field).

    This tool allows you to set, append to, or remove a description for an entity or its column.
    Useful for documenting datasets, containers, charts, dashboards, data flows, data jobs,
    ML models, ML model groups, ML feature tables, ML primary keys, tags, glossary terms,
    glossary nodes, domains, and schema fields.

    Args:
        entity_urn: Entity URN to update description for (e.g., dataset URN, container URN)
        operation: The operation to perform:
                  - "replace": Replace the existing description with the new one (default)
                  - "append": Append the new description to the existing one
                  - "remove": Remove the description (description parameter not needed)
        description: The description text to set or append (supports markdown formatting).
                    Required for "replace" and "append" operations, ignored for "remove".
        column_path: Column_path identifier (e.g., column name for schema field).
                    Optional for all entity types (use None for entity-level descriptions).
                    For column-level descriptions, provide the column name (e.g., "customer_email").
                    Verify that the column_path is correct and valid via the schemaMetadata.
                    Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - urn: The entity URN
        - column_path: The column path (if applicable)
        - message: Success or error message

    Examples:
        # Update description for a container (entity-level)
        update_description(
            entity_urn="urn:li:container:12345",
            operation="replace",
            description="Production data warehouse"
        )

        # Update description for a dataset
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="replace",
            description="User's table",
        )

        # Update description for a dataset field (column-level)
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="replace",
            description="User's primary email address",
            column_path="email"
        )

        # Append to existing field description
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="append",
            description=" (PII)",
            column_path="email"
        )

        # Remove field description
        update_description(
            entity_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
            operation="remove",
            column_path="old_field"
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = update_description(
                entity_urn="urn:li:dataset:(...)",
                operation="replace",
                description="User table"
            )
    """
    graph = get_graph()
    # Validate inputs
    if not entity_urn:
        raise ValueError("entity_urn cannot be empty")

    if operation in ("replace", "append"):
        if not description:
            raise ValueError(f"description is required for '{operation}' operation")
    elif operation == "remove":
        # For remove operation, ignore description parameter
        description = ""
    else:
        raise ValueError(
            f"Invalid operation '{operation}'. Must be 'replace', 'append', or 'remove'"
        )

    # For append operation, we need to fetch existing description first
    existing_description = ""
    if operation == "append":
        existing_description = _get_existing_description(entity_urn, column_path)

    # Determine final description based on operation
    if operation == "append":
        final_description = (
            existing_description + description if existing_description else description
        )
    elif operation == "remove":
        final_description = ""
    else:  # replace
        final_description = description

    # GraphQL mutation
    mutation = """
        mutation updateDescription($input: DescriptionUpdateInput!) {
            updateDescription(input: $input)
        }
    """

    variables: dict = {
        "input": {
            "description": final_description,
            "resourceUrn": entity_urn,
        }
    }

    # Add subresource fields if provided (for column-level descriptions)
    if column_path:
        variables["input"]["subResource"] = column_path
        variables["input"]["subResourceType"] = "DATASET_FIELD"

    try:
        result = execute_graphql(
            graph,
            query=mutation,
            variables=variables,
            operation_name="updateDescription",
        )

        if result.get("updateDescription", False):
            action_verb = "updated" if operation in ("replace", "append") else "removed"
            return {
                "success": True,
                "urn": entity_urn,
                "column_path": column_path,
                "message": f"Description {action_verb} successfully",
            }
        else:
            action = "update" if operation in ("replace", "append") else "remove"
            raise RuntimeError(
                f"Failed to {action} description for {entity_urn}"
                + (f" column {column_path}" if column_path else "")
                + " - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        action = "update" if operation in ("replace", "append") else "remove"
        raise RuntimeError(
            f"Error {action} description for {entity_urn}"
            + (f" column {column_path}" if column_path else "")
            + f": {str(e)}"
        ) from e
