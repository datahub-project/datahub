"""Tag management tools for DataHub MCP server."""

import logging
from typing import List, Literal, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _validate_tag_urns(tag_urns: List[str]) -> None:
    """
    Validate that all tag URNs exist in DataHub.

    Raises:
        ValueError: If any tag URN does not exist or is invalid
    """
    graph = get_graph()
    query = """
        query getTags($urns: [String!]!) {
            entities(urns: $urns) {
                urn
                type
                ... on Tag {
                    properties {
                        name
                    }
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={"urns": tag_urns},
            operation_name="getTags",
        )

        entities = result.get("entities", [])

        # Build a map of found URNs
        found_urns = {entity["urn"] for entity in entities if entity is not None}

        # Check for missing or invalid tags
        missing_urns = [urn for urn in tag_urns if urn not in found_urns]

        if missing_urns:
            raise ValueError(
                f"The following tag URNs do not exist in DataHub: {', '.join(missing_urns)}. "
                f"Please use the search tool with entity_type filter to find existing tags, "
                f"or create the tags first before assigning them."
            )

        # Verify all returned entities are actually Tags
        non_tag_entities = [
            entity["urn"]
            for entity in entities
            if entity and entity.get("type") != "TAG"
        ]
        if non_tag_entities:
            raise ValueError(
                f"The following URNs are not tag entities: {', '.join(non_tag_entities)}"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate tag URNs: {str(e)}") from e


def _batch_modify_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: Literal["add", "remove"],
) -> dict:
    """
    Internal helper for batch tag operations (add/remove).

    Validates inputs, constructs GraphQL mutation, and executes the operation.
    """
    graph = get_graph()
    # Validate inputs
    if not tag_urns:
        raise ValueError("tag_urns cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate that all tag URNs exist
    _validate_tag_urns(tag_urns)

    # Handle column_paths - if not provided, create list of Nones
    if column_paths is None:
        column_paths = [None] * len(entity_urns)
    elif len(column_paths) != len(entity_urns):
        raise ValueError(
            f"column_paths length ({len(column_paths)}) must match entity_urns length ({len(entity_urns)})"
        )

    # Build the resources list for GraphQL mutation
    resources = []
    for resource_urn, column_path in zip(entity_urns, column_paths, strict=True):
        resource_input = {"resourceUrn": resource_urn}

        # Add subresource fields if provided (for column-level tagging)
        if column_path:
            resource_input["subResource"] = column_path
            resource_input["subResourceType"] = "DATASET_FIELD"

        resources.append(resource_input)

    # Determine mutation and operation name based on operation type
    if operation == "add":
        mutation = """
            mutation batchAddTags($input: BatchAddTagsInput!) {
                batchAddTags(input: $input)
            }
        """
        operation_name = "batchAddTags"
        success_verb = "added"
        failure_verb = "add"
    else:  # remove
        mutation = """
            mutation batchRemoveTags($input: BatchRemoveTagsInput!) {
                batchRemoveTags(input: $input)
            }
        """
        operation_name = "batchRemoveTags"
        success_verb = "removed"
        failure_verb = "remove"

    variables = {"input": {"tagUrns": tag_urns, "resources": resources}}

    try:
        result = execute_graphql(
            graph,
            query=mutation,
            variables=variables,
            operation_name=operation_name,
        )

        success = result.get(operation_name, False)
        if success:
            preposition = "to" if operation == "add" else "from"
            return {
                "success": True,
                "message": f"Successfully {success_verb} {len(tag_urns)} tag(s) {preposition} {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError(
                f"Failed to {failure_verb} tags - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error {failure_verb} tags: {str(e)}") from e


def add_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Add one or more tags to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to tag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tagging operations like marking multiple datasets as PII, deprecated, or applying
    governance classifications.

    Args:
        tag_urns: List of tag URNs to add (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to tag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tags.
                     For column-level tags, provide the column name (e.g., "email_address").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add tags to multiple datasets
        add_tags(
            tag_urns=["urn:li:tag:PII", "urn:li:tag:Sensitive"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add tags to specific columns
        add_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "phone_number"]
        )

        # Mix entity-level and column-level tags
        add_tags(
            tag_urns=["urn:li:tag:Deprecated"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "deprecated_column"]  # Tag whole table and a specific column
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = add_tags(
                tag_urns=["urn:li:tag:PII"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_tags(tag_urns, entity_urns, column_paths, "add")


def remove_tags(
    tag_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Remove one or more tags from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to untag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tag removal operations like removing deprecated tags, correcting misapplied classifications,
    or cleaning up governance metadata.

    Args:
        tag_urns: List of tag URNs to remove (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to untag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tag removal.
                     For column-level tag removal, provide the column name (e.g., "email_address").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove tags from multiple datasets
        remove_tags(
            tag_urns=["urn:li:tag:Deprecated", "urn:li:tag:Legacy"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove tags from specific columns
        remove_tags(
            tag_urns=["urn:li:tag:PII"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_email_field", "deprecated_phone"]
        )

        # Mix entity-level and column-level tag removal
        remove_tags(
            tag_urns=["urn:li:tag:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "test_column"]  # Remove from whole table and a specific column
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = remove_tags(
                tag_urns=["urn:li:tag:Deprecated"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_tags(tag_urns, entity_urns, column_paths, "remove")
