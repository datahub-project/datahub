"""Owner management tools for DataHub MCP server."""

import logging
from typing import List, Literal, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _validate_owner_urns(owner_urns: List[str]) -> None:
    """
    Validate that all owner URNs exist in DataHub and are either CorpUser or CorpGroup entities.

    Raises:
        ValueError: If any owner URN does not exist or is not a valid owner entity type
    """
    graph = get_graph()
    # Query to check if owners exist and are valid types
    query = """
        query getOwners($urns: [String!]!) {
            entities(urns: $urns) {
                urn
                type
                ... on CorpUser {
                    username
                }
                ... on CorpGroup {
                    name
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={"urns": owner_urns},
            operation_name="getOwners",
        )

        entities = result.get("entities", [])

        # Build a map of found URNs
        found_urns = {entity["urn"] for entity in entities if entity is not None}

        # Check for missing owners
        missing_urns = [urn for urn in owner_urns if urn not in found_urns]

        if missing_urns:
            raise ValueError(
                f"The following owner URNs do not exist in DataHub: {', '.join(missing_urns)}. "
                f"Please use the search tool with entity_type filter to find existing users or groups, "
                f"or create the owners first before assigning them."
            )

        # Verify all returned entities are either CorpUser or CorpGroup
        invalid_type_entities = [
            entity["urn"]
            for entity in entities
            if entity and entity.get("type") not in ("CORP_USER", "CORP_GROUP")
        ]
        if invalid_type_entities:
            raise ValueError(
                f"The following URNs are not valid owner entities (must be CorpUser or CorpGroup): {', '.join(invalid_type_entities)}"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate owner URNs: {str(e)}") from e


def _batch_modify_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type_urn: Optional[str],
    operation: Literal["add", "remove"],
) -> dict:
    """
    Internal helper for batch owner operations (add/remove).

    Validates inputs, constructs GraphQL mutation, and executes the operation.
    """
    graph = get_graph()
    # Validate inputs
    if not owner_urns:
        raise ValueError("owner_urns cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate that all owner URNs exist and are valid types
    _validate_owner_urns(owner_urns)

    # Build the resources list for GraphQL mutation
    resources = []
    for resource_urn in entity_urns:
        resource_input = {"resourceUrn": resource_urn}
        resources.append(resource_input)

    # Determine mutation and operation name based on operation type
    if operation == "add":
        # For adding owners, we need to include ownerEntityType
        # Determine owner entity types from URNs
        owners = []
        for owner_urn in owner_urns:
            owner_entity_type = (
                "CORP_USER" if ":corpuser:" in owner_urn.lower() else "CORP_GROUP"
            )
            owner_input: dict = {
                "ownerUrn": owner_urn,
                "ownerEntityType": owner_entity_type,
            }
            # Add ownership type if provided
            if ownership_type_urn:
                owner_input["ownershipTypeUrn"] = ownership_type_urn

            owners.append(owner_input)

        mutation = """
            mutation batchAddOwners($input: BatchAddOwnersInput!) {
                batchAddOwners(input: $input)
            }
        """
        add_input: dict = {
            "owners": owners,
            "resources": resources,
        }
        if ownership_type_urn:
            add_input["ownershipTypeUrn"] = ownership_type_urn

        variables = {"input": add_input}

        operation_name = "batchAddOwners"
        success_verb = "added"
        failure_verb = "add"
    else:  # remove
        mutation = """
            mutation batchRemoveOwners($input: BatchRemoveOwnersInput!) {
                batchRemoveOwners(input: $input)
            }
        """
        remove_input: dict = {
            "ownerUrns": owner_urns,
            "resources": resources,
        }
        if ownership_type_urn:
            remove_input["ownershipTypeUrn"] = ownership_type_urn

        variables = {"input": remove_input}

        operation_name = "batchRemoveOwners"
        success_verb = "removed"
        failure_verb = "remove"

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
                "message": f"Successfully {success_verb} {len(owner_urns)} owner(s) {preposition} {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError(
                f"Failed to {failure_verb} owners - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error {failure_verb} owners: {str(e)}") from e


def add_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type_urn: Optional[str] = None,
) -> dict:
    """Add one or more owners to multiple DataHub entities.

    This tool allows you to assign multiple entities with multiple owners in a single operation.
    Useful for bulk ownership assignment operations like assigning data stewards, technical owners,
    or business owners to datasets, dashboards, and other DataHub entities.

    Note: Ownership in DataHub is entity-level only. For field-level metadata, use tags or glossary terms instead.

    Args:
        owner_urns: List of owner URNs to add (must be CorpUser or CorpGroup URNs).
                   Examples: ["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"]
        entity_urns: List of entity URNs to assign ownership to (e.g., dataset URNs, dashboard URNs)
        ownership_type_urn: Optional ownership type URN to specify the type of ownership
                          (e.g., "urn:li:ownershipType:dataowner", "urn:li:ownershipType:technical_owner").
                          If not provided, ownership type will be set based on the mutation default.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add owners to multiple datasets
        add_owners(
            owner_urns=["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add technical owner with specific ownership type
        add_owners(
            owner_urns=["urn:li:corpuser:jane.smith"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type_urn="urn:li:ownershipType:technical_owner"
        )

        # Add data owner to multiple entities
        add_owners(
            owner_urns=["urn:li:corpuser:data.steward"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,sales_dashboard,PROD)"
            ],
            ownership_type_urn="urn:li:ownershipType:dataowner"
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = add_owners(
                owner_urns=["urn:li:corpuser:john.doe"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_owners(owner_urns, entity_urns, ownership_type_urn, "add")


def remove_owners(
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type_urn: Optional[str] = None,
) -> dict:
    """Remove one or more owners from multiple DataHub entities.

    This tool allows you to unassign multiple entities from multiple owners in a single operation.
    Useful for bulk ownership removal operations like removing owners when they change roles,
    cleaning up stale ownership, or correcting misassigned ownership.

    Note: Ownership in DataHub is entity-level only. For field-level metadata, use tags or glossary terms instead.

    Args:
        owner_urns: List of owner URNs to remove (must be CorpUser or CorpGroup URNs).
                   Examples: ["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"]
        entity_urns: List of entity URNs to remove ownership from (e.g., dataset URNs, dashboard URNs)
        ownership_type_urn: Optional ownership type URN to specify which type of ownership to remove
                          (e.g., "urn:li:ownershipType:dataowner").
                          If not provided, will remove ownership regardless of type.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove owners from multiple datasets
        remove_owners(
            owner_urns=["urn:li:corpuser:former.employee", "urn:li:corpGroup:old-team"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Remove technical owner with specific ownership type
        remove_owners(
            owner_urns=["urn:li:corpuser:john.doe"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type_urn="urn:li:ownershipType:technical_owner"
        )

        # Remove temporary owner from multiple entities
        remove_owners(
            owner_urns=["urn:li:corpuser:temp.owner"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.stable_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,temp_dashboard,PROD)"
            ]
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = remove_owners(
                owner_urns=["urn:li:corpuser:former.employee"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_owners(owner_urns, entity_urns, ownership_type_urn, "remove")
