"""Terms management tools for DataHub MCP server."""

import logging
from typing import List, Literal, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _validate_glossary_term_urns(term_urns: List[str]) -> None:
    """
    Validate that all glossary term URNs exist in DataHub.

    Raises:
        ValueError: If any term URN does not exist or is invalid
    """
    graph = get_graph()
    # Query to check if glossary terms exist
    query = """
        query getGlossaryTerms($urns: [String!]!) {
            entities(urns: $urns) {
                urn
                type
                ... on GlossaryTerm {
                    name
                }
            }
        }
    """

    try:
        result = execute_graphql(
            graph,
            query=query,
            variables={"urns": term_urns},
            operation_name="getGlossaryTerms",
        )

        entities = result.get("entities", [])

        # Build a map of found URNs
        found_urns = {entity["urn"] for entity in entities if entity is not None}

        # Check for missing or invalid terms
        missing_urns = [urn for urn in term_urns if urn not in found_urns]

        if missing_urns:
            raise ValueError(
                f"The following glossary term URNs do not exist in DataHub: {', '.join(missing_urns)}. "
                f"Please use the search tool with entity_type filter to find existing glossary terms, "
                f"or create the terms first before assigning them."
            )

        # Verify all returned entities are actually GlossaryTerms
        non_term_entities = [
            entity["urn"]
            for entity in entities
            if entity and entity.get("type") != "GLOSSARY_TERM"
        ]
        if non_term_entities:
            raise ValueError(
                f"The following URNs are not glossary term entities: {', '.join(non_term_entities)}"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate glossary term URNs: {str(e)}") from e


def _batch_modify_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: Literal["add", "remove"],
) -> dict:
    """
    Internal helper for batch glossary term operations (add/remove).

    Validates inputs, constructs GraphQL mutation, and executes the operation.
    """
    graph = get_graph()
    # Validate inputs
    if not term_urns:
        raise ValueError("term_urns cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    # Validate that all glossary term URNs exist
    _validate_glossary_term_urns(term_urns)

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

        # Add subresource fields if provided (for column-level glossary terms)
        if column_path:
            resource_input["subResource"] = column_path
            resource_input["subResourceType"] = "DATASET_FIELD"

        resources.append(resource_input)

    # Determine mutation and operation name based on operation type
    if operation == "add":
        mutation = """
            mutation batchAddTerms($input: BatchAddTermsInput!) {
                batchAddTerms(input: $input)
            }
        """
        operation_name = "batchAddTerms"
        success_verb = "added"
        failure_verb = "add"
    else:  # remove
        mutation = """
            mutation batchRemoveTerms($input: BatchRemoveTermsInput!) {
                batchRemoveTerms(input: $input)
            }
        """
        operation_name = "batchRemoveTerms"
        success_verb = "removed"
        failure_verb = "remove"

    variables = {"input": {"termUrns": term_urns, "resources": resources}}

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
                "message": f"Successfully {success_verb} {len(term_urns)} glossary term(s) {preposition} {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError(
                f"Failed to {failure_verb} glossary terms - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error {failure_verb} glossary terms: {str(e)}") from e


def add_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Add one or more glossary terms (terms) to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to associate multiple entities or their columns with multiple glossary terms in a single operation.
    Useful for bulk term assignment operations like applying business definitions, standardizing terminology,
    or enriching metadata with domain knowledge.

    Args:
        term_urns: List of glossary term URNs to add (e.g., ["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:SensitiveInfo"])
        entity_urns: List of entity URNs to annotate (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary terms.
                     For column-level glossary terms, provide the column name (e.g., "customer_email").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Add glossary terms to multiple datasets
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:CustomerData", "urn:li:glossaryTerm:PersonalInformation"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Add glossary terms to specific columns
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:EmailAddress"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["email", "contact_email"]
        )

        # Mix entity-level and column-level glossary terms
        add_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Revenue"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.transactions,PROD)"
            ],
            column_paths=[None, "total_amount"]  # Term for whole table and a specific column
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = add_glossary_terms(
                term_urns=["urn:li:glossaryTerm:CustomerData"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_glossary_terms(term_urns, entity_urns, column_paths, "add")


def remove_glossary_terms(
    term_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]] = None,
) -> dict:
    """Remove one or more glossary terms (terms) from multiple DataHub entities or their column_paths (e.g., schema fields).

    This tool allows you to disassociate multiple entities or their columns from multiple glossary terms in a single operation.
    Useful for bulk term removal operations like correcting misapplied business definitions, updating terminology,
    or cleaning up metadata.

    Args:
        term_urns: List of glossary term URNs to remove (e.g., ["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:Legacy"])
        entity_urns: List of entity URNs to remove terms from (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level glossary term removal.
                     For column-level glossary term removal, provide the column name (e.g., "old_field").
                     Verify that the column_paths are correct and valid via the schemaMetadata.
                     Use get_entity tool to verify.

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove glossary terms from multiple datasets
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Deprecated", "urn:li:glossaryTerm:LegacySystem"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_customers,PROD)"
            ]
        )

        # Remove glossary terms from specific columns
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Confidential"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=["old_ssn_field", "legacy_tax_id"]
        )

        # Mix entity-level and column-level glossary term removal
        remove_glossary_terms(
            term_urns=["urn:li:glossaryTerm:Experimental"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.production_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
            ],
            column_paths=[None, "beta_feature"]  # Remove from whole table and a specific column
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = remove_glossary_terms(
                term_urns=["urn:li:glossaryTerm:Deprecated"],
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    return _batch_modify_glossary_terms(term_urns, entity_urns, column_paths, "remove")
