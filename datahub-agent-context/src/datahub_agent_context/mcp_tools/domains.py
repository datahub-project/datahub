"""Domain management tools for DataHub MCP server."""

import logging
from typing import List

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import execute_graphql

logger = logging.getLogger(__name__)


def _validate_domain_urn(domain_urn: str) -> None:
    """
    Validate that the domain URN exists in DataHub.

    Raises:
        ValueError: If the domain URN does not exist or is invalid
    """
    graph = get_graph()
    query = """
        query getDomain($urn: String!) {
            entity(urn: $urn) {
                urn
                type
                ... on Domain {
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
            variables={"urn": domain_urn},
            operation_name="getDomain",
        )

        entity = result.get("entity")

        if entity is None:
            raise ValueError(
                f"Domain URN does not exist in DataHub: {domain_urn}. "
                f"Please use the search tool with entity_type filter to find existing domains, "
                f"or create the domain first before assigning it."
            )

        if entity.get("type") != "DOMAIN":
            raise ValueError(
                f"The URN is not a domain entity: {domain_urn} (type: {entity.get('type')})"
            )

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        raise ValueError(f"Failed to validate domain URN: {str(e)}") from e


def set_domains(
    domain_urn: str,
    entity_urns: List[str],
) -> dict:
    """Set domain for multiple DataHub entities.

    This tool allows you to assign a domain to multiple entities in a single operation.
    Useful for organizing datasets, dashboards, and other entities into logical business domains.

    Note: Domain assignment in DataHub is entity-level only. Each entity can belong to exactly one domain.
    Setting a new domain will replace any existing domain assignment.

    Args:
        domain_urn: Domain URN to assign (e.g., "urn:li:domain:marketing")
        entity_urns: List of entity URNs to assign to the domain (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Set domain for multiple datasets
        set_domains(
            domain_urn="urn:li:domain:marketing",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.campaigns,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Set domain for dashboards
        set_domains(
            domain_urn="urn:li:domain:finance",
            entity_urns=[
                "urn:li:dashboard:(urn:li:dataPlatform:looker,revenue_dashboard,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,expense_dashboard,PROD)"
            ]
        )

        # Set domain for mixed entity types
        set_domains(
            domain_urn="urn:li:domain:engineering",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.logs,PROD)",
                "urn:li:dataFlow:(urn:li:dataPlatform:airflow,etl_pipeline,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:superset,metrics,PROD)"
            ]
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = set_domains(
                domain_urn="urn:li:domain:marketing",
                entity_urns=["urn:li:dataset:(...)"]
            )
    """
    graph = get_graph()
    if not domain_urn:
        raise ValueError("domain_urn cannot be empty")
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    _validate_domain_urn(domain_urn)

    resources = []
    for resource_urn in entity_urns:
        resources.append({"resourceUrn": resource_urn})

    mutation = """
        mutation batchSetDomain($input: BatchSetDomainInput!) {
            batchSetDomain(input: $input)
        }
    """

    variables = {"input": {"domainUrn": domain_urn, "resources": resources}}

    try:
        result = execute_graphql(
            graph,
            query=mutation,
            variables=variables,
            operation_name="batchSetDomain",
        )

        if result.get("batchSetDomain", False):
            return {
                "success": True,
                "message": f"Successfully set domain for {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError("Failed to set domain - operation returned false")

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error setting domain: {str(e)}") from e


def remove_domains(
    entity_urns: List[str],
) -> dict:
    """Remove domain assignment from multiple DataHub entities.

    This tool allows you to unset the domain for multiple entities in a single operation.
    Useful for removing domain assignments when reorganizing entities or correcting misassignments.

    Args:
        entity_urns: List of entity URNs to remove domain from (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - message: Success or error message

    Examples:
        # Remove domain from multiple datasets
        remove_domains(
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.deprecated,PROD)"
            ]
        )

        # Remove domain from dashboards
        remove_domains(
            entity_urns=[
                "urn:li:dashboard:(urn:li:dataPlatform:looker,old_dashboard,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:looker,temp_dashboard,PROD)"
            ]
        )

        # Remove domain from mixed entity types
        remove_domains(
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.temp,PROD)",
                "urn:li:dataFlow:(urn:li:dataPlatform:airflow,old_pipeline,PROD)",
                "urn:li:dashboard:(urn:li:dataPlatform:superset,test,PROD)"
            ]
        )

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client.graph):
            result = remove_domains(entity_urns=["urn:li:dataset:(...)"])
    """
    graph = get_graph()
    if not entity_urns:
        raise ValueError("entity_urns cannot be empty")

    resources = []
    for resource_urn in entity_urns:
        resources.append({"resourceUrn": resource_urn})

    mutation = """
        mutation batchSetDomain($input: BatchSetDomainInput!) {
            batchSetDomain(input: $input)
        }
    """

    variables = {"input": {"domainUrn": None, "resources": resources}}

    try:
        result = execute_graphql(
            graph,
            query=mutation,
            variables=variables,
            operation_name="batchSetDomain",
        )

        if result.get("batchSetDomain", False):
            return {
                "success": True,
                "message": f"Successfully removed domain from {len(entity_urns)} entit(ies)",
            }
        else:
            raise RuntimeError("Failed to remove domain - operation returned false")

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error removing domain: {str(e)}") from e
