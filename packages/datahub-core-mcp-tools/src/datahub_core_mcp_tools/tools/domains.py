"""Domain tools: set_domains puts assets in a domain; remove_domains takes them out."""

from typing import Optional

from fastmcp.exceptions import ToolError

from .. import arg_validation, graphql_helpers
from ..arg_validation import StrList
from ..label_writes import require_entity_urns
from ..read_backend import get_read_backend
from ..version_requirements import destructive, idempotent, min_version

DOMAIN_PREFIX = "urn:li:domain:"
_FIND_DOMAINS = (
    'Find domains with search(query="<name>", filter="entity_type = domain")'
)


def require_domain(value: str, *, tool: str, param: str) -> str:
    """``value`` as the URN of an existing domain, or a ToolError."""
    problem = arg_validation.urn_error_message(
        value, tool=tool, param=param, example=f"{DOMAIN_PREFIX}marketing"
    )
    if problem is not None:
        raise ToolError(problem)
    urn = value.strip()
    if not urn.startswith(DOMAIN_PREFIX):
        raise ToolError(
            f"{tool}: `{param}` must be a domain URN ({DOMAIN_PREFIX}...), got "
            f"{urn!r}. {_FIND_DOMAINS}."
        )
    # GMS checks the domain too, but its refusal does not say which URN failed.
    if not get_read_backend().exists(urn):
        raise ToolError(
            f"{tool}: domain {urn} does not exist in DataHub. {_FIND_DOMAINS}."
        )
    return urn


def _batch_set_domain(
    domain_urn: Optional[str], entity_urns: list[str], *, tool: str
) -> int:
    resources = [{"resourceUrn": urn} for urn in entity_urns]
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("domains.gql"),
        operation_name="batchSetDomain",
        variables={"input": {"domainUrn": domain_urn, "resources": resources}},
        unknown_outcome_hint=(
            f"Check the assets' domain with get_entities; calling {tool} again is "
            "also safe, since it sets the same domain."
        ),
    )
    if not result.get("batchSetDomain"):
        raise ToolError(
            f"{tool}: DataHub did not apply the change (batchSetDomain returned false)."
        )
    return len(resources)


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def set_domains(
    domain_urn: str,
    entity_urns: StrList,
) -> dict:
    """Set domain for multiple DataHub entities.

    This tool allows you to assign a domain to multiple entities in a single operation.
    Useful for organizing datasets, dashboards, and other entities into logical business domains.

    Note: Domain assignment in DataHub is entity-level only. Each entity can belong to exactly one domain.
    Setting a new domain will replace any existing domain assignment.

    The domain must already exist: find it with search(filter="entity_type = domain").

    Args:
        domain_urn: Domain URN to assign (e.g., "urn:li:domain:marketing")
        entity_urns: List of entity URNs to assign to the domain (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: True when the domain was set
        - domain_urn: The domain assigned
        - entities_updated: How many entities the call covered
        - message: What was done

    Examples:
        # Set domain for multiple datasets
        set_domains(
            domain_urn="urn:li:domain:marketing",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.campaigns,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Set domain for mixed entity types
        set_domains(
            domain_urn="urn:li:domain:engineering",
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.logs,PROD)",
                "urn:li:dataFlow:(airflow,etl_pipeline,PROD)",
                "urn:li:dashboard:(superset,metrics)"
            ]
        )
    """
    tool = "set_domains"
    domain = require_domain(domain_urn, tool=tool, param="domain_urn")
    entities = require_entity_urns(entity_urns, tool=tool)
    count = _batch_set_domain(domain, entities, tool=tool)
    return {
        "success": True,
        "domain_urn": domain,
        "entities_updated": count,
        "message": (
            f"Successfully set domain for {count} entit(ies)"
            f"{graphql_helpers.PER_CALL_SCOPE_NOTE}"
        ),
    }


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_domains(
    entity_urns: StrList,
) -> dict:
    """Remove domain assignment from multiple DataHub entities.

    This tool allows you to unset the domain for multiple entities in a single operation.
    Useful for removing domain assignments when reorganizing entities or correcting misassignments.

    Args:
        entity_urns: List of entity URNs to remove domain from (e.g., dataset URNs, dashboard URNs)

    Returns:
        Dictionary with:
        - success: True when the domain was removed
        - entities_updated: How many entities the call covered
        - message: What was done

    Examples:
        # Remove domain from multiple datasets
        remove_domains(
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.old_table,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.deprecated,PROD)"
            ]
        )
    """
    tool = "remove_domains"
    entities = require_entity_urns(entity_urns, tool=tool)
    count = _batch_set_domain(None, entities, tool=tool)
    return {
        "success": True,
        "entities_updated": count,
        "message": (
            f"Successfully removed domain from {count} entit(ies)"
            f"{graphql_helpers.PER_CALL_SCOPE_NOTE}"
        ),
    }
