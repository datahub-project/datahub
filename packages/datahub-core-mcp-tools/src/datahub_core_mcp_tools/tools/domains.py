"""Domain tools: create_domain makes a domain; set_domains puts assets in a
domain; remove_domains takes them out."""

import uuid
from typing import Optional

from fastmcp.exceptions import ToolError

from .. import arg_validation, graphql_helpers
from ..arg_validation import StrList
from ..label_writes import require_entity_urns
from ..read_backend import get_read_backend
from ..version_requirements import destructive, idempotent, min_version

DOMAIN_PREFIX = "urn:li:domain:"
_SEARCH_DOMAINS = (
    'Find domains with search(query="<name>", filter="entity_type = domain")'
)
_FIND_DOMAINS = f"{_SEARCH_DOMAINS}, or make one with create_domain"
# GMS's refusal of a second domain with the same name under the same parent.
_NAME_CONFLICT = "already exists in this domain"
_DOMAIN_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "urn:li:domain")
# Children of one parent to compare against the name: an exact match ranks near
# the top of a search for it.
_NAME_MATCH_CANDIDATES = 50


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

    The domain must already exist: find it with search(filter="entity_type = domain"),
    or make it with create_domain and pass the urn it returns here.

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


def _domain_urn_for(name: str, parent: Optional[str]) -> str:
    """The URN create_domain gives the domain ``name`` under ``parent``.

    The id is derived from the name and parent rather than random, so a repeated
    call finds the domain by a direct read. GMS's own check for a sibling with
    the same name runs on the search index, which lags writes: two quick calls
    with random ids would both pass it and make two domains. A known id also
    lets a create that gets no reply name the domain to look for.
    """
    key = f"{parent or ''}/{name}"
    return f"{DOMAIN_PREFIX}{uuid.uuid5(_DOMAIN_ID_NAMESPACE, key)}"


def _existing(urn: str, *, description_given: bool) -> dict:
    response: dict = {"urn": urn, "created": False}
    if description_given:
        response["note"] = (
            "The domain already existed, so its description was not applied."
        )
    return response


def _sibling_named(name: str, parent: Optional[str]) -> Optional[str]:
    """The URN of the domain called exactly ``name`` directly under ``parent``."""
    for domain in get_read_backend().domains_named(
        name, parent, _NAME_MATCH_CANDIDATES
    ):
        properties = domain.get("properties")
        display = properties.get("name") if isinstance(properties, dict) else None
        # GMS compares names exactly, so this does too.
        if display == name:
            return str(domain["urn"])
    return None


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def create_domain(
    name: str,
    description: Optional[str] = None,
    parent_domain: Optional[str] = None,
) -> dict:
    """Create a domain, or return the existing domain with that name under the same parent.

    Domains group assets by business area (e.g. "Marketing", "Finance"); assets
    are put in a domain with set_domains, which takes the domain URN. To put
    assets in a domain that may not exist yet: call create_domain(name=...), then
    pass the `urn` it returns to set_domains. Calling create_domain again with the
    same name and parent is safe: it returns the same domain with created=false
    instead of making a second one.

    Names are unique among a parent's direct children (or among the top-level
    domains), compared exactly: the same name under a different parent is a
    different domain.

    Args:
        name: The domain's display name, e.g. "Marketing".
        description: Optional description. Only applied when the domain is new.
        parent_domain: Optional URN of an existing domain to nest this one under
            (e.g. "urn:li:domain:marketing"). Leave it out for a top-level domain.

    Returns:
        Dictionary with:
        - urn: The domain's URN, to pass to set_domains
        - created: True when this call made the domain, false when it already existed
        - note: When a description was given for an existing domain
    """
    tool = "create_domain"
    domain_name = name.strip()
    if not domain_name:
        raise ToolError(f"{tool}: `name` is required.")
    parent = (
        require_domain(parent_domain, tool=tool, param="parent_domain")
        if parent_domain and parent_domain.strip()
        else None
    )
    description_text = (description or "").strip()
    urn = _domain_urn_for(domain_name, parent)
    properties = (get_read_backend().aspects(urn) or {}).get("domainProperties")
    if isinstance(properties, dict):
        if (
            properties.get("name") == domain_name
            and properties.get("parentDomain") == parent
        ):
            return _existing(urn, description_given=bool(description_text))
        # This tool made it, and it has since been renamed or moved: the name
        # is free here, so the new domain gets an id of its own.
        urn = f"{DOMAIN_PREFIX}{uuid.uuid4()}"

    domain_input: dict = {"id": urn[len(DOMAIN_PREFIX) :], "name": domain_name}
    if description_text:
        domain_input["description"] = description_text
    if parent is not None:
        domain_input["parentDomain"] = parent
    try:
        result = graphql_helpers.execute_write(
            graphql_helpers.get_datahub_client()._graph,
            query=graphql_helpers.load_gql_document("domains.gql"),
            operation_name="createDomain",
            variables={"input": domain_input},
            unknown_outcome_hint=(
                f"Call {tool} again with the same name and parent: it returns "
                f"{urn} with created=false if the domain was made."
            ),
        )
    except ToolError as e:
        if _NAME_CONFLICT not in str(e):
            raise
        # A domain with this name that this tool did not make, e.g. one made
        # in the UI.
        existing = _sibling_named(domain_name, parent)
        if existing is None:
            raise ToolError(
                f"{tool}: DataHub reports a domain named {domain_name!r} already "
                f"exists {'under ' + parent if parent else 'at the top level'}, but "
                f"it was not found. {_SEARCH_DOMAINS}."
            ) from e
        return _existing(existing, description_given=bool(description_text))
    return {"urn": str(result.get("createDomain") or urn), "created": True}
