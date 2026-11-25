"""Owner tools: add_owners and remove_owners assign users and groups to assets as
owners, or take them off."""

from enum import Enum
from typing import Annotated, List, Literal, Optional

from fastmcp.exceptions import ToolError
from pydantic import WithJsonSchema

from .. import graphql_helpers
from ..arg_validation import StrList
from ..label_writes import require_entity_urns
from ..read_backend import get_read_backend
from ..version_requirements import destructive, idempotent, min_version

OWNERSHIP_TYPE_URN_PREFIX = "urn:li:ownershipType:"
_OWNER_ENTITY_TYPES = {
    "urn:li:corpuser:": "CORP_USER",
    "urn:li:corpGroup:": "CORP_GROUP",
}
_FIND_OWNERS = (
    'Find users and groups with search(query="<name>", filter="entity_type = '
    'corpuser") or filter="entity_type = corpGroup".'
)
# listOwnershipTypes matches loosely; an exact name is among the first results.
_OWNERSHIP_TYPE_CANDIDATES = 100


class OwnershipType(str, Enum):
    """Built-in ownership types in DataHub."""

    TECHNICAL_OWNER = "__system__technical_owner"
    BUSINESS_OWNER = "__system__business_owner"
    DATA_STEWARD = "__system__data_steward"

    def to_urn(self) -> str:
        return f"{OWNERSHIP_TYPE_URN_PREFIX}{self.value}"


# A plain string in the published schema. A union with the enum would publish
# anyOf [enum, string], which constrains nothing (any string is accepted) and
# which several OpenAI-compatible endpoints and Gemini reject outright, failing
# the whole tool list. The built-in names are in the tool descriptions.
OwnershipTypeArg = Annotated[str, WithJsonSchema({"type": "string"})]


def _resolve_ownership_type_urn(ownership_type: str, *, tool: str) -> str:
    """The URN of a built-in type (by name or value), a custom type by its name
    (case-insensitive, as shown in the UI), or an ownership type URN as given."""
    candidate = ownership_type.strip()
    if not candidate:
        raise ToolError(
            f"{tool}: `ownership_type` must name an ownership type, e.g. "
            "TECHNICAL_OWNER, BUSINESS_OWNER or DATA_STEWARD."
        )
    built_in_name = candidate.upper().replace(" ", "_")
    if built_in_name in OwnershipType.__members__:
        return OwnershipType[built_in_name].to_urn()
    if candidate in OwnershipType._value2member_map_:
        return OwnershipType(candidate).to_urn()
    if candidate.startswith(OWNERSHIP_TYPE_URN_PREFIX):
        # GMS checks that the type exists when owners are added.
        return candidate

    # Custom types have generated ids, so callers know them by name.
    wanted = candidate.casefold()
    matches: List[str] = []
    for found in get_read_backend().ownership_types_named(
        candidate, _OWNERSHIP_TYPE_CANDIDATES
    ):
        info = found.get("info")
        name = info.get("name") if isinstance(info, dict) else None
        if isinstance(name, str) and name.strip().casefold() == wanted:
            matches.append(str(found["urn"]))
    if not matches:
        raise ToolError(
            f"{tool}: ownership type {candidate!r} was not found in DataHub. Use "
            "TECHNICAL_OWNER, BUSINESS_OWNER or DATA_STEWARD, or the name of a "
            "custom ownership type that exists. Names are found by search, which "
            "can miss a type made in the last few seconds: pass its URN instead."
        )
    if len(matches) > 1:
        raise ToolError(
            f"{tool}: ownership type name {candidate!r} matches several types: "
            f"{', '.join(matches)}. Pass the URN of the one you mean."
        )
    return matches[0]


def _owner_entity_type(urn: str) -> Optional[str]:
    for prefix, entity_type in _OWNER_ENTITY_TYPES.items():
        if urn.startswith(prefix):
            return entity_type
    return None


def _require_owner_urns(values: List[str], *, tool: str, must_exist: bool) -> List[str]:
    if not values:
        raise ToolError(f"{tool}: `owner_urns` must name at least one user or group.")
    urns = list(dict.fromkeys(value.strip() for value in values))
    not_owners = [urn for urn in urns if _owner_entity_type(urn) is None]
    if not_owners:
        raise ToolError(
            f"{tool}: owners must be user (urn:li:corpuser:...) or group "
            f"(urn:li:corpGroup:...) URNs, got {', '.join(map(repr, not_owners))}. "
            f"{_FIND_OWNERS}"
        )
    if must_exist:
        backend = get_read_backend()
        missing = [urn for urn in urns if not backend.exists(urn)]
        if missing:
            raise ToolError(
                f"{tool}: these owners do not exist in DataHub: "
                f"{', '.join(missing)}. {_FIND_OWNERS}"
            )
    return urns


def _batch_modify_owners(
    *,
    tool: str,
    owner_urns: List[str],
    entity_urns: List[str],
    ownership_type: Optional[str],
    operation: Literal["add", "remove"],
) -> dict:
    # Only adding needs the owners to exist: removing a user who was since
    # deleted is how that dangling ownership gets cleaned up.
    owners = _require_owner_urns(owner_urns, tool=tool, must_exist=operation == "add")
    entities = require_entity_urns(entity_urns, tool=tool)
    if operation == "remove" and ownership_type is not None:
        # Clients often send "" for an omitted optional string; on remove that
        # means "any type", not a type named "".
        ownership_type = ownership_type.strip() or None
    type_urn = (
        _resolve_ownership_type_urn(ownership_type, tool=tool)
        if ownership_type is not None
        else None
    )
    resources = [{"resourceUrn": urn} for urn in entities]

    write_input: dict
    if operation == "add":
        operation_name = "batchAddOwners"
        write_input = {
            "owners": [
                {
                    "ownerUrn": urn,
                    "ownerEntityType": _owner_entity_type(urn),
                    "ownershipTypeUrn": type_urn,
                }
                for urn in owners
            ],
            "ownershipTypeUrn": type_urn,
            "resources": resources,
        }
        no_op = "adding an owner twice"
    else:
        operation_name = "batchRemoveOwners"
        write_input = {"ownerUrns": owners, "resources": resources}
        if type_urn is not None:
            write_input["ownershipTypeUrn"] = type_urn
        no_op = "removing an owner that is gone"

    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("owners.gql"),
        operation_name=operation_name,
        variables={"input": write_input},
        unknown_outcome_hint=(
            f"Check the assets' owners with get_entities; calling {tool} again is "
            f"also safe, since {no_op} changes nothing."
        ),
    )
    if not result.get(operation_name):
        raise ToolError(
            f"{tool}: DataHub did not apply the change ({operation_name} returned "
            "false)."
        )
    verb, preposition = ("added", "to") if operation == "add" else ("removed", "from")
    return {
        "success": True,
        "entities_updated": len(resources),
        "message": (
            f"Successfully {verb} {len(owners)} owner(s) {preposition} "
            f"{len(resources)} entit(ies){graphql_helpers.PER_CALL_SCOPE_NOTE}"
        ),
    }


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_owners(
    owner_urns: StrList,
    entity_urns: StrList,
    ownership_type: OwnershipTypeArg,
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
        ownership_type: The type of ownership to assign. Accepts either:
                       - A built-in ownership type:
                         - TECHNICAL_OWNER: Involved in production, maintenance, or distribution
                         - BUSINESS_OWNER: Principal stakeholders or domain experts
                         - DATA_STEWARD: Involved in governance
                       - A custom ownership type, passed by its user-facing name
                         (e.g. "Producer"). Custom types are looked up by name
                         (case-insensitive) and must already exist.

    Returns:
        Dictionary with:
        - success: True when the owners were added
        - entities_updated: How many entities the call covered
        - message: What was done

    Examples:
        # Add technical owners to multiple datasets
        add_owners(
            owner_urns=["urn:li:corpuser:john.doe", "urn:li:corpGroup:data-engineering"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ],
            ownership_type="TECHNICAL_OWNER"
        )

        # Add a data steward to a dataset and a dashboard
        add_owners(
            owner_urns=["urn:li:corpuser:data.steward"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.sales,PROD)",
                "urn:li:dashboard:(looker,sales_dashboard)"
            ],
            ownership_type="DATA_STEWARD"
        )
    """
    return _batch_modify_owners(
        tool="add_owners",
        owner_urns=owner_urns,
        entity_urns=entity_urns,
        ownership_type=ownership_type,
        operation="add",
    )


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_owners(
    owner_urns: StrList,
    entity_urns: StrList,
    ownership_type: Optional[OwnershipTypeArg] = None,
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
        ownership_type: Optional ownership type to specify which type of ownership to remove.
                       If not provided, will remove ownership regardless of type.
                       Accepts a built-in type (TECHNICAL_OWNER, BUSINESS_OWNER,
                       DATA_STEWARD) or a custom ownership type by its user-facing
                       name (e.g. "Producer"). Custom types are looked up by name
                       (case-insensitive) and must already exist in DataHub.

    Returns:
        Dictionary with:
        - success: True when the owners were removed
        - entities_updated: How many entities the call covered
        - message: What was done

    Examples:
        # Remove owners from multiple datasets (any ownership type)
        remove_owners(
            owner_urns=["urn:li:corpuser:former.employee", "urn:li:corpGroup:old-team"],
            entity_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.customers,PROD)"
            ]
        )

        # Remove only the technical ownership
        remove_owners(
            owner_urns=["urn:li:corpuser:john.doe"],
            entity_urns=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"],
            ownership_type="TECHNICAL_OWNER"
        )
    """
    return _batch_modify_owners(
        tool="remove_owners",
        owner_urns=owner_urns,
        entity_urns=entity_urns,
        ownership_type=ownership_type,
        operation="remove",
    )
