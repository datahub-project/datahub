"""Entity edits for the `datahub edit` CLI: what the UI changes on an entity page.

Every edit is an :class:`EditOp` that :func:`apply_op` sends to DataHub as one
GraphQL mutation (the same mutations the web UI uses). The ``plan_*`` functions
read the entity first and return the operation together with the operation
that reverts it, so edits can be undone. An edit that would change nothing
returns ``None`` and is never sent.
"""

import dataclasses
import json
import sys
from enum import Enum
from typing import Any, Dict, List, Optional

import click

from datahub.emitter.mce_builder import (
    make_domain_urn,
    make_group_urn,
    make_tag_urn,
    make_term_urn,
    make_user_urn,
)
from datahub.ingestion.graph.client import DataHubGraph

EXIT_GENERAL = 1
EXIT_NOT_FOUND = 3


class EditCliError(click.ClickException):
    """Structured error for the edit and catalog commands.

    When stderr is not a TTY (agent/pipe context), it is printed as JSON:
      {"error": "<error_type>", "message": "...", "suggestion": "..."}
    """

    def __init__(
        self,
        message: str,
        error_type: str = "edit_error",
        suggestion: Optional[str] = None,
        exit_code: int = EXIT_GENERAL,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.suggestion = suggestion
        self.exit_code = exit_code

    def show(self, file: Any = None) -> None:
        if sys.stderr.isatty():
            super().show(file=file)
            return
        err: Dict[str, str] = {"error": self.error_type, "message": self.message}
        if self.suggestion:
            err["suggestion"] = self.suggestion
        click.echo(json.dumps(err), err=True)


class EditKind(str, Enum):
    ADD_TAG = "add_tag"
    REMOVE_TAG = "remove_tag"
    ADD_TERM = "add_term"
    REMOVE_TERM = "remove_term"
    ADD_OWNER = "add_owner"
    REMOVE_OWNER = "remove_owner"
    SET_DOMAIN = "set_domain"
    UNSET_DOMAIN = "unset_domain"
    SET_DESCRIPTION = "set_description"
    SET_DEPRECATION = "set_deprecation"


@dataclasses.dataclass(frozen=True)
class EditOp:
    """One edit to one entity. Only the fields its ``kind`` needs are set."""

    kind: EditKind
    urn: str
    tag: Optional[str] = None
    term: Optional[str] = None
    owner: Optional[str] = None
    ownership_type: Optional[str] = None
    domain: Optional[str] = None
    description: Optional[str] = None
    deprecated: Optional[bool] = None
    note: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {"kind": self.kind.value, "urn": self.urn}
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.name not in ("kind", "urn") and value is not None:
                out[field.name] = value
        return out

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EditOp":
        return cls(**{**data, "kind": EditKind(data["kind"])})


@dataclasses.dataclass(frozen=True)
class EditPlan:
    forward: EditOp
    inverse: EditOp


@dataclasses.dataclass(frozen=True)
class Owner:
    owner: str
    ownership_type: Optional[str]


@dataclasses.dataclass(frozen=True)
class EntitySummary:
    """The fields the UI shows at the top of an entity page."""

    urn: str
    type: str
    name: Optional[str]
    platform: Optional[str]
    description: Optional[str]
    editable_description: Optional[str]
    tags: List[str]
    terms: List[str]
    owners: List[Owner]
    domain: Optional[str]
    deprecated: bool
    deprecation_note: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


# --- reading ---------------------------------------------------------------------

# Entity types that carry the editable metadata shown on an entity page.
_EDITABLE_TYPES = (
    "Dataset",
    "Dashboard",
    "Chart",
    "DataJob",
    "DataFlow",
    "Container",
    "MLModel",
    "MLModelGroup",
    "MLFeatureTable",
)

_EDITABLE_FIELDS = """
      tags { tags { tag { urn } } }
      glossaryTerms { terms { term { urn } } }
      ownership {
        owners {
          owner { ... on CorpUser { urn } ... on CorpGroup { urn } }
          ownershipType { urn }
        }
      }
      domain { domain { urn } }
      deprecation { deprecated note }
"""

_NAMED_TYPES = (
    "Dashboard",
    "Chart",
    "DataJob",
    "DataFlow",
    "Container",
)

ENTITY_SUMMARY_QUERY = (
    "query getEntitySummary($urn: String!) {\n  entity(urn: $urn) {\n    urn\n    type\n"
    "    ... on Dataset { name platform { urn } properties { name description }"
    " editableProperties { description } }\n"
    + "".join(
        f"    ... on {t} {{ properties {{ name description }}"
        " editableProperties { description } }\n"
        for t in _NAMED_TYPES
    )
    + "    ... on MLModel { name properties { description }"
    " editableProperties { description } }\n"
    + "    ... on MLModelGroup { name properties { description }"
    " editableProperties { description } }\n"
    + "    ... on MLFeatureTable { name properties { description }"
    " editableProperties { description } }\n"
    + "".join(f"    ... on {t} {{{_EDITABLE_FIELDS}    }}\n" for t in _EDITABLE_TYPES)
    + "  }\n}\n"
)


def _nested(data: Optional[Dict[str, Any]], *path: str) -> Any:
    for key in path:
        if not isinstance(data, dict):
            return None
        data = data.get(key)
    return data


def require_exists(graph: DataHubGraph, urn: str, create_hint: str) -> None:
    """Fail before any edit when a referenced tag, term, owner or domain is missing.

    DataHub rejects associations to entities that do not exist; checking up
    front avoids failing halfway through a multi-entity edit.
    """
    if not graph.exists(urn):
        raise EditCliError(
            f"{urn} does not exist",
            error_type="not_found",
            suggestion=create_hint,
            exit_code=EXIT_NOT_FOUND,
        )


def get_entity_summary(graph: DataHubGraph, urn: str) -> EntitySummary:
    if not graph.exists(urn):
        raise EditCliError(
            f"Entity not found: {urn}",
            error_type="not_found",
            suggestion="Check the URN with `datahub search` or `datahub exists`.",
            exit_code=EXIT_NOT_FOUND,
        )
    raw = graph.execute_graphql(ENTITY_SUMMARY_QUERY, variables={"urn": urn})["entity"]
    if not raw:
        raise EditCliError(
            f"Entity not found: {urn}", error_type="not_found", exit_code=EXIT_NOT_FOUND
        )
    owners = _nested(raw, "ownership", "owners") or []
    return EntitySummary(
        urn=raw["urn"],
        type=raw["type"],
        name=_nested(raw, "properties", "name") or raw.get("name"),
        platform=_nested(raw, "platform", "urn"),
        description=_nested(raw, "editableProperties", "description")
        or _nested(raw, "properties", "description"),
        # Undoing a description writes "" back (GraphQL cannot delete the
        # editable description); the UI then falls back to the source one.
        editable_description=_nested(raw, "editableProperties", "description") or None,
        tags=sorted(t["tag"]["urn"] for t in _nested(raw, "tags", "tags") or []),
        terms=sorted(
            t["term"]["urn"] for t in _nested(raw, "glossaryTerms", "terms") or []
        ),
        owners=sorted(
            (
                Owner(
                    owner=o["owner"]["urn"],
                    ownership_type=_nested(o, "ownershipType", "urn"),
                )
                for o in owners
                if _nested(o, "owner", "urn")
            ),
            key=lambda o: (o.owner, o.ownership_type or ""),
        ),
        domain=_nested(raw, "domain", "domain", "urn"),
        deprecated=bool(_nested(raw, "deprecation", "deprecated")),
        deprecation_note=_nested(raw, "deprecation", "note") or None,
    )


# --- applying --------------------------------------------------------------------


def _owner_entity_type(owner_urn: str) -> str:
    return "CORP_GROUP" if owner_urn.startswith("urn:li:corpGroup:") else "CORP_USER"


def _mutation_for(op: EditOp) -> Dict[str, Any]:
    """Return {"query": ..., "variables": ...} for one operation."""
    kind = op.kind
    if kind in (EditKind.ADD_TAG, EditKind.REMOVE_TAG):
        field = "addTag" if kind == EditKind.ADD_TAG else "removeTag"
        return {
            "query": f"mutation m($input: TagAssociationInput!) {{ {field}(input: $input) }}",
            "variables": {"input": {"tagUrn": op.tag, "resourceUrn": op.urn}},
        }
    if kind in (EditKind.ADD_TERM, EditKind.REMOVE_TERM):
        field = "addTerm" if kind == EditKind.ADD_TERM else "removeTerm"
        return {
            "query": f"mutation m($input: TermAssociationInput!) {{ {field}(input: $input) }}",
            "variables": {"input": {"termUrn": op.term, "resourceUrn": op.urn}},
        }
    if kind == EditKind.ADD_OWNER:
        assert op.owner is not None
        return {
            "query": "mutation m($input: AddOwnerInput!) { addOwner(input: $input) }",
            "variables": {
                "input": {
                    "ownerUrn": op.owner,
                    "ownerEntityType": _owner_entity_type(op.owner),
                    "ownershipTypeUrn": op.ownership_type,
                    "resourceUrn": op.urn,
                }
            },
        }
    if kind == EditKind.REMOVE_OWNER:
        return {
            "query": "mutation m($input: RemoveOwnerInput!) { removeOwner(input: $input) }",
            "variables": {
                "input": {
                    "ownerUrn": op.owner,
                    "ownershipTypeUrn": op.ownership_type,
                    "resourceUrn": op.urn,
                }
            },
        }
    if kind == EditKind.SET_DOMAIN:
        return {
            "query": "mutation m($e: String!, $d: String!) "
            "{ setDomain(entityUrn: $e, domainUrn: $d) }",
            "variables": {"e": op.urn, "d": op.domain},
        }
    if kind == EditKind.UNSET_DOMAIN:
        return {
            "query": "mutation m($e: String!) { unsetDomain(entityUrn: $e) }",
            "variables": {"e": op.urn},
        }
    if kind == EditKind.SET_DESCRIPTION:
        return {
            "query": "mutation m($input: DescriptionUpdateInput!) "
            "{ updateDescription(input: $input) }",
            "variables": {
                "input": {"description": op.description, "resourceUrn": op.urn}
            },
        }
    if kind == EditKind.SET_DEPRECATION:
        return {
            "query": "mutation m($input: UpdateDeprecationInput!) "
            "{ updateDeprecation(input: $input) }",
            "variables": {
                "input": {
                    "urn": op.urn,
                    "deprecated": bool(op.deprecated),
                    "note": op.note or "",
                }
            },
        }
    raise AssertionError(f"Unhandled edit kind {kind}")


def apply_op(graph: DataHubGraph, op: EditOp) -> None:
    mutation = _mutation_for(op)
    data = graph.execute_graphql(mutation["query"], variables=mutation["variables"])
    # Every mutation used here returns a single Boolean.
    if not any(data.values()):
        raise EditCliError(
            f"DataHub did not apply {op.kind.value} on {op.urn}",
            error_type="not_applied",
        )


# --- planning --------------------------------------------------------------------

SYSTEM_OWNERSHIP_TYPES = {
    "technical": "urn:li:ownershipType:__system__technical_owner",
    "business": "urn:li:ownershipType:__system__business_owner",
    "data_steward": "urn:li:ownershipType:__system__data_steward",
    "none": "urn:li:ownershipType:__system__none",
}


def ownership_type_urn(value: str) -> str:
    if value.startswith("urn:li:ownershipType:"):
        return value
    try:
        return SYSTEM_OWNERSHIP_TYPES[value]
    except KeyError:
        raise EditCliError(
            f"Unknown ownership type {value!r}",
            error_type="usage",
            suggestion=f"Use one of {sorted(SYSTEM_OWNERSHIP_TYPES)} "
            "or an urn:li:ownershipType URN.",
        ) from None


def owner_urn(value: str) -> str:
    """``alice`` is a user, ``group:eng`` a group; full URNs pass through."""
    if value.startswith("group:"):
        return make_group_urn(value[len("group:") :])
    return make_user_urn(value)


def plan_tag(graph: DataHubGraph, urn: str, tag: str, add: bool) -> Optional[EditPlan]:
    tag_urn = make_tag_urn(tag)
    if (tag_urn in get_entity_summary(graph, urn).tags) == add:
        return None
    adding = EditOp(EditKind.ADD_TAG, urn, tag=tag_urn)
    removing = EditOp(EditKind.REMOVE_TAG, urn, tag=tag_urn)
    return EditPlan(adding, removing) if add else EditPlan(removing, adding)


def plan_term(
    graph: DataHubGraph, urn: str, term: str, add: bool
) -> Optional[EditPlan]:
    term_urn = make_term_urn(term)
    if (term_urn in get_entity_summary(graph, urn).terms) == add:
        return None
    adding = EditOp(EditKind.ADD_TERM, urn, term=term_urn)
    removing = EditOp(EditKind.REMOVE_TERM, urn, term=term_urn)
    return EditPlan(adding, removing) if add else EditPlan(removing, adding)


def plan_owner(
    graph: DataHubGraph, urn: str, owner: str, ownership_type: str, add: bool
) -> Optional[EditPlan]:
    target = Owner(owner_urn(owner), ownership_type_urn(ownership_type))
    if (target in get_entity_summary(graph, urn).owners) == add:
        return None
    adding = EditOp(
        EditKind.ADD_OWNER,
        urn,
        owner=target.owner,
        ownership_type=target.ownership_type,
    )
    removing = dataclasses.replace(adding, kind=EditKind.REMOVE_OWNER)
    return EditPlan(adding, removing) if add else EditPlan(removing, adding)


def _domain_op(urn: str, domain: Optional[str]) -> EditOp:
    if domain:
        return EditOp(EditKind.SET_DOMAIN, urn, domain=domain)
    return EditOp(EditKind.UNSET_DOMAIN, urn)


def plan_domain(
    graph: DataHubGraph, urn: str, domain: Optional[str]
) -> Optional[EditPlan]:
    new = make_domain_urn(domain) if domain else None
    old = get_entity_summary(graph, urn).domain
    if new == old:
        return None
    return EditPlan(_domain_op(urn, new), _domain_op(urn, old))


def plan_description(
    graph: DataHubGraph, urn: str, description: str
) -> Optional[EditPlan]:
    old = get_entity_summary(graph, urn).editable_description or ""
    if description == old:
        return None
    return EditPlan(
        EditOp(EditKind.SET_DESCRIPTION, urn, description=description),
        EditOp(EditKind.SET_DESCRIPTION, urn, description=old),
    )


def plan_deprecation(
    graph: DataHubGraph, urn: str, deprecated: bool, note: str = ""
) -> Optional[EditPlan]:
    summary = get_entity_summary(graph, urn)
    old_note = summary.deprecation_note or ""
    new_note = note if deprecated else ""
    if summary.deprecated == deprecated and (not deprecated or old_note == new_note):
        return None
    return EditPlan(
        EditOp(EditKind.SET_DEPRECATION, urn, deprecated=deprecated, note=new_note),
        EditOp(
            EditKind.SET_DEPRECATION,
            urn,
            deprecated=summary.deprecated,
            note=old_note,
        ),
    )
