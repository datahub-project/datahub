"""Adding labels (tags, glossary terms) to assets and columns, and removing them.

Tags and glossary terms are attached the same way: one batch mutation names the
labels and the resources, where a resource is an asset or, with a column path,
one of a dataset's columns (resolved to its schema fieldPath, see column_paths).
The add and remove tools of both share this code.
"""

import dataclasses
from typing import Dict, List, Literal, Optional

from datahub.metadata.urns import DatasetUrn, Urn
from fastmcp.exceptions import ToolError

from . import graphql_helpers
from .arg_validation import urn_error_message
from .column_paths import read_schema_field_paths, resolve_column_path
from .read_backend import get_read_backend


@dataclasses.dataclass(frozen=True)
class LabelKind:
    noun: str
    """How messages name one label, e.g. "tag"."""

    urn_prefix: str
    param: str
    """The tool parameter that takes the label URNs, e.g. "tag_urns"."""

    input_key: str
    """The batch input field for the label URNs, e.g. "tagUrns"."""

    gql_document: str
    add_operation: str
    remove_operation: str
    find_hint: str
    """How to find or create a label of this kind, for error messages."""


def _require_label_urns(kind: LabelKind, values: List[str], *, tool: str) -> List[str]:
    if not values:
        raise ToolError(f"{tool}: `{kind.param}` must name at least one {kind.noun}.")
    urns: List[str] = []
    problems: List[str] = []
    for index, value in enumerate(values):
        candidate = value.strip()
        if not candidate.startswith(kind.urn_prefix):
            problems.append(
                f"`{kind.param}[{index}]` must be a {kind.noun} URN "
                f"({kind.urn_prefix}...), got {value!r}."
            )
        else:
            urns.append(candidate)
    if problems:
        raise ToolError(f"{tool}: " + " ".join(problems) + f" {kind.find_hint}")
    return list(dict.fromkeys(urns))


def require_entity_urns(values: List[str], *, tool: str) -> List[str]:
    if not values:
        raise ToolError(f"{tool}: `entity_urns` must name at least one asset.")
    urns: List[str] = []
    problems: List[str] = []
    for index, value in enumerate(values):
        problem = urn_error_message(value, tool=tool, param=f"entity_urns[{index}]")
        if problem is None:
            urns.append(value.strip())
        else:
            problems.append(problem)
    # A write applies to all of its resources or none: one bad entry fails the
    # call instead of silently shrinking it.
    if problems:
        raise ToolError("\n".join(problems))
    return urns


def _resources(
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    *,
    tool: str,
) -> List[dict]:
    if column_paths is None:
        column_paths = [None] * len(entity_urns)
    elif len(column_paths) != len(entity_urns):
        raise ToolError(
            f"{tool}: `column_paths` has {len(column_paths)} entries but "
            f"`entity_urns` has {len(entity_urns)}; they pair one-for-one. Use null "
            "(or an empty string) for an entry that targets the whole asset."
        )
    resources: List[dict] = []
    field_paths: Dict[str, List[str]] = {}
    for urn, column_path in zip(entity_urns, column_paths, strict=True):
        resource: dict = {"resourceUrn": urn}
        column = (column_path or "").strip()
        if column:
            if not isinstance(Urn.from_string(urn), DatasetUrn):
                raise ToolError(
                    f"{tool}: column {column!r} is paired with {urn}, which is not a "
                    "dataset. Only datasets have columns; use null for other assets."
                )
            if urn not in field_paths:
                field_paths[urn] = read_schema_field_paths(urn, tool=tool)
            resource["subResource"] = resolve_column_path(
                field_paths[urn], column, urn=urn, tool=tool
            )
            resource["subResourceType"] = "DATASET_FIELD"
        resources.append(resource)
    return resources


def _require_existing_labels(kind: LabelKind, urns: List[str], *, tool: str) -> None:
    backend = get_read_backend()
    missing = [urn for urn in urns if not backend.exists(urn)]
    if missing:
        raise ToolError(
            f"{tool}: these {kind.noun}s do not exist in DataHub: "
            f"{', '.join(missing)}. {kind.find_hint}"
        )


def batch_modify_labels(
    kind: LabelKind,
    *,
    tool: str,
    label_urns: List[str],
    entity_urns: List[str],
    column_paths: Optional[List[Optional[str]]],
    operation: Literal["add", "remove"],
) -> dict:
    """Add ``label_urns`` to, or remove them from, every resource in one write."""
    labels = _require_label_urns(kind, label_urns, tool=tool)
    entities = require_entity_urns(entity_urns, tool=tool)
    resources = _resources(entities, column_paths, tool=tool)
    # Only adding needs the labels to exist. Removing a reference to a label that
    # was since deleted is how that dangling reference gets cleaned up, and GMS
    # allows it.
    if operation == "add":
        _require_existing_labels(kind, labels, tool=tool)

    if operation == "add":
        operation_name, no_op = kind.add_operation, f"adding a {kind.noun} twice"
    else:
        operation_name, no_op = (
            kind.remove_operation,
            f"removing a {kind.noun} that is gone",
        )
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document(kind.gql_document),
        operation_name=operation_name,
        variables={"input": {kind.input_key: labels, "resources": resources}},
        unknown_outcome_hint=(
            f"Check the assets with get_entities; calling {tool} again is also "
            f"safe, since {no_op} changes nothing."
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
            f"Successfully {verb} {len(labels)} {kind.noun}(s) {preposition} "
            f"{len(resources)} entit(ies){graphql_helpers.PER_CALL_SCOPE_NOTE}"
        ),
    }
