"""Tag tools: create_tag makes a tag; add_tags and remove_tags attach tags to
assets and columns, or take them off."""

import re
from typing import Optional

from fastmcp.exceptions import ToolError
from loguru import logger

from .. import graphql_helpers
from ..arg_validation import OptionalNullableItemStrList, StrList
from ..label_writes import LabelKind, batch_modify_labels
from ..read_backend import get_read_backend
from ..version_requirements import destructive, idempotent, min_version

TAG = LabelKind(
    noun="tag",
    urn_prefix="urn:li:tag:",
    param="tag_urns",
    input_key="tagUrns",
    gql_document="tags.gql",
    add_operation="batchAddTags",
    remove_operation="batchRemoveTags",
    find_hint=(
        'Find tags with search(query="<name>", filter="entity_type = tag"), or '
        "make one with create_tag."
    ),
)

# A tag's name becomes its URN id, where these characters delimit URN parts.
_RESERVED_NAME_CHARS = ("(", ")", ",")
_COLOR = re.compile(r"#?([0-9a-fA-F]{6}|[0-9a-fA-F]{3})")
# Name search results to compare: the same name differing only in case is near
# the top of a search for it.
_NAME_MATCH_CANDIDATES = 20


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def add_tags(
    tag_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
) -> dict:
    """Add one or more tags to multiple DataHub entities or their columns (e.g., schema fields).

    This tool allows you to tag multiple entities or their columns with multiple tags in a single operation.
    Useful for bulk tagging operations like marking multiple datasets as PII, deprecated, or applying
    governance classifications. The tags must already exist: find them with
    search(filter="entity_type = tag"), or create a missing one with create_tag
    and pass the urn it returns here.

    Args:
        tag_urns: List of tag URNs to add (e.g., ["urn:li:tag:PII", "urn:li:tag:Sensitive"])
        entity_urns: List of entity URNs to tag (e.g., dataset URNs, dashboard URNs)
        column_paths: Optional list of column_path identifiers (e.g., column names for schema fields).
                     Must be same length as entity_urns if provided.
                     Use None or empty string for entity-level tags.
                     For column-level tags, provide the column name (e.g., "email_address").
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the tags were added
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

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
    """
    return batch_modify_labels(
        TAG,
        tool="add_tags",
        label_urns=tag_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="add",
    )


@destructive
@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def remove_tags(
    tag_urns: StrList,
    entity_urns: StrList,
    column_paths: OptionalNullableItemStrList = None,
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
                     Name a nested column by its dotted path (e.g., "address.city"). The exact
                     fieldPath list_schema_fields returns (e.g., "[version=2.0].[type=string].email")
                     works too; a column name is resolved to it.

    Returns:
        Dictionary with:
        - success: True when the tags were removed
        - entities_updated: How many entities (or columns) the call covered
        - message: What was done

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
    """
    return batch_modify_labels(
        TAG,
        tool="remove_tags",
        label_urns=tag_urns,
        entity_urns=entity_urns,
        column_paths=column_paths,
        operation="remove",
    )


def _normalize_color(color: str) -> str:
    match = _COLOR.fullmatch(color.strip())
    if match is None:
        raise ToolError(
            f"create_tag: `color` must be a hex color such as #1890FF, got {color!r}."
        )
    return f"#{match.group(1).upper()}"


def _tag_with_name(name: str) -> Optional[str]:
    """The URN of a tag whose display name is ``name`` in any case, if search finds one.

    Best effort: a failed search must not stop the create, which then relies on
    the id check alone.
    """
    try:
        candidates = get_read_backend().tags_named(name, _NAME_MATCH_CANDIDATES)
    except Exception as e:
        logger.warning(f"create_tag: tag name search failed; creating by id only: {e}")
        return None
    wanted = name.casefold()
    for tag in candidates:
        properties = tag.get("properties")
        display = properties.get("name") if isinstance(properties, dict) else None
        if isinstance(display, str) and display.strip().casefold() == wanted:
            return str(tag["urn"])
    return None


def _existing(urn: str, *, matched: str, ignored: list[str]) -> dict:
    result: dict = {"urn": urn, "created": False, "matched": matched}
    if ignored:
        verb = "were" if len(ignored) > 1 else "was"
        result["note"] = (
            f"The tag already existed, so its {' and '.join(ignored)} {verb} "
            "not applied."
        )
    return result


@idempotent
@min_version(cloud="0.3.16", oss="1.4.0")
def create_tag(
    name: str,
    description: Optional[str] = None,
    color: Optional[str] = None,
) -> dict:
    """Create a tag, or return the existing tag with that name.

    Tags are applied to assets and columns with add_tags, which takes tag URNs.
    To tag something with a tag that may not exist yet: call create_tag(name=...),
    then pass the `urn` it returns to add_tags. Calling create_tag again with the
    same name is safe: it returns the same tag with created=false instead of
    making a second one.

    The name becomes the tag's id (urn:li:tag:<name>), so it may not contain
    "(", ")" or ",". An existing tag whose display name matches the name in any
    case is returned too (e.g. "pii" finds a tag named "PII").

    Args:
        name: The tag's name, e.g. "PII" or "tier-1".
        description: Optional description. Only applied when the tag is new.
        color: Optional hex color such as "#1890FF". Only applied when the tag
            is new.

    Returns:
        Dictionary with:
        - urn: The tag's URN, to pass to add_tags
        - created: True when this call made the tag, false when it already existed
        - matched: For an existing tag, "id" or "name" (which one matched)
        - note: When description or color were given for an existing tag
        - color_error: When the tag was created but its color could not be set
    """
    tag_name = name.strip()
    if not tag_name:
        raise ToolError("create_tag: `name` is required.")
    reserved = [c for c in _RESERVED_NAME_CHARS if c in tag_name]
    if reserved:
        raise ToolError(
            f"create_tag: `name` may not contain {' '.join(repr(c) for c in reserved)}: "
            f"it becomes the tag's URN id. Got {tag_name!r}."
        )
    color_hex = _normalize_color(color) if color and color.strip() else None
    ignored = [
        label
        for label, value in (("description", description), ("color", color_hex))
        if value
    ]
    urn = f"urn:li:tag:{tag_name}"

    if get_read_backend().exists(urn):
        return _existing(urn, matched="id", ignored=ignored)
    by_name = _tag_with_name(tag_name)
    if by_name is not None:
        return _existing(by_name, matched="name", ignored=ignored)

    graph = graphql_helpers.get_datahub_client()._graph
    tag_input: dict = {"id": tag_name, "name": tag_name}
    if description and description.strip():
        tag_input["description"] = description.strip()
    try:
        created = graphql_helpers.execute_write(
            graph,
            query=graphql_helpers.load_gql_document("tags.gql"),
            operation_name="createTag",
            variables={"input": tag_input},
            unknown_outcome_hint=(
                f"Call create_tag again with the same name: it returns {urn} with "
                "created=false if the tag was made."
            ),
        )
    except ToolError as e:
        # Made by someone else between the check and this write.
        if "already exists" in str(e):
            return _existing(urn, matched="id", ignored=ignored)
        raise
    created_urn = str(created.get("createTag") or urn)

    result: dict = {"urn": created_urn, "created": True}
    if color_hex is not None:
        # CreateTagInput has no color, so it is a second write. The tag exists
        # either way: a failure here is reported, not raised.
        try:
            colored = graphql_helpers.execute_write(
                graph,
                query=graphql_helpers.load_gql_document("tags.gql"),
                operation_name="setTagColor",
                variables={"urn": created_urn, "colorHex": color_hex},
                unknown_outcome_hint=f"Check the color of {created_urn} with get_entities.",
            )
        except ToolError as e:
            result["color_error"] = str(e)
        else:
            if not colored.get("setTagColor"):
                result["color_error"] = (
                    "DataHub did not set the color (returned false)."
                )
    return result
