"""update_document: change part of an existing document without sending it whole.

save_document replaces the body on every update, so an agent would round-trip the
entire document for each edit. update_document takes a ``command``:

- ``update_content``: anchored ``{old_str, new_str}`` edits of the body
- ``replace_content``: replace the whole body (with a guard against discarding it)
- ``append_content``: add text to the end of the body
- ``update_properties``: change title, topics, links or status, not the body
- ``move``: put the document (and what is nested under it) under another document
"""

from typing import Callable, Dict, FrozenSet, List, Literal, Optional, Tuple, cast

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata import schema_classes as models
from datahub.sdk.document import Document
from fastmcp.exceptions import ToolError
from loguru import logger

# Pydantic builds the tool's JSON schema from these, and needs typing_extensions'
# TypedDict on Python < 3.12.
from typing_extensions import TypedDict

from .. import graphql_helpers
from ..document_edit_policy import (
    DocumentField,
    document_edit_refusal,
    ensure_document_editable,
)
from ..read_backend import get_read_backend
from ..version_requirements import destructive, min_version, pre_approval_validator
from .save_document import (
    CONFIGURATION_EDIT_ERROR,
    DOCUMENT_PREFIX,
    ROOT_PARENT_URN,
    gms_error_reason,
    in_shared_folder,
    is_configuration_document,
    require_document_urn,
    require_urns,
    restrict_updates_to_shared_folder,
    write_document,
)

TOOL = "update_document"

Command = Literal[
    "update_content", "replace_content", "append_content", "update_properties", "move"
]


class ContentEdit(TypedDict, total=False):
    """One search-and-replace of the document body."""

    old_str: str
    new_str: str
    replace_all_matches: bool


class DocumentProperties(TypedDict, total=False):
    """Fields that can change without the body."""

    title: str
    topics: List[str]
    related_documents: List[str]
    related_assets: List[str]
    status: Literal["PUBLISHED", "UNPUBLISHED"]


# Which arguments each command takes, so a call that mixes commands up fails
# before any network I/O with a message naming the right command.
_ALLOWED_ARGS: Dict[str, set] = {
    "update_content": {"content_edits"},
    "replace_content": {"new_content", "allow_deleting_content"},
    "append_content": {"new_content"},
    "update_properties": {"properties"},
    "move": {"parent_urn"},
}
_REQUIRED_ARGS: Dict[str, set] = {
    "update_content": {"content_edits"},
    "replace_content": {"new_content"},
    "append_content": {"new_content"},
    "update_properties": {"properties"},
    "move": {"parent_urn"},
}
_OWNING_COMMAND = {
    "content_edits": "use command=update_content",
    "new_content": "use command=replace_content or command=append_content",
    "properties": "use command=update_properties",
    "allow_deleting_content": "use command=replace_content",
    "parent_urn": "use command=move",
}
_ARG_EXAMPLES = {
    "content_edits": "content_edits=[{old_str, new_str}, ...]",
    "new_content": "new_content=...",
    "properties": "properties={...}",
    "parent_urn": "parent_urn='urn:li:document:...'",
}
_CONTENT_COMMANDS = frozenset({"update_content", "replace_content", "append_content"})


def _command_args_error(
    command: str,
    content_edits: Optional[List[ContentEdit]],
    new_content: Optional[str],
    properties: Optional[DocumentProperties],
    allow_deleting_content: bool,
    parent_urn: Optional[str],
) -> Optional[str]:
    """Why the arguments do not fit ``command`` (another command's argument, or
    a missing one), or None."""
    if command not in _ALLOWED_ARGS:
        return f"Unknown command '{command}'. Use one of: {', '.join(sorted(_ALLOWED_ARGS))}."
    # allow_deleting_content defaults to False, so only True counts as passed.
    passed = {
        "content_edits": content_edits is not None,
        "new_content": new_content is not None,
        "properties": properties is not None,
        "allow_deleting_content": allow_deleting_content,
        "parent_urn": parent_urn is not None,
    }
    foreign = sorted(
        n for n, was in passed.items() if was and n not in _ALLOWED_ARGS[command]
    )
    if foreign:
        hints = "; ".join(f"`{n}` → {_OWNING_COMMAND[n]}" for n in foreign)
        return (
            f"command={command} does not use these args: "
            f"{', '.join('`' + n + '`' for n in foreign)}. {hints}."
        )
    # An empty list or dict would be a no-op, almost certainly by mistake.
    usable = {
        "content_edits": bool(content_edits),
        "new_content": new_content is not None,
        "properties": bool(properties),
        "parent_urn": bool(parent_urn),
    }
    missing = sorted(n for n in _REQUIRED_ARGS[command] if not usable[n])
    if missing:
        return f"command={command} requires {', '.join(_ARG_EXAMPLES[n] for n in missing)}."
    return None


def _fields_changed(command: object, properties: object) -> FrozenSet[DocumentField]:
    """The document fields a call would change, for the host's edit policy."""
    if command in _CONTENT_COMMANDS:
        return frozenset({"content"})
    if command == "move":
        return frozenset({"parent"})
    if command == "update_properties" and isinstance(properties, dict):
        return frozenset(
            cast(DocumentField, key)
            for key, value in properties.items()
            if value is not None and key in DocumentProperties.__annotations__
        )
    return frozenset()


def _validate_update_document(args: Dict[str, object]) -> Optional[str]:
    """Pre-approval check: refuse a locked edit before a human approves it."""
    urn = args.get("urn")
    if not isinstance(urn, str) or not urn.startswith(DOCUMENT_PREFIX):
        return None
    return document_edit_refusal(
        urn, _fields_changed(args.get("command"), args.get("properties"))
    )


def _apply_content_edits(text: str, edits: List[ContentEdit]) -> str:
    """Apply anchored edits in order. Each ``old_str`` must match exactly once,
    unless ``replace_all_matches`` is set: an ambiguous anchor would garble the
    document."""
    result = text
    for index, edit in enumerate(edits):
        old_str = edit.get("old_str", "")
        new_str = edit.get("new_str")
        replace_all = bool(edit.get("replace_all_matches", False))
        if not old_str:
            raise ToolError(f"{TOOL}: edit #{index} has no old_str to anchor it.")
        # A missing new_str would delete old_str; deleting takes new_str="".
        if new_str is None:
            raise ToolError(
                f"{TOOL}: edit #{index} has no new_str. Pass the replacement text, "
                'or new_str="" to delete old_str.'
            )
        matches = result.count(old_str)
        if matches == 0:
            raise ToolError(
                f"{TOOL}: edit #{index}: old_str not found in the document. Read the "
                "current content with get_entities first; the anchor must match "
                "exactly."
            )
        if matches > 1 and not replace_all:
            raise ToolError(
                f"{TOOL}: edit #{index}: old_str matches {matches} places. Extend it "
                "to make it unique, or set replace_all_matches=true."
            )
        result = (
            result.replace(old_str, new_str)
            if replace_all
            else result.replace(old_str, new_str, 1)
        )
    return result


# One write of update_properties: the fields it changes (for the result) and
# the write itself.
_PropertyWrite = Tuple[List[str], Callable[[], None]]


def _property_writes(urn: str, properties: DocumentProperties) -> List[_PropertyWrite]:
    """The writes ``properties`` needs, checked before any of them runs.

    Each goes through a field-scoped write: GMS's document mutations rewrite
    documentInfo from its stored value, and topics are their own aspect. An
    upsert of the document as read would write back its body, parent and links
    too, reverting a concurrent edit of them.
    """
    hint = f"Check {urn} with get_entities; the same properties can be set again."
    writes: List[_PropertyWrite] = []

    def mutation(fields: List[str], operation: str, values: Dict[str, object]) -> None:
        writes.append(
            (
                fields,
                lambda: _document_mutation(
                    operation,
                    {"input": {"urn": urn, **values}},
                    failure=f"could not update the {', '.join(fields)} of '{urn}'",
                    unknown_outcome_hint=hint,
                ),
            )
        )

    title = properties.get("title")
    if title is not None:
        if not title.strip():
            raise ToolError(f"{TOOL}: properties.title cannot be empty.")
        mutation(["title"], "updateDocumentContents", {"title": title})

    topics = properties.get("topics")
    if topics is not None:
        tag_urns = dict.fromkeys(f"urn:li:tag:{t.strip()}" for t in topics if t.strip())
        tags = models.GlobalTagsClass(
            tags=[models.TagAssociationClass(tag=tag) for tag in tag_urns]
        )
        writes.append(
            (
                ["topics"],
                lambda: write_document(
                    lambda: graphql_helpers.get_datahub_client()._graph.emit_mcp(
                        MetadataChangeProposalWrapper(entityUrn=urn, aspect=tags)
                    ),
                    what=f"{TOOL}: updating the topics of '{urn}'",
                    unknown_outcome_hint=hint,
                ),
            )
        )

    related_fields: List[str] = []
    related: Dict[str, object] = {}
    related_documents = require_urns(
        properties.get("related_documents"),
        tool=TOOL,
        param="properties.related_documents",
        prefix=DOCUMENT_PREFIX,
    )
    if related_documents is not None:
        if urn in related_documents:
            raise ToolError(f"{TOOL}: a document cannot be related to itself.")
        related["relatedDocuments"] = related_documents
        related_fields.append("related_documents")
    related_assets = require_urns(
        properties.get("related_assets"), tool=TOOL, param="properties.related_assets"
    )
    if related_assets is not None:
        related["relatedAssets"] = related_assets
        related_fields.append("related_assets")
    if related:
        mutation(related_fields, "updateDocumentRelatedEntities", related)

    status = properties.get("status")
    if status is not None:
        if status not in ("PUBLISHED", "UNPUBLISHED"):
            raise ToolError(
                f"{TOOL}: status must be 'PUBLISHED' or 'UNPUBLISHED', got {status!r}."
            )
        mutation([f"status={status}"], "updateDocumentStatus", {"state": status})
    return writes


def _update_properties(urn: str, properties: DocumentProperties) -> List[str]:
    """Run the writes ``properties`` needs; return what changed."""
    writes = _property_writes(urn, properties)
    if not writes:
        raise ToolError(
            f"{TOOL}: properties has no recognized field. Supported: title, "
            "topics, related_documents, related_assets, status."
        )
    changed: List[str] = []
    for fields, write in writes:
        try:
            write()
        except ToolError as e:
            if not changed:
                raise
            raise ToolError(
                f"{e} Already applied before this failure: {', '.join(changed)}."
            ) from e
        changed.extend(fields)
    return changed


def _document_mutation(
    operation: str,
    variables: Dict[str, object],
    *,
    failure: str,
    unknown_outcome_hint: str,
) -> None:
    """Run one of GMS's document mutations, which return true when applied."""
    try:
        result = graphql_helpers.execute_write(
            graphql_helpers.get_datahub_client()._graph,
            query=graphql_helpers.load_gql_document("document_writes.gql"),
            operation_name=operation,
            variables=variables,
            unknown_outcome_hint=unknown_outcome_hint,
        )
    except ToolError as e:
        message = f"{TOOL}: {failure}: {e}"
        if "Unauthorized" in str(e) or "unauthorized" in str(e):
            message += " Do not retry: the user needs edit rights on the document."
        raise ToolError(message) from e
    if not result.get(operation):
        raise ToolError(f"{TOOL}: {failure} ({operation} returned false).")


def _move(urn: str, old_parent: Optional[str], parent_urn: str) -> dict:
    """Re-parent through GMS's moveDocument, which checks that the new parent
    exists and is neither the document nor one of its descendants (an upsert of
    documentInfo does not)."""
    _document_mutation(
        "moveDocument",
        {"input": {"urn": urn, "parentDocument": parent_urn}},
        failure=f"could not move '{urn}'",
        unknown_outcome_hint=(
            f"Check the parent of {urn} with get_entities; moving it again to the "
            "same parent is safe."
        ),
    )
    logger.info(f"update_document(move) {urn}: {old_parent} -> {parent_urn}")
    return {
        "success": True,
        "urn": urn,
        "command": "move",
        "changed": [f"parent ({old_parent or 'none'} -> {parent_urn})"],
        "message": f"Moved document under {parent_urn}",
    }


def _new_body(
    command: str,
    existing_text: str,
    content_edits: Optional[List[ContentEdit]],
    new_content: Optional[str],
    allow_deleting_content: bool,
) -> Tuple[str, str]:
    """The body a content command leaves, and how it changed."""
    if command == "update_content":
        assert content_edits is not None
        text = _apply_content_edits(existing_text, content_edits)
        return text, f"content ({len(text) - len(existing_text):+d} chars)"
    assert new_content is not None
    if command == "replace_content":
        if existing_text and not allow_deleting_content:
            raise ToolError(
                f"{TOOL}: replace_content would discard the current body "
                f"({len(existing_text)} chars). Pass allow_deleting_content=true to "
                "confirm, or use update_content / append_content instead."
            )
        return new_content, f"content (replaced, {len(new_content)} chars)"
    text = existing_text + new_content
    return text, (f"content (appended +{len(new_content)} chars, total {len(text)})")


@destructive
@pre_approval_validator(_validate_update_document)
@min_version(cloud="0.3.16", oss="1.4.0")
def update_document(
    urn: str,
    command: Command,
    content_edits: Optional[List[ContentEdit]] = None,
    new_content: Optional[str] = None,
    properties: Optional[DocumentProperties] = None,
    allow_deleting_content: bool = False,
    parent_urn: Optional[str] = None,
) -> dict:
    """Partial update of an existing Document — send the diff, not the whole body.

    Use this for any edit to an existing document; ``save_document`` is for
    creation and full-replace only.

    Commands:

    - ``update_content`` — apply one or more ``{old_str, new_str,
      replace_all_matches?}`` edits to the body. Fetch the doc first
      (``get_entities``) so ``old_str`` matches exactly. Multiple matches
      fail unless ``replace_all_matches=true``.
    - ``replace_content`` — overwrite the body with ``new_content``. Requires
      ``allow_deleting_content=true`` if the doc currently has content.
    - ``append_content`` — concatenate ``new_content`` to the end of the body.
    - ``update_properties`` — change ``title``, ``topics``, ``related_documents``,
      ``related_assets``, or ``status`` (``"PUBLISHED"`` / ``"UNPUBLISHED"``)
      without touching the body.
    - ``move`` — move the document, with everything nested under it, into
      the document (folder) ``parent_urn``. Find folder URNs with
      ``search_documents``. A document cannot move under itself or one of
      its own descendants.

    Every command but ``move`` keeps the document where it is: its parent,
    owners and visibility are left as they are.

    Works on any document the user can edit. Confirm with the user before
    ``replace_content`` or publishing.

    Invalid arguments, a missing document, or a failed write fail the call with
    a tool error that says what to fix.

    Examples:

        update_document(
            urn="urn:li:document:my-doc",
            command="update_content",
            content_edits=[{
                "old_str": "## ARR\\nARR is defined as ...",
                "new_str": "## ARR\\nARR = MRR × 12. Excludes one-time fees ...",
            }],
        )

        update_document(
            urn="urn:li:document:my-doc",
            command="update_properties",
            properties={"status": "PUBLISHED"},
        )

        update_document(
            urn="urn:li:document:my-doc",
            command="move",
            parent_urn="urn:li:document:team-runbooks",
        )
    """
    document_urn = require_document_urn(urn, tool=TOOL, param="urn")
    args_error = _command_args_error(
        command,
        content_edits,
        new_content,
        properties,
        allow_deleting_content,
        parent_urn,
    )
    if args_error is not None:
        raise ToolError(f"{TOOL}: {args_error}")
    if parent_urn is not None:
        parent_urn = require_document_urn(parent_urn, tool=TOOL, param="parent_urn")
    if is_configuration_document(document_urn):
        raise ToolError(CONFIGURATION_EDIT_ERROR)

    # The same opt-in guardrail as save_document. A move must also stay inside
    # the folder, or it would take the document out of the guardrail's reach.
    if restrict_updates_to_shared_folder():
        refusal = in_shared_folder(document_urn)
        if refusal is not None:
            raise ToolError(refusal)
        if parent_urn is not None and parent_urn != ROOT_PARENT_URN:
            if in_shared_folder(parent_urn) is not None:
                raise ToolError(
                    f"Cannot move '{document_urn}' under '{parent_urn}': documents can "
                    "only be moved within the shared documents folder."
                )

    client = graphql_helpers.get_datahub_client()
    try:
        stored = get_read_backend().exists(document_urn)
        doc = client.entities.get(document_urn) if stored else None
    except Exception as e:
        raise ToolError(
            f"{TOOL}: could not read '{document_urn}', so nothing was changed: "
            f"{gms_error_reason(e)}"
        ) from e
    if doc is None:
        raise ToolError(
            f"{TOOL}: document '{document_urn}' not found. Use save_document to create "
            "new documents."
        )
    if not isinstance(doc, Document):
        raise ToolError(f"{TOOL}: '{document_urn}' is not a document.")
    if is_configuration_document(document_urn, doc):
        raise ToolError(CONFIGURATION_EDIT_ERROR)
    ensure_document_editable(doc, _fields_changed(command, properties))

    if command == "move":
        assert parent_urn is not None
        return _move(document_urn, doc.parent_document, parent_urn)

    if command in _CONTENT_COMMANDS:
        text, change = _new_body(
            command,
            doc.text or "",
            content_edits,
            new_content,
            allow_deleting_content,
        )
        # Through GMS's updateDocumentContents, not a whole-entity upsert: GMS then
        # keeps what follows the body in step (semanticText, which the embedding
        # pipeline prefers over the body, and documentInfo.lastModified), and
        # nothing else the document holds is written back as it was read.
        hint = f"Check {document_urn} with get_entities before calling {TOOL} again"
        hint += (
            ": appending again would add the text twice."
            if command == "append_content"
            else "."
        )
        _document_mutation(
            "updateDocumentContents",
            {"input": {"urn": document_urn, "contents": {"text": text}}},
            failure=f"could not update the content of '{document_urn}'",
            unknown_outcome_hint=hint,
        )
        logger.info(f"update_document({command}) {document_urn}: {change}")
        return {
            "success": True,
            "urn": document_urn,
            "command": command,
            "changed": [change],
            "message": f"Updated document via {command}: {change}",
        }

    assert properties is not None
    changed = _update_properties(document_urn, properties)
    logger.info(f"update_document({command}) {document_urn}: {changed}")
    return {
        "success": True,
        "urn": document_urn,
        "command": command,
        "changed": changed,
        "message": f"Updated document via {command}: {', '.join(changed)}",
    }
