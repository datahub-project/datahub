"""save_document: save a standalone document (an insight, decision, FAQ, ...) to
DataHub's knowledge base, or replace one in place.

New documents go under ``parent_urn`` when given, else under a shared folder the
tool keeps (optionally with a subfolder per author). Configuration:

- SAVE_DOCUMENT_TOOL_ENABLED: "false" leaves the document tools out (default on;
  TOOLS_IS_MUTATION_ENABLED=false also leaves them out).
- SAVE_DOCUMENT_PARENT_TITLE: title of the shared folder (default "Shared").
- SAVE_DOCUMENT_ORGANIZE_BY_USER: "true" groups new documents by author.
- SAVE_DOCUMENT_RESTRICT_UPDATES: "true" allows updates only inside the shared
  folder (default off: any document the caller may edit, which GMS checks).
"""

import os
import re
import uuid
from datetime import datetime
from typing import Callable, Dict, FrozenSet, List, Literal, Optional, cast

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.configuration.common import GraphError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import DocumentUrn, Urn
from datahub.sdk.document import Document
from datahub.sdk.main_client import DataHubClient
from fastmcp.exceptions import ToolError
from loguru import logger

from .. import graphql_helpers
from ..arg_validation import OptionalStrList
from ..document_edit_policy import (
    DocumentField,
    document_edit_refusal,
    ensure_document_editable,
)
from ..read_backend import JsonObject, get_read_backend
from ..result_policy import POLICY_DOCUMENT_SUBTYPE, is_configuration
from ..version_requirements import destructive, min_version, pre_approval_validator

DOCUMENT_PREFIX = "urn:li:document:"
# The shared folder's id is fixed, independent of its title, so the title can
# change without a migration.
ROOT_PARENT_DOC_ID = "__system_shared_documents"
ROOT_PARENT_URN = f"{DOCUMENT_PREFIX}{ROOT_PARENT_DOC_ID}"

DocumentType = Literal[
    "Insight",
    "Decision",
    "FAQ",
    "Analysis",
    "Summary",
    "Recommendation",
    "Note",
    "Context",
]

CONFIGURATION_EDIT_ERROR = (
    "Retrieval policy configuration cannot be edited with document tools; "
    "an admin publishes it with datahub-curator policy publish."
)

# GMS reports a rejected write as a ValidationExceptionCollection that prints each
# rejected item in full (aspect value, system metadata) before the validator's reason,
# which comes last in each AspectValidationException as "msg=...".
_VALIDATION_REASON = re.compile(
    r", msg=((?:(?!, msg=).)+?)\)(?=\]|, AspectValidationException\()"
)


def is_save_document_enabled() -> bool:
    """Whether the document tools are registered (SAVE_DOCUMENT_TOOL_ENABLED, on
    by default). save_document and update_document ship together."""
    return get_boolean_env_variable("SAVE_DOCUMENT_TOOL_ENABLED", default=True)


def _parent_title() -> str:
    return os.environ.get("SAVE_DOCUMENT_PARENT_TITLE", "Shared")


def _organize_by_user() -> bool:
    return get_boolean_env_variable("SAVE_DOCUMENT_ORGANIZE_BY_USER", default=False)


def restrict_updates_to_shared_folder() -> bool:
    """SAVE_DOCUMENT_RESTRICT_UPDATES, off by default: GMS still checks that the
    caller may edit the document."""
    return get_boolean_env_variable("SAVE_DOCUMENT_RESTRICT_UPDATES", default=False)


def _make_safe_id(text: str, max_length: int = 30) -> str:
    safe_id = "".join(c if c.isalnum() else "-" for c in text.lower())[:max_length]
    return re.sub(r"-+", "-", safe_id).strip("-")


def require_document_urn(value: str, *, tool: str, param: str) -> str:
    urn = value.strip()
    if not urn.startswith(DOCUMENT_PREFIX):
        raise ToolError(
            f"{tool}: `{param}` must be a document URN ({DOCUMENT_PREFIX}...), got "
            f"{value!r}. Find documents with search_documents."
        )
    return urn


def get_author() -> Optional[Dict]:
    """The caller's user, for ownership and audit stamps; None when unknown.

    Best effort: the save still works without it, only less well attributed.
    """
    try:
        result = graphql_helpers.execute_graphql(
            graphql_helpers.get_datahub_client()._graph,
            query=graphql_helpers.load_gql_document("document_writes.gql"),
            variables={},
            operation_name="getDocumentAuthor",
        )
    except Exception as e:
        logger.warning(f"save_document: could not read the current user: {e}")
        return None
    me = result.get("me") or {}
    return me.get("corpUser") or None


def _display_name(user: Optional[Dict]) -> str:
    if not user:
        return "Unknown User"
    editable = user.get("editableProperties") or {}
    info = user.get("info") or {}
    return (
        editable.get("displayName")
        or info.get("displayName")
        or info.get("fullName")
        or f"{info.get('firstName') or ''} {info.get('lastName') or ''}".strip()
        or user.get("username")
        or "Unknown User"
    )


def gms_error_reason(error: Exception, limit: int = 300) -> str:
    """A short reason from a failed GMS call, for a tool error message.

    The SDK wraps GMS's answer in ``OperationalError.info``; a GraphQL call made
    with ``format_exception=False`` raises a ``GraphError`` with the errors list.
    Its message (an authorization failure, a validator's reason) tells the agent
    whether retrying can help, where the exception's repr would not.
    """
    reason: Optional[str] = None
    if isinstance(error, GraphError) and error.args and isinstance(error.args[0], list):
        reason = "; ".join(
            str(e.get("message")) for e in error.args[0] if isinstance(e, dict)
        )
    info = getattr(error, "info", None)
    if isinstance(info, dict):
        errors = info.get("errors")
        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
            reason = errors[0].get("message")
        reason = reason or info.get("message")
    text = " ".join(str(reason or error).split())
    validation_reasons = _VALIDATION_REASON.findall(text)
    if validation_reasons:
        text = "; ".join(validation_reasons)
    return text[:limit] or type(error).__name__


def write_document(
    action: Callable[[], object], *, what: str, unknown_outcome_hint: str
) -> None:
    """Run one write of a document and turn its failure into a ToolError.

    Like ``graphql_helpers.execute_write``: no retry; a refusal gives GMS's
    reason; no answer means the write may or may not have been applied, and the
    message names what to check instead of inviting a blind retry.
    """
    try:
        action()
    except Exception as e:
        status = graphql_helpers.http_status(e)
        if status is not None and status < 500:
            raise ToolError(f"DataHub rejected {what}: {gms_error_reason(e)}") from e
        raise ToolError(
            f"{what} got no reply from DataHub ({status or type(e).__name__}), so it "
            f"may or may not have been applied. {unknown_outcome_hint}"
        ) from e


def drop_certification(doc: Document) -> None:
    """Leave the document's certification out of a whole-entity upsert.

    A copied certification is a certification write: GMS rejects it unless the
    editor may certify, and one rejected aspect fails the whole edit. Certifying
    is a separate action.
    """
    # By name, through a plain dict: the OSS SDK has no certification aspect type.
    cast(Dict[str, object], doc._aspects).pop("certification", None)


def stamp_settings(doc: Document, actor: str, *, created: bool = False) -> None:
    """Stamp a save by ``actor``: DocumentSettings and DocumentInfo get a fresh
    lastModified, and a new document (``created``) its creator too.

    GraphQL, search (lastModifiedAt, creator) and the sidebar's recency sort read
    DocumentInfo's stamps, which GMS's document mutations move on every edit. The
    SDK never moves them on an edit and stamps a new document with its own
    ingestion actor. It writes DocumentSettings only when hiding a document, so
    that aspect is rewritten here, keeping the visibility.
    """
    stamp = models.AuditStampClass(
        time=int(datetime.now().timestamp() * 1000), actor=actor
    )
    doc._set_aspect(
        models.DocumentSettingsClass(
            showInGlobalContext=doc.show_in_global_context, lastModified=stamp
        )
    )
    info = doc._get_aspect(models.DocumentInfoClass)
    if info is not None:
        info.lastModified = stamp
        if created:
            info.created = models.AuditStampClass(time=stamp.time, actor=actor)


def _document_info(urn: str) -> Optional[JsonObject]:
    """The documentInfo aspect of ``urn``; None when it is not a stored document."""
    aspects = get_read_backend().aspects(urn) or {}
    info = aspects.get("documentInfo")
    return info if isinstance(info, dict) else None


def _parent_of(info: JsonObject) -> Optional[str]:
    parent = info.get("parentDocument")
    document = parent.get("document") if isinstance(parent, dict) else None
    return document if isinstance(document, str) else None


def in_shared_folder(urn: str) -> Optional[str]:
    """Why ``urn`` may not be updated under SAVE_DOCUMENT_RESTRICT_UPDATES, or
    None when it is inside the shared folder (or does not exist yet)."""
    if urn == ROOT_PARENT_URN:
        return (
            "Cannot update the root shared documents folder. Only documents within "
            "this folder can be updated."
        )
    try:
        info = _document_info(urn)
        if info is None:
            # Not stored yet: save_document creates it in the shared folder.
            return None
        visited: set[str] = set()
        parent = _parent_of(info)
        while parent and parent not in visited:
            if parent == ROOT_PARENT_URN:
                return None
            visited.add(parent)
            parent_info = _document_info(parent)
            parent = _parent_of(parent_info) if parent_info else None
    except Exception as e:
        # Fail closed, but as a failed call: a GMS error is not a validation result.
        raise ToolError(
            f"Failed to verify that '{urn}' is in the shared documents folder: "
            f"{gms_error_reason(e)}"
        ) from e
    return (
        f"Document '{urn}' is not in the shared documents folder. Only documents in "
        "this folder can be updated."
    )


def is_configuration_document(urn: str, existing: Optional[Document] = None) -> bool:
    """Whether a document holds retrieval-policy configuration, which an agent must
    not rewrite even for a user who may change it."""
    if is_configuration(urn):
        return True
    if existing is None:
        return False
    subtypes = existing._get_aspect(models.SubTypesClass)
    return subtypes is not None and POLICY_DOCUMENT_SUBTYPE in subtypes.typeNames


def _ensure_folder(
    client: DataHubClient,
    doc_id: str,
    title: str,
    description: str,
    parent_urn: Optional[str],
) -> str:
    """Make sure a folder document exists and is visible; return its URN.

    A soft-deleted folder takes its whole subtree out of the sidebar (search drops
    removed entities before the document visibility filters run), so it is
    restored. A failure stops the save: a document under a missing folder would
    be hidden.
    """
    urn = f"{DOCUMENT_PREFIX}{doc_id}"
    what = f"creating the folder {urn}"
    hint = "Call save_document again: creating the folder twice changes nothing."
    aspects = get_read_backend().aspects(urn) or {}
    if "documentInfo" in aspects:
        status = aspects.get("status")
        if isinstance(status, dict) and status.get("removed") is False:
            return urn
        # Re-emit the stored Status with removed cleared, keeping its other fields
        # (the lifecycle stage). Folders that predate this write carry none.
        restored = (
            models.StatusClass.from_obj(status)
            if isinstance(status, dict)
            else models.StatusClass(removed=False)
        )
        restored.removed = False
        write_document(
            lambda: client._graph.emit_mcp(
                MetadataChangeProposalWrapper(entityUrn=urn, aspect=restored)
            ),
            what=f"restoring the folder {urn}",
            unknown_outcome_hint=hint,
        )
        return urn

    folder = Document.create_document(
        id=doc_id,
        title=title,
        text=description,
        subtype="Folder",
        parent_document=parent_urn,
        show_in_global_context=True,
    )
    # Clear removed in place: replacing the aspect would drop the lifecycle stage.
    folder._setdefault_aspect(models.StatusClass(removed=False)).removed = False
    write_document(
        lambda: client.entities.upsert(folder), what=what, unknown_outcome_hint=hint
    )
    return urn


def _shared_parent(client: DataHubClient, author: Optional[Dict]) -> str:
    """The folder a new document without ``parent_urn`` goes in."""
    root = _ensure_folder(
        client,
        ROOT_PARENT_DOC_ID,
        _parent_title(),
        "Contains shared documents authored through AI agents like Ask DataHub.",
        None,
    )
    if not _organize_by_user() or not author:
        return root
    name = _display_name(author)
    return _ensure_folder(
        client,
        f"agent-docs-user-{_make_safe_id(author.get('username') or 'unknown')}",
        name,
        f"Contains documents authored in sessions for {name}.",
        root,
    )


def _fields_saved(
    topics: object, related_documents: object, related_assets: object
) -> FrozenSet[DocumentField]:
    """The fields an update through save_document changes: title, body and type,
    plus the optional lists that were passed."""
    optional: Dict[DocumentField, object] = {
        "topics": topics,
        "related_documents": related_documents,
        "related_assets": related_assets,
    }
    always: FrozenSet[DocumentField] = frozenset({"title", "content", "type"})
    return always | frozenset(f for f, value in optional.items() if value is not None)


def _validate_save_document(args: Dict[str, object]) -> Optional[str]:
    """Pre-approval check: refuse a locked update before a human approves it."""
    urn = args.get("urn")
    if not isinstance(urn, str) or not urn.startswith(DOCUMENT_PREFIX):
        return None
    return document_edit_refusal(
        urn,
        _fields_saved(
            args.get("topics"),
            args.get("related_documents"),
            args.get("related_assets"),
        ),
    )


def _require_urns(
    values: Optional[List[str]], *, tool: str, param: str, prefix: str = ""
) -> Optional[List[str]]:
    if values is None:
        return None
    urns: List[str] = []
    for index, value in enumerate(values):
        candidate = value.strip()
        try:
            Urn.from_string(candidate)
        except Exception:
            raise ToolError(
                f"{tool}: `{param}[{index}]` must be a DataHub URN, got {value!r}."
            ) from None
        if prefix and not candidate.startswith(prefix):
            raise ToolError(
                f"{tool}: `{param}[{index}]` must be a document URN ({prefix}...), "
                f"got {value!r}."
            )
        urns.append(candidate)
    return list(dict.fromkeys(urns))


@destructive
@pre_approval_validator(_validate_save_document)
@min_version(cloud="0.3.16", oss="1.4.0")
def save_document(
    document_type: DocumentType,
    title: str,
    content: str,
    urn: Optional[str] = None,
    topics: OptionalStrList = None,
    related_documents: OptionalStrList = None,
    related_assets: OptionalStrList = None,
    parent_urn: Optional[str] = None,
) -> dict:
    """Save or update a STANDALONE document in DataHub's knowledge base. Once saved,
    a document will be visible to all users of DataHub and to AI assistants.

    NOTE: This tool is for creating standalone documents (insights, FAQs, notes, etc.),
    NOT for updating descriptions on data assets like datasets or dashboards.
    Use update_description for asset descriptions.

    WHEN TO USE THIS TOOL:

    Use this tool when the user explicitly requests to save information:
    - "Save this for later..."
    - "Bookmark this.."
    - "Document this insight.."
    - "Remember this.."
    - "Add this to our knowledge base.."
    - "Create a document about this.."

    Also SUGGEST using this tool when the user provides valuable information such as:
    - Useful SQL queries they want to reuse
    - Decisions about data modeling or architecture
    - FAQs or common questions about data
    - Analysis results worth sharing with the team
    - Corrections or clarifications about data, service, business definitions, etc.

    IMPORTANT: Before calling this tool, you SHOULD confirm with the user that
    they want to save this document. Present the title, content summary,
    and any related assets, and ask for their approval before proceeding. Do not attempt to save
    information that would be private or user-specific.

    Documents are organized hierarchically:
    - Under `parent_urn` when given
    - Otherwise under a configurable parent folder (default: "Shared" for global context),
      optionally grouped by the user who authored them

    UPSERT BEHAVIOR:
    - If `urn` is NOT provided: Creates a NEW document with a unique URN
    - If `urn` IS provided: Updates the EXISTING document with that URN in place. The title,
      content and type are replaced. Owners, status, visibility and the parent are kept, as
      are topics and related links you do not pass. (A `urn` that does not exist yet is
      created in the shared folder.)

    REQUIRED PARAMETERS:

    document_type: The type of document being saved. For example:
        - "Insight": Data insights or discoveries
        - "Decision": Documented decisions with rationale
        - "FAQ": Frequently asked questions and answers
        - "Analysis": Data analysis findings
        - "Summary": Summaries of complex information
        - "Recommendation": Suggested actions or improvements
        - "Note": General notes or observations

    title: A descriptive title for the document.
        - Example: "Sales Data Quality Issues - Q4 2024"

    content: The full content of the document (supports markdown formatting).
        - Example: "## Summary\\n\\nThe orders table shows 15% null values..."

    OPTIONAL PARAMETERS:

    urn: The URN of an existing document to update.
        - ONLY use after a search_documents or get_entities call returns a document URN
        - Example: "urn:li:document:shared-3f2a..."

    topics: List of topic tags for categorization and discovery.
        - These become tags in DataHub that users can click to find related documents
        - Example: ["data-quality", "customer-data", "Q4-2024"]

    related_documents: URNs of related documents.
        - Example: ["urn:li:document:shared-3f2a..."]

    related_assets: URNs of related data assets (tables, dashboards, etc).
        - Example: ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)"]
        - Users then see this document when viewing those assets

    parent_urn: URN of an existing document to create this document under.
        - Example: "urn:li:document:team-runbooks"
        - Only for new documents; defaults to the shared folder

    Returns:
        Dictionary with:
        - success: True
        - urn: The URN of the created/updated document
        - message: What was saved
        - author: The user who authored the document (if available)

        Invalid input or a failed save fails the call with a tool error whose
        message says what to fix.

    EXAMPLE USAGE:

    1. Create a new insight (after user confirmation):
        save_document(
            document_type="Insight",
            title="High Null Rate in Customer Emails",
            content="## Finding\\n\\n23% of customer records have null email...",
            topics=["data-quality", "customer-data"],
            related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"]
        )

    2. Update an existing document (after finding it via search_documents):
        save_document(
            urn="urn:li:document:shared-3f2a...",
            document_type="Insight",
            title="High Null Rate in Customer Emails (Updated)",
            content="## Finding\\n\\nUpdated: Now 18% of customer records have null email...",
        )
    """
    tool = "save_document"
    if not title or not title.strip():
        raise ToolError(f"{tool}: `title` cannot be empty.")
    if not content or not content.strip():
        raise ToolError(f"{tool}: `content` cannot be empty.")
    related_documents = _require_urns(
        related_documents, tool=tool, param="related_documents", prefix=DOCUMENT_PREFIX
    )
    related_assets = _require_urns(related_assets, tool=tool, param="related_assets")
    tag_urns = (
        [f"urn:li:tag:{t.strip()}" for t in topics if t.strip()] if topics else None
    )

    backend = get_read_backend()
    client = graphql_helpers.get_datahub_client()
    document_urn = require_document_urn(urn, tool=tool, param="urn") if urn else None
    if parent_urn is not None:
        # GMS checks moves for loops (a document under its own descendant) only in
        # moveDocument, so an existing document is never re-parented here.
        if document_urn is not None:
            raise ToolError(
                f"{tool}: `parent_urn` can only be set when creating a document."
            )
        parent_urn = require_document_urn(parent_urn, tool=tool, param="parent_urn")
        # A missing or soft-deleted parent would hide the new document from the
        # sidebar; GMS's createDocument refuses both. Unlike the shared folder, the
        # caller's parent is not restored: undeleting it would not bring back the
        # subtree its deletion removed.
        parent_aspects = backend.aspects(parent_urn)
        if parent_aspects is None:
            raise ToolError(f"{tool}: parent document '{parent_urn}' does not exist.")
        parent_status = parent_aspects.get("status")
        if isinstance(parent_status, dict) and parent_status.get("removed") is True:
            raise ToolError(
                f"{tool}: parent document '{parent_urn}' is deleted, so a document "
                "under it would be hidden. Pick another parent_urn (find one with "
                "search_documents) or leave it out to use the shared folder."
            )

    if document_urn is not None:
        if is_configuration_document(document_urn):
            raise ToolError(CONFIGURATION_EDIT_ERROR)
        if restrict_updates_to_shared_folder():
            refusal = in_shared_folder(document_urn)
            if refusal is not None:
                raise ToolError(refusal)
    author = get_author()
    author_urn = (author or {}).get("urn")

    existing: Optional[Document] = None
    if document_urn is not None:
        # Edit a stored document in place. Rebuilding it would replace
        # documentInfo and reset its parent, source, status and created stamp,
        # and would make a private document global. A failed read stops here
        # rather than falling through to creating the document again.
        try:
            fetched = (
                client.entities.get(DocumentUrn.from_string(document_urn))
                if backend.exists(document_urn)
                else None
            )
        except Exception as e:
            raise ToolError(
                f"{tool}: could not read '{document_urn}', so nothing was saved: "
                f"{gms_error_reason(e)}"
            ) from e
        if fetched is not None and not isinstance(fetched, Document):
            raise ToolError(f"{tool}: '{document_urn}' is not a document.")
        existing = fetched

    try:
        doc, document_urn, action, hint = _prepare(
            existing,
            document_urn,
            client=client,
            author=author,
            document_type=document_type,
            title=title,
            content=content,
            tag_urns=tag_urns,
            topics=topics,
            related_documents=related_documents,
            related_assets=related_assets,
            parent_urn=parent_urn,
        )
    except SdkUsageError as e:
        raise ToolError(f"{tool}: {e}") from e

    stamp_settings(
        doc, author_urn or "urn:li:corpuser:datahub", created=action == "created"
    )
    write_document(
        lambda: client.entities.upsert(doc),
        what=f"saving {document_urn}",
        unknown_outcome_hint=hint,
    )
    logger.info(f"save_document {action} {document_urn}")
    return {
        "success": True,
        "urn": document_urn,
        "message": f"Successfully {action} document: {title}",
        "author": _display_name(author) if author else None,
    }


def _prepare(
    existing: Optional[Document],
    document_urn: Optional[str],
    *,
    client: DataHubClient,
    author: Optional[Dict],
    document_type: str,
    title: str,
    content: str,
    tag_urns: Optional[List[str]],
    topics: Optional[List[str]],
    related_documents: Optional[List[str]],
    related_assets: Optional[List[str]],
    parent_urn: Optional[str],
) -> tuple[Document, str, str, str]:
    """The document to write, its URN, "created" or "updated", and what to check
    if the write gets no reply."""
    if existing is not None:
        assert document_urn is not None
        if is_configuration_document(document_urn, existing):
            raise ToolError(CONFIGURATION_EDIT_ERROR)
        ensure_document_editable(
            existing, _fields_saved(topics, related_documents, related_assets)
        )
        doc = existing
        doc.set_title(title)
        doc.set_text(content)
        # The upsert re-emits every aspect read, so a stored semanticText (which
        # embeddings and keyword search prefer over the body) would keep the old
        # body. It follows the new body, as GMS's updateDocumentContents does.
        # Without one, readers already fall back to the body.
        if doc._get_aspect(models.SemanticTextClass) is not None:
            doc._set_aspect(models.SemanticTextClass(text=content))
        doc.set_subtype(document_type)
        if tag_urns is not None:
            doc.set_tags(tag_urns)
        if related_documents is not None:
            doc.set_related_documents(related_documents)
        if related_assets is not None:
            doc.set_related_assets(related_assets)
        drop_certification(doc)
        hint = (
            f"Check {document_urn} with get_entities; saving it again with the same "
            "arguments is safe."
        )
        return doc, document_urn, "updated", hint

    new_urn = document_urn or f"{DOCUMENT_PREFIX}shared-{uuid.uuid4()}"
    if parent_urn is None:
        parent_urn = _shared_parent(client, author)
    doc = Document.create_document(
        id=new_urn[len(DOCUMENT_PREFIX) :],
        title=title,
        text=content,
        subtype=document_type,
        parent_document=parent_urn,
        related_documents=related_documents,
        related_assets=related_assets,
        # The author owns a new document; an update keeps its owners.
        owners=[author["urn"]] if author and author.get("urn") else None,
        tags=tag_urns,
        show_in_global_context=True,
    )
    hint = (
        f"Check whether {new_urn} exists with get_entities; to retry, pass "
        f"urn={new_urn!r} so a second document is not created."
    )
    return doc, new_urn, "created", hint
