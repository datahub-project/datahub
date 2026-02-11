"""Document saving tool for DataHub MCP server.

This tool enables AI agents to save documents to DataHub's knowledge base.
Documents are organized under a configurable parent folder (default: "Shared"),
optionally with per-user subfolders for organization.

Configuration via environment variables:
- SAVE_DOCUMENT_TOOL_ENABLED: Set to "false" to disable this tool (default: enabled). Also requires TOOLS_IS_MUTATION_ENABLED enabled.
- SAVE_DOCUMENT_PARENT_TITLE: Custom title for the parent folder (default: "Shared")
- SAVE_DOCUMENT_ORGANIZE_BY_USER: Set to "true" to enable per-user organization (default: false)
- SAVE_DOCUMENT_RESTRICT_UPDATES: Set to "false" to allow updating any document (default: true - only agent-created docs can be updated)
"""

import logging
import os
import re
import uuid
from datetime import datetime
from typing import Dict, List, Literal, Optional, Tuple

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.metadata import schema_classes as models
from datahub.sdk import Document

from ..version_requirements import min_version

logger = logging.getLogger(__name__)

# Fixed root parent document ID - independent of title for future flexibility
ROOT_PARENT_DOC_ID = "__system_shared_documents"


def _get_parent_title() -> str:
    """Get the configurable parent document title from environment."""
    return os.environ.get("SAVE_DOCUMENT_PARENT_TITLE", "Shared")


def _is_organize_by_user_enabled() -> bool:
    """Check if per-user organization is enabled (default: False)."""
    value = os.environ.get("SAVE_DOCUMENT_ORGANIZE_BY_USER", "false")
    return value.lower() in ("true", "1", "yes")


def _restrict_updates_to_shared_folder() -> bool:
    """Check if updates should be restricted to the shared folder (default: True).

    When enabled, only documents inside the shared folder can be updated.
    This prevents accidental modification of user-created or imported documents.
    """
    value = os.environ.get("SAVE_DOCUMENT_RESTRICT_UPDATES", "true")
    return value.lower() in ("true", "1", "yes")


def _make_safe_id(text: str, max_length: int = 30) -> str:
    """Convert text to a safe ID string."""
    safe_id = "".join(c if c.isalnum() else "-" for c in text.lower())[:max_length]
    safe_id = re.sub(r"-+", "-", safe_id)  # Collapse multiple dashes
    return safe_id.strip("-")


def _get_root_parent_id() -> str:
    """Get the root parent document ID.

    Uses a fixed ID independent of the display title to allow changing
    the title without requiring data migration.
    """
    return ROOT_PARENT_DOC_ID


def _get_root_parent_urn() -> str:
    """Get the root parent document URN."""
    return f"urn:li:document:{_get_root_parent_id()}"


# Supported document types (subtypes)
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


def _get_current_user_info() -> Optional[Dict]:
    """Fetch the current authenticated user's information."""
    from ..mcp_server import execute_graphql, get_datahub_client

    client = get_datahub_client()

    query = """
        query getMe {
            me {
                corpUser {
                    urn
                    username
                    info {
                        displayName
                        fullName
                        firstName
                        lastName
                    }
                    editableProperties {
                        displayName
                    }
                }
            }
        }
    """

    try:
        result = execute_graphql(
            client._graph,
            query=query,
            variables={},
            operation_name="getMe",
        )
        me_data = result.get("me", {})
        return me_data.get("corpUser") if me_data else None
    except Exception as e:
        logger.warning(f"Failed to get current user info: {e}")
        return None


def _get_user_display_name(user_info: Optional[Dict]) -> str:
    """Extract the best display name from user info."""
    if not user_info:
        return "Unknown User"

    # Try editable displayName first, then info fields
    editable = user_info.get("editableProperties") or {}
    info = user_info.get("info") or {}

    return (
        editable.get("displayName")
        or info.get("displayName")
        or info.get("fullName")
        or f"{info.get('firstName', '')} {info.get('lastName', '')}".strip()
        or user_info.get("username")
        or "Unknown User"
    )


def _generate_document_id() -> str:
    """Generate a unique document ID using UUID.

    Each save creates a new document with a unique ID.
    Format: shared-<uuid>
    """
    unique_id = str(uuid.uuid4())
    return f"shared-{unique_id}"


def _is_document_in_shared_folder(document_urn: str) -> Tuple[bool, Optional[str]]:
    """Check if a document is within the shared documents folder.

    Simple validation: document must have the shared folder as a parent/ancestor.

    Returns:
        Tuple of (is_valid, error_message)
        - (True, None) if document is in the shared folder
        - (False, error_message) if document is outside the folder
    """
    from ..mcp_server import get_datahub_client

    client = get_datahub_client()
    root_parent_urn = _get_root_parent_urn()

    # Can't update the root folder itself
    if document_urn == root_parent_urn:
        return False, (
            "Cannot update the root shared documents folder. "
            "Only documents within this folder can be updated."
        )

    try:
        # Fetch the document
        doc = client.entities.get(document_urn)
        logger.debug(
            f"Validating document {document_urn} for update, fetched: {doc is not None}"
        )
        if doc is None:
            # Document doesn't exist yet - allow (will be created)
            logger.debug(f"Document {document_urn} does not exist, allowing update")
            return True, None

        # Get documentInfo aspect
        aspects = getattr(doc, "aspects", None) or getattr(doc, "_aspects", {})
        logger.debug(
            f"Document aspects type: {type(aspects)}, keys: {list(aspects.keys()) if isinstance(aspects, dict) else 'N/A'}"
        )
        doc_info = aspects.get("documentInfo") if isinstance(aspects, dict) else None

        if doc_info is None:
            logger.debug(f"Document {document_urn} has no documentInfo aspect")
            return False, (
                f"Document '{document_urn}' has no document info. "
                "Cannot verify it's in the shared folder."
            )

        # Walk up the parent chain looking for the shared folder
        # parentDocument is a ParentDocumentClass with a .document field containing the URN
        parent_doc_obj = getattr(doc_info, "parentDocument", None)
        logger.debug(f"Document parentDocument object: {parent_doc_obj}")
        current_parent_urn = (
            getattr(parent_doc_obj, "document", None) if parent_doc_obj else None
        )
        logger.debug(
            f"Document parent URN: {current_parent_urn}, looking for root: {root_parent_urn}"
        )
        visited = set()

        while current_parent_urn:
            if current_parent_urn in visited:
                break
            visited.add(current_parent_urn)

            # Found the shared folder - document is valid
            if current_parent_urn == root_parent_urn:
                return True, None

            # Fetch parent and continue walking up
            try:
                parent_doc = client.entities.get(current_parent_urn)
                if parent_doc is None:
                    break
                parent_aspects = getattr(parent_doc, "aspects", None) or getattr(
                    parent_doc, "_aspects", {}
                )
                parent_info = (
                    parent_aspects.get("documentInfo")
                    if isinstance(parent_aspects, dict)
                    else None
                )
                if parent_info is None:
                    break
                # Get next parent - again, it's a ParentDocumentClass object
                next_parent_obj = getattr(parent_info, "parentDocument", None)
                current_parent_urn = (
                    getattr(next_parent_obj, "document", None)
                    if next_parent_obj
                    else None
                )
            except Exception:
                break

        return False, (
            f"Document '{document_urn}' is not in the shared documents folder. "
            "Only documents in this folder can be updated."
        )

    except Exception as e:
        logger.error(f"Failed to validate document hierarchy: {e}", exc_info=True)
        # Fail closed - if we can't validate, don't allow the update
        return False, (
            f"Failed to validate document hierarchy for '{document_urn}': {str(e)}. "
            "Cannot update document without verifying it's in the shared folder."
        )


def _ensure_document_exists(
    doc_id: str,
    title: str,
    description: str,
    parent_urn: Optional[str] = None,
) -> str:
    """Ensure a document exists, creating it if necessary. Returns the URN."""
    from ..mcp_server import get_datahub_client

    client = get_datahub_client()
    doc_urn = f"urn:li:document:{doc_id}"

    try:
        existing = client.entities.get(doc_urn)
        if existing is not None:
            return doc_urn
    except Exception:
        pass

    # Create the document
    doc = Document.create_document(
        id=doc_id,
        title=title,
        text=description,
        subtype="Folder",
        parent_document=parent_urn,
        show_in_global_context=True,
    )

    try:
        client.entities.upsert(doc)
        logger.info(f"Created folder document: {doc_urn}")
    except Exception as e:
        logger.warning(f"Failed to create folder document (may already exist): {e}")

    return doc_urn


def _ensure_parent_hierarchy(user_info: Optional[Dict]) -> Tuple[str, Optional[str]]:
    """Ensure the parent document hierarchy exists.

    Returns:
        Tuple of (parent_urn_for_document, user_urn_if_available)
    """
    root_title = _get_parent_title()
    root_id = _get_root_parent_id()

    # Always create the root parent
    root_urn = _ensure_document_exists(
        doc_id=root_id,
        title=root_title,
        description="Contains shared documents authored through AI agents like Ask DataHub.",
        parent_urn=None,
    )

    # If per-user organization is disabled, return root as parent
    if not _is_organize_by_user_enabled():
        return root_urn, user_info.get("urn") if user_info else None

    # Create user-specific folder if we have user info
    if user_info:
        user_urn = user_info.get("urn")
        username = user_info.get("username", "unknown")
        display_name = _get_user_display_name(user_info)

        # Create user folder under root
        user_folder_id = f"agent-docs-user-{_make_safe_id(username, max_length=30)}"
        user_folder_urn = _ensure_document_exists(
            doc_id=user_folder_id,
            title=display_name,
            description=f"Contains documents authored in sessions for {display_name}.",
            parent_urn=root_urn,
        )
        return user_folder_urn, user_urn

    # No user info available, use root
    return root_urn, None


@min_version(cloud="0.3.16", oss="1.4.0")
def save_document(
    document_type: DocumentType,
    title: str,
    content: str,
    urn: Optional[str] = None,
    topics: Optional[List[str]] = None,
    related_documents: Optional[List[str]] = None,
    related_assets: Optional[List[str]] = None,
) -> dict:
    """Save or update a STANDALONE document in DataHub's knowledge base. Once saved,
    a document will be visible to all users of DataHub and to Ask DataHub AI assistant.

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

    ⚠️  IMPORTANT: Before calling this tool, you SHOULD confirm with the user that
    they want to save this document. Present the title, content summary,
    and any related assets, and ask for their approval before proceeding. Do not attempt to save
    information that would be private or user-specific.

    This tool persists insights, decisions, FAQs, and other contextual information
    as documents in DataHub. Documents are organized hierarchically:
    - Under a configurable parent folder (default: "Shared" for global context)
    - Optionally grouped by the user who authored them

    UPSERT BEHAVIOR:
    - If `urn` is NOT provided: Creates a NEW document with a unique URN
    - If `urn` IS provided: Updates the EXISTING document with that URN

    IMPORTANT USAGE GUIDELINES:
    - Always confirm with the user before saving
    - Provide a clear summary of what will be saved
    - Ask if the user wants to proceed with creating/updating the document

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
        - Example: "Decision: Deprecating Legacy Customer Table"

    content: The full content of the document (supports markdown formatting).
        - Can include headers, lists, code blocks, tables, etc.
        - Example: "## Summary\\n\\nThe orders table shows 15% null values..."

    OPTIONAL PARAMETERS:

    urn: The URN of an existing document to update.
        - ONLY use after a search_documents or get_entity call returns a document URN
        - Example: "urn:li:document:agent-insight-abc123"
        - If not provided, a new document is created with a unique URN
        - If provided, the existing document is updated (upsert operation)

    topics: List of topic tags for categorization and discovery (like a word cloud).
        - These become searchable tags in DataHub that users can click to find related documents
        - Example: ["data-quality", "customer-data", "Q4-2024"]
        - Example: ["high-priority", "sales", "email", "null-values"]

    related_documents: URNs of related documents.
        - Example: ["urn:li:document:agent-insight-sales-abc123"]
        - Creates links between related knowledge

    related_assets: URNs of related data assets (tables, dashboards, etc).
        - Example: ["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.orders,PROD)"]
        - Links the document to specific data assets in the catalog
        - Users can then see this document when viewing those assets

    Returns:
        Dictionary with:
        - success: Boolean indicating if the operation succeeded
        - urn: The URN of the created/updated document
        - message: Success or error message
        - author: The user who authored the document (if available)

    RECOMMENDED WORKFLOW:

    1. Gather information you want to save publicly
    2. Present a summary to the user:
       "I'd like to save the following insight to DataHub:
        - Title: High Null Rate in Customer Emails
        - Type: Insight
        - Related to: customers table
        Would you like me to save this?"
    3. Only call save_document after user confirms

    EXAMPLE USAGE:

    1. Create a new insight (after user confirmation):
        save_document(
            document_type="Insight",
            title="High Null Rate in Customer Emails",
            content="## Finding\\n\\n23% of customer records have null email...",
            topics=["data-quality", "customer-data", "email", "high-severity"],
            related_assets=["urn:li:dataset:(urn:li:dataPlatform:snowflake,customers,PROD)"]
        )

    2. Update an existing document (after finding it via search_documents):
        save_document(
            urn="urn:li:document:agent-insight-abc123",  # From search_documents result
            document_type="Insight",
            title="High Null Rate in Customer Emails (Updated)",
            content="## Finding\\n\\nUpdated: Now 18% of customer records have null email...",
            topics=["data-quality", "customer-data", "email", "resolved"]
        )

    3. Document a decision:
        save_document(
            document_type="Decision",
            title="Migrating to New Production Database",
            content="## Decision\\n\\nWe will migrate to v2 schema...\\n\\n## Rationale\\n...",
            topics=["architecture", "data-model", "migration", "approved"]
        )
    """
    from ..mcp_server import get_datahub_client

    client = get_datahub_client()

    # Validate inputs
    if not title or not title.strip():
        return {
            "success": False,
            "urn": None,
            "message": "title cannot be empty",
            "author": None,
        }

    if not content or not content.strip():
        return {
            "success": False,
            "urn": None,
            "message": "content cannot be empty",
            "author": None,
        }

    valid_document_types = [
        "Insight",
        "Decision",
        "FAQ",
        "Analysis",
        "Summary",
        "Recommendation",
        "Note",
        "Context",
    ]
    if document_type not in valid_document_types:
        return {
            "success": False,
            "urn": None,
            "message": f"Invalid document_type '{document_type}'. Must be one of: {', '.join(valid_document_types)}",
            "author": None,
        }

    # Validate URN format if provided
    if urn is not None:
        if not urn.startswith("urn:li:document:"):
            return {
                "success": False,
                "urn": None,
                "message": f"Invalid urn format '{urn}'. Must start with 'urn:li:document:'",
                "author": None,
            }

        # Validate that the document is within the agent-authored hierarchy
        # This prevents accidental modification of user-created or imported documents
        if _restrict_updates_to_shared_folder():
            is_valid, error_message = _is_document_in_shared_folder(urn)
            if not is_valid:
                return {
                    "success": False,
                    "urn": None,
                    "message": error_message,
                    "author": None,
                }

        is_update = True
        document_urn = urn
        # Extract document ID from URN
        document_id = urn.replace("urn:li:document:", "")
    else:
        is_update = False
        # Generate new document ID
        document_id = _generate_document_id()
        document_urn = f"urn:li:document:{document_id}"

    try:
        # Get current user info for attribution and organization
        user_info = _get_current_user_info()
        user_display_name = _get_user_display_name(user_info) if user_info else None
        user_urn = user_info.get("urn") if user_info else None

        # Ensure parent hierarchy exists and get the parent URN for our document
        # For updates, we still want to maintain proper parent hierarchy
        parent_urn, _ = _ensure_parent_hierarchy(user_info)

        # Set current user as owner only for NEW documents (captures authorship)
        # For updates, preserve existing ownership by not setting owners
        # The SDK expects owner URNs as strings in a list
        if is_update:
            owners = None  # Don't overwrite existing ownership on updates
            logger.info("Updating existing document - preserving existing ownership")
        else:
            owners = [user_urn] if user_urn else None
            logger.info(f"Creating new document - setting owners: {owners}")

        # Convert topics to tag URNs (DataHub expects full URNs)
        # TODO: Decide whether tags are the right abstraction here.
        # Alternative: Use Structured Properties, custom properties, or custom label.
        # Just needs to be searchable later on.
        tag_urns = None
        if topics:
            tag_urns = [f"urn:li:tag:{topic}" for topic in topics]

        # Create the document
        doc = Document.create_document(
            id=document_id,
            title=title,
            text=content,
            subtype=document_type,
            parent_document=parent_urn,
            related_documents=related_documents,
            related_assets=related_assets,
            owners=owners,
            tags=tag_urns,  # Use topics as tags for searchability
            show_in_global_context=True,
        )

        # Manually add DocumentSettings to ensure showInGlobalContext=True is set
        # This is a workaround until the SDK is updated to always emit this aspect
        # TODO: Remove this once SDK properly emits DocumentSettings for show_in_global_context=True
        actor_urn = user_urn or "urn:li:corpuser:datahub"
        settings_audit = models.AuditStampClass(
            time=int(datetime.now().timestamp() * 1000),
            actor=actor_urn,
        )
        document_settings = models.DocumentSettingsClass(
            showInGlobalContext=True,
            lastModified=settings_audit,
        )
        doc._set_aspect(document_settings)

        # Log document details before upsert
        logger.info(
            f"Document to upsert: URN={document_urn}, owners={owners}, parent={parent_urn}"
        )

        # Upsert the document
        try:
            client.entities.upsert(doc)
            logger.info("Upsert completed successfully")
        except Exception as upsert_error:
            logger.error(f"Failed to upsert document: {upsert_error}", exc_info=True)
            raise

        action = "updated" if is_update else "created"
        logger.info(f"Successfully {action} document: {document_urn}")

        return {
            "success": True,
            "urn": document_urn,
            "message": f"Successfully {action} document: {title}",
            "author": user_display_name,
        }

    except Exception as e:
        logger.error(f"Failed to save document: {e}")
        return {
            "success": False,
            "urn": None,
            "message": f"Error saving document: {str(e)}",
            "author": None,
        }


def is_save_document_enabled() -> bool:
    """Check if the save_document tool should be enabled.

    Defaults to True. Set SAVE_DOCUMENT_TOOL_ENABLED=false to disable.
    """
    # Default to enabled (True) if not explicitly set to false
    env_value = os.environ.get("SAVE_DOCUMENT_TOOL_ENABLED")
    if env_value is None:
        return True  # Default to enabled
    return get_boolean_env_variable("SAVE_DOCUMENT_TOOL_ENABLED")
