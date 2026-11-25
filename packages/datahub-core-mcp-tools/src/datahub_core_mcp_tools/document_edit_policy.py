"""Host-provided rules for which fields of a document the document tools may change.

The tools call :func:`ensure_document_editable` on the document they already fetched,
naming the fields their command would change. With no provider installed every edit
is allowed. A host locks fields by registering a :class:`DocumentEditPolicy` factory
under the ``datahub.mcp.document_edit_policy`` entry point group, so host-specific
document kinds never need to be known here.
"""

import functools
import importlib.metadata
from typing import FrozenSet, Literal, Optional, Protocol

from datahub.errors import ItemNotFoundError
from datahub.sdk.document import Document
from fastmcp.exceptions import ToolError
from loguru import logger

from . import graphql_helpers

ENTRY_POINT_GROUP = "datahub.mcp.document_edit_policy"

DocumentField = Literal[
    "title",
    "content",
    "type",
    "topics",
    "related_documents",
    "related_assets",
    "status",
    "parent",
]


class DocumentEditPolicy(Protocol):
    def ensure_editable(self, doc: Document, fields: FrozenSet[DocumentField]) -> None:
        """Raise ``ToolError`` with an actionable message if any field is locked."""
        ...


@functools.lru_cache(maxsize=1)
def _load_policy() -> Optional[DocumentEditPolicy]:
    providers = list(importlib.metadata.entry_points(group=ENTRY_POINT_GROUP))
    if not providers:
        return None
    if len(providers) > 1:
        names = ", ".join(sorted(p.value for p in providers))
        raise RuntimeError(f"More than one {ENTRY_POINT_GROUP} provider: {names}")
    # A declared provider that fails to load raises instead of allowing every edit:
    # silently dropping the host's locks would be the worse failure.
    return providers[0].load()()


def ensure_document_editable(doc: Document, fields: FrozenSet[DocumentField]) -> None:
    """Raise ``ToolError`` if the installed policy locks any of ``fields`` on ``doc``."""
    policy = _load_policy()
    if policy is not None and fields:
        policy.ensure_editable(doc, fields)


def document_edit_refusal(urn: str, fields: FrozenSet[DocumentField]) -> Optional[str]:
    """The policy's refusal for editing ``fields`` of ``urn``, or None.

    For pre-approval validators, which run before a human is asked to approve the
    call. Fails open when the document can't be fetched or isn't a Document: the
    tool runs the same check on the document it fetches, which stays the backstop.
    """
    if not fields or _load_policy() is None:
        return None
    try:
        doc = graphql_helpers.get_datahub_client().entities.get(urn)
    except ItemNotFoundError:
        return None
    except Exception:
        logger.opt(exception=True).warning(
            "Could not fetch {} to check its edit policy before approval", urn
        )
        return None
    if not isinstance(doc, Document):
        return None
    try:
        ensure_document_editable(doc, fields)
    except ToolError as e:
        return str(e)
    return None


def clear_document_edit_policy_cache() -> None:
    """Drop the loaded policy. For tests that install or remove a provider."""
    _load_policy.cache_clear()
