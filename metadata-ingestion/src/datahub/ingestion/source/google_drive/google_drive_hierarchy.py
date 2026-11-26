from typing import Dict, List, Optional

from datahub.ingestion.source.google_drive.constants import DOCUMENT_ID_PREFIX
from datahub.ingestion.source.google_drive.models import FolderNode
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class
from datahub.metadata.urns import DocumentUrn


def make_document_id(file_id: str) -> str:
    return DOCUMENT_ID_PREFIX + file_id


def make_document_urn(file_id: str) -> str:
    return str(DocumentUrn(make_document_id(file_id)))


def resolve_parent_urn(
    folder: Optional[FolderNode],
    create_root_documents: bool,
    parent_document_urn: Optional[str],
) -> Optional[str]:
    """Parent document URN for an item located directly inside `folder`.

    Pass `folder=None` for a configured root itself.
    """
    if folder is None:
        return parent_document_urn
    if folder.is_root and not create_root_documents:
        return parent_document_urn
    return make_document_urn(folder.id)


def ancestor_chain(
    folder: Optional[FolderNode], folders: Dict[str, FolderNode]
) -> List[FolderNode]:
    """Folders from the root down to `folder` inclusive."""
    chain: List[FolderNode] = []
    current = folder
    while current is not None:
        chain.append(current)
        if current.parent_id is None:
            break
        current = folders.get(current.parent_id)
    chain.reverse()
    return chain


def build_browse_path_v2(
    folder: Optional[FolderNode],
    folders: Dict[str, FolderNode],
    create_root_documents: bool,
    parent_document_urn: Optional[str],
) -> Optional[BrowsePathsV2Class]:
    """Browse path describing the ancestors of an item located inside `folder`.

    The item itself is not part of its own browse path. Pass `folder=None` for a
    configured root, whose only possible ancestor is `parent_document_urn`.
    """
    entries: List[BrowsePathEntryClass] = []
    if parent_document_urn:
        entries.append(
            BrowsePathEntryClass(id=parent_document_urn, urn=parent_document_urn)
        )
    for ancestor in ancestor_chain(folder, folders):
        if ancestor.is_root and not create_root_documents:
            # No document exists for the root, so the entry is a plain label.
            entries.append(BrowsePathEntryClass(id=ancestor.title))
        else:
            entries.append(
                BrowsePathEntryClass(
                    id=ancestor.title, urn=make_document_urn(ancestor.id)
                )
            )
    if not entries:
        return None
    return BrowsePathsV2Class(path=entries)
