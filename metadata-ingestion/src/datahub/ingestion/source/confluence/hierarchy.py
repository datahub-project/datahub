from typing import Dict, Iterable, List, Optional, Set

from pydantic import BaseModel, Field

from datahub.ingestion.source.confluence.models import (
    ConfluenceAncestor,
    ConfluencePage,
    ConfluenceSpaceRef,
)
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class

PLATFORM = "confluence"


class FolderNode(BaseModel):
    """A Confluence folder found among the ancestors of an ingested page.

    The page APIs never return folders, so they are rebuilt from page
    ancestry: an ancestor's parent is the entry before it.
    """

    id: str
    title: str
    # Immediate parent (a page or folder); None when directly under the space.
    parent_id: Optional[str]
    space: ConfluenceSpaceRef
    # Ancestors above this folder, for its browse path.
    ancestor_prefix: List[ConfluenceAncestor] = Field(default_factory=list)


def document_id(instance_id: str, content_id: str) -> str:
    return f"{PLATFORM}-{instance_id}-{content_id}"


def document_urn(instance_id: str, content_id: str) -> str:
    return f"urn:li:document:{document_id(instance_id, content_id)}"


def build_browse_path(
    space: ConfluenceSpaceRef,
    ancestors: List[ConfluenceAncestor],
    instance_id: str,
    ingested_ids: Set[str],
) -> Optional[BrowsePathsV2Class]:
    """Space / ancestor / ... / ancestor. Ancestors that are ingested link to
    their document; the rest are shown by title only."""
    space_label = space.name or space.key
    if not space_label:
        return None
    path = [BrowsePathEntryClass(id=space_label)]
    for ancestor in ancestors:
        urn = (
            document_urn(instance_id, ancestor.id)
            if ancestor.id in ingested_ids
            else None
        )
        path.append(BrowsePathEntryClass(id=ancestor.title or ancestor.id, urn=urn))
    return BrowsePathsV2Class(path=path)


def collect_folder_nodes(pages: Iterable[ConfluencePage]) -> Dict[str, FolderNode]:
    """Folders that appear among the ingested pages' ancestors. Empty folders
    never appear in any page's ancestry, so they are not emitted."""
    folders: Dict[str, FolderNode] = {}
    for page in pages:
        for index, ancestor in enumerate(page.ancestors):
            if not ancestor.is_folder or ancestor.id in folders:
                continue
            folders[ancestor.id] = FolderNode(
                id=ancestor.id,
                title=ancestor.title or f"Folder {ancestor.id}",
                parent_id=page.ancestors[index - 1].id if index > 0 else None,
                space=page.space,
                ancestor_prefix=page.ancestors[:index],
            )
    return folders
