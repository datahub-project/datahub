"""Hierarchy extraction logic for Notion pages."""

from typing import Any, Callable, Dict, List, Optional, Set

from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class


class NotionHierarchyExtractor:
    """Static utilities for deriving parent-child hierarchy from Notion metadata.

    Notion's per-page ``additional_metadata`` only records the *immediate* parent
    (``{"type": "page_id"|"database_id"|"workspace", ...}``). Unlike Confluence,
    Notion does not return a full ancestor array, so the complete chain is
    reconstructed by walking parent links across the page metadata map.
    """

    @staticmethod
    def extract_parent_id(
        additional_metadata: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        """Extract the immediate parent page/database ID for a single page.

        Workspace parents are treated as the root (no parent page).

        Args:
            additional_metadata: A page's ``additional_metadata`` dict, which may
                contain a ``parent`` entry.

        Returns:
            Parent page/database ID, or None if the page is at the workspace root.
        """
        if not additional_metadata or not isinstance(additional_metadata, dict):
            return None

        parent_info = additional_metadata.get("parent")
        if not parent_info or not isinstance(parent_info, dict):
            return None

        parent_type = parent_info.get("type")
        if parent_type == "page_id":
            parent_id = parent_info.get("page_id")
        elif parent_type == "database_id":
            parent_id = parent_info.get("database_id")
        else:
            # workspace (or unknown) parents are the root - no parent page
            return None

        return str(parent_id) if parent_id else None

    @staticmethod
    def extract_ancestor_chain(
        page_id: str,
        parent_metadata: Dict[str, Any],
        browse_path_root_ids: Optional[Set[str]] = None,
    ) -> List[str]:
        """Reconstruct the ancestor chain for a page (root first, immediate parent last).

        Walks the parent links recorded in ``parent_metadata``. The traversal is
        cycle-safe: it stops if a page is revisited or a parent is not present in
        the metadata map.

        When ``browse_path_root_ids`` is set (explicit ``page_ids`` / ``database_ids``
        in the recipe), those IDs act as browse-path anchors: the chain for an
        anchored page is empty, and descendants stop walking above their nearest
        anchored ancestor.

        Args:
            page_id: ID of the page whose ancestors to resolve.
            parent_metadata: Map of page_id -> ``additional_metadata`` for all
                pages discovered during ingestion.
            browse_path_root_ids: Page/database IDs configured as ingestion roots.

        Returns:
            Ordered list of ancestor page IDs from root down to the immediate
            parent. Empty if the page is at the workspace root.
        """
        if browse_path_root_ids and page_id in browse_path_root_ids:
            return []

        chain: List[str] = []
        visited: Set[str] = {page_id}
        current = page_id

        while True:
            parent_id = NotionHierarchyExtractor.extract_parent_id(
                parent_metadata.get(current)
            )
            if not parent_id or parent_id in visited:
                break
            chain.append(parent_id)
            visited.add(parent_id)

            if browse_path_root_ids and parent_id in browse_path_root_ids:
                break

            current = parent_id

        chain.reverse()
        return chain

    @staticmethod
    def build_browse_path_v2(
        page_id: str,
        parent_metadata: Dict[str, Any],
        urn_builder: Callable[[str], str],
        title_resolver: Callable[[str], str],
        ingested_page_ids: Optional[Set[str]] = None,
        browse_path_root_ids: Optional[Set[str]] = None,
    ) -> Optional[BrowsePathsV2Class]:
        """Build a BrowsePathsV2 aspect describing a Notion page's ancestry.

        Creates a hierarchical browse path: Ancestor 1 / ... / Ancestor N (the
        page itself is not included). Only ancestors that are part of the current
        ingestion run are included, so browse paths never reference pages outside
        the scoped set.

        Args:
            page_id: ID of the page the browse path is for.
            parent_metadata: Map of page_id -> ``additional_metadata`` for all
                pages discovered during ingestion.
            urn_builder: Callable mapping an ancestor page ID to its document URN.
            title_resolver: Callable mapping an ancestor page ID to a display label.
            ingested_page_ids: Set of page IDs being ingested (for URN validation).
            browse_path_root_ids: Explicitly configured ingestion roots; browse
                paths do not walk above these IDs.

        Returns:
            BrowsePathsV2Class with one entry per ancestor, or None for root pages
            (which have no ancestors).
        """
        if not page_id:
            return None

        chain = NotionHierarchyExtractor.extract_ancestor_chain(
            page_id, parent_metadata, browse_path_root_ids
        )
        if ingested_page_ids is not None:
            chain = [
                ancestor_id for ancestor_id in chain if ancestor_id in ingested_page_ids
            ]
        if not chain:
            return None

        path_entries: List[BrowsePathEntryClass] = []
        for ancestor_id in chain:
            ancestor_urn = urn_builder(ancestor_id)
            path_entries.append(
                BrowsePathEntryClass(id=title_resolver(ancestor_id), urn=ancestor_urn)
            )

        return BrowsePathsV2Class(path=path_entries)
