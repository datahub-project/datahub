"""Hierarchy extraction logic for GitHub documents."""

from typing import Dict, List, Optional, Set

from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class


class GitHubHierarchyExtractor:
    """Static utilities for deriving browse paths from GitHub document hierarchy.

    GitHub documents form a tree that mirrors the repository folder structure:
    an optional repo-root document, intermediate folder documents, and leaf file
    documents. Each document records its immediate parent's ``source_id``; the
    full ancestor chain is reconstructed by walking those parent links.

    All folder/repo ancestors are themselves emitted as documents, so their URNs
    are always resolvable (unlike Confluence/Notion, where ancestors can fall
    outside the ingestion scope).
    """

    @staticmethod
    def build_ancestor_chain(
        source_id: str,
        parent_links: Dict[str, Optional[str]],
    ) -> List[str]:
        """Reconstruct the ancestor chain for a document (root first, parent last).

        Walks the ``source_id`` -> parent ``source_id`` links. The traversal is
        cycle-safe: it stops if a node is revisited or a parent is absent.

        Args:
            source_id: The document whose ancestors to resolve.
            parent_links: Map of source_id -> parent source_id (None at the root).

        Returns:
            Ordered list of ancestor source IDs from root down to the immediate
            parent. Empty if the document has no in-repo parent.
        """
        chain: List[str] = []
        visited: Set[str] = {source_id}
        current = parent_links.get(source_id)

        while current and current not in visited:
            visited.add(current)
            chain.append(current)
            current = parent_links.get(current)

        chain.reverse()
        return chain

    @staticmethod
    def build_browse_path_v2(
        source_id: str,
        parent_links: Dict[str, Optional[str]],
        titles: Dict[str, str],
        urns: Dict[str, str],
        prefix_entries: Optional[List[BrowsePathEntryClass]] = None,
    ) -> Optional[BrowsePathsV2Class]:
        """Build a BrowsePathsV2 aspect describing a document's ancestry.

        Creates a hierarchical browse path of the document's ancestors (the
        document itself is not included). Any ``prefix_entries`` (e.g. a
        configured parent document, organization, or repository name) are
        prepended ahead of the reconstructed ancestor chain.

        Args:
            source_id: The document the browse path is for.
            parent_links: Map of source_id -> parent source_id.
            titles: Map of source_id -> display label.
            urns: Map of source_id -> document URN.
            prefix_entries: Optional leading entries to root the path under.

        Returns:
            BrowsePathsV2Class with the prefix followed by one entry per
            ancestor, or None when there are no entries at all (e.g. a repo-root
            document with no prefix).
        """
        if not source_id:
            return None

        path_entries: List[BrowsePathEntryClass] = list(prefix_entries or [])

        for ancestor_id in GitHubHierarchyExtractor.build_ancestor_chain(
            source_id, parent_links
        ):
            path_entries.append(
                BrowsePathEntryClass(
                    id=titles.get(ancestor_id, ancestor_id),
                    urn=urns.get(ancestor_id),
                )
            )

        if not path_entries:
            return None

        return BrowsePathsV2Class(path=path_entries)
