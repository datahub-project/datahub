"""Hierarchy extraction logic for Confluence pages."""

from typing import Any, Dict, List, Optional, Set

from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class


class ConfluenceHierarchyExtractor:
    """Static utility class for extracting parent-child relationships from Confluence metadata."""

    @staticmethod
    def extract_parent_id(page_metadata: Optional[Dict[str, Any]]) -> Optional[str]:
        """
        Extract parent page ID from Confluence page metadata.

        Confluence API returns an "ancestors" array containing the full hierarchy path.
        The immediate parent is the last item in this array.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API containing page info

        Returns:
            Parent page ID as string, or None if this is a root page
        """
        if not page_metadata:
            return None

        ancestors = page_metadata.get("ancestors", [])
        if not ancestors or not isinstance(ancestors, list):
            return None

        # The immediate parent is the last ancestor in the list
        immediate_parent = ancestors[-1]
        if not isinstance(immediate_parent, dict):
            return None

        parent_id = immediate_parent.get("id")
        if parent_id:
            return str(parent_id)

        return None

    @staticmethod
    def build_parent_urn(
        parent_id: str,
        platform: str = "confluence",
        instance_id: Optional[str] = None,
    ) -> str:
        """
        Construct a DataHub document URN for a parent page.

        Args:
            parent_id: Confluence page ID (numeric string)
            platform: Platform name (default: "confluence")
            instance_id: Optional instance identifier for URN uniqueness

        Returns:
            DataHub document URN in format:
            - With instance_id: urn:li:document:{platform}-{instance_id}-{page_id}
            - Without instance_id: urn:li:document:{platform}-{page_id}
        """
        if instance_id:
            return f"urn:li:document:{platform}-{instance_id}-{parent_id}"
        else:
            return f"urn:li:document:{platform}-{parent_id}"

    @staticmethod
    def extract_space_key(page_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Extract space key from Confluence page metadata.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API

        Returns:
            Space key as string, or None if not found
        """
        if not page_metadata:
            return None

        # Try nested structure first (common in API v2 responses)
        space = page_metadata.get("space", {})
        if isinstance(space, dict):
            space_key = space.get("key")
            if space_key:
                return str(space_key)

        # Try top-level field (common in some API responses)
        space_key = page_metadata.get("spaceKey")
        if space_key:
            return str(space_key)

        return None

    @staticmethod
    def extract_page_title(page_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Extract page title from Confluence page metadata.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API

        Returns:
            Page title as string, or None if not found
        """
        if not page_metadata:
            return None

        title = page_metadata.get("title")
        if title:
            return str(title)

        return None

    @staticmethod
    def extract_page_url(page_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Extract page URL from Confluence page metadata.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API

        Returns:
            Full page URL as string, or None if not found
        """
        if not page_metadata:
            return None

        # Try _links.webui (common in API responses)
        links = page_metadata.get("_links", {})
        if isinstance(links, dict):
            webui = links.get("webui")
            if webui:
                return str(webui)

        # Try direct url field
        url = page_metadata.get("url")
        if url:
            return str(url)

        return None

    @staticmethod
    def extract_space_name(page_metadata: Dict[str, Any]) -> Optional[str]:
        """
        Extract space name from Confluence page metadata.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API

        Returns:
            Space name as string, or None if not found
        """
        if not page_metadata:
            return None

        # Try nested structure (common in API v2 responses)
        space = page_metadata.get("space", {})
        if isinstance(space, dict):
            space_name = space.get("name")
            if space_name:
                return str(space_name)

        return None

    @staticmethod
    def extract_ancestors(page_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract full ancestors array from Confluence page metadata.

        Args:
            page_metadata: Raw metadata dictionary from Confluence API

        Returns:
            List of ancestor dictionaries in order (root to parent), or empty list if none found
        """
        if not page_metadata:
            return []

        ancestors = page_metadata.get("ancestors", [])
        if not ancestors or not isinstance(ancestors, list):
            return []

        # Validate each ancestor has both 'id' and 'title' fields
        valid_ancestors = []
        for ancestor in ancestors:
            if isinstance(ancestor, dict) and "id" in ancestor and "title" in ancestor:
                valid_ancestors.append(ancestor)

        return valid_ancestors

    @staticmethod
    def build_browse_path_v2(
        page_metadata: Dict[str, Any],
        platform: str = "confluence",
        instance_id: Optional[str] = None,
        ingested_page_ids: Optional[Set[str]] = None,
    ) -> Optional[BrowsePathsV2Class]:
        """
        Build BrowsePathsV2 aspect for a Confluence page.

        Creates hierarchical browse path: Space Name / Ancestor 1 / ... / Ancestor N

        Args:
            page_metadata: Raw metadata dictionary from Confluence API
            platform: Platform name (default: "confluence")
            instance_id: Optional instance identifier for URN construction
            ingested_page_ids: Set of page IDs being ingested (for URN validation)

        Returns:
            BrowsePathsV2Class with hierarchical path entries, or None if space not found
        """
        if not page_metadata:
            return None

        # Extract space name (fallback to space key if name missing)
        space_name = ConfluenceHierarchyExtractor.extract_space_name(page_metadata)
        if not space_name:
            space_key = ConfluenceHierarchyExtractor.extract_space_key(page_metadata)
            if not space_key:
                return None
            space_name = space_key

        # Initialize path with space entry (no URN for spaces)
        path_entries: List[BrowsePathEntryClass] = [BrowsePathEntryClass(id=space_name)]

        # Add ancestor entries with URNs if they're being ingested
        ancestors = ConfluenceHierarchyExtractor.extract_ancestors(page_metadata)
        for ancestor in ancestors:
            ancestor_id = str(ancestor["id"])
            ancestor_title = str(ancestor["title"])

            # Build URN only if ancestor is being ingested
            ancestor_urn = None
            if ingested_page_ids and ancestor_id in ingested_page_ids:
                ancestor_urn = ConfluenceHierarchyExtractor.build_parent_urn(
                    parent_id=ancestor_id,
                    platform=platform,
                    instance_id=instance_id,
                )

            path_entries.append(
                BrowsePathEntryClass(id=ancestor_title, urn=ancestor_urn)
            )

        return BrowsePathsV2Class(path=path_entries)
