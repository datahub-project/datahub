"""Hierarchy extraction logic for Confluence pages."""

from typing import Any, Dict, Optional


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
