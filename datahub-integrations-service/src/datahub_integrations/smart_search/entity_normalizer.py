"""
Entity normalizer for GraphQL responses.

Provides consistent access to entity fields across different GraphQL schema versions
and entity types, handling variations in field locations (properties, editableProperties,
info, editableInfo, etc.).
"""

from typing import Any, Dict


class EntityNormalizer:
    """
    Normalizes GraphQL entity responses into consistent field access.

    Different entity types in DataHub store fields in different locations due to
    schema evolution and type-specific patterns. This class provides a single
    interface to extract common fields regardless of their location.
    """

    @staticmethod
    def get_name(entity: Dict[str, Any]) -> str:
        """
        Extract name from entity, checking multiple possible locations.

        In DataHub GraphQL, names can appear in different locations depending on entity type:
        - editableProperties.name (user-edited name, highest priority for Dataset)
        - properties.name (most common)
        - properties.displayName (for CorpUser, CorpGroup)
        - info.name (deprecated field for Dashboard, Chart)
        - info.displayName (deprecated field for CorpUser)
        - editableInfo.displayName (deprecated field for CorpUser)
        - top-level name (deprecated for Dataset)

        Args:
            entity: Entity dict from GraphQL

        Returns:
            Name string, or empty string if not found
        """
        # Try editableProperties first (user-edited names have priority)
        editable_properties = entity.get("editableProperties")
        if isinstance(editable_properties, dict):
            if name := editable_properties.get("name"):
                return name
            if display_name := editable_properties.get("displayName"):
                return display_name

        # Try properties
        properties = entity.get("properties")
        if isinstance(properties, dict):
            if name := properties.get("name"):
                return name
            if display_name := properties.get("displayName"):
                return display_name

        # Try info (deprecated)
        info = entity.get("info")
        if isinstance(info, dict):
            if name := info.get("name"):
                return name
            if display_name := info.get("displayName"):
                return display_name

        # Try editableInfo (deprecated)
        editable_info = entity.get("editableInfo")
        if isinstance(editable_info, dict):
            if display_name := editable_info.get("displayName"):
                return display_name

        # Try top-level name (deprecated for Dataset)
        if name := entity.get("name"):
            return name

        return ""

    @staticmethod
    def get_description(entity: Dict[str, Any]) -> str:
        """
        Extract description from entity, checking multiple possible locations.

        In DataHub GraphQL, descriptions can appear in different locations depending on entity type:
        - properties.description (most common)
        - editableProperties.description (user-edited descriptions)
        - info.description (for entity types like Dashboard, Chart, Assertion)
        - editableInfo.description (deprecated field for some entity types)

        Args:
            entity: Entity dict from GraphQL

        Returns:
            Description string, or empty string if not found
        """
        # Try properties first
        properties = entity.get("properties")
        if isinstance(properties, dict):
            if desc := properties.get("description"):
                return desc

        # Try editableProperties
        editable_properties = entity.get("editableProperties")
        if isinstance(editable_properties, dict):
            if desc := editable_properties.get("description"):
                return desc

        # Try info
        info = entity.get("info")
        if isinstance(info, dict):
            if desc := info.get("description"):
                return desc

        # Try editableInfo (deprecated but still used)
        editable_info = entity.get("editableInfo")
        if isinstance(editable_info, dict):
            if desc := editable_info.get("description"):
                return desc

        # Try top-level description
        if desc := entity.get("description"):
            return desc

        return ""

    @staticmethod
    def get_qualified_name(entity: Dict[str, Any]) -> str:
        """
        Extract qualified name from entity.

        The qualified name is the fully-qualified technical path (e.g., "database.schema.table")
        and only exists in properties (not editable).

        Args:
            entity: Entity dict from GraphQL

        Returns:
            Qualified name string, or empty string if not found
        """
        properties = entity.get("properties")
        if isinstance(properties, dict):
            qualified_name = properties.get("qualifiedName", "")
            return qualified_name if qualified_name else ""
        return ""
