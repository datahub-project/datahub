"""
External Tags Module

This module provides tag types that integrate with external systems like DataHub and Unity Catalog.
It builds on top of RestrictedText to provide sanitized, truncated tag handling with original value preservation.

Classes:
    - ExternalTag: DataHub-compatible tag with key/value parsing from URNs

Example Usage:
    # DataHub Tags
    tag = ExternalTag.from_urn("urn:li:tag:environment:production")
    datahub_urn = tag.get_datahub_tag  # Returns TagUrn object or string

"""

from __future__ import annotations

from typing import Any, Optional, Tuple, Union

from pydantic import BaseModel

from datahub.api.entities.external.restricted_text import RestrictedText
from datahub.metadata.urns import TagUrn


class ExternalTag(BaseModel):
    """A tag type that parses DataHub Tag URNs into key-value pairs with RestrictedText properties."""

    key: RestrictedText
    value: Optional[RestrictedText] = None

    def __init__(
        self,
        key: Optional[Union[str, RestrictedText]] = None,
        value: Optional[Union[str, RestrictedText]] = None,
        **data: Any,
    ) -> None:
        """
        Initialize ExternalTag from either a DataHub Tag URN or explicit key/value.

        Args:
            key: Explicit key value (optional for Pydantic initialization)
            value: Explicit value (optional)
            **data: Additional Pydantic data
        """
        if key is not None:
            # Direct initialization with key/value
            processed_key = (
                RestrictedText(key) if not isinstance(key, RestrictedText) else key
            )
            processed_value = None
            if value is not None:
                processed_value = (
                    RestrictedText(value)
                    if not isinstance(value, RestrictedText)
                    else value
                )

            super().__init__(
                key=processed_key,
                value=processed_value,
                **data,
            )
        else:
            # Standard pydantic initialization
            super().__init__(**data)

    @staticmethod
    def _parse_tag_name(tag_name: str) -> Tuple[str, Optional[str]]:
        """
        Parse tag name into key and optional value.

        If tag_name contains ':', split on first ':' into key:value
        Otherwise, use entire tag_name as key with no value.

        Args:
            tag_name: The tag name portion from the URN

        Returns:
            Tuple of (key, value) where value may be None
        """
        if ":" in tag_name:
            parts = tag_name.split(":", 1)  # Split on first ':' only
            return parts[0], parts[1]
        else:
            return tag_name, None

    def to_datahub_tag_urn(self) -> TagUrn:
        """
        Generate a DataHub Tag URN from the key and value.
        This method creates the URN using the original (unprocessed) values.

        Returns:
            'urn:li:tag:key:value' if value exists, otherwise 'urn:li:tag:key'
        """
        if self.value is not None:
            tag_name = f"{self.key.original}:{self.value.original}"
        else:
            tag_name = self.key.original

        return TagUrn(name=tag_name)

    @classmethod
    def from_urn(cls, tag_urn: Union[str, "TagUrn"]) -> "ExternalTag":
        """
        Create an ExternalTag from a DataHub Tag URN.

        Args:
            tag_urn: DataHub Tag URN string or TagUrn object

        Returns:
            ExternalTag instance
        """
        if isinstance(tag_urn, str):
            tag_urn = TagUrn.from_string(tag_urn)
        key, value = cls._parse_tag_name(tag_urn.name)
        return cls(key=key, value=value)

    @classmethod
    def from_key_value(cls, key: str, value: Optional[str] = None) -> "ExternalTag":
        """
        Create an ExternalTag from explicit key and value.

        Args:
            key: Tag key
            value: Optional tag value

        Returns:
            ExternalTag instance
        """
        return cls(key=key, value=value)

    def __str__(self) -> str:
        """String representation of the tag."""
        if self.value is not None:
            return f"{self.key}:{self.value}"
        else:
            return str(self.key)

    def __repr__(self) -> str:
        if self.value is not None:
            return f"ExternalTag(key={self.key!r}, value={self.value!r})"
        else:
            return f"ExternalTag(key={self.key!r})"
