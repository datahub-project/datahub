# Import RestrictedText from your existing module
# Uncomment and adjust the import path as needed:
# from your_restricted_text_module import RestrictedText
# The following is a list of tag constraints:
# You can assign a maximum of 50 tags to a single securable object.
# The maximum length of a tag key is 255 characters.
# The maximum length of a tag value is 1000 characters.
# The following characters are not allowed in tag keys:
# . , - = / :
# Tag search using the workspace search UI is supported only for tables, views, and table columns.
# Tag search requires exact term matching.
# https://learn.microsoft.com/en-us/azure/databricks/database-objects/tags#constraint
from typing import Any, Dict, Optional, Union

from typing_extensions import ClassVar

from datahub.api.entities.external.external_tag import ExternalTag
from datahub.api.entities.external.restricted_text import RestrictedText


class LakeFormationTagKeyText(RestrictedText):
    """RestrictedText configured for Unity Catalog tag keys."""

    _default_max_length: ClassVar[int] = 50
    # Unity Catalog tag keys: alphanumeric, hyphens, underscores, periods only
    _default_replacement_char: ClassVar[str] = "_"
    _default_truncation_suffix: ClassVar[str] = ""  # No suffix for clean identifiers


class LakeFormationTagValueText(RestrictedText):
    """RestrictedText configured for Unity Catalog tag values."""

    _default_max_length: ClassVar[int] = 50
    # Unity Catalog tag values are more permissive but still have some restrictions
    _default_replacement_char: ClassVar[str] = " "
    _default_truncation_suffix: ClassVar[str] = "..."


class LakeFormationTag(ExternalTag):
    """
    A tag type specifically designed for LakeFormation tag restrictions.

    LakeFormation Tag Restrictions:
    - Key: Max 127 characters, alphanumeric + hyphens, underscores, periods only
    - Value: Max 256 characters, more permissive but no control characters
    """

    key: LakeFormationTagKeyText
    value: Optional[LakeFormationTagValueText] = None
    catalog: Optional[str] = None

    def __init__(
        self,
        key: Optional[Union[str, LakeFormationTagKeyText]] = None,
        value: Optional[Union[str, LakeFormationTagValueText]] = None,
        **data: Any,
    ) -> None:
        """
        Initialize LakeFormation Tag from either a DataHub Tag URN or explicit key/value.

        Args:
            key: Explicit key value (optional for Pydantic initialization)
            value: Explicit value (optional)
            **data: Additional Pydantic data
        """
        if key is not None:
            # Direct initialization with key/value
            processed_key = (
                LakeFormationTagKeyText(key)
                if not isinstance(key, LakeFormationTagKeyText)
                else key
            )
            processed_value = None
            if value is not None:
                processed_value = (
                    LakeFormationTagValueText(value)
                    if not isinstance(value, LakeFormationTagValueText)
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

    def __eq__(self, other: object) -> bool:
        """Check equality based on key and value."""
        if not isinstance(other, LakeFormationTag):
            return False
        return str(self.key) == str(other.key) and (
            str(self.value) if self.value else None
        ) == (str(other.value) if other.value else None)

    def __hash__(self) -> int:
        """Make LakeFormationTag hashable based on key and value."""
        return hash((str(self.key), str(self.value) if self.value else None))

    @classmethod
    def from_dict(cls, tag_dict: Dict[str, Any]) -> "LakeFormationTag":
        """
        Create a LakeFormationTag from a dictionary with 'key' and optional 'value'.

        Args:
            tag_dict: Dictionary with 'key' and optional 'value' keys

        Returns:
            LakeFormationTag instance
        """
        return cls(key=tag_dict["key"], value=tag_dict.get("value"))

    @classmethod
    def from_key_value(
        cls, key: str, value: Optional[str] = None
    ) -> "LakeFormationTag":
        """
        Create a LakeFormationTagPlatformResource from explicit key and value.

        Overrides the parent method to return the correct type.

        Args:
            key: Tag key
            value: Optional tag value

        Returns:
            LakeFormationTag instance
        """
        return cls(key=key, value=value)

    def to_dict(self) -> Dict[str, str]:
        """
        Convert to dictionary format suitable for LakeFormation tag.

        Returns:
            Dictionary with 'key' and optionally 'value'
        """
        result: Dict[str, str] = {"key": self.key.original}
        if self.value is not None:
            result["value"] = self.value.original
        return result

    def to_display_dict(self) -> Dict[str, str]:
        """
        Convert to dictionary format showing processed values.

        Returns:
            Dictionary with processed 'key' and optional 'value'
        """
        result: Dict[str, str] = {"key": str(self.key)}
        if self.value is not None:
            result["value"] = str(self.value)
        return result

    def __repr__(self) -> str:
        if self.value:
            return f"LakeFormationTag(key={self.key!r}, value={self.value!r})"
        else:
            return f"LakeFormationTag(key={self.key!r})"
