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
from typing import Any, Dict, Optional, Set

# Import validator for Pydantic v1 (always needed since we removed conditional logic)
from pydantic import validator
from typing_extensions import ClassVar

from datahub.api.entities.external.external_tag import ExternalTag
from datahub.api.entities.external.restricted_text import RestrictedText


class UnityCatalogTagKeyText(RestrictedText):
    """RestrictedText configured for Unity Catalog tag keys."""

    _default_max_length: ClassVar[int] = 255
    # Unity Catalog tag keys: alphanumeric, hyphens, underscores, periods only
    _default_forbidden_chars: ClassVar[Set[str]] = {
        "\t",
        "\n",
        "\r",
        ".",
        ",",
        "-",
        "=",
        "/",
        ":",
    }
    _default_replacement_char: ClassVar[str] = "_"
    _default_truncation_suffix: ClassVar[str] = ""  # No suffix for clean identifiers


class UnityCatalogTagValueText(RestrictedText):
    """RestrictedText configured for Unity Catalog tag values."""

    _default_max_length: ClassVar[int] = 1000
    # Unity Catalog tag values are more permissive but still have some restrictions
    _default_forbidden_chars: ClassVar[Set[str]] = {"\t", "\n", "\r"}
    _default_replacement_char: ClassVar[str] = " "
    _default_truncation_suffix: ClassVar[str] = "..."


class UnityCatalogTag(ExternalTag):
    """
    A tag type specifically designed for Unity Catalog tag restrictions.

    Unity Catalog Tag Restrictions:
    - Key: Max 127 characters, alphanumeric + hyphens, underscores, periods only
    - Value: Max 256 characters, more permissive but no control characters
    """

    key: UnityCatalogTagKeyText
    value: Optional[UnityCatalogTagValueText] = None

    def __init__(self, **data: Any) -> None:
        """Initialize UnityCatalogTag with proper field processing."""
        # For Pydantic v2, we need to handle RestrictedText creation manually
        # because the validation system isn't working properly
        processed_data = self._process_tag_data(data)

        # Skip ExternalTag.__init__ and call BaseModel.__init__ directly
        from pydantic import BaseModel

        BaseModel.__init__(self, **processed_data)

        # WORKAROUND: Manually set the fields after initialization
        # This is necessary because RestrictedText doesn't work properly with Pydantic v2
        if "key" in processed_data:
            object.__setattr__(self, "key", processed_data["key"])
        if "value" in processed_data:
            object.__setattr__(self, "value", processed_data["value"])

    @staticmethod
    def _process_tag_data(data: Any) -> Any:
        """Common processing logic for both Pydantic v1 and v2."""
        if isinstance(data, dict):
            # Handle key field
            if "key" in data and data["key"] is not None:
                key_value = data["key"]
                if not isinstance(key_value, UnityCatalogTagKeyText):
                    # If we get a RestrictedText object, use its original value
                    if hasattr(key_value, "original"):
                        data["key"] = UnityCatalogTagKeyText(key_value.original)
                    else:
                        data["key"] = UnityCatalogTagKeyText(key_value)

            # Handle value field
            if "value" in data and data["value"] is not None:
                value_data = data["value"]
                if not isinstance(value_data, UnityCatalogTagValueText):
                    # If we get a RestrictedText object, use its original value
                    if hasattr(value_data, "original"):
                        original_value = value_data.original
                        # If value is an empty string, set it to None
                        if not str(original_value):
                            data["value"] = None
                        else:
                            data["value"] = UnityCatalogTagValueText(original_value)
                    else:
                        # If value is an empty string, set it to None
                        if not str(value_data):
                            data["value"] = None
                        else:
                            data["value"] = UnityCatalogTagValueText(value_data)

        return data

    # Pydantic v1 validators
    @validator("key", pre=True)
    @classmethod
    def _validate_key(cls, v: Any) -> UnityCatalogTagKeyText:
        """Validate and convert key field for Pydantic v1."""
        if isinstance(v, UnityCatalogTagKeyText):
            return v

        # If we get a RestrictedText object from parent class validation, use its original value
        if hasattr(v, "original"):
            return UnityCatalogTagKeyText(v.original)

        return UnityCatalogTagKeyText(v)

    @validator("value", pre=True)
    @classmethod
    def _validate_value(cls, v: Any) -> Optional[UnityCatalogTagValueText]:
        """Validate and convert value field for Pydantic v1."""
        if v is None:
            return None

        if isinstance(v, UnityCatalogTagValueText):
            return v

        # If we get a RestrictedText object from parent class validation, use its original value
        if hasattr(v, "original"):
            original_value = v.original
            # If value is an empty string, set it to None to not generate empty value in DataHub tag
            if not str(original_value):
                return None
            return UnityCatalogTagValueText(original_value)

        # If value is an empty string, set it to None to not generate empty value in DataHub tag
        if not str(v):
            return None

        return UnityCatalogTagValueText(v)

    def __eq__(self, other: object) -> bool:
        """Check equality based on key and value."""
        if not isinstance(other, UnityCatalogTag):
            return False
        return str(self.key) == str(other.key) and (
            str(self.value) if self.value else None
        ) == (str(other.value) if other.value else None)

    def __hash__(self) -> int:
        """Make UnityCatalogTag hashable based on key and value."""
        return hash((str(self.key), str(self.value) if self.value else None))

    @classmethod
    def from_dict(cls, tag_dict: Dict[str, Any]) -> "UnityCatalogTag":
        """
        Create a UnityCatalogTag from a dictionary with 'key' and optional 'value'.

        Args:
            tag_dict: Dictionary with 'key' and optional 'value' keys

        Returns:
            UnityCatalogTag instance
        """
        return cls(**tag_dict)

    @classmethod
    def from_key_value(cls, key: str, value: Optional[str] = None) -> "UnityCatalogTag":
        """
        Create a UnityCatalogTag from explicit key and value.

        Overrides the parent method to return the correct type.

        Args:
            key: Tag key
            value: Optional tag value

        Returns:
            UnityCatalogTag instance
        """
        return cls(key=key, value=value)

    def to_dict(self) -> Dict[str, str]:
        """
        Convert to dictionary format suitable for Unity Catalog API.

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
            return f"UnityCatalogTag(key={self.key!r}, value={self.value!r})"
        else:
            return f"UnityCatalogTag(key={self.key!r})"


# Note: Pydantic v2 validation is handled in the __init__ method due to
# issues with RestrictedText compatibility with Pydantic v2's validation system
