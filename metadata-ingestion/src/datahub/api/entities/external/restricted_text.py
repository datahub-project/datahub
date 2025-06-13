"""The `RestrictedText` module provides a custom Pydantic type that stores the original
value but returns a truncated and sanitized version when accessed.

Features:
- Configurable maximum length with truncation
- Character replacement (default replaces with underscore)
- Preserves original value internally
- Customizable truncation suffix
- Compatible with both Pydantic v1 and v2
"""

from __future__ import annotations

from typing import Any, ClassVar, Optional, Set, Union

# Check Pydantic version and import accordingly
try:
    from pydantic import VERSION

    PYDANTIC_V2 = int(VERSION.split(".")[0]) >= 2
except (ImportError, AttributeError):
    # Fallback for older versions that don't have VERSION
    PYDANTIC_V2 = False

if PYDANTIC_V2:
    from pydantic import GetCoreSchemaHandler  # type: ignore[attr-defined]
    from pydantic_core import core_schema
else:
    from pydantic.validators import str_validator


class RestrictedTextConfig:
    """Configuration class for RestrictedText."""

    def __init__(
        self,
        max_length: Optional[int] = None,
        forbidden_chars: Optional[Set[str]] = None,
        replacement_char: Optional[str] = None,
        truncation_suffix: Optional[str] = None,
    ):
        self.max_length = max_length
        self.forbidden_chars = forbidden_chars
        self.replacement_char = replacement_char
        self.truncation_suffix = truncation_suffix


class RestrictedText(str):
    """A string type that stores the original value but returns a truncated and sanitized version.

    This type allows you to:
    - Set a maximum length for the displayed value
    - Replace specific characters with a replacement character
    - Access both the original and processed values

    ```python
    from pydantic import BaseModel

    class TestModel(BaseModel):
        # Basic usage with default settings
        name: RestrictedText

        # Custom max length and character replacement using Field
        custom_field: RestrictedText = RestrictedText.with_config(
            max_length=10,
            forbidden_chars={' ', '-', '.'},
            replacement_char='_'
        )

    # Usage example
    model = TestModel(
        name="This is a very long string with special characters!",
        custom_field="hello-world.test"
    )

    print(model.name)  # Truncated and sanitized version
    print(model.name.original)  # Original value
    print(model.custom_field)  # "hello_worl..."
    ```
    """

    # Default configuration
    _default_max_length: ClassVar[Optional[int]] = 50
    _default_forbidden_chars: ClassVar[Set[str]] = {" ", "\t", "\n", "\r"}
    _default_replacement_char: ClassVar[str] = "_"
    _default_truncation_suffix: ClassVar[str] = "..."

    def __new__(cls, value: str = "") -> "RestrictedText":
        """Create a new string instance."""
        instance = str.__new__(cls, "")  # We'll set the display value later
        return instance

    def __init__(self, value: str = ""):
        """Initialize the RestrictedText with a value."""
        self.original: str = value
        self.max_length = self._default_max_length
        self.forbidden_chars = self._default_forbidden_chars
        self.replacement_char = self._default_replacement_char
        self.truncation_suffix = self._default_truncation_suffix

        # Process the value
        self._processed_value = self._process_value(value)

    def _configure(
        self,
        max_length: Optional[int] = None,
        forbidden_chars: Optional[Set[str]] = None,
        replacement_char: Optional[str] = None,
        truncation_suffix: Optional[str] = None,
    ) -> "RestrictedText":
        """Configure this instance with custom settings."""
        if max_length is not None:
            self.max_length = max_length
        if forbidden_chars is not None:
            self.forbidden_chars = forbidden_chars
        if replacement_char is not None:
            self.replacement_char = replacement_char
        if truncation_suffix is not None:
            self.truncation_suffix = truncation_suffix

        # Reprocess the value with new configuration
        self._processed_value = self._process_value(self.original)
        return self

    def _process_value(self, value: str) -> str:
        """Process the value by replacing characters and truncating."""
        # Replace specified characters
        processed = value
        for char in self.forbidden_chars:
            processed = processed.replace(char, self.replacement_char)

        # Truncate if necessary
        if self.max_length is not None and len(processed) > self.max_length:
            if len(self.truncation_suffix) >= self.max_length:
                # If suffix is too long, just truncate without suffix
                processed = processed[: self.max_length]
            else:
                # Truncate and add suffix
                truncate_length = self.max_length - len(self.truncation_suffix)
                processed = processed[:truncate_length] + self.truncation_suffix

        return processed

    def __str__(self) -> str:
        """Return the processed (truncated and sanitized) value."""
        return self._processed_value

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._processed_value!r})"

    @property
    def processed(self) -> str:
        """Get the processed (truncated and sanitized) value."""
        return self._processed_value

    @classmethod
    def with_config(
        cls,
        max_length: Optional[int] = None,
        forbidden_chars: Optional[Set[str]] = None,
        replacement_char: Optional[str] = None,
        truncation_suffix: Optional[str] = None,
    ) -> RestrictedTextConfig:
        """Create a configuration object for use as field default.

        Args:
            max_length: Maximum length of the processed string
            forbidden_chars: Set of characters to replace
            replacement_char: Character to use as replacement
            truncation_suffix: Suffix to add when truncating

        Returns:
            A configuration object that can be used as field default
        """
        return RestrictedTextConfig(
            max_length=max_length,
            forbidden_chars=forbidden_chars,
            replacement_char=replacement_char,
            truncation_suffix=truncation_suffix,
        )

    # Pydantic v2 methods
    if PYDANTIC_V2:

        @classmethod
        def _validate(
            cls,
            __input_value: Union[str, "RestrictedText"],
            _: core_schema.ValidationInfo,
        ) -> "RestrictedText":
            """Validate and create a RestrictedText instance."""
            if isinstance(__input_value, RestrictedText):
                return __input_value
            return cls(__input_value)

        @classmethod
        def __get_pydantic_core_schema__(
            cls, source: type[Any], handler: GetCoreSchemaHandler
        ) -> core_schema.CoreSchema:
            """Get the Pydantic core schema for this type."""
            return core_schema.with_info_after_validator_function(
                cls._validate,
                core_schema.str_schema(),
                field_name=cls.__name__,
            )

    # Pydantic v1 methods
    else:

        @classmethod
        def __get_validators__(cls):
            """Pydantic v1 validator method."""
            yield cls.validate

        @classmethod
        def validate(cls, v, field=None):
            """Validate and create a RestrictedText instance for Pydantic v1."""
            if isinstance(v, RestrictedText):
                return v

            if not isinstance(v, str):
                # Let pydantic handle the string validation
                v = str_validator(v)

            # Create instance
            instance = cls(v)

            # Check if there's a field default that contains configuration
            if (
                field
                and hasattr(field, "default")
                and isinstance(field.default, RestrictedTextConfig)
            ):
                config = field.default
                instance._configure(
                    max_length=config.max_length,
                    forbidden_chars=config.forbidden_chars,
                    replacement_char=config.replacement_char,
                    truncation_suffix=config.truncation_suffix,
                )

            return instance

        @classmethod
        def __modify_schema__(cls, field_schema):
            """Modify the JSON schema for Pydantic v1."""
            field_schema.update(type="string", examples=["example string"])
