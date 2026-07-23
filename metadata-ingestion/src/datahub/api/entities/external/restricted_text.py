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

from typing import ClassVar, Optional, Set

from datahub.configuration.common import ConfigModel


class RestrictedText(ConfigModel):
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

        # Custom max length and character replacement
        custom_field: RestrictedText = RestrictedText(
            text="hello-world.test",
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
    print(model.name.text)  # Original value
    print(model.custom_field)  # "hello_worl..."
    ```
    """

    # Default configuration
    DEFAULT_MAX_LENGTH: ClassVar[Optional[int]] = 50
    DEFAULT_FORBIDDEN_CHARS: ClassVar[Set[str]] = {" ", "\t", "\n", "\r"}
    DEFAULT_REPLACEMENT_CHAR: ClassVar[str] = "_"
    DEFAULT_TRUNCATION_SUFFIX: ClassVar[str] = "..."

    raw_text: str
    max_length: Optional[int] = None
    forbidden_chars: Optional[Set[str]] = None
    replacement_char: Optional[str] = None
    truncation_suffix: Optional[str] = None
    _processed_value: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        self.validate_text()

    @classmethod
    def __get_validators__(cls):
        yield cls.pydantic_accept_raw_text
        yield cls.validate
        yield cls.pydantic_validate_text

    @classmethod
    def pydantic_accept_raw_text(cls, v):
        if isinstance(v, (RestrictedText, dict)):
            return v
        assert isinstance(v, str), "text must be a string"
        return {"text": v}

    @classmethod
    def pydantic_validate_text(cls, v):
        assert isinstance(v, RestrictedText)
        assert v.validate_text()
        return v

    @classmethod
    def validate(cls, v):
        """Validate and create a RestrictedText instance."""
        if isinstance(v, RestrictedText):
            return v

        # This should be a dict at this point from pydantic_accept_raw_text
        if isinstance(v, dict):
            instance = cls(**v)
            instance.validate_text()
            return instance

        raise ValueError(f"Unable to validate RestrictedText from {type(v)}")

    def validate_text(self) -> bool:
        """Validate the text and apply restrictions."""
        # Set defaults if not provided
        max_length = (
            self.max_length if self.max_length is not None else self.DEFAULT_MAX_LENGTH
        )
        forbidden_chars = (
            self.forbidden_chars
            if self.forbidden_chars is not None
            else self.DEFAULT_FORBIDDEN_CHARS
        )
        replacement_char = (
            self.replacement_char
            if self.replacement_char is not None
            else self.DEFAULT_REPLACEMENT_CHAR
        )
        truncation_suffix = (
            self.truncation_suffix
            if self.truncation_suffix is not None
            else self.DEFAULT_TRUNCATION_SUFFIX
        )

        # Store processed value
        self._processed_value = self._process_value(
            self.raw_text,
            max_length,
            forbidden_chars,
            replacement_char,
            truncation_suffix,
        )
        return True

    def _process_value(
        self,
        value: str,
        max_length: Optional[int],
        forbidden_chars: Set[str],
        replacement_char: str,
        truncation_suffix: str,
    ) -> str:
        """Process the value by replacing characters and truncating."""
        # Replace specified characters
        processed = value
        for char in forbidden_chars:
            processed = processed.replace(char, replacement_char)

        # Truncate if necessary
        if max_length is not None and len(processed) > max_length:
            if len(truncation_suffix) >= max_length:
                # If suffix is too long, just truncate without suffix
                processed = processed[:max_length]
            else:
                # Truncate and add suffix
                truncate_length = max_length - len(truncation_suffix)
                processed = processed[:truncate_length] + truncation_suffix

        return processed

    def __str__(self) -> str:
        """Return the processed (truncated and sanitized) value."""
        return self._processed_value or ""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.raw_text!r})"

    @property
    def processed(self) -> str:
        """Get the processed (truncated and sanitized) value."""
        return self._processed_value or ""
