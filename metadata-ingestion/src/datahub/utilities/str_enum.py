from enum import Enum


class StrEnum(str, Enum):
    """String Enum class."""

    # This is required for compatibility with Python 3.11+, which changed the
    # behavior of enums in format() and f-strings.
    # Once we're using only Python 3.11+, we can replace this with enum.StrEnum.
    # See https://blog.pecar.me/python-enum for more details.

    def __str__(self) -> str:
        """Return the string representation of the enum."""
        return str(self.value)
