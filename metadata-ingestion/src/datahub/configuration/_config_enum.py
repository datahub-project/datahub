from enum import Enum

import pydantic
import pydantic.types
import pydantic.validators


class ConfigEnum(Enum):
    # Ideally we would use @staticmethod here, but some versions of Python don't support it.
    # See https://github.com/python/mypy/issues/7591.
    def _generate_next_value_(  # type: ignore
        name: str, start, count, last_values
    ) -> str:
        # This makes the enum value match the enum option name.
        # From https://stackoverflow.com/a/44785241/5004662.
        return name

    @classmethod
    def __get_validators__(cls) -> "pydantic.types.CallableGenerator":
        # We convert the text to uppercase before attempting to match it to an enum value.
        yield cls.validate
        yield pydantic.validators.enum_member_validator

    @classmethod
    def validate(cls, v):  # type: ignore[no-untyped-def]
        if v and isinstance(v, str):
            return v.upper()
        return v
