from enum import Enum
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


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
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_before_validator_function(
            cls.validate, handler(source_type)
        )

    @classmethod
    def validate(cls, v):  # type: ignore[no-untyped-def]
        if v and isinstance(v, str):
            return v.upper()
        return v
