from enum import Enum


class ConfigEnum(Enum):
    def _generate_next_value_(  # type: ignore
        name: str, start, count, last_values
    ) -> str:
        return name

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):  # type: ignore
        from pydantic_core import core_schema

        return core_schema.no_info_before_validator_function(
            cls.validate, handler(source_type)
        )

    @classmethod
    def validate(cls, v):  # type: ignore[no-untyped-def]
        if v and isinstance(v, str):
            return v.upper()
        return v
