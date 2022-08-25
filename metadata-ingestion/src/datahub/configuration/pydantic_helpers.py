import pydantic


def _make_uppercase(cls: pydantic.BaseModel, value: str) -> str:
    if value and isinstance(value, str):
        return value.upper()
    return value


def pydantic_enum_case_insensitive(field: str) -> classmethod:
    return pydantic.validator(field, pre=True, allow_reuse=True)(_make_uppercase)
