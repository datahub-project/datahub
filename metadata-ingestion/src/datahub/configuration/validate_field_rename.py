import warnings
from typing import TypeVar

import pydantic

_T = TypeVar("_T")


def pydantic_renamed_field(old_name: str, new_name: str) -> classmethod:
    def _validate_field_rename(v: _T, values: dict) -> _T:
        if old_name in values:
            if new_name in values:
                raise ValueError(
                    f"Cannot specify both {old_name} and {new_name} in the same config. Note that {old_name} has been deprecated in favor of {new_name}."
                )
            else:
                warnings.warn(
                    f"The {old_name} is deprecated, please use {new_name} instead.",
                    UserWarning,
                )
                return values.pop(old_name)
        return v

    return pydantic.validator(new_name, always=True, pre=True, allow_reuse=True)(
        _validate_field_rename
    )
