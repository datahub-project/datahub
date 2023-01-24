import warnings
from typing import Callable, Type, TypeVar

import pydantic

from datahub.configuration.common import ConfigurationWarning

_T = TypeVar("_T")


def _default_rename_transform(value: _T) -> _T:
    return value


def pydantic_renamed_field(
    old_name: str,
    new_name: str,
    transform: Callable = _default_rename_transform,
    print_warning: bool = True,
) -> classmethod:
    def _validate_field_rename(cls: Type, values: dict) -> dict:
        if old_name in values:
            if new_name in values:
                raise ValueError(
                    f"Cannot specify both {old_name} and {new_name} in the same config. Note that {old_name} has been deprecated in favor of {new_name}."
                )
            else:
                if print_warning:
                    warnings.warn(
                        f"{old_name} is deprecated, please use {new_name} instead.",
                        ConfigurationWarning,
                        stacklevel=2,
                    )
                values[new_name] = transform(values.pop(old_name))
        return values

    # Why aren't we using pydantic.validator here?
    # The `values` argument that is passed to field validators only contains items
    # that have already been validated in the pre-process phase, which happens if
    # they have an associated field and a pre=True validator. However, the root
    # validator with pre=True gets all the values that were passed in.
    # Given that a renamed field doesn't show up in the fields list, we can't use
    # the field-level validator, even with a different field name.
    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_field_rename)
