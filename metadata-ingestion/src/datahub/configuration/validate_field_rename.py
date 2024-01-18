import warnings
from typing import Callable, Type, TypeVar

import pydantic

from datahub.configuration.common import ConfigurationWarning
from datahub.utilities.global_warning_util import add_global_warning

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
                    msg = f"{old_name} is deprecated, please use {new_name} instead."
                    add_global_warning(msg)
                    warnings.warn(
                        msg,
                        ConfigurationWarning,
                        stacklevel=2,
                    )
                values[new_name] = transform(values.pop(old_name))
        return values

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field renames do not overwrite each other.
    _validate_field_rename.__name__ = f"{_validate_field_rename.__name__}_{old_name}"

    # Why aren't we using pydantic.validator here?
    # The `values` argument that is passed to field validators only contains items
    # that have already been validated in the pre-process phase, which happens if
    # they have an associated field and a pre=True validator. However, the root
    # validator with pre=True gets all the values that were passed in.
    # Given that a renamed field doesn't show up in the fields list, we can't use
    # the field-level validator, even with a different field name.
    return pydantic.root_validator(pre=True, skip_on_failure=True, allow_reuse=True)(
        _validate_field_rename
    )
