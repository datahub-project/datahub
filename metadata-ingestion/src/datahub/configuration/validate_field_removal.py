import warnings
from typing import Type

import pydantic

from datahub.configuration.common import ConfigurationWarning


def pydantic_removed_field(
    field: str,
    print_warning: bool = True,
) -> classmethod:
    def _validate_field_rename(cls: Type, values: dict) -> dict:
        if field in values:
            if print_warning:
                warnings.warn(
                    f"The {field} was removed, please remove it from your recipe.",
                    ConfigurationWarning,
                    stacklevel=2,
                )
            values.pop(field)
        return values

    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_field_rename)
