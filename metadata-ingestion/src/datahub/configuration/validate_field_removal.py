import warnings
from typing import Type

import pydantic

from datahub.configuration.common import ConfigurationWarning


def pydantic_removed_field(
    field: str,
    print_warning: bool = True,
) -> classmethod:
    def _validate_field_removal(cls: Type, values: dict) -> dict:
        if field in values:
            if print_warning:
                warnings.warn(
                    f"The {field} was removed, please remove it from your recipe.",
                    ConfigurationWarning,
                    stacklevel=2,
                )
            values.pop(field)
        return values

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field removals do not overwrite each other.
    _validate_field_removal.__name__ = f"{_validate_field_removal.__name__}_{field}"
    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_field_removal)
