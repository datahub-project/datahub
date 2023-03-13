import warnings
from typing import Optional, Type

import pydantic

from datahub.configuration.common import ConfigurationWarning
from datahub.utilities.global_warning_util import add_global_warning


def pydantic_field_deprecated(
    field: str, new_field: Optional[str] = None, message: Optional[str] = None
) -> classmethod:
    if message:
        output = message
    else:
        output = f"{field} is deprecated and will be removed in a future release. Please remove it from your config."

    def _validate_deprecated(cls: Type, values: dict) -> dict:
        if field in values and new_field not in values:
            add_global_warning(output)
            warnings.warn(output, ConfigurationWarning, stacklevel=2)
        return values

    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_deprecated)
