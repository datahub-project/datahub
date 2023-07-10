import warnings
from typing import Optional, Type

import pydantic

from datahub.configuration.common import ConfigurationWarning
from datahub.utilities.global_warning_util import add_global_warning


def pydantic_field_deprecated(field: str, message: Optional[str] = None) -> classmethod:
    if message:
        output = message
    else:
        output = f"{field} is deprecated and will be removed in a future release. Please remove it from your config."

    def _validate_deprecated(cls: Type, values: dict) -> dict:
        if field in values:
            add_global_warning(output)
            warnings.warn(output, ConfigurationWarning, stacklevel=2)
        return values

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field deprecated do not overwrite each other.
    _validate_deprecated.__name__ = f"{_validate_deprecated.__name__}_{field}"
    return pydantic.root_validator(pre=True, allow_reuse=True)(_validate_deprecated)
