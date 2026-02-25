import warnings
from typing import TYPE_CHECKING, Any, Optional, Type

from pydantic import model_validator

from datahub.configuration.common import ConfigurationWarning
from datahub.utilities.global_warning_util import add_global_warning

if TYPE_CHECKING:
    from pydantic.deprecated.class_validators import V1RootValidator

_unset = object()


def pydantic_field_deprecated(
    field: str,
    warn_if_value_is_not: Any = _unset,
    message: Optional[str] = None,
) -> "V1RootValidator":
    if message:
        output = message
    else:
        output = f"{field} is deprecated and will be removed in a future release. Please remove it from your config."

    def _validate_deprecated(cls: Type, values: dict) -> dict:
        if field in values and (
            warn_if_value_is_not is _unset or values[field] != warn_if_value_is_not
        ):
            add_global_warning(output)
            warnings.warn(output, ConfigurationWarning, stacklevel=2)
        return values

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field deprecated do not overwrite each other.
    _validate_deprecated.__name__ = f"{_validate_deprecated.__name__}_{field}"
    return model_validator(mode="before")(_validate_deprecated)
