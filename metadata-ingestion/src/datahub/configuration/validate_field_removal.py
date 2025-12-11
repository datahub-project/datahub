# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import warnings
from typing import TYPE_CHECKING, Type

from pydantic import model_validator

from datahub.configuration.common import ConfigurationWarning

if TYPE_CHECKING:
    from pydantic.deprecated.class_validators import V1RootValidator


def pydantic_removed_field(
    field: str,
    print_warning: bool = True,
) -> "V1RootValidator":
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

    # Mark the function as handling a removed field for doc generation
    _validate_field_removal._doc_removed_field = field  # type: ignore[attr-defined]

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field removals do not overwrite each other.
    _validate_field_removal.__name__ = f"{_validate_field_removal.__name__}_{field}"
    return model_validator(mode="before")(_validate_field_removal)
