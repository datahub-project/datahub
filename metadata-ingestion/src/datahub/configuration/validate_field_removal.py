import warnings
from typing import Any, Type

from pydantic import model_validator

from datahub.configuration.common import (
    REMOVED_FIELD_MARKER,
    ConfigurationWarning,
    RemovedField,
)


def pydantic_removed_field(
    field: str,
    month: str,
    year: int,
    print_warning: bool = True,
) -> Any:
    def _validate_field_removal(cls: Type, values: dict) -> dict:
        if field in values:
            if print_warning:
                warnings.warn(
                    f"The {field} was removed on {month} {year}, please remove it from your recipe.",
                    ConfigurationWarning,
                    stacklevel=2,
                )
            values.pop(field)
        return values

    # Consumed by ConfigModel's json_schema_extra hook.
    setattr(
        _validate_field_removal,
        REMOVED_FIELD_MARKER,
        RemovedField(name=field, month=month, year=year),
    )

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field removals do not overwrite each other.
    _validate_field_removal.__name__ = f"{_validate_field_removal.__name__}_{field}"
    return model_validator(mode="before")(_validate_field_removal)
