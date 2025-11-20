from typing import TYPE_CHECKING, Optional, Type, Union

import pydantic
from pydantic import field_validator

if TYPE_CHECKING:
    from pydantic.deprecated.class_validators import V1Validator


def pydantic_multiline_string(field: str) -> "V1Validator":
    """If the field is present and contains an escaped newline, replace it with a real newline.

    This makes the assumption that the field value is never supposed to have a
    r"\n" in it, and instead should only have newline characters. This is generally
    a safe assumption for SSH keys and similar.

    The purpose of this helper is to make us more forgiving of small formatting issues
    in recipes, without sacrificing correctness across the board.
    """

    def _validate_field(
        cls: Type, v: Union[None, str, pydantic.SecretStr]
    ) -> Optional[str]:
        if v is not None:
            if isinstance(v, pydantic.SecretStr):
                v = v.get_secret_value()
            v = v.replace(r"\n", "\n")

        return v

    # Hack: Pydantic maintains unique list of validators by referring its __name__.
    # https://github.com/pydantic/pydantic/blob/v1.10.9/pydantic/main.py#L264
    # This hack ensures that multiple field deprecated do not overwrite each other.
    _validate_field.__name__ = f"{_validate_field.__name__}_{field}"
    return field_validator(field, mode="before")(_validate_field)
