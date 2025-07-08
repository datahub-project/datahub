from pydantic import BaseModel as GenericModel, PydanticDeprecatedSince20
from pydantic.v1 import (  # type: ignore
    BaseModel as v1_BaseModel,
    Extra as v1_Extra,
    Field as v1_Field,
    root_validator as v1_root_validator,
    validator as v1_validator,
)


class v1_ConfigModel(v1_BaseModel):
    """A simplified variant of our main ConfigModel class.

    This one only uses pydantic v1 features.
    """

    class Config:
        extra = v1_Extra.forbid
        underscore_attrs_are_private = True


# TODO: Remove the warning ignore on PydanticDeprecatedSince20.

__all__ = [
    "PydanticDeprecatedSince20",
    "GenericModel",
    "v1_ConfigModel",
    "v1_Field",
    "v1_root_validator",
    "v1_validator",
]
