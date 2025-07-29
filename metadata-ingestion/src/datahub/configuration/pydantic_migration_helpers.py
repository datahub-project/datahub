import pydantic.version
from packaging.version import Version

_pydantic_version = Version(pydantic.version.VERSION)

PYDANTIC_VERSION_2 = _pydantic_version >= Version("2.0")

# The pydantic.Discriminator type was added in v2.5.0.
# https://docs.pydantic.dev/latest/changelog/#v250-2023-11-13
PYDANTIC_SUPPORTS_CALLABLE_DISCRIMINATOR = _pydantic_version >= Version("2.5.0")

# This can be used to silence deprecation warnings while we migrate.
if PYDANTIC_VERSION_2:
    from pydantic import PydanticDeprecatedSince20  # type: ignore
else:

    class PydanticDeprecatedSince20(Warning):  # type: ignore
        pass


if PYDANTIC_VERSION_2:
    from pydantic import BaseModel as GenericModel
    from pydantic.v1 import (  # type: ignore
        BaseModel as v1_BaseModel,
        Extra as v1_Extra,
        Field as v1_Field,
        root_validator as v1_root_validator,
        validator as v1_validator,
    )
else:
    from pydantic import (  # type: ignore
        BaseModel as v1_BaseModel,
        Extra as v1_Extra,
        Field as v1_Field,
        root_validator as v1_root_validator,
        validator as v1_validator,
    )
    from pydantic.generics import GenericModel  # type: ignore


class v1_ConfigModel(v1_BaseModel):
    """A simplified variant of our main ConfigModel class.

    This one only uses pydantic v1 features.
    """

    class Config:
        extra = v1_Extra.forbid
        underscore_attrs_are_private = True


__all__ = [
    "PYDANTIC_VERSION_2",
    "PYDANTIC_SUPPORTS_CALLABLE_DISCRIMINATOR",
    "PydanticDeprecatedSince20",
    "GenericModel",
    "v1_ConfigModel",
    "v1_Field",
    "v1_root_validator",
    "v1_validator",
]
