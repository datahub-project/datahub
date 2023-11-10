import pydantic.version
from packaging.version import Version

PYDANTIC_VERSION_2: bool
if Version(pydantic.version.VERSION) >= Version("2.0"):
    PYDANTIC_VERSION_2 = True
else:
    PYDANTIC_VERSION_2 = False


# This can be used to silence deprecation warnings while we migrate.
if PYDANTIC_VERSION_2:
    from pydantic import PydanticDeprecatedSince20  # type: ignore
else:

    class PydanticDeprecatedSince20(Warning):  # type: ignore
        pass


if PYDANTIC_VERSION_2:
    from pydantic import BaseModel as GenericModel
else:
    from pydantic.generics import GenericModel  # type: ignore


__all__ = [
    "PYDANTIC_VERSION_2",
    "PydanticDeprecatedSince20",
    "GenericModel",
]
