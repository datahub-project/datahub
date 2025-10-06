from typing import TYPE_CHECKING, Type, TypeVar, Union

import pydantic

from datahub.ingestion.api.registry import import_path

if TYPE_CHECKING:
    from pydantic.deprecated.class_validators import V1Validator

_T = TypeVar("_T")


def _pydantic_resolver(cls: Type, v: Union[str, _T]) -> _T:
    return import_path(v) if isinstance(v, str) else v


def pydantic_resolve_key(field: str) -> "V1Validator":
    return pydantic.validator(field, pre=True, allow_reuse=True)(_pydantic_resolver)
