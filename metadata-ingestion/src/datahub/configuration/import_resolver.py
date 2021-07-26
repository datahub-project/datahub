from typing import TypeVar, Union

import pydantic

from datahub.ingestion.api.registry import import_key

T = TypeVar("T")


def _pydantic_resolver(v: Union[T, str]) -> T:
    if isinstance(v, str):
        return import_key(v)
    return v


def pydantic_resolve_key(field: str) -> classmethod:
    return pydantic.validator(field, pre=True, allow_reuse=True)(_pydantic_resolver)
