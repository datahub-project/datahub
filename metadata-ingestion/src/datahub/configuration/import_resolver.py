from typing import TypeVar, Union

import pydantic

from datahub.ingestion.api.registry import import_path

T = TypeVar("T")


def _pydantic_resolver(v: Union[T, str]) -> T:
    return import_path(v) if isinstance(v, str) else v


def pydantic_resolve_key(field: str) -> classmethod:
    return pydantic.validator(field, pre=True, allow_reuse=True)(_pydantic_resolver)
