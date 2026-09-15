from typing import Any, Type, TypeVar, Union

from pydantic import field_validator

from datahub.ingestion.api.registry import import_path

_T = TypeVar("_T")


def _pydantic_resolver(cls: Type, v: Union[str, _T]) -> _T:
    return import_path(v) if isinstance(v, str) else v


def pydantic_resolve_key(field: str) -> Any:
    return field_validator(field, mode="before")(_pydantic_resolver)
