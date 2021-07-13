import pydantic

from datahub.ingestion.api.registry import import_key


def _pydantic_resolver(v):
    if isinstance(v, str):
        return import_key(v)
    return v


def pydantic_resolve_key(field):
    return pydantic.validator(field, pre=True, allow_reuse=True)(_pydantic_resolver)
