from contextvars import ContextVar
from functools import wraps
from typing import Dict, Literal, cast

from pydantic import Field
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.typedef import Identifier
from pyiceberg.types import NestedField, PrimitiveType

VARIANT: Literal["variant"] = "variant"


class VariantType(PrimitiveType):
    root: Literal["variant"] = Field(default=VARIANT)


def _adapt_field(value: object) -> object:
    if not isinstance(value, dict):
        return value
    field = cast(Dict[str, object], value)
    return {**field, "type": _adapt_type(field.get("type"))}


def _adapt_type(value: object) -> object:
    if value == VARIANT:
        return VariantType()
    if not isinstance(value, dict):
        return value
    field_type = cast(Dict[str, object], value)
    kind = field_type.get("type")
    if kind == "struct":
        fields = field_type.get("fields")
        if isinstance(fields, list):
            return {**field_type, "fields": [_adapt_field(f) for f in fields]}
    elif kind == "list":
        return {**field_type, "element": _adapt_type(field_type.get("element"))}
    elif kind == "map":
        return {
            **field_type,
            "key": _adapt_type(field_type.get("key")),
            "value": _adapt_type(field_type.get("value")),
        }
    return value


# PyIceberg rejects variant metadata before catalog.load_table returns:
# https://github.com/apache/iceberg-python/issues/1819
# Remove this adapter when the pinned PyIceberg version can parse VariantType.
# Adapt schema inputs for metadata ingestion only; do not enable value decoding or
# mutate type names in docs/defaults. A context variable isolates concurrent loads
# from other PyIceberg callers without temporarily replacing a shared constructor.
_VARIANT_METADATA: ContextVar[bool] = ContextVar(
    "iceberg_variant_metadata", default=False
)
_schema_init = Schema.__init__


@wraps(_schema_init)
def _init_schema(self: Schema, *fields: NestedField, **data: object) -> None:
    raw_fields = data.get("fields")
    if _VARIANT_METADATA.get() and isinstance(raw_fields, (list, tuple)):
        data["fields"] = [_adapt_field(field) for field in raw_fields]
    _schema_init(self, *fields, **data)


Schema.__init__ = _init_schema  # type: ignore[method-assign]


def load_table_with_variant_support(catalog: Catalog, identifier: Identifier) -> Table:
    token = _VARIANT_METADATA.set(True)
    try:
        return catalog.load_table(identifier)
    finally:
        _VARIANT_METADATA.reset(token)
