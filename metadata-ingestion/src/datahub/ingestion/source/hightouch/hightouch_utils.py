import logging
from typing import Dict, List, Optional, Tuple, Type, Union

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    TimeTypeClass,
)

logger = logging.getLogger(__name__)

# Shared, run-scoped cache of SchemaMetadata keyed by dataset URN. The preload,
# model-schema, and lineage steps all resolve the same upstream/destination URNs,
# so caching here avoids up to four DataHub reads per table model.
SchemaMetadataCache = Dict[str, Optional[SchemaMetadataClass]]


def fetch_schema_metadata_cached(
    graph: DataHubGraph, cache: SchemaMetadataCache, urn: str
) -> Optional[SchemaMetadataClass]:
    if urn in cache:
        return cache[urn]
    result = graph.get_schema_metadata(urn)
    cache[urn] = result
    return result


# Exceptions that signal a bug in our own code rather than an operational or
# environmental failure. These are re-raised (fail fast) instead of degraded.
_PROGRAMMING_ERRORS = (AttributeError, TypeError, KeyError, ValueError)


def normalize_column_name(name: str) -> str:
    return name.lower().replace("_", "").replace("-", "")


# Substrings tested against a lowercased native type, mapped to a DataHub type
# class. Ordered by specificity: more specific patterns (e.g. "timestamp",
# "datetime") come before broader ones ("time", "date", "int") so the first match
# wins. Unknown types fall back to string.
_FieldTypeClass = Union[
    Type[BooleanTypeClass],
    Type[BytesTypeClass],
    Type[DateTypeClass],
    Type[NumberTypeClass],
    Type[StringTypeClass],
    Type[TimeTypeClass],
]
_NATIVE_TYPE_PATTERNS: List[Tuple[str, _FieldTypeClass]] = [
    ("bool", BooleanTypeClass),
    ("timestamp", TimeTypeClass),
    ("datetime", TimeTypeClass),
    ("date", DateTypeClass),
    ("time", TimeTypeClass),
    ("bigint", NumberTypeClass),
    ("smallint", NumberTypeClass),
    ("tinyint", NumberTypeClass),
    ("int", NumberTypeClass),
    ("numeric", NumberTypeClass),
    ("number", NumberTypeClass),
    ("decimal", NumberTypeClass),
    ("float", NumberTypeClass),
    ("double", NumberTypeClass),
    ("real", NumberTypeClass),
    ("binary", BytesTypeClass),
    ("byte", BytesTypeClass),
    ("varchar", StringTypeClass),
    ("char", StringTypeClass),
    ("text", StringTypeClass),
    ("string", StringTypeClass),
    ("uuid", StringTypeClass),
]


def resolve_datahub_field_type(native_type: Optional[str]) -> SchemaFieldDataTypeClass:
    # Map a source-declared native type to the closest DataHub type class so numeric,
    # boolean, and temporal columns are not all surfaced as strings. Unknown or empty
    # types fall back to string.
    if native_type:
        lowered = native_type.lower()
        for pattern, type_class in _NATIVE_TYPE_PATTERNS:
            if pattern in lowered:
                return SchemaFieldDataTypeClass(type=type_class())
    return SchemaFieldDataTypeClass(type=StringTypeClass())


def reraise_if_programming_error(e: Exception, context: str) -> None:
    # Called first inside best-effort `except Exception` blocks so real bugs fail
    # fast while operational failures fall through to the caller's degrade path.
    if isinstance(e, _PROGRAMMING_ERRORS):
        logger.error(
            f"Programming error {context}: {type(e).__name__}: {e}",
            exc_info=True,
        )
        raise e
