import logging
from typing import Dict, Optional, Tuple, Type, Union

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


_FieldTypeClass = Union[
    Type[BooleanTypeClass],
    Type[BytesTypeClass],
    Type[DateTypeClass],
    Type[NumberTypeClass],
    Type[StringTypeClass],
    Type[TimeTypeClass],
]
# Exact (normalized) native scalar type name -> DataHub type class. Matching is by
# exact base name — not substring — so unrelated types are not misclassified
# (e.g. "interval" must not read as "int", and "array<int>" must not read as a
# number). Parameters like "varchar(255)"/"decimal(10,2)" are stripped before
# lookup; compound/container types are handled separately below.
_NATIVE_TYPE_ALIASES: Dict[str, _FieldTypeClass] = {
    "bool": BooleanTypeClass,
    "boolean": BooleanTypeClass,
    "timestamp": TimeTypeClass,
    "timestamptz": TimeTypeClass,
    "datetime": TimeTypeClass,
    "time": TimeTypeClass,
    "timetz": TimeTypeClass,
    "date": DateTypeClass,
    "bigint": NumberTypeClass,
    "int8": NumberTypeClass,
    "integer": NumberTypeClass,
    "int": NumberTypeClass,
    "int4": NumberTypeClass,
    "smallint": NumberTypeClass,
    "int2": NumberTypeClass,
    "tinyint": NumberTypeClass,
    "numeric": NumberTypeClass,
    "number": NumberTypeClass,
    "decimal": NumberTypeClass,
    "float": NumberTypeClass,
    "float4": NumberTypeClass,
    "float8": NumberTypeClass,
    "double": NumberTypeClass,
    "real": NumberTypeClass,
    "binary": BytesTypeClass,
    "varbinary": BytesTypeClass,
    "bytea": BytesTypeClass,
    "byte": BytesTypeClass,
    "varchar": StringTypeClass,
    "nvarchar": StringTypeClass,
    "char": StringTypeClass,
    "nchar": StringTypeClass,
    "text": StringTypeClass,
    "string": StringTypeClass,
    "uuid": StringTypeClass,
}

# Markers indicating a compound/container type that has no scalar equivalent in
# the set above; these fall back to string rather than matching a scalar alias.
_CONTAINER_MARKERS: Tuple[str, ...] = (
    "<",
    "[",
    "array",
    "map",
    "struct",
    "json",
    "variant",
    "object",
    "row",
)


def resolve_datahub_field_type(native_type: Optional[str]) -> SchemaFieldDataTypeClass:
    # Map a source-declared native type to the closest DataHub type class so numeric,
    # boolean, and temporal columns are not all surfaced as strings. Unknown, compound,
    # or empty types fall back to string.
    string_type = SchemaFieldDataTypeClass(type=StringTypeClass())
    if not native_type:
        return string_type

    lowered = native_type.strip().lower()
    if any(marker in lowered for marker in _CONTAINER_MARKERS):
        return string_type

    # Strip type parameters: "varchar(255)" -> "varchar", "decimal(10,2)" -> "decimal".
    base = lowered.split("(", 1)[0].strip()
    type_class = _NATIVE_TYPE_ALIASES.get(base)
    if type_class is None:
        # Fall back to the leading token so multi-word types still resolve, e.g.
        # "double precision" -> "double", "timestamp with time zone" -> "timestamp".
        type_class = _NATIVE_TYPE_ALIASES.get(base.split(" ", 1)[0])
    if type_class is not None:
        return SchemaFieldDataTypeClass(type=type_class())
    return string_type


def reraise_if_programming_error(e: Exception, context: str) -> None:
    # Called first inside best-effort `except Exception` blocks so real bugs fail
    # fast while operational failures fall through to the caller's degrade path.
    if isinstance(e, _PROGRAMMING_ERRORS):
        logger.error(
            f"Programming error {context}: {type(e).__name__}: {e}",
            exc_info=True,
        )
        raise e
