import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import SchemaFieldUrn

# Shared parsing of Copy activity `translator` blocks, used by both the Azure
# Data Factory and Fabric Data Factory connectors (same pipeline JSON model).
# https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping

TABULAR_TRANSLATOR = "TabularTranslator"
COPY_TRANSFORM_OPERATION = "COPY"

_TRANSLATOR_TYPE_KEYS = ("type", "translatorType")
_COLUMN_MAPPINGS_KEY = "columnMappings"
_MAPPINGS_KEY = "mappings"
_MAPPING_SOURCE_KEY = "source"
_MAPPING_SINK_KEY = "sink"
_COLUMN_NAME_KEY = "name"
_COLUMN_PATH_KEY = "path"
_LEGACY_PAIR_SEPARATOR = ","
_LEGACY_NAME_SEPARATOR = ":"
_JSON_PATH_ROOT = "$"

# Matches one bracketed segment of a hierarchical path, e.g. ['id'] or ["id"].
_BRACKET_SEGMENT_RE = re.compile(r"\[\s*(['\"])(.*?)\1\s*\]")


@dataclass(frozen=True)
class CopyColumnMapping:
    source_column: str
    sink_column: str


def get_translator_type(translator: Dict[str, Any]) -> Optional[str]:
    for key in _TRANSLATOR_TYPE_KEYS:
        value = translator.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def parse_translator_mappings(translator: Dict[str, Any]) -> List[CopyColumnMapping]:
    """Parse explicit column mappings from a Copy activity translator.

    Supports the legacy ``columnMappings`` (dict ``{src: sink}`` or string
    ``"src: sink, src2: sink2"``) and the current ``mappings`` list
    (``[{source: {name|path}, sink: {name|path}}]``). Legacy mappings win when
    both are present. Returns an empty list when no explicit mapping exists.
    """
    column_mappings = translator.get(_COLUMN_MAPPINGS_KEY)
    if isinstance(column_mappings, dict) and column_mappings:
        return _parse_legacy_dict(column_mappings)
    if isinstance(column_mappings, str) and column_mappings.strip():
        return _parse_legacy_string(column_mappings)

    mappings = translator.get(_MAPPINGS_KEY)
    if isinstance(mappings, list) and mappings:
        return _parse_mappings_list(mappings)

    return []


def count_configured_mappings(translator: Dict[str, Any]) -> int:
    """Count the explicit mapping entries configured on a Copy translator.

    Unlike ``parse_translator_mappings``, this also counts entries that cannot
    become a named column pair (e.g. ordinal-only mappings for header-less
    delimited text). A non-zero count means the copy does NOT use the default
    by-name mapping, so callers must not fall back to by-name inference.
    Follows the same legacy-first precedence as ``parse_translator_mappings``.
    """
    column_mappings = translator.get(_COLUMN_MAPPINGS_KEY)
    if isinstance(column_mappings, dict) and column_mappings:
        return len(column_mappings)
    if isinstance(column_mappings, str) and column_mappings.strip():
        return sum(
            1 for pair in column_mappings.split(_LEGACY_PAIR_SEPARATOR) if pair.strip()
        )

    mappings = translator.get(_MAPPINGS_KEY)
    if isinstance(mappings, list):
        return len(mappings)

    return 0


def make_copy_fine_grained_lineage(
    source_urn: str,
    source_column: str,
    sink_urn: str,
    sink_column: str,
) -> FineGrainedLineageClass:
    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=[SchemaFieldUrn(source_urn, source_column).urn()],
        downstreams=[SchemaFieldUrn(sink_urn, sink_column).urn()],
        transformOperation=COPY_TRANSFORM_OPERATION,
    )


def _parse_legacy_dict(column_mappings: Dict[Any, Any]) -> List[CopyColumnMapping]:
    result: List[CopyColumnMapping] = []
    for source_col, sink_col in column_mappings.items():
        if not source_col or not sink_col:
            continue
        result.append(CopyColumnMapping(str(source_col), str(sink_col)))
    return result


def _parse_legacy_string(column_mappings: str) -> List[CopyColumnMapping]:
    result: List[CopyColumnMapping] = []
    for pair in column_mappings.split(_LEGACY_PAIR_SEPARATOR):
        source_col, sep, sink_col = pair.partition(_LEGACY_NAME_SEPARATOR)
        source_col, sink_col = source_col.strip(), sink_col.strip()
        if not sep or not source_col or not sink_col:
            continue
        result.append(CopyColumnMapping(source_col, sink_col))
    return result


def _parse_mappings_list(mappings: List[Any]) -> List[CopyColumnMapping]:
    result: List[CopyColumnMapping] = []
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source_col = _mapping_column(mapping.get(_MAPPING_SOURCE_KEY))
        sink_col = _mapping_column(mapping.get(_MAPPING_SINK_KEY))
        if not source_col or not sink_col:
            # e.g. ordinal-only mappings for header-less delimited text
            continue
        result.append(CopyColumnMapping(source_col, sink_col))
    return result


def _mapping_column(side: Any) -> Optional[str]:
    if not isinstance(side, dict):
        return None
    name = side.get(_COLUMN_NAME_KEY)
    if isinstance(name, str) and name:
        return name
    path = side.get(_COLUMN_PATH_KEY)
    if isinstance(path, str) and path:
        return _path_to_column(path)
    return None


def _path_to_column(path: str) -> Optional[str]:
    """Convert a hierarchical mapping path to a dotted column path.

    ``$['customer']['id']`` -> ``customer.id``; ``['id']`` -> ``id``;
    ``$.customer.id`` -> ``customer.id``.
    """
    segments = [match.group(2) for match in _BRACKET_SEGMENT_RE.finditer(path)]
    if segments:
        return ".".join(segment for segment in segments if segment) or None
    stripped = path.strip()
    if stripped.startswith(_JSON_PATH_ROOT):
        stripped = stripped[len(_JSON_PATH_ROOT) :]
    stripped = stripped.strip(".")
    return stripped or None
