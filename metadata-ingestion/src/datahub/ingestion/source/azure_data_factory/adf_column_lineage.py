"""Column-level lineage extraction for Azure Data Factory activities.

This module provides an extensible framework for extracting column-level lineage
from different ADF activity types. Currently supports Copy Activity with three
mapping modes:
1. Legacy dictionary format (columnMappings: {src: sink})
2. Current list format (mappings: [{source: {name}, sink: {name}}])
3. Auto-mapping inference from source dataset schema
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sdk._shared import DatasetUrnOrStr

logger = logging.getLogger(__name__)


@runtime_checkable
class ActivityProtocol(Protocol):
    """Protocol defining the minimal interface for ADF activity objects.

    This protocol defines only what's strictly required - the activity name.
    Other attributes (translator, type_properties) are accessed via getattr
    since they may not be present on all Activity implementations.
    """

    @property
    def name(self) -> str:
        """Activity name."""
        ...


@dataclass
class DatasetSchemaInfo:
    """Schema information for a dataset.

    Holds the column names extracted from a dataset's schema definition
    or structure property. Used as input for auto-mapping inference.
    """

    columns: list[str]
    # True for a schema ingested into DataHub by the dataset's own source,
    # which lists every column. An ADF dataset's structure/schema may list
    # only some of them.
    from_datahub: bool = False


# Type alias for schema resolver callback
SchemaResolver = Callable[[str], Optional[DatasetSchemaInfo]]


class ColumnLineageExtractor(ABC):
    """Abstract base class for activity-specific column lineage extractors.

    Subclasses implement extraction logic for specific ADF activity types.
    This enables extensibility - new activity types can be supported by
    adding new extractor implementations.

    Each subclass is responsible for:
    - Selecting which inlets/outlets to use for column lineage
    - Warning users if the activity has unsupported configurations
    - Using the schema resolver to fetch schemas as needed
    """

    @abstractmethod
    def supports_activity(self, activity_type: str) -> bool:
        """Check if this extractor supports the given activity type.

        Args:
            activity_type: The ADF activity type (e.g., "Copy", "ExecuteDataFlow")

        Returns:
            True if this extractor can handle the activity type
        """
        pass

    @abstractmethod
    def extract_column_lineage(
        self,
        activity: ActivityProtocol,
        inlets: list[DatasetUrnOrStr],
        outlets: list[DatasetUrnOrStr],
        schema_resolver: SchemaResolver,
    ) -> list[FineGrainedLineageClass]:
        """Extract column-level lineage from an activity.

        The extractor is responsible for selecting which inlets/outlets to use
        and warning if the activity configuration isn't fully supported.

        Args:
            activity: The ADF activity object (Azure SDK model or mock)
            inlets: All input dataset URNs for this activity
            outlets: All output dataset URNs for this activity
            schema_resolver: Callback to fetch schema for a dataset URN

        Returns:
            List of FineGrainedLineageClass, empty if no mappings could be extracted
        """
        pass


def get_activity_translator_config(
    activity: ActivityProtocol,
) -> Optional[dict[str, Any]]:
    """Get an activity's translator (column mapping) configuration, if
    any. The Azure SDK sometimes flattens CopyActivity properties to the
    activity level rather than nesting them in typeProperties.

    Also used outside this module (adf_source.py) to detect whether an
    activity has its own explicit column mapping before attempting any
    other column-lineage recovery, so both code paths agree on what
    counts as "explicit".
    """
    # Try SDK-flattened attribute first
    translator = activity.translator if hasattr(activity, "translator") else None
    if translator is not None:
        # Could be a dict or an SDK object - normalize to dict
        if hasattr(translator, "as_dict"):
            return translator.as_dict()
        if isinstance(translator, dict):
            return translator

    # Fall back to typeProperties (raw JSON or mock data)
    type_properties = (
        activity.type_properties if hasattr(activity, "type_properties") else None
    )
    if type_properties and isinstance(type_properties, dict):
        translator = type_properties.get("translator")
        if isinstance(translator, dict):
            return translator

    return None


def create_fine_grained_lineage(
    source_urn: str,
    source_column: str,
    sink_urn: str,
    sink_column: str,
) -> FineGrainedLineageClass:
    """Create a FineGrainedLineageClass for a single column mapping.

    Also used outside this module (adf_source.py) to build column-level
    lineage resolved via a sibling activity's run output, reusing the
    same field-URN construction as the static translator path.
    """
    upstream_field_urn = SchemaFieldUrn(source_urn, source_column)
    downstream_field_urn = SchemaFieldUrn(sink_urn, sink_column)

    return FineGrainedLineageClass(
        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
        upstreams=[str(upstream_field_urn.urn())],
        downstreams=[str(downstream_field_urn.urn())],
        transformOperation="COPY",
    )


def parse_dict_translator_format(
    column_mappings: dict[str, str],
    source_urn: str,
    sink_urn: str,
) -> list[FineGrainedLineageClass]:
    """Parse legacy dictionary format: {source_col: sink_col}."""
    lineages: list[FineGrainedLineageClass] = []
    for source_col, sink_col in column_mappings.items():
        # Skip empty column names
        if not source_col or not sink_col:
            continue
        lineages.append(
            create_fine_grained_lineage(
                source_urn, str(source_col), sink_urn, str(sink_col)
            )
        )
    return lineages


def parse_list_translator_format(
    mappings_list: list[dict[str, Any]],
    source_urn: str,
    sink_urn: str,
) -> list[FineGrainedLineageClass]:
    """Parse current list format: [{source: {name}, sink: {name}}]."""
    lineages: list[FineGrainedLineageClass] = []
    for mapping in mappings_list:
        if not isinstance(mapping, dict):
            continue

        source_info = mapping.get("source", {})
        sink_info = mapping.get("sink", {})

        if not isinstance(source_info, dict) or not isinstance(sink_info, dict):
            continue

        source_col = source_info.get("name")
        sink_col = sink_info.get("name")

        # Skip empty column names
        if not source_col or not sink_col:
            continue

        lineages.append(
            create_fine_grained_lineage(
                source_urn, str(source_col), sink_urn, str(sink_col)
            )
        )
    return lineages


def parse_translator_column_mappings(
    translator: dict[str, Any],
    source_urn: str,
    sink_urn: str,
) -> list[FineGrainedLineageClass]:
    """Parse column mappings from a translator configuration.

    Handles both legacy dict format and current list format. Also used
    outside this module (adf_source.py) to convert a translator resolved
    via a sibling activity's run output into the same
    FineGrainedLineageClass shape produced by the static translator path,
    so both agree on how translator configs are read.
    """
    # Mode 1: Legacy dictionary format - columnMappings: {src: sink}
    column_mappings = translator.get("columnMappings")
    if column_mappings and isinstance(column_mappings, dict):
        return parse_dict_translator_format(column_mappings, source_urn, sink_urn)

    # Mode 2: Current list format - mappings: [{source: {name}, sink: {name}}]
    mappings_list = translator.get("mappings")
    if mappings_list and isinstance(mappings_list, list):
        return parse_list_translator_format(mappings_list, source_urn, sink_urn)

    return []


def translator_has_explicit_mappings(translator: dict[str, Any]) -> bool:
    """Whether a translator configuration specifies actual column
    mappings, as opposed to being present but empty (e.g. a bare
    {"type": "TabularTranslator"} with no columnMappings/mappings). ADF
    itself falls back to its own default name-based auto-mapping in that
    case, exactly as when no translator is configured at all - callers
    deciding whether to attempt auto-mapping recovery must treat an
    empty translator the same as a missing one, not as an explicit one
    that should block the fallback."""
    column_mappings = translator.get("columnMappings")
    if column_mappings and isinstance(column_mappings, dict):
        return True
    mappings_list = translator.get("mappings")
    return bool(mappings_list and isinstance(mappings_list, list))


# Called with (sink_urn, column) when a source column has no unique
# case-insensitive match in the sink's known schema.
UnmatchedSinkColumnCallback = Callable[[str, str], None]


@dataclass(frozen=True)
class SinkColumnCasing:
    """Resolves auto-mapped column names to the field paths DataHub stores
    for the sink, so the downstream schemaField URN joins to a real field
    (e.g. source "Ship_Date" vs sink "ship_date").

    Matching is case-insensitive even though ADF's own default mapping is
    case-sensitive: DataHub may store a platform's column names in a
    different case than the source query or dataset reports them. Sink
    columns that differ only by case are ambiguous and never matched.
    """

    columns_by_lower: dict[str, str]
    # Only a complete schema (from DataHub) proves an unmatched column is
    # not in the sink.
    drop_unmatched: bool

    @classmethod
    def from_schema(cls, schema: Optional[DatasetSchemaInfo]) -> "SinkColumnCasing":
        if not schema or not schema.columns:
            return cls(columns_by_lower={}, drop_unmatched=False)
        columns_by_lower: dict[str, str] = {}
        ambiguous: set[str] = set()
        for col in schema.columns:
            if not col:
                continue
            key = col.lower()
            if key in columns_by_lower and columns_by_lower[key] != col:
                ambiguous.add(key)
            columns_by_lower.setdefault(key, col)
        for key in ambiguous:
            del columns_by_lower[key]
        return cls(
            columns_by_lower=columns_by_lower, drop_unmatched=schema.from_datahub
        )

    def resolve(self, column: str) -> Optional[str]:
        """The sink's casing for `column`. With no unique match: None when the
        sink schema came from DataHub, since a URN for that field would point
        at nothing; otherwise the column unchanged."""
        match = self.columns_by_lower.get(column.lower())
        if match is not None or self.drop_unmatched:
            return match
        return column


def infer_auto_mappings_by_name(
    source_urn: str,
    sink_urn: str,
    source_schema: Optional[DatasetSchemaInfo],
    sink_schema: Optional[DatasetSchemaInfo] = None,
    on_unmatched_sink_column: Optional[UnmatchedSinkColumnCallback] = None,
) -> list[FineGrainedLineageClass]:
    """Infer 1:1 column mappings from a source schema by name, as ADF does
    for a Copy activity with no explicit mappings. Also used by adf_source.py
    with a schema looked up from DataHub (e.g. for a "SELECT *" query source).

    Reference: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping#default-mapping
    """
    if not source_schema or not source_schema.columns:
        return []

    sink_casing = SinkColumnCasing.from_schema(sink_schema)

    lineages: list[FineGrainedLineageClass] = []
    for col in source_schema.columns:
        if not col:
            continue
        sink_column = sink_casing.resolve(col)
        if sink_column is None:
            if on_unmatched_sink_column is not None:
                on_unmatched_sink_column(sink_urn, col)
            continue
        lineages.append(
            create_fine_grained_lineage(source_urn, col, sink_urn, sink_column)
        )
    return lineages


class CopyActivityColumnLineageExtractor(ColumnLineageExtractor):
    """Extracts column-level lineage from ADF Copy activities.

    Copy Activity has exactly one source and one sink. If multiple inlets/outlets
    are provided (which shouldn't happen for Copy activities), a warning is logged
    and only the first pair is used.

    Supports three mapping formats:
    1. Legacy dict format: translator.columnMappings = {"src_col": "sink_col"}
    2. List format: translator.mappings = [{source: {name: "src"}, sink: {name: "sink"}}]
    3. Auto-mapping: When translator type is TabularTranslator with no explicit mappings,
       infers 1:1 column mappings from source schema

    The translator can be found either:
    - Directly on the activity (SDK flattens CopyActivity properties)
    - Inside activity.type_properties["translator"]

    References:
    - https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
    - https://learn.microsoft.com/en-us/javascript/api/@azure/arm-datafactory/tabulartranslator
    """

    def __init__(
        self, on_unmatched_sink_column: Optional[UnmatchedSinkColumnCallback] = None
    ) -> None:
        self._on_unmatched_sink_column = on_unmatched_sink_column

    def supports_activity(self, activity_type: str) -> bool:
        """Only supports Copy activities."""
        return activity_type == "Copy"

    def extract_column_lineage(
        self,
        activity: ActivityProtocol,
        inlets: list[DatasetUrnOrStr],
        outlets: list[DatasetUrnOrStr],
        schema_resolver: SchemaResolver,
    ) -> list[FineGrainedLineageClass]:
        """Extract column lineage from a Copy activity's translator configuration.

        Copy Activity has a single source and single sink. If multiple inlets/outlets
        are present, a warning is logged and only the first pair is used.
        """
        if not inlets or not outlets:
            logger.debug(
                "Skipping column lineage extraction: no inlets or outlets provided"
            )
            return []

        # Copy Activity should have exactly one source and one sink
        # Warn if there are multiple (unexpected for Copy Activity)
        if len(inlets) > 1 or len(outlets) > 1:
            logger.warning(
                f"Copy Activity '{activity.name}' has multiple inputs ({len(inlets)}) "
                f"or outputs ({len(outlets)}). Copy Activity typically has one source "
                f"and one sink. Column-level lineage will use the first input/output pair."
            )

        # Use first inlet/outlet (Copy Activity semantics)
        source_urn = str(inlets[0])
        sink_urn = str(outlets[0])

        translator = self._get_translator(activity)
        if not translator:
            return self._infer_auto_mappings(source_urn, sink_urn, schema_resolver)

        lineages = self._parse_translator(translator, source_urn, sink_urn)

        # A TabularTranslator with no explicit mappings means ADF's default
        # name-based mapping.
        if (
            not lineages
            and self._get_translator_type(translator) == "TabularTranslator"
        ):
            lineages = self._infer_auto_mappings(source_urn, sink_urn, schema_resolver)

        return lineages

    def _get_translator(self, activity: ActivityProtocol) -> Optional[dict[str, Any]]:
        """Get the translator configuration from the activity.

        The Azure SDK sometimes flattens CopyActivity properties to the activity
        level rather than nesting them in typeProperties.
        """
        return get_activity_translator_config(activity)

    def _get_translator_type(self, translator: dict[str, Any]) -> Optional[str]:
        """Get the translator type from the translator configuration."""
        return translator.get("type") or translator.get("translatorType")

    def _create_fine_grained_lineage(
        self,
        source_urn: str,
        source_column: str,
        sink_urn: str,
        sink_column: str,
    ) -> FineGrainedLineageClass:
        """Create a FineGrainedLineageClass for a single column mapping."""
        return create_fine_grained_lineage(
            source_urn, source_column, sink_urn, sink_column
        )

    def _parse_translator(
        self,
        translator: dict[str, Any],
        source_urn: str,
        sink_urn: str,
    ) -> list[FineGrainedLineageClass]:
        """Parse column mappings from translator configuration.

        Handles both legacy dict format and current list format.
        """
        return parse_translator_column_mappings(translator, source_urn, sink_urn)

    def _parse_dict_format(
        self,
        column_mappings: dict[str, str],
        source_urn: str,
        sink_urn: str,
    ) -> list[FineGrainedLineageClass]:
        """Parse legacy dictionary format: {source_col: sink_col}."""
        return parse_dict_translator_format(column_mappings, source_urn, sink_urn)

    def _parse_list_format(
        self,
        mappings_list: list[dict[str, Any]],
        source_urn: str,
        sink_urn: str,
    ) -> list[FineGrainedLineageClass]:
        """Parse current list format: [{source: {name}, sink: {name}}]."""
        return parse_list_translator_format(mappings_list, source_urn, sink_urn)

    def _infer_auto_mappings(
        self, source_urn: str, sink_urn: str, schema_resolver: SchemaResolver
    ) -> list[FineGrainedLineageClass]:
        """Same-name mappings from the source schema. The sink schema is only
        looked up once the source has columns to map."""
        source_schema = schema_resolver(source_urn)
        if not source_schema or not source_schema.columns:
            return []
        return infer_auto_mappings_by_name(
            source_urn,
            sink_urn,
            source_schema,
            schema_resolver(sink_urn),
            self._on_unmatched_sink_column,
        )
