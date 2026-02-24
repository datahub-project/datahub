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

        # Get source schema for auto-mapping inference
        # The schema resolver may raise an exception if the backend is unavailable
        source_schema: Optional[DatasetSchemaInfo] = None
        try:
            source_schema = schema_resolver(source_urn)
        except Exception as e:
            logger.debug(f"Failed to resolve schema for {source_urn}: {e}")

        # Get translator - check SDK-flattened attribute first, then typeProperties
        translator = self._get_translator(activity)
        if not translator:
            # No explicit translator - try auto-mapping if we have source schema
            return self._infer_auto_mappings(source_urn, sink_urn, source_schema)

        # Extract mappings based on translator format
        lineages = self._parse_translator(translator, source_urn, sink_urn)

        # If no explicit mappings found but translator is TabularTranslator,
        # try auto-mapping inference
        if not lineages:
            translator_type = self._get_translator_type(translator)
            if translator_type == "TabularTranslator":
                lineages = self._infer_auto_mappings(
                    source_urn, sink_urn, source_schema
                )

        return lineages

    def _get_translator(self, activity: ActivityProtocol) -> Optional[dict[str, Any]]:
        """Get the translator configuration from the activity.

        The Azure SDK sometimes flattens CopyActivity properties to the activity
        level rather than nesting them in typeProperties.
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
        upstream_field_urn = SchemaFieldUrn(source_urn, source_column)
        downstream_field_urn = SchemaFieldUrn(sink_urn, sink_column)

        return FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            upstreams=[str(upstream_field_urn.urn())],
            downstreams=[str(downstream_field_urn.urn())],
            transformOperation="COPY",
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
        # Mode 1: Legacy dictionary format - columnMappings: {src: sink}
        column_mappings = translator.get("columnMappings")
        if column_mappings and isinstance(column_mappings, dict):
            return self._parse_dict_format(column_mappings, source_urn, sink_urn)

        # Mode 2: Current list format - mappings: [{source: {name}, sink: {name}}]
        mappings_list = translator.get("mappings")
        if mappings_list and isinstance(mappings_list, list):
            return self._parse_list_format(mappings_list, source_urn, sink_urn)

        return []

    def _parse_dict_format(
        self,
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
                self._create_fine_grained_lineage(
                    source_urn, str(source_col), sink_urn, str(sink_col)
                )
            )
        return lineages

    def _parse_list_format(
        self,
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
                self._create_fine_grained_lineage(
                    source_urn, str(source_col), sink_urn, str(sink_col)
                )
            )
        return lineages

    def _infer_auto_mappings(
        self,
        source_urn: str,
        sink_urn: str,
        source_schema: Optional[DatasetSchemaInfo],
    ) -> list[FineGrainedLineageClass]:
        """Infer 1:1 column mappings from source schema.

        When no explicit column mappings are configured and the translator type
        is TabularTranslator, ADF auto-maps columns by name. We replicate this
        behavior by creating identity mappings for each source column.

        Reference: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping#default-mapping
        """
        if not source_schema or not source_schema.columns:
            return []

        lineages: list[FineGrainedLineageClass] = []
        for col in source_schema.columns:
            if not col:
                continue
            lineages.append(
                self._create_fine_grained_lineage(
                    source_urn,
                    col,
                    sink_urn,
                    col,  # Auto-mapped: same column name
                )
            )
        return lineages
