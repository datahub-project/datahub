"""Column-level lineage extraction for Azure Data Factory activities.

This module provides an extensible framework for extracting column-level lineage
from different ADF activity types. Each activity type that supports deterministic
column mappings has its own extractor implementation.

Supported activity types:
- Copy Activity: Extracts from translator.columnMappings or translator.mappings
- Copy Activity (auto-mapping): Infers 1:1 column mapping from source dataset schema
  when TabularTranslator is configured without explicit column mappings

Future activity types:
- ExecuteDataFlow: Parse Data Flow script for mapColumn(), select(), derive()
- Script/StoredProcedure: SQL parsing for column references
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional

from datahub.ingestion.source.azure_data_factory.adf_models import Activity

logger = logging.getLogger(__name__)


@dataclass
class DatasetSchemaInfo:
    """Schema information for a dataset, used for auto-mapping inference.

    Attributes:
        columns: List of column names in the dataset
    """

    columns: List[str]


@dataclass
class ColumnMapping:
    """Represents a single column-to-column mapping for fine-grained lineage.

    Attributes:
        source_dataset_urn: URN of the source dataset
        source_column: Name of the source column
        sink_dataset_urn: URN of the sink/destination dataset
        sink_column: Name of the sink/destination column
    """

    source_dataset_urn: str
    source_column: str
    sink_dataset_urn: str
    sink_column: str


class ColumnLineageExtractor(ABC):
    """Base class for activity-specific column lineage extractors.

    Implement this class to add column-level lineage support for new activity types.
    Register implementations in AzureDataFactorySource._column_lineage_extractors.
    """

    @abstractmethod
    def supports_activity(self, activity_type: str) -> bool:
        """Return True if this extractor handles the given activity type.

        Args:
            activity_type: The ADF activity type (e.g., "Copy", "ExecuteDataFlow")

        Returns:
            True if this extractor can process the activity type
        """
        pass

    @abstractmethod
    def extract_column_lineage(
        self,
        activity: Activity,
        source_dataset_urn: Optional[str],
        sink_dataset_urn: Optional[str],
        source_schema: Optional[DatasetSchemaInfo] = None,
    ) -> List[ColumnMapping]:
        """Extract column mappings from the activity.

        Args:
            activity: The ADF activity containing column mapping information
            source_dataset_urn: URN of the resolved source dataset (may be None)
            sink_dataset_urn: URN of the resolved sink dataset (may be None)
            source_schema: Schema info for the source dataset (for auto-mapping inference)

        Returns:
            List of ColumnMapping objects representing column-to-column lineage
        """
        pass


class CopyActivityColumnLineageExtractor(ColumnLineageExtractor):
    """Extracts column lineage from Copy Activity's translator.columnMappings.

    Supports three modes based on Azure Data Factory's TabularTranslator formats.

    Mode 1 - Explicit Dictionary format (legacy, deprecated but still supported):
        {"translator": {"type": "TabularTranslator", "columnMappings": {"src": "sink"}}}

    Mode 2 - Explicit List format (current, recommended):
        {"translator": {"type": "TabularTranslator", "mappings": [
            {"source": {"name": "col1"}, "sink": {"name": "dest1"}}
        ]}}

    Mode 3 - Auto-mapping inference:
        When translator has no explicit mappings but source dataset schema is available,
        infer 1:1 column mappings (source_col -> source_col) based on TabularTranslator
        default behavior.

    References:
        - Schema and data type mapping in copy activity:
          https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
        - TabularTranslator interface:
          https://learn.microsoft.com/en-us/javascript/api/@azure/arm-datafactory/tabulartranslator
    """

    def supports_activity(self, activity_type: str) -> bool:
        """Return True for Copy activities."""
        return activity_type == "Copy"

    def extract_column_lineage(
        self,
        activity: Activity,
        source_dataset_urn: Optional[str],
        sink_dataset_urn: Optional[str],
        source_schema: Optional[DatasetSchemaInfo] = None,
    ) -> List[ColumnMapping]:
        """Extract column mappings from Copy Activity's translator property.

        The Azure SDK returns translator in two ways:
        1. At activity level (activity.translator) - for SDK-parsed responses
        2. Inside typeProperties (activity.type_properties["translator"]) - for raw JSON

        If no explicit column mappings are found but source schema is available,
        infers 1:1 column mappings based on TabularTranslator auto-mapping behavior.

        Args:
            activity: The Copy activity
            source_dataset_urn: URN of the source dataset
            sink_dataset_urn: URN of the sink dataset
            source_schema: Schema info for source dataset (for auto-mapping inference)

        Returns:
            List of ColumnMapping objects, empty if no mappings found
        """
        if not source_dataset_urn or not sink_dataset_urn:
            logger.debug(
                f"Skipping column lineage for Copy activity '{activity.name}': "
                f"missing dataset URNs (source={source_dataset_urn}, sink={sink_dataset_urn})"
            )
            return []

        # Try to get translator from multiple locations:
        # 1. activity.translator - Azure SDK flattens Copy properties to activity level
        # 2. activity.type_properties["translator"] - Raw JSON format
        translator = activity.translator
        if not translator and activity.type_properties:
            translator = activity.type_properties.get("translator")

        if not translator:
            logger.debug(
                f"Copy activity '{activity.name}' has no translator, "
                "skipping column lineage"
            )
            return []

        mappings: List[ColumnMapping] = []

        # Try Mode 1: Legacy dictionary format {"columnMappings": {"src": "sink"}}
        # Ref: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
        # Note: columnMappings is deprecated but still supported for backward compatibility
        column_mappings_dict = translator.get("columnMappings")
        if column_mappings_dict and isinstance(column_mappings_dict, dict):
            mappings.extend(
                self._parse_dict_format(
                    column_mappings_dict, source_dataset_urn, sink_dataset_urn
                )
            )

        # Try Mode 2: Current list format {"mappings": [{source: {}, sink: {}}]}
        # Ref: https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-schema-and-type-mapping
        # Note: This is the recommended format; ADF authoring UI generates this format
        mappings_list = translator.get("mappings")
        if mappings_list and isinstance(mappings_list, list):
            mappings.extend(
                self._parse_list_format(
                    mappings_list, source_dataset_urn, sink_dataset_urn
                )
            )

        # Mode 3: Auto-mapping inference from source schema
        # If no explicit mappings and source schema is available, infer 1:1 mappings
        if not mappings and source_schema and source_schema.columns:
            translator_type = translator.get("type", "")
            if translator_type == "TabularTranslator":
                mappings.extend(
                    self._infer_auto_mappings(
                        source_schema, source_dataset_urn, sink_dataset_urn
                    )
                )
                if mappings:
                    logger.debug(
                        f"Inferred {len(mappings)} auto-mapped columns from source "
                        f"schema for Copy activity '{activity.name}'"
                    )

        if mappings:
            logger.debug(
                f"Extracted {len(mappings)} column mappings from Copy activity "
                f"'{activity.name}'"
            )
        else:
            logger.debug(
                f"Copy activity '{activity.name}' has translator but no column mappings "
                "(no explicit mappings and no source schema for inference)"
            )

        return mappings

    def _infer_auto_mappings(
        self,
        source_schema: DatasetSchemaInfo,
        source_urn: str,
        sink_urn: str,
    ) -> List[ColumnMapping]:
        """Infer 1:1 column mappings from source schema (auto-mapping behavior).

        TabularTranslator with no explicit columnMappings maps columns by name.

        Args:
            source_schema: Schema info with column names
            source_urn: Source dataset URN
            sink_urn: Sink dataset URN

        Returns:
            List of ColumnMapping objects with source_col -> source_col mappings
        """
        return [
            ColumnMapping(
                source_dataset_urn=source_urn,
                source_column=col,
                sink_dataset_urn=sink_urn,
                sink_column=col,  # Auto-mapped: same column name
            )
            for col in source_schema.columns
            if col  # Skip empty column names
        ]

    def _parse_dict_format(
        self,
        column_mappings: dict[str, str],
        source_urn: str,
        sink_urn: str,
    ) -> List[ColumnMapping]:
        """Parse dictionary format: {"SourceCol": "SinkCol"}.

        Args:
            column_mappings: Dict mapping source column names to sink column names
            source_urn: Source dataset URN
            sink_urn: Sink dataset URN

        Returns:
            List of ColumnMapping objects
        """
        result = []
        for source_col, sink_col in column_mappings.items():
            if source_col and sink_col:
                result.append(
                    ColumnMapping(
                        source_dataset_urn=source_urn,
                        source_column=str(source_col),
                        sink_dataset_urn=sink_urn,
                        sink_column=str(sink_col),
                    )
                )
        return result

    def _parse_list_format(
        self,
        mappings_list: List[dict[str, Any]],
        source_urn: str,
        sink_urn: str,
    ) -> List[ColumnMapping]:
        """Parse list format: [{"source": {"name": "col1"}, "sink": {"name": "dest1"}}].

        Args:
            mappings_list: List of mapping objects with source/sink nested dicts
            source_urn: Source dataset URN
            sink_urn: Sink dataset URN

        Returns:
            List of ColumnMapping objects
        """
        result = []
        for mapping in mappings_list:
            source_info = mapping.get("source", {})
            sink_info = mapping.get("sink", {})

            source_col = (
                source_info.get("name") if isinstance(source_info, dict) else None
            )
            sink_col = sink_info.get("name") if isinstance(sink_info, dict) else None

            if source_col and sink_col:
                result.append(
                    ColumnMapping(
                        source_dataset_urn=source_urn,
                        source_column=str(source_col),
                        sink_dataset_urn=sink_urn,
                        sink_column=str(sink_col),
                    )
                )
        return result
