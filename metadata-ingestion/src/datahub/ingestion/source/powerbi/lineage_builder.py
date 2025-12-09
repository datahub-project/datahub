"""
Power BI Lineage Builder

This module provides structured lineage extraction from Power BI using Pydantic models.
It breaks down the complex lineage logic into manageable, testable components.
"""

import logging
from typing import Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

import datahub.emitter.mce_builder as builder
from datahub.ingestion.source.powerbi.dax_parser import (
    DAXParseResult,
    SummarizeParseResult,
    extract_table_column_references,
    parse_dax_expression,
    parse_summarize_expression,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import (
    data_classes as powerbi_data_classes,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    UpstreamClass,
)
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
)

logger = logging.getLogger(__name__)


class TableExpressionLineageResult(BaseModel):
    """Result of extracting lineage from a table expression."""

    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Upstream table dependencies"
    )
    column_edges: List["ColumnLineageEdge"] = Field(
        default_factory=list, description="Column-level lineage edges"
    )

    class Config:
        arbitrary_types_allowed = True


class MeasureLineageResult(BaseModel):
    """Result of extracting lineage from measures."""

    measure_edges: List["MeasureLineageEdge"] = Field(
        default_factory=list, description="Measure lineage edges"
    )
    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Additional upstream tables discovered"
    )

    class Config:
        arbitrary_types_allowed = True


class TableReference(BaseModel):
    """A reference to a table in lineage extraction."""

    table_name: str = Field(description="Name of the table")
    table_urn: str = Field(description="DataHub URN for the table")
    is_dax_table: bool = Field(
        default=False, description="Whether this is a DAX calculated table"
    )


class ColumnLineageEdge(BaseModel):
    """Represents a column-level lineage edge."""

    source_table_urn: str = Field(description="URN of the source table")
    source_column: str = Field(description="Name of the source column")
    target_table_urn: str = Field(description="URN of the target table")
    target_column: str = Field(description="Name of the target column")
    transform_operation: str = Field(description="Type of transformation applied")


class MeasureLineageEdge(BaseModel):
    """Represents measure-level lineage (measure to measure or column to measure)."""

    source_urn: str = Field(description="URN of the source (can be column or measure)")
    source_name: str = Field(description="Name of the source")
    target_measure_urn: str = Field(description="URN of the target measure")
    target_measure_name: str = Field(description="Name of the target measure")
    transform_operation: str = Field(description="Type of transformation")
    is_measure_to_measure: bool = Field(
        default=False,
        description="True if source is a measure, False if source is a column",
    )


class LineageExtractionResult(BaseModel):
    """Result of extracting lineage from a Power BI table."""

    table_urn: str = Field(description="URN of the table being processed")
    table_name: str = Field(description="Name of the table")
    upstream_tables: List[UpstreamClass] = Field(
        default_factory=list, description="Table-level upstream lineage"
    )
    column_lineage_edges: List[ColumnLineageEdge] = Field(
        default_factory=list, description="Column-level lineage edges"
    )
    measure_lineage_edges: List[MeasureLineageEdge] = Field(
        default_factory=list, description="Measure lineage edges"
    )
    warnings: List[str] = Field(
        default_factory=list, description="Warnings encountered during extraction"
    )

    def to_fine_grained_lineage(self) -> List[FineGrainedLineage]:
        """Convert edges to DataHub FineGrainedLineage objects."""
        result: List[FineGrainedLineage] = []

        # Convert column lineage edges
        for edge in self.column_lineage_edges:
            source_field_urn = builder.make_schema_field_urn(
                edge.source_table_urn, edge.source_column
            )
            target_field_urn = builder.make_schema_field_urn(
                edge.target_table_urn, edge.target_column
            )

            result.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[source_field_urn],
                    downstreams=[target_field_urn],
                    transformOperation=edge.transform_operation,
                )
            )

        # Convert measure lineage edges
        for measure_edge in self.measure_lineage_edges:
            result.append(
                FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[measure_edge.source_urn],
                    downstreams=[measure_edge.target_measure_urn],
                    transformOperation=measure_edge.transform_operation,
                )
            )

        return result


class DAXLineageExtractor:
    """Extracts lineage from DAX expressions in tables, measures, and calculated columns."""

    def __init__(
        self,
        table_urn_map: Dict[str, str],
        data_model_tables: Optional[List[Dict]] = None,
    ):
        """
        Initialize the DAX lineage extractor.

        Args:
            table_urn_map: Mapping of table names to their URNs
            data_model_tables: Data model table definitions for parameter resolution
        """
        self.table_urn_map = table_urn_map
        self.data_model_tables = data_model_tables
        self.warnings: List[str] = []

    def _resolve_table_urn(self, table_name: str) -> Optional[str]:
        """Resolve a table name to its URN, trying case variations."""
        if table_name in self.table_urn_map:
            return self.table_urn_map[table_name]
        if table_name.lower() in self.table_urn_map:
            return self.table_urn_map[table_name.lower()]

        self.warnings.append(
            f"Table '{table_name}' not found in URN map. "
            f"Available: {sorted(list(self.table_urn_map.keys()))[:10]}"
        )
        return None

    def extract_from_table_expression(
        self, table: powerbi_data_classes.Table, target_table_urn: str
    ) -> "TableExpressionLineageResult":
        """
        Extract lineage from a DAX calculated table expression.

        Args:
            table: The Power BI table with DAX expression
            target_table_urn: URN of the target table

        Returns:
            TableExpressionLineageResult with upstream tables and column edges
        """
        if not table.expression:
            return TableExpressionLineageResult()

        logger.debug(f"Extracting lineage from DAX table: {table.name}")

        # Try SUMMARIZE-specific parsing first
        summarize_result = parse_summarize_expression(table.expression)

        if summarize_result:
            result = self._extract_from_summarize(
                summarize_result, target_table_urn, table.name
            )
        else:
            # Generic DAX parsing
            dax_result = parse_dax_expression(
                table.expression,
                include_measure_refs=False,
                include_advanced_analysis=False,
                extract_parameters=True,
                data_model_tables=self.data_model_tables,
                include_all_functions=True,
            )
            result = self._extract_from_dax_result(dax_result, target_table_urn, table)

        return result

    def _extract_from_summarize(
        self,
        summarize_result: SummarizeParseResult,
        target_table_urn: str,
        target_table_name: str,
    ) -> TableExpressionLineageResult:
        """Extract lineage from a SUMMARIZE expression."""
        result = TableExpressionLineageResult()

        # Add source table
        source_table_urn = self._resolve_table_urn(summarize_result.source_table)
        if source_table_urn:
            result.upstream_tables.append(
                UpstreamClass(source_table_urn, DatasetLineageTypeClass.TRANSFORMED)
            )

        # Direct mappings
        for mapping in summarize_result.direct_mappings:
            table_urn = self._resolve_table_urn(mapping.source_table)
            if table_urn:
                result.column_edges.append(
                    ColumnLineageEdge(
                        source_table_urn=table_urn,
                        source_column=mapping.source_column,
                        target_table_urn=target_table_urn,
                        target_column=mapping.target_column,
                        transform_operation="DAX_SUMMARIZE_GROUPBY",
                    )
                )

        # Calculated mappings
        for calc_mapping in summarize_result.calculated_mappings:
            refs = extract_table_column_references(calc_mapping.expression)
            for ref in refs:
                if ref.column_name:
                    table_urn = self._resolve_table_urn(ref.table_name)
                    if table_urn:
                        result.column_edges.append(
                            ColumnLineageEdge(
                                source_table_urn=table_urn,
                                source_column=ref.column_name,
                                target_table_urn=target_table_urn,
                                target_column=calc_mapping.target_column,
                                transform_operation="DAX_SUMMARIZE_CALCULATED",
                            )
                        )

        return result

    def _extract_from_dax_result(
        self,
        dax_result: DAXParseResult,
        target_table_urn: str,
        table: powerbi_data_classes.Table,
    ) -> TableExpressionLineageResult:
        """Extract lineage from a generic DAX parse result."""
        result = TableExpressionLineageResult()
        upstream_table_urns: Set[str] = set()

        # Extract unique upstream tables
        for ref in dax_result.table_column_references:
            table_urn = self._resolve_table_urn(ref.table_name)
            if table_urn and table_urn not in upstream_table_urns:
                upstream_table_urns.add(table_urn)
                result.upstream_tables.append(
                    UpstreamClass(table_urn, DatasetLineageTypeClass.TRANSFORMED)
                )

        # Map columns if target table has columns defined
        if table.columns:
            for target_column in table.columns:
                # Try name-based mapping
                for ref in dax_result.table_column_references:
                    if ref.column_name == target_column.name:
                        source_table_urn = self._resolve_table_urn(ref.table_name)
                        if source_table_urn:
                            result.column_edges.append(
                                ColumnLineageEdge(
                                    source_table_urn=source_table_urn,
                                    source_column=ref.column_name,
                                    target_table_urn=target_table_urn,
                                    target_column=target_column.name,
                                    transform_operation="DAX_CALCULATED_TABLE",
                                )
                            )
                            break

                # If column has its own expression, parse it
                if hasattr(target_column, "expression") and target_column.expression:
                    col_dax = parse_dax_expression(
                        target_column.expression,
                        include_measure_refs=False,
                        include_advanced_analysis=False,
                        extract_parameters=True,
                        data_model_tables=self.data_model_tables,
                        include_all_functions=True,
                    )
                    for ref in col_dax.table_column_references:
                        if ref.column_name:
                            source_table_urn = self._resolve_table_urn(ref.table_name)
                            if source_table_urn:
                                result.column_edges.append(
                                    ColumnLineageEdge(
                                        source_table_urn=source_table_urn,
                                        source_column=ref.column_name,
                                        target_table_urn=target_table_urn,
                                        target_column=target_column.name,
                                        transform_operation="DAX_CALCULATED_TABLE",
                                    )
                                )

        return result

    def extract_from_measures(
        self,
        table: powerbi_data_classes.Table,
        target_table_urn: str,
        all_measure_names: Set[str],
    ) -> Tuple[List[MeasureLineageEdge], List[UpstreamClass]]:
        """
        Extract lineage from DAX measures.

        Args:
            table: Power BI table containing measures
            target_table_urn: URN of the table
            all_measure_names: Set of all measure names in the dataset

        Returns:
            Tuple of (measure_lineage_edges, additional_upstream_tables)
        """
        measure_edges: List[MeasureLineageEdge] = []
        upstream_tables: List[UpstreamClass] = []
        upstream_table_urns: Set[str] = set()

        if not table.measures:
            return measure_edges, upstream_tables

        for measure in table.measures:
            if not measure.expression:
                continue

            # Parse DAX with full analysis
            dax_result = parse_dax_expression(
                measure.expression,
                include_measure_refs=True,
                include_advanced_analysis=False,
                extract_parameters=True,
                data_model_tables=self.data_model_tables,
                include_all_functions=True,
            )

            # Column references -> Measure
            for ref in dax_result.table_column_references:
                if not ref.column_name:
                    continue

                source_table_urn = self._resolve_table_urn(ref.table_name)
                if source_table_urn:
                    # Add to upstream tables if not already present
                    if source_table_urn not in upstream_table_urns:
                        upstream_table_urns.add(source_table_urn)
                        upstream_tables.append(
                            UpstreamClass(
                                source_table_urn, DatasetLineageTypeClass.TRANSFORMED
                            )
                        )

                    source_field_urn = builder.make_schema_field_urn(
                        source_table_urn, ref.column_name
                    )
                    target_measure_urn = builder.make_schema_field_urn(
                        target_table_urn, measure.name
                    )

                    measure_edges.append(
                        MeasureLineageEdge(
                            source_urn=source_field_urn,
                            source_name=f"{ref.table_name}.{ref.column_name}",
                            target_measure_urn=target_measure_urn,
                            target_measure_name=measure.name,
                            transform_operation="DAX_MEASURE",
                            is_measure_to_measure=False,
                        )
                    )

            # Measure references -> Measure
            for ref_measure_name in dax_result.measure_references:
                # Try to find the source measure's URN
                # This is simplified - in production, would need to search across tables
                source_measure_urn = builder.make_schema_field_urn(
                    target_table_urn,  # Assume same table for now
                    ref_measure_name,
                )
                target_measure_urn = builder.make_schema_field_urn(
                    target_table_urn, measure.name
                )

                measure_edges.append(
                    MeasureLineageEdge(
                        source_urn=source_measure_urn,
                        source_name=ref_measure_name,
                        target_measure_urn=target_measure_urn,
                        target_measure_name=measure.name,
                        transform_operation="DAX_MEASURE_TO_MEASURE",
                        is_measure_to_measure=True,
                    )
                )

        return measure_edges, upstream_tables

    def extract_from_calculated_columns(
        self, table: powerbi_data_classes.Table, target_table_urn: str
    ) -> Tuple[List[ColumnLineageEdge], List[UpstreamClass]]:
        """Extract lineage from calculated columns."""
        column_edges: List[ColumnLineageEdge] = []
        upstream_tables: List[UpstreamClass] = []
        upstream_table_urns: Set[str] = set()

        if not table.columns:
            return column_edges, upstream_tables

        for column in table.columns:
            if not column.expression:
                continue

            dax_result = parse_dax_expression(
                column.expression,
                include_measure_refs=False,
                include_advanced_analysis=False,
                extract_parameters=True,
                data_model_tables=self.data_model_tables,
                include_all_functions=True,
            )

            for ref in dax_result.table_column_references:
                if not ref.column_name:
                    continue

                source_table_urn = self._resolve_table_urn(ref.table_name)
                if source_table_urn:
                    # Add to upstream if not present
                    if source_table_urn not in upstream_table_urns:
                        upstream_table_urns.add(source_table_urn)
                        upstream_tables.append(
                            UpstreamClass(
                                source_table_urn, DatasetLineageTypeClass.TRANSFORMED
                            )
                        )

                    column_edges.append(
                        ColumnLineageEdge(
                            source_table_urn=source_table_urn,
                            source_column=ref.column_name,
                            target_table_urn=target_table_urn,
                            target_column=column.name,
                            transform_operation="DAX_CALCULATED_COLUMN",
                        )
                    )

        return column_edges, upstream_tables


class PowerBILineageBuilder:
    """Main class for building lineage from Power BI tables."""

    def __init__(
        self,
        table_urn_map: Dict[str, str],
        data_model_tables: Optional[List[Dict]] = None,
    ):
        """
        Initialize the lineage builder.

        Args:
            table_urn_map: Mapping of table names to their URNs
            data_model_tables: Data model table definitions for parameter resolution
        """
        self.dax_extractor = DAXLineageExtractor(table_urn_map, data_model_tables)

    def extract_lineage(
        self,
        table: powerbi_data_classes.Table,
        target_table_urn: str,
        all_measure_names: Optional[Set[str]] = None,
    ) -> LineageExtractionResult:
        """
        Extract all lineage from a Power BI table.

        Args:
            table: The Power BI table to extract lineage from
            target_table_urn: URN of the target table
            all_measure_names: Set of all measure names in the dataset

        Returns:
            LineageExtractionResult with all extracted lineage
        """
        result = LineageExtractionResult(
            table_urn=target_table_urn, table_name=table.name
        )

        # Extract from table expression (DAX calculated table)
        if table.expression:
            table_result = self.dax_extractor.extract_from_table_expression(
                table, target_table_urn
            )
            result.upstream_tables.extend(table_result.upstream_tables)
            result.column_lineage_edges.extend(table_result.column_edges)

        # Extract from measures
        if table.measures:
            measure_edges, upstream_tables = self.dax_extractor.extract_from_measures(
                table, target_table_urn, all_measure_names or set()
            )
            result.measure_lineage_edges.extend(measure_edges)
            result.upstream_tables.extend(upstream_tables)

        # Extract from calculated columns
        if table.columns:
            column_edges, upstream_tables = (
                self.dax_extractor.extract_from_calculated_columns(
                    table, target_table_urn
                )
            )
            result.column_lineage_edges.extend(column_edges)
            result.upstream_tables.extend(upstream_tables)

        # Collect warnings
        result.warnings.extend(self.dax_extractor.warnings)

        # Deduplicate upstream tables
        seen_urns: Set[str] = set()
        deduplicated_upstream: List[UpstreamClass] = []
        for upstream in result.upstream_tables:
            if upstream.dataset not in seen_urns:
                seen_urns.add(upstream.dataset)
                deduplicated_upstream.append(upstream)
        result.upstream_tables = deduplicated_upstream

        return result
