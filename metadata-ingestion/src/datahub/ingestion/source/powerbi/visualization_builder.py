"""
Power BI Visualization Builder

This module handles the extraction and transformation of Power BI visualizations
into DataHub Chart entities with proper lineage and metadata.
"""

import logging
from typing import Dict, List, Optional, Set, Tuple

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.common.subtypes import BIAssetSubTypes
from datahub.ingestion.source.powerbi.models import (
    ChartWithLineage,
    VisualizationColumn,
    VisualizationMeasure,
    VisualizationMetadata,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import (
    data_classes as powerbi_data_classes,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChartInfoClass,
    InputFieldClass,
    InputFieldsClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StatusClass,
    SubTypesClass,
)

logger = logging.getLogger(__name__)


# Models are now imported from models.py
# This enables better organization and reuse


class VisualizationProcessor:
    """Processes Power BI visualizations from PBIX metadata."""

    def __init__(
        self,
        platform_name: str,
        platform_instance: Optional[str],
        dataset_urn_map: Dict[str, str],
    ):
        """
        Initialize the visualization processor.

        Args:
            platform_name: Platform name (e.g., 'powerbi')
            platform_instance: Optional platform instance
            dataset_urn_map: Mapping of table names to dataset URNs
        """
        self.platform_name = platform_name
        self.platform_instance = platform_instance
        self.dataset_urn_map = dataset_urn_map

    def _normalize_table_name(self, table_name: str) -> str:
        """Normalize table name for lookup (replace underscores, lowercase)."""
        return table_name.replace("_", " ").lower()

    def _resolve_dataset_urn(self, table_name: str) -> Optional[str]:
        """Resolve a table name to its dataset URN."""
        normalized = self._normalize_table_name(table_name)
        return self.dataset_urn_map.get(normalized)

    def parse_visualization_from_pbix(
        self, viz_lineage: Dict
    ) -> Optional[VisualizationMetadata]:
        """
        Parse visualization metadata from PBIX lineage data.

        Args:
            viz_lineage: Dictionary with visualization lineage from PBIX parser

        Returns:
            VisualizationMetadata or None if parsing fails
        """
        viz_id = viz_lineage.get("visualizationId")
        if not viz_id:
            logger.warning("Visualization without ID found, skipping")
            return None

        # Parse columns
        columns: List[VisualizationColumn] = []
        for col_lineage in viz_lineage.get("columns", []):
            source_table = col_lineage.get("sourceTable")
            source_column = col_lineage.get("sourceColumn")

            if source_table and source_column:
                columns.append(
                    VisualizationColumn(
                        source_table=source_table,
                        source_column=source_column,
                        data_type=col_lineage.get("dataType", "String"),
                        display_name=col_lineage.get("displayName"),
                    )
                )

        # Parse measures
        measures: List[VisualizationMeasure] = []
        for measure_lineage in viz_lineage.get("measures", []):
            source_entity = measure_lineage.get("sourceEntity")
            measure_name = measure_lineage.get("measureName")

            if source_entity and measure_name:
                measures.append(
                    VisualizationMeasure(
                        source_entity=source_entity,
                        measure_name=measure_name,
                        expression=measure_lineage.get("expression"),
                    )
                )

        return VisualizationMetadata(
            visualization_id=viz_id,
            visualization_type=viz_lineage.get("visualizationType", "visual"),
            page_name=viz_lineage.get("sectionName", "Unknown Page"),
            page_id=viz_lineage.get("sectionId", "unknown"),
            columns=columns,
            measures=measures,
        )

    def build_chart_with_lineage(
        self,
        viz_metadata: VisualizationMetadata,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
    ) -> ChartWithLineage:
        """
        Build a chart entity with complete lineage from visualization metadata.

        Args:
            viz_metadata: Parsed visualization metadata
            report: Power BI report containing the visualization
            workspace: Power BI workspace

        Returns:
            ChartWithLineage with chart info and input fields
        """
        # Create chart URN
        chart_urn = builder.make_chart_urn(
            platform=self.platform_name,
            platform_instance=self.platform_instance,
            name=f"{report.get_urn_part()}.visualizations.{viz_metadata.visualization_id}",
        )

        # Build InputFields for column-level lineage
        input_fields: List[InputFieldClass] = []
        columns_processed: Set[str] = set()
        datasets_used: Set[str] = set()

        # Process columns
        for col in viz_metadata.columns:
            dataset_urn = self._resolve_dataset_urn(col.source_table)
            if dataset_urn:
                datasets_used.add(dataset_urn)
                field_key = f"{dataset_urn}:{col.source_column}"

                if field_key not in columns_processed:
                    columns_processed.add(field_key)
                    input_fields.append(
                        InputFieldClass(
                            schemaFieldUrn=builder.make_schema_field_urn(
                                parent_urn=dataset_urn,
                                field_path=col.source_column,
                            ),
                            schemaField=SchemaFieldClass(
                                fieldPath=col.source_column,
                                type=SchemaFieldDataTypeClass(
                                    type=powerbi_data_classes.FIELD_TYPE_MAPPING.get(
                                        col.data_type,
                                        powerbi_data_classes.FIELD_TYPE_MAPPING[
                                            "String"
                                        ],
                                    )
                                ),
                                nativeDataType=col.data_type,
                            ),
                        )
                    )

        # Process measures
        for measure in viz_metadata.measures:
            dataset_urn = self._resolve_dataset_urn(measure.source_entity)
            if dataset_urn:
                datasets_used.add(dataset_urn)
                field_key = f"{dataset_urn}:{measure.measure_name}"

                if field_key not in columns_processed:
                    columns_processed.add(field_key)
                    input_fields.append(
                        InputFieldClass(
                            schemaFieldUrn=builder.make_schema_field_urn(
                                parent_urn=dataset_urn,
                                field_path=measure.measure_name,
                            ),
                            schemaField=SchemaFieldClass(
                                fieldPath=measure.measure_name,
                                type=SchemaFieldDataTypeClass(
                                    type=powerbi_data_classes.FIELD_TYPE_MAPPING.get(
                                        "measure",
                                        powerbi_data_classes.FIELD_TYPE_MAPPING[
                                            "String"
                                        ],
                                    )
                                ),
                                nativeDataType="measure",
                            ),
                        )
                    )

        # Create ChartInfo
        chart_info = ChartInfoClass(
            title=f"{viz_metadata.page_name} - {viz_metadata.visualization_type}",
            description=f"{viz_metadata.visualization_type} visualization on page {viz_metadata.page_name}",
            lastModified=ChangeAuditStamps(),
            inputs=sorted(list(datasets_used)),
            customProperties={
                "visualizationType": viz_metadata.visualization_type,
                "visualizationId": viz_metadata.visualization_id,
                "pageName": viz_metadata.page_name,
                "reportId": report.id,
                "reportName": report.name,
                "columnCount": str(len(viz_metadata.columns)),
                "measureCount": str(len(viz_metadata.measures)),
            },
        )

        return ChartWithLineage(
            chart_urn=chart_urn,
            chart_info=chart_info,
            input_fields=input_fields,
            dataset_urns=datasets_used,
        )

    def create_chart_mcps(
        self,
        chart_with_lineage: ChartWithLineage,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
        report_container_key: Optional[ContainerKey],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Create all MCPs for a chart entity.

        Args:
            chart_with_lineage: Chart with lineage information
            report: Power BI report
            workspace: Power BI workspace
            report_container_key: Container key for the report

        Returns:
            List of MCPs for the chart
        """
        mcps: List[MetadataChangeProposalWrapper] = []

        # Chart info
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=chart_with_lineage.chart_urn,
                aspect=chart_with_lineage.chart_info,
            )
        )

        # Status
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=chart_with_lineage.chart_urn,
                aspect=StatusClass(removed=False),
            )
        )

        # Subtype
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=chart_with_lineage.chart_urn,
                aspect=SubTypesClass(typeNames=[BIAssetSubTypes.POWERBI_VISUALIZATION]),
            )
        )

        # Browse path
        viz_meta = chart_with_lineage.chart_info
        page_name = viz_meta.customProperties.get("pageName", "Unknown")
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=chart_with_lineage.chart_urn,
                aspect=BrowsePathsClass(
                    paths=[f"/powerbi/{workspace.name}/{report.name}/{page_name}"]
                ),
            )
        )

        # InputFields (column-level lineage)
        if chart_with_lineage.input_fields:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_with_lineage.chart_urn,
                    aspect=InputFieldsClass(
                        fields=sorted(
                            chart_with_lineage.input_fields,
                            key=lambda x: x.schemaFieldUrn,
                        )
                    ),
                )
            )

        # Container (add to report)
        if report_container_key:
            from datahub.metadata.schema_classes import ContainerClass

            container_urn = builder.make_container_urn(guid=report_container_key.guid())
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=chart_with_lineage.chart_urn,
                    aspect=ContainerClass(container=container_urn),
                )
            )

        return mcps


class VisualizationBatchProcessor:
    """Process multiple visualizations in batch."""

    def __init__(self, processor: VisualizationProcessor):
        """Initialize with a visualization processor."""
        self.processor = processor

    def process_all_visualizations(
        self,
        pbix_metadata: Dict,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
        report_container_key: Optional[ContainerKey],
    ) -> Tuple[List[MetadataChangeProposalWrapper], Dict[str, List[str]]]:
        """
        Process all visualizations from PBIX metadata.

        Args:
            pbix_metadata: PBIX metadata dictionary
            report: Power BI report
            workspace: Power BI workspace
            report_container_key: Container key for the report

        Returns:
            Tuple of (list of MCPs, page_id -> chart_urns mapping)
        """
        all_mcps: List[MetadataChangeProposalWrapper] = []
        page_to_viz_urns: Dict[str, List[str]] = {}

        lineage_info = pbix_metadata.get("lineage", {})
        visualization_lineages = lineage_info.get("visualization_lineage", [])

        if not visualization_lineages:
            logger.warning(f"No visualization lineage found for report {report.name}")
            return all_mcps, page_to_viz_urns

        logger.info(
            f"Processing {len(visualization_lineages)} visualizations from PBIX"
        )

        for viz_lineage in visualization_lineages:
            # Parse visualization metadata
            viz_metadata = self.processor.parse_visualization_from_pbix(viz_lineage)
            if not viz_metadata:
                continue

            # Build chart with lineage
            chart_with_lineage = self.processor.build_chart_with_lineage(
                viz_metadata, report, workspace
            )

            # Track for page mapping
            if viz_metadata.page_id not in page_to_viz_urns:
                page_to_viz_urns[viz_metadata.page_id] = []
            page_to_viz_urns[viz_metadata.page_id].append(chart_with_lineage.chart_urn)

            # Create MCPs
            chart_mcps = self.processor.create_chart_mcps(
                chart_with_lineage, report, workspace, report_container_key
            )
            all_mcps.extend(chart_mcps)

        logger.info(
            f"Created {len(all_mcps)} MCPs for {len(visualization_lineages)} "
            f"visualizations across {len(page_to_viz_urns)} pages"
        )

        return all_mcps, page_to_viz_urns
