#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################
import functools
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import more_itertools

import datahub.emitter.mce_builder as builder
import datahub.ingestion.source.powerbi.m_query.data_classes
import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    auto_incremental_lineage,
    convert_dashboard_info_to_patch,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import parser
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    ChangeAuditStamps,
    InputField,
    InputFields,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsClass,
    ChangeTypeClass,
    ChartInfoClass,
    ContainerClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    EdgeClass,
    GlobalTagsClass,
    NullTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StatusClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import ChartUrn, DatasetUrn
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

# Logger instance
logger = logging.getLogger(__name__)


def _is_dax_expression(expression: str) -> bool:
    """
    Determine if an expression is a DAX expression (vs M-Query).

    DAX expressions typically:
    - Use DAX functions like SUMMARIZE, ADDCOLUMNS, CALENDARAUTO, RELATED, etc.
    - Reference tables with 'TableName' or TableName[Column] syntax
    - Don't start with "let" or "Source ="

    M-Query expressions typically:
    - Start with "let" or contain "Source ="
    - Use M-Query functions
    - Have step-by-step transformations
    """
    if not expression or not expression.strip():
        return False

    expression_upper = expression.strip().upper()

    # M-Query indicators (if these are present, it's NOT DAX)
    mquery_indicators = [
        "LET ",
        "SOURCE =",
        "SOURCE=",
        '#"',  # M-Query step names use #"StepName"
        "TABLE.",  # M-Query table functions
        "LIST.",  # M-Query list functions
    ]

    for indicator in mquery_indicators:
        if indicator in expression_upper[:100]:  # Check first 100 chars
            return False

    # DAX function indicators (if these are present, it's likely DAX)
    dax_functions = [
        "SUMMARIZE(",
        "ADDCOLUMNS(",
        "CALENDARAUTO(",
        "CALENDAR(",
        "RELATED(",
        "RELATEDTABLE(",
        "CALCULATETABLE(",
        "FILTER(",
        "SUMX(",
        "AVERAGEX(",
        "COUNTX(",
        "EVALUATE",
        "ROW(",  # DAX table constructor function
        "DATATABLE(",  # DAX table constructor
        "SELECTCOLUMNS(",
        "TOPN(",
        "SAMPLE(",
    ]

    for func in dax_functions:
        if func in expression_upper:
            return True

    # If expression has table references like 'TableName'[Column] or TableName[Column]
    # it's likely DAX (M-Query uses different syntax)
    import re

    if re.search(r"'[^']+'\s*\[", expression) or re.search(
        r"\b[A-Za-z_][A-Za-z0-9_]*\s*\[", expression
    ):
        # But double-check it's not M-Query (which also uses brackets but differently)
        if "SOURCE" not in expression_upper[:50]:
            return True

    return False


class Mapper:
    """
    Transform PowerBi concepts Dashboard, Dataset and Tile to DataHub concepts Dashboard, Dataset and Chart
    """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """
        We can add EquableMetadataWorkUnit to set.
        This will avoid passing same MetadataWorkUnit to DataHub Ingestion framework.
        """

        def __eq__(self, instance):
            return self.id == instance.id

        def __hash__(self):
            return id(self.id)

    def __init__(
        self,
        ctx: PipelineContext,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
        dataplatform_instance_resolver: AbstractDataPlatformInstanceResolver,
    ):
        self.__ctx = ctx
        self.__config = config
        self.__reporter = reporter
        self.__dataplatform_instance_resolver = dataplatform_instance_resolver
        self.workspace_key: Optional[ContainerKey] = None

    @staticmethod
    def urn_to_lowercase(value: str, flag: bool) -> str:
        if flag is True:
            return lowercase_dataset_urn(value)

        return value

    def lineage_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(
            value, self.__config.convert_lineage_urns_to_lowercase
        )

    def assets_urn_to_lowercase(self, value):
        return Mapper.urn_to_lowercase(value, self.__config.convert_urns_to_lowercase)

    def new_mcp(
        self,
        entity_urn,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            changeType=change_type,
            entityUrn=entity_urn,
            aspect=aspect,
        )

    def _to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def extract_dataset_schema(
        self, table: powerbi_data_classes.Table, ds_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        schema_metadata = self.to_datahub_schema(table)
        schema_mcp = self.new_mcp(
            entity_urn=ds_urn,
            aspect=schema_metadata,
        )
        return [schema_mcp]

    def make_fine_grained_lineage_class(
        self,
        lineage: datahub.ingestion.source.powerbi.m_query.data_classes.Lineage,
        dataset_urn: str,
        upstream_tables: Optional[Dict[str, str]] = None,
    ) -> List[FineGrainedLineage]:
        """
        Create fine-grained (column-level) lineage.

        Args:
            lineage: The lineage information from M-query parsing
            dataset_urn: The URN of the downstream dataset
            upstream_tables: Optional mapping of table names/identifiers to their URNs.
                            This ensures we use actual dataset URNs instead of creating new ones.
        """
        fine_grained_lineages: List[FineGrainedLineage] = []

        if (
            self.__config.extract_column_level_lineage is False
            or self.__config.extract_lineage is False
        ):
            return fine_grained_lineages

        if lineage is None:
            return fine_grained_lineages

        logger.info("Extracting column level lineage")

        cll: List[ColumnLineageInfo] = lineage.column_lineage

        for cll_info in cll:
            downstream = (
                [builder.make_schema_field_urn(dataset_urn, cll_info.downstream.column)]
                if cll_info.downstream is not None
                and cll_info.downstream.column is not None
                else []
            )

            upstreams = []
            for column_ref in cll_info.upstreams:
                # Use the upstream_tables mapping if available to get the correct URN
                # This prevents creating new datasets with "dax_" or other prefixes
                if upstream_tables and column_ref.table in upstream_tables:
                    table_urn = upstream_tables[column_ref.table]
                    logger.debug(
                        f"Using existing dataset URN for table {column_ref.table}: {table_urn}"
                    )
                else:
                    # Fallback to the column_ref.table value (which might already be a URN)
                    table_urn = column_ref.table
                    if not table_urn.startswith("urn:li:dataset:"):
                        logger.warning(
                            f"Table reference '{column_ref.table}' is not a URN and not in upstream_tables mapping"
                        )

                upstreams.append(
                    builder.make_schema_field_urn(table_urn, column_ref.column)
                )

            fine_grained_lineages.append(
                FineGrainedLineage(
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        return fine_grained_lineages

    def _make_fine_grained_lineage(
        self,
        source_table_urn: str,
        source_column: str,
        target_table_urn: str,
        target_column: str,
        transform_operation: str,
    ) -> FineGrainedLineage:
        """
        Create a single fine-grained lineage entry.

        Args:
            source_table_urn: URN of the source table
            source_column: Name of the source column
            target_table_urn: URN of the target table
            target_column: Name of the target column
            transform_operation: Description of the transformation

        Returns:
            FineGrainedLineage object
        """
        source_field_urn = builder.make_schema_field_urn(
            source_table_urn, source_column
        )
        target_field_urn = builder.make_schema_field_urn(
            target_table_urn, target_column
        )

        return FineGrainedLineage(
            downstreamType=FineGrainedLineageDownstreamType.FIELD,
            downstreams=[target_field_urn],
            upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
            upstreams=[source_field_urn],
            transformOperation=transform_operation,
        )

    def _extract_m_query_lineage(
        self, table: powerbi_data_classes.Table
    ) -> List[datahub.ingestion.source.powerbi.m_query.data_classes.Lineage]:
        """Extract lineage from M-Query expressions."""
        if not table.expression or _is_dax_expression(table.expression):
            return []

        parameters = table.dataset.parameters if table.dataset else {}
        upstream_lineage = parser.get_upstream_tables(
            table=table,
            reporter=self.__reporter,
            platform_instance_resolver=self.__dataplatform_instance_resolver,
            ctx=self.__ctx,
            config=self.__config,
            parameters=parameters,
        )
        logger.debug(
            f"PowerBI virtual table {table.full_name} and it's upstream dataplatform tables = {upstream_lineage}"
        )
        return upstream_lineage

    def _build_upstream_tables_map(
        self, dax_table_mappings: Optional[Dict[str, str]]
    ) -> Dict[str, str]:
        """Build mapping of table names to their URNs."""
        upstream_tables_map: Dict[str, str] = {}
        if dax_table_mappings:
            upstream_tables_map.update(dax_table_mappings)
            logger.debug(
                f"Initialized upstream tables map with {len(dax_table_mappings)} DAX table mappings"
            )
        return upstream_tables_map

    def _resolve_table_urn_with_fallback(
        self, table_name: str, upstream_tables_map: Dict[str, str]
    ) -> Optional[str]:
        """Resolve a table name to its URN, trying case variations."""
        if table_name in upstream_tables_map:
            return upstream_tables_map[table_name]
        if table_name.lower() in upstream_tables_map:
            return upstream_tables_map[table_name.lower()]
        return None

    def _extract_referenced_tables_from_dax(
        self, dax_expression: str
    ) -> Tuple[Set[str], List]:
        """Extract table and column references from DAX expression."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
        )

        references = extract_table_column_references(dax_expression)
        referenced_tables = set()
        column_references = []

        for ref in references:
            referenced_tables.add(ref.table_name)
            if ref.column_name:
                column_references.append((ref.table_name, ref.column_name, None))

        return referenced_tables, column_references

    def _create_upstream_for_dax_tables(
        self,
        referenced_tables: Set[str],
        upstream_tables_map: Dict[str, str],
        table_full_name: str,
    ) -> List[UpstreamClass]:
        """Create upstream lineage entries for DAX-referenced tables."""
        upstream: List[UpstreamClass] = []

        for referenced_table in referenced_tables:
            table_urn = self._resolve_table_urn_with_fallback(
                referenced_table, upstream_tables_map
            )

            if table_urn:
                upstream.append(
                    UpstreamClass(table_urn, DatasetLineageTypeClass.TRANSFORMED)
                )
                logger.debug(
                    f"Added DAX table lineage: {table_full_name} -> {referenced_table} ({table_urn})"
                )
            else:
                available_tables = sorted(list(upstream_tables_map.keys()))
                logger.warning(
                    f"Could not find URN for referenced table '{referenced_table}' in DAX expression for {table_full_name}. "
                    f"Available tables in mapping: {available_tables}"
                )

        return upstream

    def _process_summarize_direct_mappings(
        self,
        direct_mappings: List,
        upstream_tables_map: Dict[str, str],
        ds_urn: str,
        table_name: str,
    ) -> List[FineGrainedLineage]:
        """Process SUMMARIZE direct column mappings."""
        cll_lineage: List[FineGrainedLineage] = []

        for mapping in direct_mappings:
            source_table_urn = self._resolve_table_urn_with_fallback(
                mapping.source_table, upstream_tables_map
            )

            if source_table_urn:
                source_field_urn = builder.make_schema_field_urn(
                    source_table_urn, mapping.source_column
                )
                target_field_urn = builder.make_schema_field_urn(
                    ds_urn, mapping.target_column
                )

                fine_grained_lineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[source_field_urn],
                    downstreams=[target_field_urn],
                    transformOperation="DAX_SUMMARIZE_GROUPBY",
                )
                cll_lineage.append(fine_grained_lineage)
                logger.info(
                    f"Added SUMMARIZE group-by lineage: {mapping.source_table}.{mapping.source_column} -> {table_name}.{mapping.target_column}"
                )

        return cll_lineage

    def _process_summarize_calculated_mappings(
        self,
        calculated_mappings: List,
        upstream_tables_map: Dict[str, str],
        ds_urn: str,
        table_name: str,
    ) -> List[FineGrainedLineage]:
        """Process SUMMARIZE calculated column mappings."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
        )

        cll_lineage: List[FineGrainedLineage] = []

        for mapping in calculated_mappings:
            calc_references = extract_table_column_references(mapping.expression)

            for ref in calc_references:
                if not ref.column_name:
                    continue

                source_table_urn = self._resolve_table_urn_with_fallback(
                    ref.table_name, upstream_tables_map
                )

                if source_table_urn:
                    source_field_urn = builder.make_schema_field_urn(
                        source_table_urn, ref.column_name
                    )
                    target_field_urn = builder.make_schema_field_urn(
                        ds_urn, mapping.target_column
                    )

                    fine_grained_lineage = FineGrainedLineage(
                        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamType.FIELD,
                        upstreams=[source_field_urn],
                        downstreams=[target_field_urn],
                        transformOperation="DAX_SUMMARIZE_CALCULATED",
                    )
                    cll_lineage.append(fine_grained_lineage)
                    logger.info(
                        f"Added SUMMARIZE calculated lineage: {ref.table_name}.{ref.column_name} -> {table_name}.{mapping.target_column}"
                    )

        return cll_lineage

    def _add_upstream_if_not_present(
        self, upstream: List[UpstreamClass], table_urn: str
    ) -> None:
        """Add upstream table if not already in the list."""
        if not any(u.dataset == table_urn for u in upstream):
            upstream.append(
                UpstreamClass(table_urn, DatasetLineageTypeClass.TRANSFORMED)
            )

    def _process_dax_measure_column_refs(
        self,
        measure_name: str,
        column_refs: List,
        upstream_tables_map: Dict[str, str],
        ds_urn: str,
        upstream: List[UpstreamClass],
    ) -> List[FineGrainedLineage]:
        """Process column references in a DAX measure."""
        cll_lineage: List[FineGrainedLineage] = []

        for ref in column_refs:
            if not ref.column_name:
                continue

            source_table_urn = self._resolve_table_urn_with_fallback(
                ref.table_name, upstream_tables_map
            )

            if source_table_urn:
                self._add_upstream_if_not_present(upstream, source_table_urn)

                source_field_urn = builder.make_schema_field_urn(
                    source_table_urn, ref.column_name
                )
                measure_field_urn = builder.make_schema_field_urn(ds_urn, measure_name)

                fine_grained_lineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[source_field_urn],
                    downstreams=[measure_field_urn],
                    transformOperation="DAX_MEASURE",
                )
                cll_lineage.append(fine_grained_lineage)
                logger.debug(
                    f"Added DAX measure lineage: {ref.table_name}.{ref.column_name} -> {measure_name}"
                )

        return cll_lineage

    def _process_dax_calculated_column_refs(
        self,
        column_name: str,
        column_expression: str,
        upstream_tables_map: Dict[str, str],
        ds_urn: str,
        upstream: List[UpstreamClass],
    ) -> List[FineGrainedLineage]:
        """Process column references in a DAX calculated column."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
        )

        cll_lineage: List[FineGrainedLineage] = []
        references = extract_table_column_references(column_expression)

        for ref in references:
            if not ref.column_name:
                continue

            source_table_urn = self._resolve_table_urn_with_fallback(
                ref.table_name, upstream_tables_map
            )

            if source_table_urn:
                self._add_upstream_if_not_present(upstream, source_table_urn)

                source_field_urn = builder.make_schema_field_urn(
                    source_table_urn, ref.column_name
                )
                target_field_urn = builder.make_schema_field_urn(ds_urn, column_name)

                fine_grained_lineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[source_field_urn],
                    downstreams=[target_field_urn],
                    transformOperation="DAX_CALCULATED_COLUMN",
                )
                cll_lineage.append(fine_grained_lineage)
                logger.debug(
                    f"Added DAX calculated column lineage: {ref.table_name}.{ref.column_name} -> {column_name}"
                )

        return cll_lineage

    def _extract_and_log_dax_references(
        self, dax_expression: str, entity_name: str, entity_type: str
    ) -> List:
        """Extract DAX references with logging and error handling."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
        )

        self.__reporter.dax_parse_attempts += 1
        try:
            logger.debug(f"Extracting lineage from DAX {entity_type}: {entity_name}")
            references = extract_table_column_references(dax_expression)

            if references:
                logger.debug(
                    f"Found {len(references)} table/column references in {entity_type} {entity_name}"
                )
                self.__reporter.dax_parse_successes += 1
            else:
                logger.debug(
                    f"No table/column references found in {entity_type} {entity_name}"
                )

            return references
        except Exception as e:
            logger.warning(
                f"Failed to parse DAX expression for {entity_type} {entity_name}: {e}"
            )
            self.__reporter.dax_parse_failures += 1
            return []

    def _log_dax_reference_mappings(
        self, references: List, table_urn_mapping: Dict[str, str]
    ) -> None:
        """Log DAX reference to URN mappings."""
        for ref in references:
            if ref.table_name in table_urn_mapping:
                urn = table_urn_mapping[ref.table_name]
                if ref.column_name:
                    logger.debug(f"  ✓ {ref.table_name}.{ref.column_name} -> {urn}")
                else:
                    logger.debug(f"  ✓ {ref.table_name} (table reference) -> {urn}")
            else:
                available_tables = sorted(list(table_urn_mapping.keys()))
                logger.warning(
                    f"  ✗ {ref.table_name} not found in dataset tables - cannot map to URN. "
                    f"Available: {available_tables}"
                )

    def _build_data_model_tables_for_parameters(
        self, dataset: Optional[powerbi_data_classes.PowerBIDataset]
    ) -> Optional[List[Dict]]:
        """Build data model table definitions for parameter resolution."""
        if not dataset:
            return None

        data_model_tables = []
        for ds_table in dataset.tables:
            table_def = {
                "name": ds_table.name,
                "columns": [
                    {"name": c.name, "expression": c.expression}
                    for c in (ds_table.columns or [])
                ],
                "measures": [
                    {"name": m.name, "expression": m.expression}
                    for m in (ds_table.measures or [])
                ],
            }
            data_model_tables.append(table_def)

        return data_model_tables

    def _get_all_measure_names_from_dataset(
        self, table: powerbi_data_classes.Table, measure_map: Dict[str, str]
    ) -> Set[str]:
        """Get all measure names across the entire dataset."""
        all_measure_names = set(measure_map.keys())

        if table.dataset:
            for ds_table in table.dataset.tables:
                if ds_table.measures:
                    all_measure_names.update([m.name for m in ds_table.measures])

        return all_measure_names

    def _process_measure_to_measure_dependencies(
        self,
        measure_deps: List[str],
        measure_map: Dict[str, str],
        table: powerbi_data_classes.Table,
        ds_urn: str,
        measure_name: str,
    ) -> List[FineGrainedLineage]:
        """Process measure-to-measure lineage dependencies."""
        cll_lineage: List[FineGrainedLineage] = []

        for ref_measure_name in measure_deps:
            ref_measure_table_urn = None

            # Check if it's in the current table
            if ref_measure_name in measure_map:
                ref_measure_table_urn = ds_urn
            elif table.dataset:
                # Check other tables in the dataset
                for ds_table in table.dataset.tables:
                    if ds_table.measures and any(
                        m.name == ref_measure_name for m in ds_table.measures
                    ):
                        ref_table_full_name = ds_table.full_name.lower()
                        ref_measure_table_urn = (
                            builder.make_dataset_urn_with_platform_instance(
                                platform=self.__config.platform_name,
                                name=ref_table_full_name,
                                platform_instance=self.__config.platform_instance,
                                env=self.__config.env,
                            )
                        )
                        break

            if ref_measure_table_urn:
                source_measure_urn = builder.make_schema_field_urn(
                    ref_measure_table_urn, ref_measure_name
                )
                target_measure_urn = builder.make_schema_field_urn(ds_urn, measure_name)

                fine_grained_lineage = FineGrainedLineage(
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    upstreams=[source_measure_urn],
                    downstreams=[target_measure_urn],
                    transformOperation="DAX_MEASURE_TO_MEASURE",
                )
                cll_lineage.append(fine_grained_lineage)
                logger.debug(
                    f"Added DAX measure-to-measure lineage: [{ref_measure_name}] -> {table.name}.{measure_name}"
                )
            else:
                logger.debug(
                    f"Could not find table for referenced measure [{ref_measure_name}] in measure {measure_name}"
                )

        return cll_lineage

    def _process_m_query_upstream_lineage(
        self,
        upstream_lineage: List,
        ds_urn: str,
        upstream: List[UpstreamClass],
        upstream_tables_map: Dict[str, str],
        cll_lineage: List[FineGrainedLineage],
    ) -> None:
        """Process M-Query upstream lineage and update collections in place."""
        for lineage in upstream_lineage:
            for upstream_dpt in lineage.upstreams:
                if (
                    upstream_dpt.data_platform_pair.powerbi_data_platform_name
                    not in self.__config.dataset_type_mapping
                ):
                    logger.debug(
                        f"Skipping upstream table for {ds_urn}. The platform {upstream_dpt.data_platform_pair.powerbi_data_platform_name} is not part of dataset_type_mapping",
                    )
                    continue

                upstream_table_class = UpstreamClass(
                    upstream_dpt.urn,
                    DatasetLineageTypeClass.TRANSFORMED,
                )
                upstream.append(upstream_table_class)

                # Store the URN mapping for this upstream table
                urn_parts = upstream_dpt.urn.split(",")
                if len(urn_parts) >= 2:
                    table_id = urn_parts[1]
                    upstream_tables_map[table_id] = upstream_dpt.urn
                    if "." in table_id:
                        table_name = table_id.split(".")[-1]
                        upstream_tables_map[table_name] = upstream_dpt.urn
                        upstream_tables_map[table_name.lower()] = upstream_dpt.urn

            # Add column level lineage
            cll_lineage.extend(
                self.make_fine_grained_lineage_class(
                    lineage=lineage,
                    dataset_urn=ds_urn,
                    upstream_tables=upstream_tables_map,
                )
            )

    def _process_dax_table_expression_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        upstream_tables_map: Dict[str, str],
        data_model_tables: Optional[List[Dict]],
    ) -> Tuple[List[UpstreamClass], List[FineGrainedLineage]]:
        """Process lineage from DAX table expression."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
            parse_summarize_expression,
        )

        if not (table.expression and _is_dax_expression(table.expression)):
            return [], []

        logger.info(f"Processing DAX calculated table: {table.full_name}")

        upstream: List[UpstreamClass] = []
        cll_lineage: List[FineGrainedLineage] = []

        try:
            # Try SUMMARIZE parsing first
            summarize_parsed = parse_summarize_expression(table.expression)
            references = extract_table_column_references(table.expression)

            if summarize_parsed:
                logger.info(
                    f"Parsed SUMMARIZE expression: {len(summarize_parsed.direct_mappings)} direct mappings, "
                    f"{len(summarize_parsed.calculated_mappings)} calculated mappings"
                )

            # Extract referenced tables and create upstream
            referenced_tables, _ = self._extract_referenced_tables_from_dax(
                table.expression
            )
            upstream = self._create_upstream_for_dax_tables(
                referenced_tables, upstream_tables_map, table.full_name
            )

            # Process SUMMARIZE or fallback to generic processing
            if summarize_parsed:
                cll_lineage.extend(
                    self._process_summarize_direct_mappings(
                        summarize_parsed.direct_mappings,
                        upstream_tables_map,
                        ds_urn,
                        table.name,
                    )
                )
                cll_lineage.extend(
                    self._process_summarize_calculated_mappings(
                        summarize_parsed.calculated_mappings,
                        upstream_tables_map,
                        ds_urn,
                        table.name,
                    )
                )
            elif table.columns:
                # Fallback: try name-based mapping
                cll_lineage.extend(
                    self._process_dax_column_name_matching(
                        table.columns,
                        references,
                        upstream_tables_map,
                        ds_urn,
                        table.name,
                    )
                )

        except Exception as e:
            logger.warning(f"Failed to extract DAX table lineage for {table.name}: {e}")

        return upstream, cll_lineage

    def _process_dax_column_name_matching(
        self,
        columns: List,
        references: List,
        upstream_tables_map: Dict[str, str],
        ds_urn: str,
        table_name: str,
    ) -> List[FineGrainedLineage]:
        """Process column lineage using name-based matching."""
        from datahub.ingestion.source.powerbi.dax_parser import (
            extract_table_column_references,
        )

        cll_lineage: List[FineGrainedLineage] = []

        for target_column in columns:
            # Try direct name matching
            for ref in references:
                if ref.column_name and ref.column_name == target_column.name:
                    source_table_urn = self._resolve_table_urn_with_fallback(
                        ref.table_name, upstream_tables_map
                    )
                    if source_table_urn:
                        cll_lineage.append(
                            self._make_fine_grained_lineage(
                                source_table_urn,
                                ref.column_name,
                                ds_urn,
                                target_column.name,
                                "DAX_CALCULATED_TABLE",
                            )
                        )
                        break

            # Process column's own expression
            if hasattr(target_column, "expression") and target_column.expression:
                try:
                    col_refs = extract_table_column_references(target_column.expression)
                    for ref in col_refs:
                        if not ref.column_name:
                            continue
                        source_table_urn = self._resolve_table_urn_with_fallback(
                            ref.table_name, upstream_tables_map
                        )
                        if source_table_urn:
                            cll_lineage.append(
                                self._make_fine_grained_lineage(
                                    source_table_urn,
                                    ref.column_name,
                                    ds_urn,
                                    target_column.name,
                                    "DAX_CALCULATED_TABLE",
                                )
                            )
                except Exception as e:
                    logger.debug(
                        f"Failed to parse column expression for {target_column.name}: {e}"
                    )

        return cll_lineage

    def extract_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        dax_table_mappings: Optional[Dict[str, str]] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []

        upstream: List[UpstreamClass] = []
        cll_lineage: List[FineGrainedLineage] = []

        logger.debug(
            f"Extracting lineage for table {table.full_name} in dataset {table.dataset.name if table.dataset else None}"
        )

        # Extract M-Query lineage (if not DAX)
        upstream_lineage = self._extract_m_query_lineage(table)

        # Build upstream tables map for DAX reference resolution
        upstream_tables_map = self._build_upstream_tables_map(dax_table_mappings)

        # Handle DAX table expressions (calculated tables)
        if table.expression and _is_dax_expression(table.expression):
            data_model_tables = self._build_data_model_tables_for_parameters(
                table.dataset
            )
            dax_upstream, dax_cll = self._process_dax_table_expression_lineage(
                table, ds_urn, upstream_tables_map, data_model_tables
            )
            upstream.extend(dax_upstream)
            cll_lineage.extend(dax_cll)

        # Handle DAX measures
        if table.measures:
            from datahub.ingestion.source.powerbi.dax_parser import (
                parse_dax_expression,
            )

            measure_map = {m.name: m.expression for m in table.measures if m.expression}
            data_model_tables = self._build_data_model_tables_for_parameters(
                table.dataset
            )

            measure_lineage_count = 0
            for measure in table.measures:
                if measure.expression and _is_dax_expression(measure.expression):
                    try:
                        # Use comprehensive DAX parsing with parameter resolution
                        # This will resolve field parameters to ALL possible columns they can reference
                        dax_result = parse_dax_expression(
                            measure.expression,
                            include_measure_refs=True,
                            include_advanced_analysis=False,  # Skip for performance, use if needed
                            extract_parameters=True,
                            data_model_tables=data_model_tables,
                            include_all_functions=True,
                        )

                        # Handle column references using helper
                        measure_cll = self._process_dax_measure_column_refs(
                            measure.name,
                            dax_result.table_column_references,
                            upstream_tables_map,
                            ds_urn,
                            upstream,
                        )
                        cll_lineage.extend(measure_cll)
                        measure_lineage_count += len(measure_cll)

                        # Handle measure-to-measure dependencies
                        measure_to_measure_cll = (
                            self._process_measure_to_measure_dependencies(
                                dax_result.measure_references,
                                measure_map,
                                table,
                                ds_urn,
                                measure.name,
                            )
                        )
                        cll_lineage.extend(measure_to_measure_cll)
                        measure_lineage_count += len(measure_to_measure_cll)

                    except Exception as e:
                        logger.debug(
                            f"Failed to extract DAX lineage for measure {measure.name}: {e}"
                        )

            if measure_lineage_count > 0:
                logger.info(
                    f"Created {measure_lineage_count} column-level lineage edges from {len(table.measures)} DAX measures in {table.name}"
                )

        # Handle DAX calculated columns
        if table.columns:
            calculated_column_lineage_count = 0
            for column in table.columns:
                if column.expression and _is_dax_expression(column.expression):
                    try:
                        calc_cll = self._process_dax_calculated_column_refs(
                            column.name,
                            column.expression,
                            upstream_tables_map,
                            ds_urn,
                            upstream,
                        )
                        cll_lineage.extend(calc_cll)
                        calculated_column_lineage_count += len(calc_cll)
                    except Exception as e:
                        logger.debug(
                            f"Failed to extract DAX lineage for calculated column {column.name}: {e}"
                        )

            if calculated_column_lineage_count > 0:
                logger.info(
                    f"Created {calculated_column_lineage_count} column-level lineage edges from DAX calculated columns in {table.name}"
                )

        # Process M-Query upstream lineage
        self._process_m_query_upstream_lineage(
            upstream_lineage, ds_urn, upstream, upstream_tables_map, cll_lineage
        )

        if len(upstream) > 0:
            upstream_lineage_class: UpstreamLineageClass = UpstreamLineageClass(
                upstreams=upstream,
                fineGrainedLineages=cll_lineage or None,
            )

            logger.debug(f"Dataset urn = {ds_urn} and its lineage = {upstream_lineage}")

            mcp = MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=upstream_lineage_class,
            )
            mcps.append(mcp)

        return mcps

    def create_datahub_owner_urn(self, user: str) -> str:
        """
        Create corpuser urn from PowerBI (configured by| modified by| created by) user
        """
        if self.__config.ownership.remove_email_suffix:
            return builder.make_user_urn(user.split("@")[0])
        return builder.make_user_urn(f"users.{user}")

    def to_datahub_schema_field(
        self,
        field: Union[powerbi_data_classes.Column, powerbi_data_classes.Measure],
    ) -> SchemaFieldClass:
        data_type = field.dataType
        if isinstance(field, powerbi_data_classes.Measure):
            description = (
                f"{field.expression} {field.description}"
                if field.description
                else field.expression
            )
        elif field.description:
            description = field.description
        else:
            description = None

        schema_field = SchemaFieldClass(
            fieldPath=f"{field.name}",
            type=SchemaFieldDataTypeClass(type=field.datahubDataType),
            nativeDataType=data_type,
            description=description,
        )
        return schema_field

    def to_datahub_schema(
        self,
        table: powerbi_data_classes.Table,
    ) -> SchemaMetadataClass:
        fields = []
        table_fields = (
            [self.to_datahub_schema_field(column) for column in table.columns]
            if table.columns
            else []
        )
        measure_fields = (
            [self.to_datahub_schema_field(measure) for measure in table.measures]
            if table.measures
            else []
        )
        fields.extend(table_fields)
        fields.extend(measure_fields)

        schema_metadata = SchemaMetadataClass(
            schemaName=table.name,
            platform=self.__config.platform_urn,
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

        return schema_metadata

    def to_datahub_dataset(
        self,
        dataset: Optional[powerbi_data_classes.PowerBIDataset],
        workspace: powerbi_data_classes.Workspace,
        dax_table_mappings: Optional[Dict[str, str]] = None,
        report_container_key: Optional[ContainerKey] = None,
        is_embedded: bool = False,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
        In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.

        Args:
            dataset: The PowerBI dataset to convert
            workspace: The workspace containing the dataset
            dax_table_mappings: Optional DAX table mappings for lineage
            report_container_key: If provided, place dataset tables in this Report container (for embedded datasources)
            is_embedded: Whether this is an embedded datasource (affects container placement)
        """

        dataset_mcps: List[MetadataChangeProposalWrapper] = []

        if dataset is None:
            return dataset_mcps

        logger.debug(f"Processing dataset {dataset.name} (embedded={is_embedded})")

        if not any(
            [
                self.__config.filter_dataset_endorsements.allowed(tag)
                for tag in (dataset.tags or [""])
            ]
        ):
            logger.debug(
                "Returning empty dataset_mcps as no dataset tag matched with filter_dataset_endorsements"
            )
            return dataset_mcps

        logger.debug(
            f"Mapping dataset={dataset.name}(id={dataset.id}) to datahub dataset"
        )

        if self.__config.extract_datasets_to_containers:
            dataset_mcps.extend(self.generate_container_for_dataset(dataset))

        for table in dataset.tables:
            # Create a URN for dataset (always lowercase for consistency)
            ds_urn = builder.make_dataset_urn_with_platform_instance(
                platform=self.__config.platform_name,
                name=table.full_name.lower(),
                platform_instance=self.__config.platform_instance,
                env=self.__config.env,
            )

            logger.debug(f"dataset_urn={ds_urn}")
            # Create datasetProperties mcp
            if table.expression:
                view_properties = ViewPropertiesClass(
                    materialized=False,
                    viewLogic=table.expression,
                    viewLanguage="m_query",
                )
                view_prop_mcp = self.new_mcp(
                    entity_urn=ds_urn,
                    aspect=view_properties,
                )
                dataset_mcps.extend([view_prop_mcp])
            ds_properties = DatasetPropertiesClass(
                name=table.name,
                description=dataset.description,
                externalUrl=dataset.webUrl,
                customProperties={
                    "datasetId": dataset.id,
                },
            )

            info_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=ds_properties,
            )

            # Remove status mcp
            status_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=StatusClass(removed=False),
            )
            if self.__config.extract_dataset_schema:
                dataset_mcps.extend(self.extract_dataset_schema(table, ds_urn))

            subtype_mcp = self.new_mcp(
                entity_urn=ds_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        BIContainerSubTypes.POWERBI_DATASET_TABLE,
                    ]
                ),
            )
            # normally, the person who configures the dataset will be the most accurate person for ownership
            if (
                self.__config.extract_ownership
                and self.__config.ownership.dataset_configured_by_as_owner
                and dataset.configuredBy
            ):
                # Dashboard Ownership
                user_urn = self.create_datahub_owner_urn(dataset.configuredBy)
                owner_class = OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
                # Dashboard owner MCP
                ownership = OwnershipClass(owners=[owner_class])
                owner_mcp = self.new_mcp(
                    entity_urn=ds_urn,
                    aspect=ownership,
                )
                dataset_mcps.extend([owner_mcp])

            dataset_mcps.extend([info_mcp, status_mcp, subtype_mcp])

            if self.__config.extract_lineage is True:
                dataset_mcps.extend(
                    self.extract_lineage(table, ds_urn, dax_table_mappings)
                )

            # For embedded datasources in V2 mode, place in Report container
            # Otherwise, use standard container hierarchy (dataset or workspace)
            if is_embedded and report_container_key:
                logger.debug(
                    f"Adding embedded datasource table {table.name} to Report container"
                )
                self.append_container_mcp(
                    dataset_mcps,
                    ds_urn,
                    report_container_key,
                )
            else:
                self.append_container_mcp(
                    dataset_mcps,
                    ds_urn,
                    dataset=dataset,
                )

            self.append_tag_mcp(
                dataset_mcps,
                ds_urn,
                Constant.DATASET,
                dataset.tags,
            )
            self.extract_profile(dataset_mcps, workspace, dataset, table, ds_urn)

        return dataset_mcps

    def extract_profile(
        self,
        dataset_mcps: List[MetadataChangeProposalWrapper],
        workspace: powerbi_data_classes.Workspace,
        dataset: powerbi_data_classes.PowerBIDataset,
        table: powerbi_data_classes.Table,
        ds_urn: str,
    ) -> None:
        if not self.__config.profiling.enabled:
            # Profiling not enabled
            return

        if not self.__config.profile_pattern.allowed(
            f"{workspace.name}.{dataset.name}.{table.name}"
        ):
            logger.info(
                f"Table {table.name} in {dataset.name}, not allowed for profiling"
            )
            return
        logger.debug(f"Profiling table: {table.name}")

        profile = DatasetProfileClass(timestampMillis=builder.get_sys_time())
        profile.rowCount = table.row_count
        profile.fieldProfiles = []

        columns: List[
            Union[powerbi_data_classes.Column, powerbi_data_classes.Measure]
        ] = [*(table.columns or []), *(table.measures or [])]
        for column in columns:
            allowed_column = self.__config.profile_pattern.allowed(
                f"{workspace.name}.{dataset.name}.{table.name}.{column.name}"
            )
            if column.isHidden or not allowed_column:
                logger.info(f"Column {column.name} not allowed for profiling")
                continue
            measure_profile = column.measure_profile
            if measure_profile:
                field_profile = DatasetFieldProfileClass(column.name or "")
                field_profile.sampleValues = measure_profile.sample_values
                field_profile.min = measure_profile.min
                field_profile.max = measure_profile.max
                field_profile.uniqueCount = measure_profile.unique_count
                profile.fieldProfiles.append(field_profile)

        profile.columnCount = table.column_count

        mcp = MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=profile,
        )
        dataset_mcps.append(mcp)

    @staticmethod
    def transform_tags(tags: List[str]) -> GlobalTagsClass:
        return GlobalTagsClass(
            tags=[
                TagAssociationClass(builder.make_tag_urn(tag_to_add))
                for tag_to_add in tags
            ]
        )

    def to_datahub_chart_mcp(
        self,
        tile: powerbi_data_classes.Tile,
        ds_mcps: List[MetadataChangeProposalWrapper],
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi tile to datahub chart
        """
        logger.info(f"Converting tile {tile.title}(id={tile.id}) to chart")
        # Create a URN for chart
        chart_urn = builder.make_chart_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=tile.get_urn_part(),
        )

        logger.info(f"{Constant.CHART_URN}={chart_urn}")

        ds_input: List[str] = self.to_urn_set(
            [x for x in ds_mcps if x.entityType == Constant.DATASET]
        )

        def tile_custom_properties(tile: powerbi_data_classes.Tile) -> dict:
            custom_properties: dict = {
                Constant.CREATED_FROM: tile.createdFrom.value,
            }

            if tile.dataset_id is not None:
                custom_properties[Constant.DATASET_ID] = tile.dataset_id

            if tile.dataset is not None and tile.dataset.webUrl is not None:
                custom_properties[Constant.DATASET_WEB_URL] = tile.dataset.webUrl

            if tile.report_id is not None:
                custom_properties[Constant.REPORT_ID] = tile.report_id

            if tile.report is not None and tile.report.webUrl is not None:
                custom_properties[Constant.REPORT_WEB_URL] = tile.report.webUrl

            return custom_properties

        # Create chartInfo mcp
        # Set chartUrl only if tile is created from Report
        chart_info_instance = ChartInfoClass(
            title=tile.title or "",
            description=tile.title or "",
            lastModified=ChangeAuditStamps(),
            inputs=ds_input,
            externalUrl=tile.report.webUrl if tile.report else None,
            customProperties=tile_custom_properties(tile),
        )

        info_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=StatusClass(removed=False),
        )

        # Subtype mcp
        subtype_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=SubTypesClass(
                typeNames=[BIAssetSubTypes.POWERBI_TILE],
            ),
        )

        # ChartKey aspect
        # Note: we previously would emit a ChartKey aspect with incorrect information.
        # Explicitly emitting this aspect isn't necessary, but we do it here to ensure that
        # the old, bad data gets overwritten.
        chart_key_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=ChartUrn.from_string(chart_urn).to_key_aspect(),
        )

        # Browse path
        browse_path = BrowsePathsClass(paths=[f"/powerbi/{workspace.name}"])
        browse_path_mcp = self.new_mcp(
            entity_urn=chart_urn,
            aspect=browse_path,
        )
        result_mcps = [
            info_mcp,
            status_mcp,
            subtype_mcp,
            chart_key_mcp,
            browse_path_mcp,
        ]

        self.append_container_mcp(
            result_mcps,
            chart_urn,
        )

        return result_mcps

    # written in this style to fix linter error
    def to_urn_set(self, mcps: List[MetadataChangeProposalWrapper]) -> List[str]:
        return deduplicate_list(
            [
                mcp.entityUrn
                for mcp in mcps
                if mcp is not None and mcp.entityUrn is not None
            ]
        )

    def to_datahub_dashboard_mcp(
        self,
        dashboard: powerbi_data_classes.Dashboard,
        workspace: powerbi_data_classes.Workspace,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
        dashboard_edges: List[EdgeClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dashboard to Datahub dashboard
        """

        dashboard_urn = builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=dashboard.get_urn_part(),
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        def chart_custom_properties(dashboard: powerbi_data_classes.Dashboard) -> dict:
            return {
                Constant.CHART_COUNT: str(len(dashboard.tiles)),
                Constant.WORKSPACE_NAME: dashboard.workspace_name,
                Constant.WORKSPACE_ID: dashboard.workspace_id,
            }

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=dashboard.description,
            title=dashboard.displayName or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=dashboard.webUrl,
            customProperties={**chart_custom_properties(dashboard)},
            dashboards=dashboard_edges,
        )

        info_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_key_cls,
        )

        # Dashboard Ownership
        owners = [
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            # Dashboard owner MCP
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=ownership,
            )

        # Dashboard browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{dashboard.workspace_name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=browse_path,
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
        ]

        if owner_mcp is not None:
            list_of_mcps.append(owner_mcp)

        self.append_container_mcp(
            list_of_mcps,
            dashboard_urn,
        )

        self.append_tag_mcp(
            list_of_mcps,
            dashboard_urn,
            Constant.DASHBOARD,
            dashboard.tags,
        )

        return list_of_mcps

    def append_container_mcp(
        self,
        list_of_mcps: List[MetadataChangeProposalWrapper],
        entity_urn: str,
        container_key: Optional[ContainerKey] = None,
        dataset: Optional[powerbi_data_classes.PowerBIDataset] = None,
    ) -> None:
        # If a specific container key is provided, use it (e.g., Report container)
        if container_key is not None:
            pass  # Use the provided container_key
        elif self.__config.extract_datasets_to_containers and isinstance(
            dataset, powerbi_data_classes.PowerBIDataset
        ):
            container_key = dataset.get_dataset_key(self.__config.platform_name)
        elif self.__config.extract_workspaces_to_containers and self.workspace_key:
            container_key = self.workspace_key
        else:
            return None

        container_urn = builder.make_container_urn(
            guid=container_key.guid(),
        )
        mcp = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=ContainerClass(container=f"{container_urn}"),
        )
        list_of_mcps.append(mcp)

    def generate_container_for_workspace(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        self.workspace_key = workspace.get_workspace_key(
            platform_name=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            workspace_id_as_urn_part=self.__config.workspace_id_as_urn_part,
        )
        container_work_units = gen_containers(
            container_key=self.workspace_key,
            name=workspace.name,
            sub_types=[workspace.type],
            extra_properties={
                "workspace_id": workspace.id,
                "workspace_name": workspace.name,
                "workspace_type": workspace.type,
            },
        )
        return container_work_units

    def generate_container_for_dataset(
        self, dataset: powerbi_data_classes.PowerBIDataset
    ) -> Iterable[MetadataChangeProposalWrapper]:
        dataset_key = dataset.get_dataset_key(self.__config.platform_name)
        container_work_units = gen_containers(
            container_key=dataset_key,
            name=dataset.name if dataset.name else dataset.id,
            parent_container_key=self.workspace_key,
            sub_types=[BIContainerSubTypes.POWERBI_DATASET],
        )

        # The if statement here is just to satisfy mypy
        return [
            wu.metadata
            for wu in container_work_units
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]

    def append_tag_mcp(
        self,
        list_of_mcps: List[MetadataChangeProposalWrapper],
        entity_urn: str,
        entity_type: str,
        tags: List[str],
    ) -> None:
        if self.__config.extract_endorsements_to_tags and tags:
            tags_mcp = self.new_mcp(
                entity_urn=entity_urn,
                aspect=self.transform_tags(tags),
            )
            list_of_mcps.append(tags_mcp)

    def to_datahub_user(
        self, user: powerbi_data_classes.User
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi user to datahub user
        """

        logger.debug(f"Mapping user {user.displayName}(id={user.id}) to datahub's user")

        # Create an URN for user
        user_id = user.get_urn_part(
            use_email=self.__config.ownership.use_powerbi_email,
            remove_email_suffix=self.__config.ownership.remove_email_suffix,
        )
        user_urn = builder.make_user_urn(user_id)
        user_key = CorpUserKeyClass(username=user.id)

        user_key_mcp = self.new_mcp(
            entity_urn=user_urn,
            aspect=user_key,
        )

        return [user_key_mcp]

    def to_datahub_users(
        self, users: List[powerbi_data_classes.User]
    ) -> List[MetadataChangeProposalWrapper]:
        user_mcps = []

        for user in users:
            if user:
                user_rights = [
                    user.datasetUserAccessRight,
                    user.reportUserAccessRight,
                    user.dashboardUserAccessRight,
                    user.groupUserAccessRight,
                ]
                if (
                    user.principalType == "User"
                    and self.__config.ownership.owner_criteria
                    and len(
                        set(user_rights) & set(self.__config.ownership.owner_criteria)
                    )
                    > 0
                ) or self.__config.ownership.owner_criteria is None:
                    user_mcps.extend(self.to_datahub_user(user))
                else:
                    continue

        return user_mcps

    def to_datahub_chart(
        self,
        tiles: List[powerbi_data_classes.Tile],
        workspace: powerbi_data_classes.Workspace,
    ) -> Tuple[
        List[MetadataChangeProposalWrapper], List[MetadataChangeProposalWrapper]
    ]:
        ds_mcps = []
        chart_mcps = []

        # Return empty list if input list is empty
        if not tiles:
            return [], []

        logger.info(f"Converting tiles(count={len(tiles)}) to charts")

        for tile in tiles:
            if tile is None:
                continue
            # First convert the dataset to MCP, because dataset mcp is used in input attribute of chart mcp
            dataset_mcps = self.to_datahub_dataset(tile.dataset, workspace)
            # Now convert tile to chart MCP
            chart_mcp = self.to_datahub_chart_mcp(tile, dataset_mcps, workspace)

            ds_mcps.extend(dataset_mcps)
            chart_mcps.extend(chart_mcp)

        # Return dataset and chart MCPs

        return ds_mcps, chart_mcps

    def to_datahub_work_units(
        self,
        dashboard: powerbi_data_classes.Dashboard,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[EquableMetadataWorkUnit]:
        mcps: List[MetadataChangeProposalWrapper] = []

        logger.info(
            f"Converting dashboard={dashboard.displayName} to datahub dashboard"
        )

        # Convert user to CorpUser
        user_mcps: List[MetadataChangeProposalWrapper] = self.to_datahub_users(
            dashboard.users
        )
        # Convert tiles to charts
        ds_mcps, chart_mcps = self.to_datahub_chart(dashboard.tiles, workspace)

        # collect all downstream reports (dashboards)
        dashboard_edges = []
        for t in dashboard.tiles:
            if t.report:
                dashboard_urn = builder.make_dashboard_urn(
                    platform=self.__config.platform_name,
                    platform_instance=self.__config.platform_instance,
                    name=t.report.get_urn_part(),
                )
                edge = EdgeClass(
                    destinationUrn=dashboard_urn,
                )
                dashboard_edges.append(edge)

        # Lets convert dashboard to datahub dashboard
        dashboard_mcps: List[MetadataChangeProposalWrapper] = (
            self.to_datahub_dashboard_mcp(
                dashboard=dashboard,
                workspace=workspace,
                chart_mcps=chart_mcps,
                user_mcps=user_mcps,
                dashboard_edges=dashboard_edges,
            )
        )

        # Now add MCPs in sequence
        mcps.extend(ds_mcps)
        if self.__config.ownership.create_corp_user:
            mcps.extend(user_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(dashboard_mcps)

        # Convert MCP to work_units
        work_units = map(self._to_work_unit, mcps)
        # Return set of work_unit
        return deduplicate_list([wu for wu in work_units if wu is not None])

    def extract_visualizations_from_pbix(
        self,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
        pbix_metadata: Dict[str, Any],
    ) -> Tuple[List[MetadataChangeProposalWrapper], Dict[str, List[str]]]:
        """
        Extract individual visualizations from PBIX metadata and create Chart entities.

        Returns:
            Tuple of (visualization_mcps, page_to_viz_urns_map)
        """
        visualization_mcps: List[MetadataChangeProposalWrapper] = []
        page_to_viz_urns: Dict[str, List[str]] = {}

        lineage_info = pbix_metadata.get("lineage", {})
        visualization_lineages = lineage_info.get("visualization_lineage", [])

        if not visualization_lineages:
            logger.warning(
                f"No visualization lineage found in PBIX for report {report.name}"
            )
            return visualization_mcps, page_to_viz_urns

        logger.info(
            f"Processing {len(visualization_lineages)} visualizations from PBIX"
        )

        # Build dataset URN map
        dataset_urn_map: Dict[str, str] = {}
        for mcp in ds_mcps:
            if mcp.entityType == DatasetUrn.ENTITY_TYPE and mcp.entityUrn:
                urn_parts = mcp.entityUrn.split(",")
                if len(urn_parts) >= 2:
                    dataset_id = urn_parts[1]
                    if "." in dataset_id:
                        table_name = dataset_id.split(".")[-1]
                        normalized_name = table_name.replace("_", " ").lower()
                        dataset_urn_map[normalized_name] = mcp.entityUrn

        for viz_lineage in visualization_lineages:
            viz_id = viz_lineage.get("visualizationId")
            viz_type = viz_lineage.get("visualizationType", "visual")
            section_name = viz_lineage.get("sectionName", "Unknown Page")
            section_id = viz_lineage.get("sectionId")

            if not viz_id:
                logger.warning(f"Skipping visualization without ID in {section_name}")
                continue

            # Create chart URN for this visualization
            chart_urn = builder.make_chart_urn(
                platform=self.__config.platform_name,
                platform_instance=self.__config.platform_instance,
                name=f"{report.get_urn_part()}.visualizations.{viz_id}",
            )

            # Track this visualization for the page
            if section_id:
                if section_id not in page_to_viz_urns:
                    page_to_viz_urns[section_id] = []
                page_to_viz_urns[section_id].append(chart_urn)

            # Build InputFields for column-level lineage
            input_fields: List[InputField] = []
            columns_processed = set()
            datasets_used = set()

            # Process columns
            for col_lineage in viz_lineage.get("columns", []):
                source_table = col_lineage.get("sourceTable")
                source_column = col_lineage.get("sourceColumn")

                if source_table and source_column:
                    dataset_urn = dataset_urn_map.get(
                        source_table.replace("_", " ").lower()
                    )
                    if dataset_urn:
                        datasets_used.add(dataset_urn)
                        field_key = f"{dataset_urn}:{source_column}"
                        if field_key not in columns_processed:
                            columns_processed.add(field_key)
                            input_fields.append(
                                InputField(
                                    schemaFieldUrn=builder.make_schema_field_urn(
                                        parent_urn=dataset_urn,
                                        field_path=source_column,
                                    ),
                                    schemaField=SchemaFieldClass(
                                        fieldPath=source_column,
                                        type=SchemaFieldDataTypeClass(
                                            type=powerbi_data_classes.FIELD_TYPE_MAPPING.get(
                                                col_lineage.get("dataType", "String"),
                                                powerbi_data_classes.FIELD_TYPE_MAPPING[
                                                    "String"
                                                ],
                                            )
                                        ),
                                        nativeDataType=col_lineage.get(
                                            "dataType", "String"
                                        ),
                                    ),
                                )
                            )

            # Process measures
            for measure_lineage in viz_lineage.get("measures", []):
                source_entity = measure_lineage.get("sourceEntity")
                measure_name = measure_lineage.get("measureName")

                if source_entity and measure_name:
                    dataset_urn = dataset_urn_map.get(
                        source_entity.replace("_", " ").lower()
                    )
                    if dataset_urn:
                        datasets_used.add(dataset_urn)
                        field_key = f"{dataset_urn}:{measure_name}"
                        if field_key not in columns_processed:
                            columns_processed.add(field_key)
                            input_fields.append(
                                InputField(
                                    schemaFieldUrn=builder.make_schema_field_urn(
                                        parent_urn=dataset_urn,
                                        field_path=measure_name,
                                    ),
                                    schemaField=SchemaFieldClass(
                                        fieldPath=measure_name,
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
            chart_inputs = sorted(list(datasets_used)) if datasets_used else []
            chart_info = ChartInfoClass(
                title=f"{section_name} - {viz_type}",
                description=f"{viz_type} visualization on page {section_name}",
                lastModified=ChangeAuditStamps(),
                inputs=chart_inputs,
                customProperties={
                    "visualizationType": viz_type,
                    "visualizationId": viz_id,
                    "pageName": section_name,
                    "reportId": report.id,
                    "reportName": report.name,
                },
            )

            # Create MCPs for this visualization
            chart_mcps_list = [
                self.new_mcp(entity_urn=chart_urn, aspect=chart_info),
                self.new_mcp(entity_urn=chart_urn, aspect=StatusClass(removed=False)),
                self.new_mcp(
                    entity_urn=chart_urn,
                    aspect=SubTypesClass(
                        typeNames=[BIAssetSubTypes.POWERBI_VISUALIZATION]
                    ),
                ),
                self.new_mcp(
                    entity_urn=chart_urn,
                    aspect=BrowsePathsClass(
                        paths=[
                            f"/{Constant.PLATFORM_NAME}/{workspace.name}/{report.name}/{section_name}"
                        ]
                    ),
                ),
            ]

            # Add InputFields if we have column-level lineage
            if input_fields:
                chart_mcps_list.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=chart_urn,
                        aspect=InputFields(
                            fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                        ),
                    )
                )

            # Add to Report container
            report_container_key = self.gen_report_key(report.id)
            self.append_container_mcp(chart_mcps_list, chart_urn, report_container_key)

            visualization_mcps.extend(chart_mcps_list)

        logger.info(
            f"Created {len(visualization_mcps)} MCPs for {len(visualization_lineages)} visualizations "
            f"across {len(page_to_viz_urns)} pages"
        )

        return visualization_mcps, page_to_viz_urns

    def pages_as_dashboards_v2(
        self,
        report: powerbi_data_classes.Report,
        pages: List[powerbi_data_classes.Page],
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
        page_to_viz_urns: Dict[str, List[str]],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Create Power BI Pages as Dashboard entities with links to their visualizations.
        """
        dashboard_mcps = []

        if not pages:
            return []

        logger.debug(f"Converting {len(pages)} pages to dashboards")

        for page in pages:
            if page is None:
                continue

            # Create dashboard URN for the page
            dashboard_urn = builder.make_dashboard_urn(
                platform=self.__config.platform_name,
                platform_instance=self.__config.platform_instance,
                name=f"{report.get_urn_part()}.pages.{page.id}",
            )

            # Get visualization URNs for this page
            chart_urn_list = page_to_viz_urns.get(page.id, [])

            logger.debug(
                f"Converting page {page.displayName} to dashboard with {len(chart_urn_list)} visualizations"
            )

            # Create DashboardInfo
            dashboard_info = DashboardInfoClass(
                title=page.displayName or f"Page {page.order}",
                description=f"Power BI Page from report {report.name}",
                charts=chart_urn_list,
                lastModified=ChangeAuditStamps(),
                customProperties={
                    "order": str(page.order),
                    "reportId": report.id,
                    "reportName": report.name,
                    "pageId": page.id,
                    "visualizationCount": str(len(chart_urn_list)),
                },
            )

            info_mcp = self.new_mcp(entity_urn=dashboard_urn, aspect=dashboard_info)
            status_mcp = self.new_mcp(
                entity_urn=dashboard_urn, aspect=StatusClass(removed=False)
            )
            subtype_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=SubTypesClass(
                    typeNames=[BIAssetSubTypes.POWERBI_PAGE_AS_DASHBOARD]
                ),
            )
            browse_path_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=BrowsePathsClass(
                    paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}/{report.name}"]
                ),
            )

            list_of_mcps = [info_mcp, status_mcp, subtype_mcp, browse_path_mcp]

            # Add to Report container
            report_container_key = self.gen_report_key(report.id)
            self.append_container_mcp(list_of_mcps, dashboard_urn, report_container_key)

            dashboard_mcps.extend(list_of_mcps)

        return dashboard_mcps

    def pages_to_chart(
        self,
        pages: List[powerbi_data_classes.Page],
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Legacy method: Convert pages to charts.
        For better alignment with Tableau, use pages_as_dashboards instead.
        """
        chart_mcps = []

        # Return empty list if input list is empty
        if not pages:
            return []

        logger.debug(f"Converting pages(count={len(pages)}) to charts")

        def to_chart_mcps(
            page: powerbi_data_classes.Page,
            ds_mcps: List[MetadataChangeProposalWrapper],
        ) -> List[MetadataChangeProposalWrapper]:
            logger.debug(f"Converting page {page.displayName} to chart")
            # Create a URN for chart
            chart_urn = builder.make_chart_urn(
                platform=self.__config.platform_name,
                platform_instance=self.__config.platform_instance,
                name=page.get_urn_part(),
            )

            logger.debug(f"{Constant.CHART_URN}={chart_urn}")

            ds_input: List[str] = self.to_urn_set(
                [x for x in ds_mcps if x.entityType == Constant.DATASET]
            )

            # Create chartInfo mcp
            # Set chartUrl only if tile is created from Report
            chart_info_instance = ChartInfoClass(
                title=page.displayName or "",
                description=page.displayName or "",
                lastModified=ChangeAuditStamps(),
                inputs=ds_input,
                customProperties={Constant.ORDER: str(page.order)},
            )

            info_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=chart_info_instance,
            )

            # removed status mcp
            status_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=StatusClass(removed=False),
            )
            # Subtype mcp
            subtype_mcp = MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=SubTypesClass(
                    typeNames=[BIAssetSubTypes.POWERBI_PAGE_AS_DASHBOARD],
                ),
            )

            # Browse path
            browse_path = BrowsePathsClass(
                paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}"]
            )
            browse_path_mcp = self.new_mcp(
                entity_urn=chart_urn,
                aspect=browse_path,
            )
            list_of_mcps = [info_mcp, status_mcp, subtype_mcp, browse_path_mcp]

            self.append_container_mcp(
                list_of_mcps,
                chart_urn,
            )

            return list_of_mcps

        for page in pages:
            if page is None:
                continue
            # Now convert tile to chart MCP
            chart_mcp = to_chart_mcps(page, ds_mcps)
            chart_mcps.extend(chart_mcp)

        return chart_mcps

    def gen_report_key(self, report_id: str) -> powerbi_data_classes.ReportKey:
        """Generate a container key for a Power BI Report."""
        return powerbi_data_classes.ReportKey(
            platform=self.__config.platform_name,
            instance=self.__config.platform_instance,
            report=report_id,
        )

    def emit_report_as_container(
        self,
        workspace: powerbi_data_classes.Workspace,
        report: powerbi_data_classes.Report,
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit Power BI Report as a Container entity (similar to Tableau Workbook).
        This aligns with Tableau's entity structure where workbooks are containers.
        """
        report_container_key = self.gen_report_key(report.id)

        user_urn_list: List[str] = self.to_urn_set(user_mcps)
        owner_urn = user_urn_list[0] if user_urn_list else None

        parent_container_key = workspace.get_workspace_key(
            self.__config.platform_name,
            self.__config.platform_instance,
            self.__config.workspace_id_as_urn_part,
        )

        yield from gen_containers(
            container_key=report_container_key,
            name=report.name or "",
            parent_container_key=parent_container_key,
            description=report.description,
            sub_types=[BIContainerSubTypes.POWERBI_REPORT],
            owner_urn=owner_urn,
            external_url=report.webUrl,
            tags=report.tags if report.tags else None,
        )

    def report_to_dashboard(
        self,
        workspace: powerbi_data_classes.Workspace,
        report: powerbi_data_classes.Report,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
        dataset_edges: List[EdgeClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi report to Datahub dashboard.
        NOTE: This is the legacy method. For better entity structure alignment with Tableau,
        use emit_report_as_container + pages_as_dashboards instead.
        """

        dashboard_urn = builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=report.get_urn_part(),
        )

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self.to_urn_set(user_mcps)

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=report.description,
            title=report.name or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=report.webUrl,
            datasetEdges=dataset_edges
            if self.__config.extract_dataset_to_report_lineage
            else None,
        )

        info_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(report.id),
        )

        # Dashboard key
        dashboard_key_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=dashboard_key_cls,
        )
        # Report Ownership
        owners = [
            OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
            for user_urn in user_urn_list
            if user_urn is not None
        ]

        owner_mcp = None
        if len(owners) > 0:
            # Report owner MCP
            ownership = OwnershipClass(owners=owners)
            owner_mcp = self.new_mcp(
                entity_urn=dashboard_urn,
                aspect=ownership,
            )

        # Report browsePaths
        browse_path = BrowsePathsClass(
            paths=[f"/{Constant.PLATFORM_NAME}/{workspace.name}"]
        )
        browse_path_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=browse_path,
        )

        sub_type_mcp = self.new_mcp(
            entity_urn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[report.type.value]),
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
            sub_type_mcp,
        ]

        if owner_mcp is not None:
            list_of_mcps.append(owner_mcp)

        self.append_container_mcp(
            list_of_mcps,
            dashboard_urn,
        )

        self.append_tag_mcp(
            list_of_mcps,
            dashboard_urn,
            Constant.DASHBOARD,
            report.tags,
        )

        return list_of_mcps

    def report_to_datahub_work_units(
        self,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Convert Power BI Report to DataHub entities.
        Supports two modes:
        1. Legacy: Report -> Dashboard, Pages -> Charts
        2. Tableau-style: Report -> Container, Pages -> Dashboards, Visualizations -> Charts
        """
        mcps: List[MetadataChangeProposalWrapper] = []

        # Convert users to CorpUser
        user_mcps = self.to_datahub_users(report.users)

        # Convert dataset tables to dataset entities
        ds_mcps = self.to_datahub_dataset(report.dataset, workspace)

        # Collect all upstream datasets for lineage
        dataset_urns = {
            dataset.entityUrn
            for dataset in ds_mcps
            if dataset.entityType == DatasetUrn.ENTITY_TYPE and dataset.entityUrn
        }
        dataset_edges = [
            EdgeClass(destinationUrn=dataset_urn) for dataset_urn in dataset_urns
        ]

        if self.__config.extract_reports_as_containers:
            logger.debug(
                f"Converting report={report.name} with Reports as Containers (enhanced hierarchy)"
            )

            # Enhanced hierarchy: Report is a Container, Pages are Dashboards, Visualizations are Charts

            # Emit Report as Container
            yield from self.emit_report_as_container(workspace, report, user_mcps)

            # Get report container key for embedded datasources
            report_container_key = self.gen_report_key(report.id)

            # Convert dataset tables - if embedded, place in Report container
            is_embedded_dataset = (
                report.dataset is not None and report.dataset_id is not None
            )
            ds_mcps = self.to_datahub_dataset(
                report.dataset,
                workspace,
                report_container_key=report_container_key
                if is_embedded_dataset
                else None,
                is_embedded=is_embedded_dataset,
            )

            # Extract individual visualizations as Charts if we have PBIX data
            visualization_mcps: List[MetadataChangeProposalWrapper] = []
            page_to_viz_map: Dict[str, List[str]] = {}  # page_id -> [viz_urns]

            if hasattr(report, "pbix_metadata") and report.pbix_metadata:
                logger.info(
                    f"Extracting individual visualizations from PBIX for report {report.name}"
                )
                visualization_mcps, page_to_viz_map = (
                    self.extract_visualizations_from_pbix(
                        report, workspace, ds_mcps, report.pbix_metadata
                    )
                )
                logger.info(f"Extracted {len(visualization_mcps)} visualization MCPs")
            else:
                logger.debug(
                    "No PBIX metadata available, skipping individual visualization extraction"
                )

            # Create Pages as Dashboards within the Report container
            page_dashboard_mcps = self.pages_as_dashboards_v2(
                report, report.pages, workspace, ds_mcps, page_to_viz_map
            )

            mcps.extend(ds_mcps)
            if self.__config.ownership.create_corp_user:
                mcps.extend(user_mcps)
            mcps.extend(visualization_mcps)
            mcps.extend(page_dashboard_mcps)
        else:
            logger.debug(
                f"Converting report={report.name} using legacy hierarchy (backward compatible)"
            )

            # Legacy hierarchy (default, backward compatible): Report is a Dashboard, Pages are Charts

            # Convert pages to charts
            chart_mcps = self.pages_to_chart(report.pages, workspace, ds_mcps)

            # Convert report to dashboard
            report_mcps = self.report_to_dashboard(
                workspace=workspace,
                report=report,
                chart_mcps=chart_mcps,
                user_mcps=user_mcps,
                dataset_edges=dataset_edges,
            )

            mcps.extend(ds_mcps)
            if self.__config.ownership.create_corp_user:
                mcps.extend(user_mcps)
            mcps.extend(chart_mcps)
            mcps.extend(report_mcps)

        return map(self._to_work_unit, mcps)  # type: ignore[return-value]


@platform_name("PowerBI")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.POWERBI_WORKSPACE,
        SourceCapabilityModifier.POWERBI_REPORT,
        SourceCapabilityModifier.POWERBI_DATASET,
    ],
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Disabled by default, configured using `extract_ownership`",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, configured using `extract_lineage`.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Disabled by default, configured using `extract_column_level_lineage`. ",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration profiling.enabled",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class PowerBiDashboardSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Power BI dashboards, tiles and datasets
    - Names, descriptions and URLs of dashboard and tile
    - Owners of dashboards
    """

    source_config: PowerBiDashboardSourceConfig
    reporter: PowerBiDashboardSourceReport
    dataplatform_instance_resolver: AbstractDataPlatformInstanceResolver
    accessed_dashboards: int = 0
    platform: str = "powerbi"

    def __init__(self, config: PowerBiDashboardSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.reporter = PowerBiDashboardSourceReport()
        self.dataplatform_instance_resolver = create_dataplatform_instance_resolver(
            self.source_config
        )
        try:
            self.powerbi_client = PowerBiAPI(
                config=self.source_config,
                reporter=self.reporter,
            )
        except Exception as e:
            logger.warning(e)
            exit(
                1
            )  # Exit pipeline as we are not able to connect to PowerBI API Service. This exit will avoid raising
            # unwanted stacktrace on console

        self.mapper = Mapper(
            ctx, config, self.reporter, self.dataplatform_instance_resolver
        )

        # Create and register the stateful ingestion use-case handler.
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, self.source_config, self.ctx
        )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            PowerBiAPI(
                PowerBiDashboardSourceConfig.parse_obj_allow_extras(config_dict),
                PowerBiDashboardSourceReport(),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = PowerBiDashboardSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_allowed_workspaces(self) -> List[powerbi_data_classes.Workspace]:
        all_workspaces = self.powerbi_client.get_workspaces()
        logger.info(f"Number of workspaces = {len(all_workspaces)}")
        self.reporter.all_workspace_count = len(all_workspaces)
        logger.debug(
            f"All workspaces: {[workspace.format_name_for_logger() for workspace in all_workspaces]}"
        )

        allowed_workspaces = []
        for workspace in all_workspaces:
            if not self.source_config.workspace_id_pattern.allowed(
                workspace.id
            ) or not self.source_config.workspace_name_pattern.allowed(workspace.name):
                self.reporter.filtered_workspace_names.append(
                    f"{workspace.id} - {workspace.name}"
                )
                continue
            elif workspace.type not in self.source_config.workspace_type_filter:
                self.reporter.filtered_workspace_types.append(
                    f"{workspace.id} - {workspace.name} (type = {workspace.type})"
                )
                continue
            else:
                allowed_workspaces.append(workspace)

        logger.info(f"Number of allowed workspaces = {len(allowed_workspaces)}")
        logger.debug(
            f"Allowed workspaces: {[workspace.format_name_for_logger() for workspace in allowed_workspaces]}"
        )

        return allowed_workspaces

    def validate_dataset_type_mapping(self):
        powerbi_data_platforms: List[str] = [
            data_platform.value.powerbi_data_platform_name
            for data_platform in SupportedDataPlatform
        ]

        for key in self.source_config.dataset_type_mapping:
            if key not in powerbi_data_platforms:
                raise ValueError(f"PowerBI DataPlatform {key} is not supported")

        logger.debug(
            f"Dataset lineage would get ingested for data-platform = {self.source_config.dataset_type_mapping}"
        )

    def extract_independent_datasets(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.extract_independent_datasets is False:
            if workspace.independent_datasets:
                self.reporter.info(
                    title="Skipped Independent Dataset",
                    message="Some datasets are not used in any visualizations. To ingest them, enable the `extract_independent_datasets` flag",
                    context=",".join(
                        [
                            dataset.name
                            for dataset in workspace.independent_datasets.values()
                            if dataset.name
                        ]
                    ),
                )
            return

        for dataset in workspace.independent_datasets.values():
            yield from auto_workunit(
                stream=self.mapper.to_datahub_dataset(
                    dataset=dataset,
                    workspace=workspace,
                )
            )

    def pbix_to_powerbi_dataset(
        self,
        pbix_metadata: Dict,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
    ) -> powerbi_data_classes.PowerBIDataset:
        """Convert PBIX data model to PowerBIDataset object."""
        from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
            new_powerbi_dataset,
        )

        logger.debug(
            f"Converting PBIX data model to PowerBIDataset for report {report.name}"
        )

        # Create dataset from report's existing dataset or create new one
        if report.dataset:
            dataset = report.dataset
        else:
            # Create a new dataset based on the report
            dataset = new_powerbi_dataset(
                workspace=workspace,
                raw_instance={
                    "id": report.dataset_id or report.id,
                    "name": report.name,
                },
            )

        # Update dataset with PBIX metadata
        dataset.workspace_id = workspace.id
        dataset.workspace_name = workspace.name

        return dataset

    def pbix_to_powerbi_tables(
        self,
        pbix_tables: List[Dict],
        dataset: powerbi_data_classes.PowerBIDataset,
    ) -> List[powerbi_data_classes.Table]:
        """Convert PBIX tables to Table objects with M-query expressions."""
        from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
            FIELD_TYPE_MAPPING,
            Column,
            Measure,
            Table,
        )

        logger.debug(f"Converting {len(pbix_tables)} PBIX tables to Table objects")

        tables = []
        for pbix_table in pbix_tables:
            table_name = pbix_table.get("name", "Unknown")
            logger.debug(f"Processing table: {table_name}")

            # Create columns
            columns = []
            for pbix_col in pbix_table.get("columns", []):
                col_name = pbix_col.get("name", "Unknown")
                data_type = pbix_col.get("dataType", "String")

                column = Column(
                    name=col_name,
                    dataType=data_type,
                    isHidden=pbix_col.get("isHidden", False),
                    datahubDataType=FIELD_TYPE_MAPPING.get(
                        data_type, FIELD_TYPE_MAPPING["String"]
                    ),
                    columnType=pbix_col.get("columnType"),
                    expression=pbix_col.get("expression"),
                    description=pbix_col.get("formatString"),
                )
                columns.append(column)

            # Create measures
            measures = []
            for pbix_measure in pbix_table.get("measures", []):
                measure_name = pbix_measure.get("name", "Unknown")

                data_type = FIELD_TYPE_MAPPING.get("String")
                measure = Measure(
                    name=measure_name,
                    expression=pbix_measure.get("expression", ""),
                    isHidden=pbix_measure.get("isHidden", False),
                    dataType="measure",
                    datahubDataType=data_type
                    if data_type is not None
                    else NullTypeClass(),
                    description=pbix_measure.get("formatString"),
                )
                measures.append(measure)

            # Extract M-query expression from partitions
            expression = None
            partitions = pbix_table.get("partitions", [])
            if partitions:
                # Use the first partition's source expression
                partition = partitions[0]
                source = partition.get("source", {})
                expression = source.get("expression")

            # Create table with full_name using friendly format: dataset.name.table.name
            table = Table(
                name=table_name,
                full_name=f"{dataset.name}.{table_name}",
                columns=columns,
                measures=measures,
                dataset=dataset,
                expression=expression,
            )
            tables.append(table)

        logger.debug(f"Created {len(tables)} Table objects from PBIX")
        return tables

    def pbix_to_powerbi_pages(
        self,
        pbix_sections: List[Dict],
        dataset: powerbi_data_classes.PowerBIDataset,
    ) -> List[powerbi_data_classes.Page]:
        """Convert PBIX layout sections to Page objects."""
        from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Page

        logger.debug(f"Converting {len(pbix_sections)} PBIX sections to Page objects")

        pages = []
        for idx, section in enumerate(pbix_sections):
            page_name = section.get("displayName", section.get("name", "Unknown"))

            page = Page(
                id=section.get("id", page_name),
                displayName=page_name,
                name=section.get("name", page_name),
                order=idx,  # Use index as order since not available in PBIX
            )
            pages.append(page)

        logger.debug(f"Created {len(pages)} Page objects from PBIX")
        return pages

    def process_pbix_visualizations(
        self,
        pbix_metadata: Dict[str, Any],
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Process visualizations from PBIX and create charts with column-level lineage.

        This extracts visualization data and column-level lineage from the PBIX parser's
        output and creates chart MCPs with InputFields for column-level tracking.
        """
        chart_mcps = []

        lineage_info = pbix_metadata.get("lineage", {})
        visualization_lineages = lineage_info.get("visualization_lineage", [])

        logger.info(
            f"Processing {len(visualization_lineages)} visualizations from PBIX"
        )

        # Build dataset URN map with table names
        dataset_urn_map: Dict[str, str] = {}  # table_name -> dataset_urn
        for mcp in ds_mcps:
            if mcp.entityType == DatasetUrn.ENTITY_TYPE and mcp.entityUrn:
                # Extract table name from dataset URN
                # URN format: urn:li:dataset:(urn:li:dataPlatform:powerbi,<workspace_id>.<dataset_id>.<table_name>,PROD)
                urn_parts = mcp.entityUrn.split(",")
                if len(urn_parts) >= 2:
                    dataset_id = urn_parts[1]
                    if "." in dataset_id:
                        table_name = dataset_id.split(".")[-1]
                        # Normalize: replace underscores with spaces for consistent lookup
                        normalized_name = table_name.replace("_", " ").lower()
                        dataset_urn_map[normalized_name] = mcp.entityUrn

        for viz_lineage in visualization_lineages:
            viz_id = viz_lineage.get("visualizationId")
            viz_type = viz_lineage.get("visualizationType", "visual")
            section_name = viz_lineage.get("sectionName", "Unknown Page")

            # Create chart URN
            chart_urn = builder.make_chart_urn(
                platform=self.source_config.platform_name,
                platform_instance=self.source_config.platform_instance,
                name=f"{report.get_urn_part()}.{viz_id}",
            )

            logger.debug(f"Processing visualization: {viz_type} on page {section_name}")

            # Build InputFields for column-level lineage
            input_fields: List[InputField] = []
            columns_processed = set()  # Track unique column references
            datasets_used = set()  # Track which datasets are actually used

            # Process columns
            for col_lineage in viz_lineage.get("columns", []):
                source_table = col_lineage.get("sourceTable")
                source_column = col_lineage.get("sourceColumn")

                if source_table and source_column:
                    # Find the matching dataset URN (normalize spaces to match map keys)
                    dataset_urn = dataset_urn_map.get(
                        source_table.replace("_", " ").lower()
                    )
                    if dataset_urn:
                        # Track that this dataset is used
                        datasets_used.add(dataset_urn)

                        # Create unique key to avoid duplicates
                        field_key = f"{dataset_urn}:{source_column}"
                        if field_key not in columns_processed:
                            columns_processed.add(field_key)

                            # Create InputField for this column
                            input_fields.append(
                                InputField(
                                    schemaFieldUrn=builder.make_schema_field_urn(
                                        parent_urn=dataset_urn,
                                        field_path=source_column,
                                    ),
                                    schemaField=SchemaFieldClass(
                                        fieldPath=source_column,
                                        type=SchemaFieldDataTypeClass(
                                            type=powerbi_data_classes.FIELD_TYPE_MAPPING.get(
                                                col_lineage.get("dataType", "String"),
                                                powerbi_data_classes.FIELD_TYPE_MAPPING[
                                                    "String"
                                                ],
                                            )
                                        ),
                                        nativeDataType=col_lineage.get(
                                            "dataType", "String"
                                        ),
                                    ),
                                )
                            )

                        logger.debug(
                            f"  Added InputField: {source_table}.{source_column}"
                        )

            # Process measures
            for measure_lineage in viz_lineage.get("measures", []):
                source_entity = measure_lineage.get("sourceEntity")
                measure_name = measure_lineage.get("measureName")

                if source_entity and measure_name:
                    # Find the matching dataset URN (normalize spaces to match map keys)
                    normalized_entity = source_entity.replace("_", " ").lower()
                    dataset_urn = dataset_urn_map.get(normalized_entity)

                    if not dataset_urn:
                        logger.debug(
                            f"  ⚠ Could not find URN for measure source '{source_entity}' (normalized: '{normalized_entity}')"
                        )

                    if dataset_urn:
                        # Track that this dataset is used
                        datasets_used.add(dataset_urn)

                        # Create unique key to avoid duplicates
                        field_key = f"{dataset_urn}:{measure_name}"
                        if field_key not in columns_processed:
                            columns_processed.add(field_key)

                            # Create InputField for this measure (measures are treated as schema fields)
                            input_fields.append(
                                InputField(
                                    schemaFieldUrn=builder.make_schema_field_urn(
                                        parent_urn=dataset_urn,
                                        field_path=measure_name,
                                    ),
                                    schemaField=SchemaFieldClass(
                                        fieldPath=measure_name,
                                        type=SchemaFieldDataTypeClass(
                                            type=powerbi_data_classes.FIELD_TYPE_MAPPING.get(
                                                "measure",  # Measures are a special type
                                                powerbi_data_classes.FIELD_TYPE_MAPPING[
                                                    "String"
                                                ],
                                            )
                                        ),
                                        nativeDataType="measure",
                                    ),
                                )
                            )

                            logger.debug(
                                f"  Added InputField (measure): {source_entity}.{measure_name}"
                            )

            # Create chart info and InputFields - they must be consistent
            # IMPORTANT: For proper display in DataHub, charts need both:
            # 1. Table-level lineage (inputs field)
            # 2. Column-level lineage (InputFields aspect)
            # These should be aligned - only include datasets that have column references

            if datasets_used and input_fields:
                # We have column-level tracking - use only datasets with columns
                chart_inputs = sorted(list(datasets_used))
                logger.debug(
                    f"  Visualization uses {len(datasets_used)} dataset(s) with {len(input_fields)} columns tracked"
                )
            else:
                # No column-level tracking available - skip this visualization or use all datasets
                # For now, log a warning and include all datasets without InputFields
                chart_inputs = [
                    str(mcp.entityUrn)
                    for mcp in ds_mcps
                    if mcp.entityType == DatasetUrn.ENTITY_TYPE
                    and mcp.entityUrn is not None
                ]
                logger.warning(
                    f"  Visualization {viz_id} has no column-level tracking - including all {len(chart_inputs)} dataset(s) without InputFields"
                )

            chart_info = ChartInfoClass(
                title=f"{section_name} - {viz_type}",
                description=f"Visualization of type {viz_type}",
                lastModified=ChangeAuditStamps(),
                inputs=chart_inputs,
            )

            # Browse path
            browse_path = BrowsePathsClass(
                paths=[f"/powerbi/{workspace.name}/{report.name}/{section_name}"]
            )

            # Create MCPs for this chart (core aspects)
            chart_mcps.extend(
                MetadataChangeProposalWrapper.construct_many(
                    entityUrn=chart_urn,
                    aspects=[
                        chart_info,
                        browse_path,
                        StatusClass(removed=False),
                        SubTypesClass(typeNames=[BIAssetSubTypes.CHART]),
                    ],
                )
            )

            # Add InputFields aspect ONLY if we have column-level information
            # This ensures consistency between table-level and column-level lineage
            if input_fields and datasets_used:
                chart_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=chart_urn,
                        aspect=InputFields(
                            fields=sorted(input_fields, key=lambda x: x.schemaFieldUrn)
                        ),
                    )
                )
                logger.debug(
                    f"  Added InputFields aspect with {len(input_fields)} fields for {len(datasets_used)} dataset(s)"
                )

        logger.info(f"Created {len(chart_mcps)} chart MCPs from PBIX visualizations")
        return chart_mcps

    def extract_dax_column_lineage(
        self,
        table: powerbi_data_classes.Table,
        dataset: powerbi_data_classes.PowerBIDataset,
        workspace: powerbi_data_classes.Workspace,
        existing_table_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """
        Extract lineage from DAX expressions in table definitions, measures, and calculated columns.

        This method parses DAX expressions and extracts table/column references,
        building a mapping of table names referenced in DAX to their actual dataset URNs.

        Args:
            existing_table_mappings: Previously built mappings to include (e.g., from earlier tables)

        Returns:
            Dictionary mapping table names (from DAX) to their actual PowerBI dataset URNs
        """

        # Build mapping of table names to their URNs within this dataset
        # This ensures DAX references like 'Sales'[Amount] map to the actual Sales table URN
        table_urn_mapping: Dict[str, str] = {}

        # Start with existing mappings if provided
        if existing_table_mappings:
            table_urn_mapping.update(existing_table_mappings)

        # Note: We don't need to rebuild the mapping here because it's already
        # built completely before any DAX processing begins (in process_report_from_pbix)
        # We just use the existing_table_mappings that were passed in

        # Process the table's own DAX expression (for DAX calculated tables)
        if table.expression and _is_dax_expression(table.expression):
            references = self.mapper._extract_and_log_dax_references(
                table.expression, table.name, "table expression"
            )
            if references:
                self.mapper._log_dax_reference_mappings(references, table_urn_mapping)

        # Process measures with DAX expressions
        if table.measures:
            for measure in table.measures:
                if measure.expression:
                    references = self.mapper._extract_and_log_dax_references(
                        measure.expression, measure.name, "measure"
                    )
                    if references:
                        self.mapper._log_dax_reference_mappings(
                            references, table_urn_mapping
                        )

        # Process calculated columns with DAX expressions
        if table.columns:
            for column in table.columns:
                if column.expression:
                    references = self.mapper._extract_and_log_dax_references(
                        column.expression, column.name, "calculated column"
                    )
                    if references:
                        self.mapper._log_dax_reference_mappings(
                            references, table_urn_mapping
                        )

        return table_urn_mapping

    def process_report_from_pbix(
        self,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
        pbix_path: str,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Extract report metadata from .pbix file instead of using REST API.

        This method:
        1. Parses the .pbix file
        2. Extracts DataModel (tables, columns, measures, relationships)
        3. Extracts Layout (pages, visualizations)
        4. Extracts M-queries from DataModel's table partitions
        5. Parses DAX expressions from measures and calculated columns
        6. Maps PBIX metadata to existing data structures
        7. Generates MCPs for ingestion
        """
        from datahub.ingestion.source.powerbi.parse_pbix import PBIXParser

        logger.info(f"Processing report {report.name} from PBIX file: {pbix_path}")

        try:
            # Parse the .pbix file
            pbix_parser = PBIXParser(pbix_path)
            pbix_metadata = pbix_parser.extract_metadata()

            logger.debug(f"Successfully parsed PBIX file for report {report.name}")

            # Store PBIX metadata on the report for later use (e.g., visualization extraction)
            report.pbix_metadata = pbix_metadata

            # Convert PBIX data to PowerBI data classes
            dataset = self.pbix_to_powerbi_dataset(pbix_metadata, report, workspace)

            # Initialize DAX table mappings
            # This ensures that DAX references use real table URNs, not synthetic "dax_" tables
            all_dax_table_mappings: Dict[str, str] = {}

            # Extract tables from PBIX data model if available
            if pbix_metadata.get("data_model_parsed"):
                pbix_tables = pbix_metadata["data_model_parsed"].get("tables", [])
                tables = self.pbix_to_powerbi_tables(pbix_tables, dataset)
                dataset.tables = tables

                logger.info(
                    f"Extracted {len(tables)} tables from PBIX for report {report.name}"
                )

            # Build complete table URN mapping from dataset tables (whether from PBIX or REST API)
            # This ensures all table references can be resolved regardless of processing order
            if dataset.tables:
                logger.debug(
                    f"Building complete table URN mapping from {len(dataset.tables)} dataset tables"
                )
                for table in dataset.tables:
                    # Use friendly name format: dataset.name.table.name (not UUIDs) - always lowercase
                    table_urn = builder.make_dataset_urn_with_platform_instance(
                        self.source_config.platform_name,
                        f"{dataset.name}.{table.name}".lower(),
                        self.source_config.platform_instance,
                        self.source_config.env,
                    )
                    # Map the exact table name as it appears in the dataset
                    all_dax_table_mappings[table.name] = table_urn
                    all_dax_table_mappings[table.name.lower()] = table_urn
                    logger.debug(f"  Pre-mapped '{table.name}' -> {table_urn}")

                logger.debug(
                    f"Pre-built table mapping with {len(all_dax_table_mappings)} entries"
                )

                # Now process DAX expressions for each table
                # All table references should now be resolvable
                for table in dataset.tables:
                    # Extract DAX lineage from measures and calculated columns
                    dax_mappings = self.extract_dax_column_lineage(
                        table, dataset, workspace, all_dax_table_mappings
                    )
                    all_dax_table_mappings.update(dax_mappings)

                # Process M-queries for lineage, using the DAX table mappings
                for table in dataset.tables:
                    if table.expression:
                        logger.debug(
                            f"Table {table.name} has M-query expression for lineage extraction"
                        )

            # Extract pages from PBIX layout
            if pbix_metadata.get("layout_parsed"):
                pbix_sections = pbix_metadata["layout_parsed"].get("sections", [])
                pages = self.pbix_to_powerbi_pages(pbix_sections, dataset)
                report.pages = pages

                logger.info(
                    f"Extracted {len(pages)} pages from PBIX for report {report.name}"
                )

            # Update report with dataset
            report.dataset = dataset

            # Generate MCPs using existing mapper methods
            # Convert dataset tables to MCPs, passing the DAX table mappings
            ds_mcps = self.mapper.to_datahub_dataset(
                dataset, workspace, all_dax_table_mappings
            )

            # Process visualizations and column-level lineage from PBIX
            chart_mcps = []
            if pbix_metadata.get("lineage") and pbix_metadata.get("layout_parsed"):
                logger.info(
                    "Processing visualizations and column-level lineage from PBIX"
                )
                chart_mcps = self.process_pbix_visualizations(
                    pbix_metadata, report, workspace, ds_mcps
                )
            else:
                # Fallback to basic page conversion if no lineage available
                chart_mcps = self.mapper.pages_to_chart(
                    report.pages, workspace, ds_mcps
                )

            # Convert users
            user_mcps = self.mapper.to_datahub_users(report.users)

            # Collect dataset edges
            dataset_urns = {
                dataset.entityUrn
                for dataset in ds_mcps
                if dataset.entityType == DatasetUrn.ENTITY_TYPE and dataset.entityUrn
            }
            dataset_edges = [
                EdgeClass(destinationUrn=dataset_urn) for dataset_urn in dataset_urns
            ]

            # Convert report to dashboard
            report_mcps = self.mapper.report_to_dashboard(
                workspace=workspace,
                report=report,
                chart_mcps=chart_mcps,
                user_mcps=user_mcps,
                dataset_edges=dataset_edges,
            )

            # Combine all MCPs
            all_mcps = []
            all_mcps.extend(ds_mcps)
            if self.source_config.ownership.create_corp_user:
                all_mcps.extend(user_mcps)
            all_mcps.extend(chart_mcps)
            all_mcps.extend(report_mcps)

            # Convert MCPs to work units
            for mcp in all_mcps:
                wu = self.mapper._to_work_unit(mcp)
                patched_wu = self._get_dashboard_patch_work_unit(wu)
                if patched_wu:
                    yield patched_wu

            logger.info(f"Successfully processed report {report.name} from PBIX file")

        except Exception as e:
            logger.error(
                f"Error processing report {report.name} from PBIX file: {e}",
                exc_info=True,
            )
            self.reporter.warning(
                title="PBIX Processing Error",
                message=f"Failed to process PBIX file for report {report.name}",
                context=f"report_id={report.id}, workspace={workspace.name}, error={str(e)}",
            )

    def emit_app(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataChangeProposalWrapper]:
        if workspace.app is None:
            return

        if not self.source_config.extract_app:
            self.reporter.info(
                title="App Ingestion Is Disabled",
                message="You are missing workspace app metadata. Please set flag `extract_app` to `true` in recipe to ingest workspace app.",
                context=f"workspace-name={workspace.name}, app-name = {workspace.app.name}",
            )
            return

        assets_within_app: List[EdgeClass] = [
            EdgeClass(
                destinationUrn=builder.make_dashboard_urn(
                    platform=self.source_config.platform_name,
                    platform_instance=self.source_config.platform_instance,
                    name=powerbi_data_classes.Dashboard.get_urn_part_by_id(
                        app_dashboard.original_dashboard_id
                    ),
                )
            )
            for app_dashboard in workspace.app.dashboards
        ]

        assets_within_app.extend(
            [
                EdgeClass(
                    destinationUrn=builder.make_dashboard_urn(
                        platform=self.source_config.platform_name,
                        platform_instance=self.source_config.platform_instance,
                        name=powerbi_data_classes.Report.get_urn_part_by_id(
                            app_report.original_report_id
                        ),
                    )
                )
                for app_report in workspace.app.reports
            ]
        )

        if assets_within_app:
            logger.debug(
                f"Emitting metadata-workunits for app {workspace.app.name}({workspace.app.id})"
            )

            app_urn: str = builder.make_dashboard_urn(
                platform=self.source_config.platform_name,
                platform_instance=self.source_config.platform_instance,
                name=powerbi_data_classes.App.get_urn_part_by_id(workspace.app.id),
            )

            dashboard_info: DashboardInfoClass = DashboardInfoClass(
                title=workspace.app.name,
                description=workspace.app.description
                if workspace.app.description
                else workspace.app.name,
                # lastModified=workspace.app.last_update,
                lastModified=ChangeAuditStamps(
                    lastModified=AuditStampClass(
                        actor="urn:li:corpuser:unknown",
                        time=int(
                            datetime.strptime(
                                workspace.app.last_update, "%Y-%m-%dT%H:%M:%S.%fZ"
                            ).timestamp()
                        ),
                    )
                    if workspace.app.last_update
                    else None
                ),
                dashboards=assets_within_app,
            )

            # Browse path
            browse_path: BrowsePathsClass = BrowsePathsClass(
                paths=[f"/powerbi/{workspace.name}"]
            )

            yield from MetadataChangeProposalWrapper.construct_many(
                entityUrn=app_urn,
                aspects=(
                    dashboard_info,
                    browse_path,
                    StatusClass(removed=False),
                    SubTypesClass(typeNames=[BIAssetSubTypes.POWERBI_APP]),
                ),
            )

    def get_workspace_workunit(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        if self.source_config.extract_workspaces_to_containers:
            workspace_workunits = self.mapper.generate_container_for_workspace(
                workspace
            )

            for workunit in workspace_workunits:
                # Return workunit to a Datahub Ingestion framework
                yield workunit

        yield from auto_workunit(self.emit_app(workspace=workspace))

        for dashboard in workspace.dashboards.values():
            try:
                # Fetch PowerBi users for dashboards
                dashboard.users = self.powerbi_client.get_dashboard_users(dashboard)
                # Increase dashboard and tiles count in report
                self.reporter.report_dashboards_scanned()
                self.reporter.report_charts_scanned(count=len(dashboard.tiles))
            except Exception as e:
                message = f"Error ({e}) occurred while loading dashboard {dashboard.displayName}(id={dashboard.id}) tiles."

                logger.exception(message, e)
                self.reporter.report_warning(dashboard.id, message)
            # Convert PowerBi Dashboard and child entities to Datahub work unit to ingest into Datahub
            workunits = self.mapper.to_datahub_work_units(dashboard, workspace)
            for workunit in workunits:
                wu = self._get_dashboard_patch_work_unit(workunit)
                if wu is not None:
                    yield wu

        for report in workspace.reports.values():
            if self.source_config.extract_from_pbix_file:
                # Try PBIX-based extraction
                import os
                import tempfile

                temp_dir = tempfile.gettempdir()
                pbix_output_path = os.path.join(temp_dir, f"powerbi_{report.id}.pbix")

                logger.info(f"Attempting PBIX export for report {report.name}")
                pbix_path = self.powerbi_client.export_report_to_pbix(
                    workspace.id, report.id, pbix_output_path
                )

                if pbix_path:
                    try:
                        yield from self.process_report_from_pbix(
                            report, workspace, pbix_path
                        )
                        logger.info(
                            f"Successfully processed report {report.name} from PBIX"
                        )
                    finally:
                        # Clean up temporary file
                        if os.path.exists(pbix_path):
                            try:
                                os.remove(pbix_path)
                                logger.debug(
                                    f"Cleaned up temporary PBIX file: {pbix_path}"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to clean up PBIX file {pbix_path}: {e}"
                                )
                    continue
                else:
                    # Skip this report on failure
                    self.reporter.warning(
                        title="PBIX Export Failed",
                        message=f"Failed to export report {report.name} to PBIX. Skipping report.",
                        context=f"report_id={report.id}, workspace={workspace.name}",
                    )
                    continue

            # Existing API-based extraction
            for work_unit in self.mapper.report_to_datahub_work_units(
                report, workspace
            ):
                wu = self._get_dashboard_patch_work_unit(work_unit)
                if wu is not None:
                    yield wu

        yield from self.extract_independent_datasets(workspace)

    def _get_dashboard_patch_work_unit(
        self, work_unit: MetadataWorkUnit
    ) -> Optional[MetadataWorkUnit]:
        dashboard_info_aspect: Optional[DashboardInfoClass] = (
            work_unit.get_aspect_of_type(DashboardInfoClass)
        )

        if dashboard_info_aspect and self.source_config.patch_metadata:
            return convert_dashboard_info_to_patch(
                work_unit.get_urn(),
                dashboard_info_aspect,
                work_unit.metadata.systemMetadata,
            )
        else:
            return work_unit

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # As modified_workspaces is not idempotent, hence workunit processors are run later for each workspace_id
        # This will result in creating a checkpoint for each workspace_id
        if self.source_config.modified_since:
            return []  # Handle these in get_workunits_internal
        else:
            return [
                *super().get_workunit_processors(),
                functools.partial(
                    auto_incremental_lineage, self.source_config.incremental_lineage
                ),
                self.stale_entity_removal_handler.workunit_processor,
            ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invokes this method
        """
        logger.info("PowerBi plugin execution is started")
        # Validate dataset type mapping
        self.validate_dataset_type_mapping()
        # Fetch PowerBi workspace for given workspace identifier

        allowed_workspaces = self.get_allowed_workspaces()

        batches = more_itertools.chunked(
            allowed_workspaces, self.source_config.scan_batch_size
        )
        for batch_workspaces in batches:
            for workspace in self.powerbi_client.fill_workspaces(
                batch_workspaces, self.reporter
            ):
                logger.info(f"Processing workspace id: {workspace.id}")

                if self.source_config.modified_since:
                    # As modified_workspaces is not idempotent, hence we checkpoint for each powerbi workspace
                    # Because job_id is used as a dictionary key, we have to set a new job_id
                    # Refer to https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stateful_ingestion_base.py#L390
                    self.stale_entity_removal_handler.set_job_id(workspace.id)
                    self.state_provider.register_stateful_ingestion_usecase_handler(
                        self.stale_entity_removal_handler
                    )

                    yield from self._apply_workunit_processors(
                        [
                            *super().get_workunit_processors(),
                            self.stale_entity_removal_handler.workunit_processor,
                        ],
                        self.get_workspace_workunit(workspace),
                    )
                else:
                    # Maintain backward compatibility
                    yield from self.get_workspace_workunit(workspace)

    def get_report(self) -> SourceReport:
        return self.reporter
