#########################################################
#
# Meta Data Ingestion From the Power BI Source
#
#########################################################
import functools
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import more_itertools
from dateutil import parser as date_parser

import datahub.emitter.mce_builder as builder
import datahub.ingestion.source.powerbi.m_query.data_classes
import datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes as powerbi_data_classes
from datahub.configuration.source_common import PlatformDetail
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
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.fabric.common.urn_generator import make_onelake_urn
from datahub.ingestion.source.identity.corp_user_status import (
    CORP_USER_STATUS_ACTIVE,
    make_corp_user_status_aspect,
)
from datahub.ingestion.source.powerbi.config import (
    PARAMETER_VALUE_MAX_LENGTH,
    POWERBI_TYPE_TO_DATA_PLATFORM_PAIR,
    SECRET_PARAMETER_MARKERS,
    Constant,
    PowerBiAppUrlPattern,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBiMetadataApiMode,
    PowerBIPlatformDetail,
    SupportedDataPlatform,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    AbstractDataPlatformInstanceResolver,
    create_dataplatform_instance_resolver,
)
from datahub.ingestion.source.powerbi.m_query import native_sql_parser, parser
from datahub.ingestion.source.powerbi.powerbi_selection import (
    WORKSPACE_ID_PATTERN,
    WORKSPACE_NAME_PATTERN,
    WORKSPACE_TYPE_FILTER,
    WorkspaceFacts,
    workspace_verdict,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.composite_model import (
    remote_model_key,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.dataflow_definition_parser import (
    compose_let_expression,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.powerbi_api import PowerBiAPI
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import (
    dax_table_references,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    auto_stale_entity_removal,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_stale_entity_removal import (
    AutoStaleEntityRemovalProcessor,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChartInfoClass,
    ChartTypeClass,
    ContainerClass,
    CorpUserInfoClass,
    CorpUserKeyClass,
    DashboardInfoClass,
    DashboardKeyClass,
    DataPlatformInstanceClass,
    DatasetFieldProfileClass,
    DatasetLineageTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    EdgeClass,
    ForeignKeyConstraintClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    OperationClass,
    OperationTypeClass,
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
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo, ColumnRef
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.urns.urn_iter import lowercase_dataset_urn

# Logger instance
logger = logging.getLogger(__name__)

# Most filters serialized into the `filters` custom property of a report or page.
MAX_FILTERS_IN_PROPERTY = 50

# Power BI visual types with an obvious DataHub chart type; anything else (cards,
# slicers, maps, gauges, custom visuals, ...) keeps only the raw `visualType`
# property.
VISUAL_TYPE_TO_CHART_TYPE: Dict[str, str] = {
    "barChart": ChartTypeClass.BAR,
    "clusteredBarChart": ChartTypeClass.BAR,
    "stackedBarChart": ChartTypeClass.BAR,
    "hundredPercentStackedBarChart": ChartTypeClass.BAR,
    "columnChart": ChartTypeClass.BAR,
    "clusteredColumnChart": ChartTypeClass.BAR,
    "stackedColumnChart": ChartTypeClass.BAR,
    "hundredPercentStackedColumnChart": ChartTypeClass.BAR,
    "lineChart": ChartTypeClass.LINE,
    "areaChart": ChartTypeClass.AREA,
    "stackedAreaChart": ChartTypeClass.AREA,
    "pieChart": ChartTypeClass.PIE,
    "donutChart": ChartTypeClass.PIE,
    "scatterChart": ChartTypeClass.SCATTER,
    "tableEx": ChartTypeClass.TABLE,
    "pivotTable": ChartTypeClass.TABLE,
    "textbox": ChartTypeClass.TEXT,
}


def _truncate(value: Optional[str]) -> Optional[str]:
    if value is None or len(value) <= Constant.REFRESH_ERROR_MAX_LENGTH:
        return value
    marker = Constant.TRUNCATION_MARKER
    return value[: Constant.REFRESH_ERROR_MAX_LENGTH - len(marker)] + marker


@dataclass
class _RefreshOperationPlan:
    """A refresh to emit as an Operation on every table of its dataset."""

    refresh: powerbi_data_classes.DatasetRefresh
    end_ms: int
    last_updated_ms: int


def _refresh_error_properties(service_exception_json: Optional[str]) -> Dict[str, str]:
    """errorCode / errorDescription from a failed refresh's serviceExceptionJson.

    Falls back to the raw (truncated) string only when it is not a JSON
    object: a truncated JSON string would be unparseable downstream.
    """
    if not service_exception_json:
        return {}
    try:
        parsed = json.loads(service_exception_json)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        return {Constant.SERVICE_EXCEPTION: _truncate(service_exception_json) or ""}
    properties: Dict[str, str] = {}
    code = parsed.get(Constant.ERROR_CODE)
    if code:
        properties[Constant.ERROR_CODE] = str(code)
    description = parsed.get(Constant.ERROR_DESCRIPTION)
    if description:
        properties[Constant.ERROR_DESCRIPTION] = _truncate(str(description)) or ""
    return properties


def _is_secret_parameter(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in SECRET_PARAMETER_MARKERS)


def _add_upstream(upstream: List[UpstreamClass], urn: str) -> None:
    """Append a coarse upstream once."""
    if not any(u.dataset == urn for u in upstream):
        upstream.append(UpstreamClass(urn, DatasetLineageTypeClass.TRANSFORMED))


def _column_names(table: powerbi_data_classes.Table) -> Dict[str, str]:
    """Case-folded column name -> column name as modelled."""
    return {c.name.casefold(): c.name for c in table.columns or []}


class _ModelReferenceResolver:
    """Resolves the DAX references of one table against the tables of its
    semantic model while collecting the coarse upstreams those references
    imply. Only tables and fields the parsed model exposes resolve;
    everything else is counted and reported so no edge is ever guessed.
    DAX identifiers are case-insensitive, so lookups fold case and the
    model's own spelling is used for the URNs."""

    def __init__(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        upstream: List[UpstreamClass],
        table_urn: Callable[[powerbi_data_classes.Table], str],
        reporter: PowerBiDashboardSourceReport,
        follow_dax_references: bool,
    ) -> None:
        assert table.dataset is not None
        self.table = table
        self.ds_urn = ds_urn
        self.upstream = upstream
        self.table_urn = table_urn
        self.reporter = reporter
        self.follow_dax_references = follow_dax_references

        self._tables_by_name: Dict[str, powerbi_data_classes.Table] = {}
        # folded table name -> folded field name -> field name as modelled
        self._fields_by_table: Dict[str, Dict[str, str]] = {}
        self._columns_by_table: Dict[str, Set[str]] = {}
        # A measure name is unique across the whole model, so a bare
        # `[Measure]` reference identifies its table without ambiguity.
        self._measure_owner_by_name: Dict[str, powerbi_data_classes.Table] = {}
        for model_table in table.dataset.tables:
            key = model_table.name.casefold()
            self._tables_by_name[key] = model_table
            columns = _column_names(model_table)
            measures = {m.name.casefold(): m.name for m in model_table.measures or []}
            self._fields_by_table[key] = {**columns, **measures}
            self._columns_by_table[key] = set(columns)
            for measure_key in measures:
                self._measure_owner_by_name.setdefault(measure_key, model_table)

    def _is_own_column(self, field_name: str) -> bool:
        return (
            field_name.casefold() in self._columns_by_table[self.table.name.casefold()]
        )

    def add_table_upstream(self, table_name: str) -> Optional[str]:
        """Add another model table as a coarse upstream; None when the model
        has no such table or it is this table."""
        other = self._tables_by_name.get(table_name.casefold())
        if other is None or other is self.table:
            return None
        other_urn = self.table_urn(other)
        _add_upstream(self.upstream, other_urn)
        return other_urn

    def resolve_table_reference(self, table_name: str) -> Optional[str]:
        """Coarse upstream for a table a calculated table's DAX reads; an
        unknown table is reported."""
        if self._tables_by_name.get(table_name.casefold()) is self.table:
            return None
        return self._record(
            self.table.name, f"'{table_name}'", self.add_table_upstream(table_name)
        )

    def field_urn(self, table_name: Optional[str], field_name: str) -> Optional[str]:
        """Schema-field URN of a model field, adding the owning table as a
        coarse upstream when it is another table. None when the model does
        not expose that table or field (renamed column, VAR name, or a
        reference the parser could not attribute)."""
        if table_name is None:
            return None
        model_table = self._tables_by_name.get(table_name.casefold())
        if model_table is None:
            return None
        modelled_name = self._fields_by_table[model_table.name.casefold()].get(
            field_name.casefold()
        )
        if modelled_name is None:
            return None
        if model_table is self.table:
            return builder.make_schema_field_urn(self.ds_urn, modelled_name)
        table_urn = self.add_table_upstream(model_table.name)
        if table_urn is None:
            return None
        return builder.make_schema_field_urn(table_urn, modelled_name)

    def measure_owner(self, measure_name: str) -> Optional[str]:
        """Table that defines a measure referenced as bare `[Measure]`."""
        owner = self._measure_owner_by_name.get(measure_name.casefold())
        return owner.name if owner is not None else None

    def resolve_measure_reference(
        self,
        measure: powerbi_data_classes.Measure,
        reference: powerbi_data_classes.DaxFieldReference,
    ) -> Optional[str]:
        """Upstream field URN for a reference in a measure's DAX, or None
        when the reference is skipped by design or does not resolve."""
        if reference.table is None:
            # A bare [Measure] is a measure defined anywhere in the model; it
            # is only followed with the DAX lineage flag.
            if not self.follow_dax_references:
                return None
            owner = self.measure_owner(reference.field)
            if owner is None and measure.has_row_context_iterators:
                # Inside SUMX('T', [Column]) a bare name that is no measure is
                # a column of the iterated table.
                return self._unattributed(measure.name, reference)
            resolved = self.field_urn(owner, reference.field)
        elif reference.table.casefold() == self.table.name.casefold():
            # A same-table column reference would be a self-edge on this
            # dataset; the DAX is already in the measure's field description.
            # A same-table measure reference is followed with the DAX flag,
            # and a same-table reference to nothing is reported like any
            # other unresolved one.
            if self._is_own_column(reference.field):
                return None
            resolved = self.field_urn(self.table.name, reference.field)
            if resolved is not None and not self.follow_dax_references:
                return None
        else:
            resolved = self.field_urn(reference.table, reference.field)
        return self._record(measure.name, _reference_text(reference), resolved)

    def resolve_column_reference(
        self,
        column: powerbi_data_classes.Column,
        reference: powerbi_data_classes.DaxFieldReference,
    ) -> Optional[str]:
        """Upstream field URN for a reference in a calculated column's DAX,
        or None when it does not resolve."""
        if reference.table is not None:
            owner: Optional[str] = reference.table
        elif self._is_own_column(reference.field):
            # In a calculated column's row context a bare [Column] is a
            # column of this table, unless the expression iterates another
            # table: then it belongs to the iterated table and cannot be
            # attributed without evaluating the DAX.
            if column.has_row_context_iterators:
                return self._unattributed(column.name, reference)
            owner = self.table.name
        else:
            owner = self.measure_owner(reference.field)
            if owner is None and column.has_row_context_iterators:
                return self._unattributed(column.name, reference)
        return self._record(
            column.name,
            _reference_text(reference),
            self.field_urn(owner, reference.field),
        )

    def _unattributed(
        self, field_name: str, reference: powerbi_data_classes.DaxFieldReference
    ) -> Optional[str]:
        """A bare `[Column]` inside a table iterator: it exists, but on the
        iterated table, so it is counted rather than reported as missing.
        Always None, typed like the resolve methods that return it."""
        key = (self.table.full_name, field_name, _reference_text(reference))
        if key not in self.reporter._dax_references_seen:
            self.reporter._dax_references_seen.add(key)
            self.reporter.dax_references_unattributed += 1
        return None

    def _record(
        self, field_name: str, reference_text: str, resolved: Optional[str]
    ) -> Optional[str]:
        """Count the outcome once per (table, field, reference) and warn on
        the first unresolved sighting."""
        key = (self.table.full_name, field_name, reference_text)
        first_sighting = key not in self.reporter._dax_references_seen
        self.reporter._dax_references_seen.add(key)
        if resolved is not None:
            if first_sighting:
                self.reporter.dax_references_resolved += 1
            return resolved
        if first_sighting:
            self.reporter.dax_references_unresolved += 1
            self.reporter.warning(
                title=Constant.DAX_REFERENCE_NOT_RESOLVED,
                message="A DAX expression references a table or field that is not in the semantic model's parsed schema, or a bare column that cannot be attributed to a table; no lineage is emitted for that reference.",
                context=f"{self.table.full_name}.{field_name} -> {reference_text}",
            )
        return None


def _reference_text(reference: powerbi_data_classes.DaxFieldReference) -> str:
    if reference.table is not None:
        return f"'{reference.table}'[{reference.field}]"
    return f"[{reference.field}]"


@dataclass
class ResolvedInputFields:
    """Outcome of matching a chart's field references against the model."""

    mcp: Optional[MetadataChangeProposalWrapper] = None
    resolved: int = 0
    unresolved: int = 0
    # Datasets the resolved fields belong to, in first-seen order; a visual's
    # table-level inputs are exactly these, never the whole model.
    dataset_urns: List[str] = field(default_factory=list)


@dataclass
class FabricEndpointMatch:
    """Lakehouse / Warehouse ids behind the endpoint a Sql.Database query
    named; empty when none matched, ambiguous when a display name matched
    more than one item."""

    item_ids: List[str] = field(default_factory=list)
    ambiguous: bool = False


class Mapper:
    """
    Transform PowerBi concepts Dashboard, Dataset and Tile to DataHub concepts Dashboard, Dataset and Chart
    """

    def __init__(
        self,
        ctx: PipelineContext,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
        dataplatform_instance_resolver: AbstractDataPlatformInstanceResolver,
        remote_model_lookup: Optional[powerbi_data_classes.RemoteModelLookup] = None,
        powerbi_client: Optional[PowerBiAPI] = None,
    ):
        self.__ctx = ctx
        self.__config = config
        self.__reporter = reporter
        self.__dataplatform_instance_resolver = dataplatform_instance_resolver
        # Resolves a composite model's source model against the datasets
        # ingested this run (PowerBiAPI.find_remote_model); None resolves
        # nothing, so no edge is ever emitted without it.
        self.__remote_model_lookup = remote_model_lookup
        # Consulted at lineage time for the dataflows ingested this run
        # (all workspaces); None means no dataflow can be linked.
        self.__powerbi_client = powerbi_client
        # (table urn, dataflow id, entity) references already counted, so a
        # model mapped several times (report + tile) is reported once.
        self.__counted_dataflow_references: Set[Tuple[str, str, str]] = set()
        # (table urn, endpoint, schema, source table) Sql.Database references
        # over Fabric endpoints already counted, for the same reason.
        self.__counted_fabric_endpoint_references: Set[
            Tuple[str, str, Optional[str], str]
        ] = set()
        self.workspace_key: Optional[ContainerKey] = None
        # Refresh Operations are decided once per dataset and emitted on each
        # of its tables, so counters and warnings fire per dataset.
        self.__refresh_plans: Dict[str, List[_RefreshOperationPlan]] = {}
        # Tables a report's pages read (from their resolved field references),
        # by report id: every tile built from the report asks for them.
        self.__report_used_dataset_urns: Dict[str, List[str]] = {}

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

    def _get_data_platform_instance_aspect(self) -> DataPlatformInstanceClass:
        """
        Generate DataPlatformInstanceClass aspect for PowerBI entities.
        """
        return DataPlatformInstanceClass(
            platform=builder.make_data_platform_urn(self.__config.platform_name),
            instance=(
                builder.make_dataplatform_instance_urn(
                    self.__config.platform_name, self.__config.platform_instance
                )
                if self.__config.platform_instance
                else None
            ),
        )

    def make_dataset_urn_for_table(self, table: powerbi_data_classes.Table) -> str:
        return self.assets_urn_to_lowercase(
            builder.make_dataset_urn_with_platform_instance(
                platform=self.__config.platform_name,
                name=table.full_name,
                platform_instance=self.__config.platform_instance,
                env=self.__config.env,
            )
        )

    def _foreign_keys_for_table(
        self, table: powerbi_data_classes.Table, ds_urn: str
    ) -> List[ForeignKeyConstraintClass]:
        """Semantic-model relationships whose `from` side is this table."""
        if not self.__config.extract_model_relationships or table.dataset is None:
            return []
        tables_by_name = {t.name: t for t in table.dataset.tables}
        constraints: List[ForeignKeyConstraintClass] = []
        for relationship in table.dataset.relationships:
            if relationship.from_table != table.name:
                continue
            to_table = tables_by_name.get(relationship.to_table)
            if to_table is None:
                self._skip_relationship(
                    relationship, table, f"unknown table {relationship.to_table}"
                )
                continue
            # A key to a field the schema does not expose would be a dangling
            # schemaField URN; renamed columns are the common cause.
            if not any(c.name == relationship.from_column for c in table.columns or []):
                self._skip_relationship(
                    relationship,
                    table,
                    f"unknown column {relationship.from_table}.{relationship.from_column}",
                )
                continue
            if not any(
                c.name == relationship.to_column for c in to_table.columns or []
            ):
                self._skip_relationship(
                    relationship,
                    table,
                    f"unknown column {relationship.to_table}.{relationship.to_column}",
                )
                continue
            to_urn = self.make_dataset_urn_for_table(to_table)
            name = (
                f"{relationship.from_table}.{relationship.from_column} -> "
                f"{relationship.to_table}.{relationship.to_column}"
            )
            if not relationship.is_active:
                name = f"{name} (inactive)"
            # ForeignKeyConstraint semantics: sourceFields live on this dataset,
            # foreignFields on the referenced (foreign) dataset.
            constraints.append(
                ForeignKeyConstraintClass(
                    name=name,
                    sourceFields=[
                        builder.make_schema_field_urn(ds_urn, relationship.from_column)
                    ],
                    foreignFields=[
                        builder.make_schema_field_urn(to_urn, relationship.to_column)
                    ],
                    foreignDataset=to_urn,
                )
            )
        return constraints

    def _skip_relationship(
        self,
        relationship: powerbi_data_classes.Relationship,
        table: powerbi_data_classes.Table,
        reason: str,
    ) -> None:
        self.__reporter.relationships_unresolved += 1
        self.__reporter.warning(
            title="Relationship Not Mapped",
            message="A semantic-model relationship references a table or column that is not in the model's schema; no foreign key is emitted for it.",
            context=f"table={table.full_name}, relationship={relationship.name}, {reason}",
        )

    def extract_dataset_schema(
        self, table: powerbi_data_classes.Table, ds_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        schema_metadata = self.to_datahub_schema(table)
        foreign_keys = self._foreign_keys_for_table(table, ds_urn)
        if foreign_keys:
            schema_metadata.foreignKeys = foreign_keys
        schema_mcp = MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=schema_metadata,
        )
        return [schema_mcp]

    def make_fine_grained_lineage_class(
        self,
        lineage: datahub.ingestion.source.powerbi.m_query.data_classes.Lineage,
        dataset_urn: str,
        upstream_urns: Optional[Set[str]] = None,
    ) -> List[FineGrainedLineage]:
        """`upstream_urns`, when given, are the (already lowercased, if
        configured) table URNs kept as coarse upstreams; column references to
        any other table are dropped so column lineage never points at a table
        the dataset_type_mapping filtered out."""
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
                if cll_info.downstream is not None and cll_info.downstream.column
                else []
            )

            upstreams = [
                builder.make_schema_field_urn(
                    # convert_lineage_urns_to_lowercase lowercases only the dataset
                    # portion of the upstream schemaField URN — matching sources'
                    # lowercase_dataset_urns, which never touches the field path.
                    # Lowercasing the column too would drop the column-level edge
                    # against warehouses that store columns in their original casing.
                    self.lineage_urn_to_lowercase(column_ref.table),
                    column_ref.column,
                )
                for column_ref in cll_info.upstreams
                if column_ref.column
                and (
                    upstream_urns is None
                    or self.lineage_urn_to_lowercase(column_ref.table) in upstream_urns
                )
            ]
            if not upstreams:
                continue

            fine_grained_lineages.append(
                FineGrainedLineage(
                    downstreamType=FineGrainedLineageDownstreamType.FIELD,
                    downstreams=downstream,
                    upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                    upstreams=upstreams,
                )
            )

        return fine_grained_lineages

    def extract_directlake_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """Extract lineage for DirectLake tables to their upstream Fabric OneLake tables.

        DirectLake tables read data directly from Fabric Lakehouses or Warehouses.
        This method creates lineage from the PowerBI table to the corresponding OneLake table.

        Args:
            table: The PowerBI table with DirectLake storage mode
            ds_urn: The URN of the downstream PowerBI dataset
            workspace: The workspace containing the Fabric artifacts

        Returns:
            List of MCPs with upstream lineage to OneLake tables
        """
        mcps: List[MetadataChangeProposalWrapper] = []
        dataset = table.dataset

        if not dataset:
            logger.debug(f"No dataset for DirectLake table {table.full_name}")
            return mcps

        if not dataset.dependent_on_artifact_id:
            logger.debug(
                f"No dependent artifact ID for DirectLake table {table.full_name}"
            )
            return mcps

        # Look up the Fabric artifact (Lakehouse/Warehouse) this dataset depends on
        artifact = workspace.fabric_artifacts.get(dataset.dependent_on_artifact_id)
        if not artifact:
            logger.debug(
                f"Fabric artifact {dataset.dependent_on_artifact_id} not found "
                f"for DirectLake table {table.full_name}. Available artifacts: "
                f"{list(workspace.fabric_artifacts.keys())}"
            )
            return mcps

        if not table.source_expression:
            logger.debug(f"No source expression for DirectLake table {table.full_name}")
            return mcps

        platform_detail = self._onelake_platform_detail(workspace)

        # Resolve item ids for lineage URN. For SQLAnalyticsEndpoint we use physical_item_ids
        # (from relations.dependentOnArtifactId) so lineage URNs match the OneLake connector;
        # we do not model SQLAnalyticsEndpoint as an entity (if we do, align with OneLake connector).
        # Lineage to an SQLAnalyticsEndpoint id is not emitted; we require at least one resolved physical id.
        if artifact.artifact_type == FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT:
            item_ids = artifact.physical_item_ids if artifact.physical_item_ids else []
            if not item_ids:
                logger.warning(
                    "DirectLake lineage skipped: SQLAnalyticsEndpoint artifact %s has no "
                    "resolvable relation to a Lakehouse/Warehouse (table: %s)",
                    artifact.id,
                    table.full_name,
                )
                return mcps
        else:
            item_ids = [artifact.id]

        upstreams: List[UpstreamClass] = []
        for item_id in item_ids:
            upstream_urn = make_onelake_urn(
                workspace_id=workspace.id,
                item_id=item_id,
                table_name=table.source_expression,
                schema_name=table.source_schema,  # Can be None for schemas-disabled lakehouses
                env=platform_detail.env,
                platform_instance=platform_detail.platform_instance,  # None if not mapped
            )
            upstream_urn = self.lineage_urn_to_lowercase(upstream_urn)
            upstreams.append(
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                )
            )

        upstream_lineage = UpstreamLineageClass(upstreams=upstreams)
        logger.info(
            "DirectLake lineage: %s -> %s upstream(s) (artifact: %s, type: %s)",
            table.full_name,
            len(upstreams),
            artifact.name,
            artifact.artifact_type,
        )

        mcp = MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=upstream_lineage,
        )
        mcps.append(mcp)

        return mcps

    def extract_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        mcps: List[MetadataChangeProposalWrapper] = []

        # Check for DirectLake storage mode first - use Fabric lineage path
        if table.storage_mode == Constant.DIRECT_LAKE:
            return self._directlake_lineage_with_model_internal(
                table, ds_urn, workspace
            )

        # Existing M-Query parsing logic for other storage modes (Import, DirectQuery, etc.)
        # table.dataset should always be set, but we check it just in case.
        parameters = table.dataset.parameters if table.dataset else {}

        upstream: List[UpstreamClass] = []
        cll_lineage: List[FineGrainedLineage] = []

        logger.debug(
            f"Extracting lineage for table {table.full_name} in dataset {table.dataset.name if table.dataset else None}"
        )

        upstream_lineage: List[
            datahub.ingestion.source.powerbi.m_query.data_classes.Lineage
        ] = parser.get_upstream_tables(
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

        self._resolve_fabric_endpoint_references(upstream_lineage, workspace, ds_urn)

        self._add_upstreams_from_m_lineage(
            upstream_lineage, ds_urn, upstream, cll_lineage, column_lineage=True
        )

        self._add_composite_model_lineage(table, ds_urn, upstream, cll_lineage)
        self._add_model_internal_lineage(table, ds_urn, upstream, cll_lineage)

        # Fine-grained edges between fields of this same dataset (a calculated
        # column or a measure reading a same-table measure) have no coarse
        # upstream, so the aspect is also emitted when only those exist.
        if upstream or cll_lineage:
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

    def _resolve_fabric_endpoint_references(
        self,
        upstream_lineage: List[
            datahub.ingestion.source.powerbi.m_query.data_classes.Lineage
        ],
        workspace: powerbi_data_classes.Workspace,
        ds_urn: str,
    ) -> None:
        """Swap each ``Sql.Database`` upstream over a Fabric SQL analytics
        endpoint for the OneLake table(s) behind it, so the edge lands on the
        same URN the OneLake connector emits instead of a generic ``mssql``
        one. An endpoint that is not among the workspace's Fabric items (a
        lakehouse in another workspace, or one whose properties were refused)
        keeps the ``mssql`` edge the connector always emitted and is
        reported, since only the OneLake edge is lost."""
        for lineage in upstream_lineage:
            for reference in lineage.fabric_endpoint_references:
                reference_key = (
                    ds_urn,
                    reference.database.lower(),
                    reference.schema_name,
                    reference.table_name,
                )
                first_sighting = (
                    reference_key not in self.__counted_fabric_endpoint_references
                )
                self.__counted_fabric_endpoint_references.add(reference_key)
                match = self._fabric_endpoint_match(workspace, reference.database)
                if match.ambiguous:
                    if first_sighting:
                        self.__reporter.fabric_endpoint_references_ambiguous += 1
                    self._warn_fabric_endpoint_reference(
                        title="Fabric Endpoint Name Ambiguous",
                        message="A Sql.Database query names a Fabric SQL analytics endpoint by display name, and several Lakehouses or Warehouses of the workspace share that name; the table keeps a generic mssql upstream instead of guessing the OneLake table.",
                        reference=reference,
                        workspace=workspace,
                        ds_urn=ds_urn,
                    )
                    continue
                if not match.item_ids:
                    if first_sighting:
                        self.__reporter.fabric_endpoint_references_unresolved += 1
                    self._warn_fabric_endpoint_reference(
                        title="Fabric Endpoint Not Resolved",
                        message="A Sql.Database query reads a Fabric SQL analytics endpoint that is not a Lakehouse or Warehouse of its workspace (for example an item in a workspace outside the workspace patterns, or one whose properties could not be fetched); the table keeps a generic mssql upstream instead of the OneLake table.",
                        reference=reference,
                        workspace=workspace,
                        ds_urn=ds_urn,
                    )
                    continue
                if first_sighting:
                    self.__reporter.fabric_endpoint_references_resolved += 1
                platform_detail = self._onelake_platform_detail(workspace)
                onelake_urns = [
                    make_onelake_urn(
                        workspace_id=workspace.id,
                        item_id=item_id,
                        table_name=reference.table_name,
                        schema_name=reference.schema_name,
                        env=platform_detail.env,
                        platform_instance=platform_detail.platform_instance,
                    )
                    for item_id in match.item_ids
                ]
                self._replace_upstream_urn(lineage, reference.mssql_urn, onelake_urns)

    def _onelake_platform_detail(
        self, workspace: powerbi_data_classes.Workspace
    ) -> PlatformDetail:
        """platform_instance and env of OneLake URNs come only from
        server_to_platform_instance keyed by the workspace id (the OneLake
        connector groups workspaces by tenant the same way); the env falls
        back to the mapping's own default, not the Power BI env."""
        return self.__dataplatform_instance_resolver.get_platform_instance(
            PowerBIPlatformDetail(
                data_platform_pair=SupportedDataPlatform.FABRIC_ONELAKE.value,
                data_platform_server=workspace.id,
            )
        )

    def _warn_fabric_endpoint_reference(
        self,
        title: str,
        message: str,
        reference: datahub.ingestion.source.powerbi.m_query.data_classes.FabricSqlEndpointReference,
        workspace: powerbi_data_classes.Workspace,
        ds_urn: str,
    ) -> None:
        self.__reporter.warning(
            title=title,
            message=message,
            context=(
                f"table={ds_urn}, workspace={workspace.name}, "
                f"server={reference.server}, database={reference.database}, "
                f"source={reference.schema_name}.{reference.table_name}"
            ),
        )

    @staticmethod
    def _fabric_endpoint_match(
        workspace: powerbi_data_classes.Workspace, database: str
    ) -> FabricEndpointMatch:
        """The OneLake item(s) behind the endpoint a ``Sql.Database`` query
        named: by endpoint id first (as DirectLake's shared expression
        writes it), else by item display name (as Power BI Desktop writes
        it). A name shared by several items is ambiguous rather than a
        guess."""
        key = database.strip().lower()
        by_id = [a for a in workspace.fabric_artifacts.values() if a.id.lower() == key]
        if by_id:
            return FabricEndpointMatch(item_ids=Mapper._physical_item_ids(by_id))
        by_name = [
            a for a in workspace.fabric_artifacts.values() if a.name.lower() == key
        ]
        item_ids = Mapper._physical_item_ids(by_name)
        return FabricEndpointMatch(item_ids=item_ids, ambiguous=len(item_ids) > 1)

    @staticmethod
    def _physical_item_ids(
        artifacts: List[powerbi_data_classes.FabricArtifact],
    ) -> List[str]:
        """Distinct Lakehouse / Warehouse ids the artifacts stand for: an
        endpoint artifact contributes its physical items, any other its own
        id."""
        item_ids: List[str] = []
        for artifact in artifacts:
            candidates = (
                artifact.physical_item_ids or []
                if artifact.artifact_type == FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT
                else [artifact.id]
            )
            for item_id in candidates:
                if item_id.lower() not in {i.lower() for i in item_ids}:
                    item_ids.append(item_id)
        return item_ids

    @staticmethod
    def _replace_upstream_urn(
        lineage: datahub.ingestion.source.powerbi.m_query.data_classes.Lineage,
        old_urn: str,
        new_urns: List[str],
    ) -> None:
        onelake_pair = SupportedDataPlatform.FABRIC_ONELAKE.value
        upstreams: List[
            datahub.ingestion.source.powerbi.m_query.data_classes.DataPlatformTable
        ] = []
        for upstream in lineage.upstreams:
            if upstream.urn != old_urn:
                upstreams.append(upstream)
                continue
            upstreams.extend(
                datahub.ingestion.source.powerbi.m_query.data_classes.DataPlatformTable(
                    data_platform_pair=onelake_pair,
                    urn=urn,
                    from_fabric_endpoint=True,
                )
                for urn in new_urns
            )
        lineage.upstreams = upstreams
        column_lineage: List[ColumnLineageInfo] = []
        for cll in lineage.column_lineage:
            column_refs: List[ColumnRef] = []
            for column_ref in cll.upstreams:
                if column_ref.table != old_urn:
                    column_refs.append(column_ref)
                    continue
                column_refs.extend(
                    ColumnRef(table=urn, column=column_ref.column) for urn in new_urns
                )
            column_lineage.append(
                ColumnLineageInfo(
                    downstream=cll.downstream, upstreams=column_refs, logic=cll.logic
                )
            )
        lineage.column_lineage = column_lineage

    def _add_upstreams_from_m_lineage(
        self,
        upstream_lineage: List[
            datahub.ingestion.source.powerbi.m_query.data_classes.Lineage
        ],
        ds_urn: str,
        upstream: List[UpstreamClass],
        cll_lineage: List[FineGrainedLineage],
        column_lineage: bool,
    ) -> None:
        """Turn parsed M lineage into upstream edges on *ds_urn*: external
        tables filtered by dataset_type_mapping, plus dataflow entities that
        were ingested in this run."""
        for lineage in upstream_lineage:
            mapped_upstreams = 0
            for upstream_dpt in lineage.upstreams:
                platform_name = (
                    upstream_dpt.data_platform_pair.powerbi_data_platform_name
                )
                if (
                    not upstream_dpt.from_fabric_endpoint
                    and not self.__config.is_platform_in_dataset_type_mapping(
                        platform_name
                    )
                ):
                    logger.debug(
                        f"Skipping upstream table for {ds_urn}. The platform {platform_name} is not part of dataset_type_mapping",
                    )
                    continue

                upstream_table_class = UpstreamClass(
                    self.lineage_urn_to_lowercase(upstream_dpt.urn),
                    DatasetLineageTypeClass.TRANSFORMED,
                )

                upstream.append(upstream_table_class)
                mapped_upstreams += 1

            # Column lineage belongs to the Lineage object, not to each of its
            # upstream tables; adding it inside the loop above duplicated every
            # fine-grained edge once per upstream table.
            if column_lineage and mapped_upstreams > 0:
                cll_lineage.extend(
                    self.make_fine_grained_lineage_class(
                        lineage=lineage,
                        dataset_urn=ds_urn,
                        upstream_urns={u.dataset for u in upstream},
                    )
                )

            for reference in lineage.dataflow_references:
                entity_urn = self._ingested_dataflow_entity_urn(reference, ds_urn)
                if entity_urn is None:
                    continue
                upstream.append(
                    UpstreamClass(entity_urn, DatasetLineageTypeClass.TRANSFORMED)
                )

    def _ingested_dataflow_entity_urn(
        self,
        reference: datahub.ingestion.source.powerbi.m_query.data_classes.DataflowReference,
        ds_urn: str,
    ) -> Optional[str]:
        """The URN of the dataflow entity a PowerPlatform.Dataflows reference
        points at, or None (with a warning) when that entity was not
        ingested: an edge to a dataset that does not exist is worse than
        none, and a dataflow outside the workspace allow pattern, or in a
        run without extract_dataflows, never gets one."""
        context = (
            f"table={ds_urn}, workspace_id={reference.workspace_id}, "
            f"dataflow_id={reference.dataflow_id}, entity={reference.entity}"
        )
        reference_key = (ds_urn, reference.dataflow_id.lower(), reference.entity)
        first_sighting = reference_key not in self.__counted_dataflow_references
        self.__counted_dataflow_references.add(reference_key)

        dataflow = (
            self.__powerbi_client.get_ingested_dataflow(reference.dataflow_id)
            if self.__powerbi_client is not None
            else None
        )
        if dataflow is None:
            if first_sighting:
                self.__reporter.dataflow_references_not_ingested += 1
            # Without extract_dataflows (always the case in admin_scan) no
            # dataflow is ingested by choice, so a skipped edge is expected on
            # every run and is not something to fix: report it as info.
            report_skip = (
                self.__reporter.warning
                if self.__config.extract_dataflows
                else self.__reporter.info
            )
            report_skip(
                title="Dataflow Not Ingested",
                message=self._dataflow_not_ingested_advice(reference.dataflow_id),
                context=context,
            )
            return None
        entity = dataflow.get_entity(reference.entity)
        if entity is None:
            if first_sighting:
                self.__reporter.dataflow_references_not_ingested += 1
            self.__reporter.warning(
                title="Dataflow Not Ingested",
                message=(
                    "A semantic model reads an entity that the ingested dataflow's "
                    "definition does not load, so the lineage edge is skipped."
                ),
                context=f"{context}, dataflow={dataflow.name}, dataflow_workspace={dataflow.workspace_name}",
            )
            return None
        if first_sighting:
            self.__reporter.dataflow_references_resolved += 1
        return self.make_dataset_urn_for_dataflow_entity(dataflow, entity)

    def _dataflow_not_ingested_advice(self, dataflow_id: str) -> str:
        skipped = "A semantic model reads a dataflow that was not ingested in this run, so the lineage edge is skipped. "
        if self.__config.metadata_api_mode != PowerBiMetadataApiMode.WORKSPACE_APIS:
            return (
                skipped + "Dataflow ingestion needs metadata_api_mode: workspace_apis "
                "with extract_dataflows enabled."
            )
        if not self.__config.extract_dataflows:
            return skipped + "Enable extract_dataflows to ingest dataflows."
        if (
            self.__powerbi_client is not None
            and self.__powerbi_client.was_dataflow_listed(dataflow_id)
        ):
            return (
                skipped
                + "The dataflow was listed but its definition could not be fetched "
                "or parsed; see the failure reported for it."
            )
        return (
            skipped + "Include the dataflow's workspace in workspace_id_pattern. Only "
            "Dataflow Gen2 items are ingested, so a Gen1 dataflow is never resolved."
        )

    def make_dataset_urn_for_dataflow_entity(
        self,
        dataflow: powerbi_data_classes.Dataflow,
        entity: powerbi_data_classes.DataflowQuery,
    ) -> str:
        # Keyed by the dataflow id rather than its name, like the dashboard,
        # chart and semantic-model container URNs: it cannot collide with a
        # semantic model of the same name and it is what a
        # PowerPlatform.Dataflows reference carries. Deliberately unaffected
        # by include_workspace_name_in_dataset_urn (the id is already unique).
        return self.assets_urn_to_lowercase(
            builder.make_dataset_urn_with_platform_instance(
                platform=self.__config.platform_name,
                name=f"{dataflow.get_urn_part()}.{powerbi_data_classes.urn_safe_name(entity.name)}",
                platform_instance=self.__config.platform_instance,
                env=self.__config.env,
            )
        )

    def extract_dataflow_entity_lineage(
        self,
        dataflow: powerbi_data_classes.Dataflow,
        entity: powerbi_data_classes.DataflowQuery,
        ds_urn: str,
        bindings: Dict[str, str],
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """Upstream lineage of one dataflow entity. *bindings* holds the
        mashup's parseable queries; wrapping them in one let lets a query
        that references another query resolve to that query's source.
        *workspace* supplies the Fabric items a Sql.Database query over an
        endpoint host is mapped to, as for semantic-model tables."""
        if entity.name not in bindings:
            return []
        table = powerbi_data_classes.Table(
            name=entity.name,
            full_name=f"{dataflow.name}.{entity.name}",
            expression=compose_let_expression(bindings, entity.name),
        )
        upstream_lineage = parser.get_upstream_tables(
            table=table,
            reporter=self.__reporter,
            platform_instance_resolver=self.__dataplatform_instance_resolver,
            ctx=self.__ctx,
            config=self.__config,
        )
        self._resolve_fabric_endpoint_references(upstream_lineage, workspace, ds_urn)
        upstream: List[UpstreamClass] = []
        cll_lineage: List[FineGrainedLineage] = []
        # Dataflow entities carry no schema, so there is nothing for a
        # column-level edge to land on.
        self._add_upstreams_from_m_lineage(
            upstream_lineage, ds_urn, upstream, cll_lineage, column_lineage=False
        )
        if not upstream:
            return []
        return [
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=UpstreamLineageClass(upstreams=upstream),
            )
        ]

    def _parseable_dataflow_bindings(
        self, dataflow: powerbi_data_classes.Dataflow
    ) -> Dict[str, str]:
        """name -> M expression of every query that parses on its own, so one
        unparseable query cannot take the composed let of the others down."""
        bindings: Dict[str, str] = {}
        for query in dataflow.queries:
            error = parser.parse_error(
                query.expression, self.__config.m_query_parse_timeout
            )
            if error is None:
                bindings[query.name] = query.expression
                continue
            self.__reporter.dataflow_queries_unparseable += 1
            self.__reporter.warning(
                title="Dataflow Query Not Parsed",
                message="A dataflow query's M expression could not be parsed; lineage through it is missing.",
                context=f"workspace={dataflow.workspace_name}, dataflow={dataflow.name} id={dataflow.id}, query={query.name}, error={error}",
            )
        return bindings

    def _dataflow_entity_mcps(
        self,
        dataflow: powerbi_data_classes.Dataflow,
        entity: powerbi_data_classes.DataflowQuery,
        workspace: powerbi_data_classes.Workspace,
        dataflow_key: ContainerKey,
        bindings: Dict[str, str],
    ) -> List[MetadataChangeProposalWrapper]:
        ds_urn = self.make_dataset_urn_for_dataflow_entity(dataflow, entity)
        mcps = [self._view_properties_mcp(ds_urn, entity.expression, materialized=True)]
        mcps.extend(
            self._dataset_identity_mcps(
                ds_urn,
                properties=DatasetPropertiesClass(
                    name=entity.name,
                    description=dataflow.description,
                    externalUrl=dataflow.webUrl,
                    customProperties={
                        Constant.DATAFLOW_ID: dataflow.id,
                        Constant.DATAFLOW_NAME: dataflow.name,
                    },
                ),
                subtype=DatasetSubTypes.POWERBI_DATAFLOW_ENTITY,
                browse_path=[
                    BrowsePathEntryClass(
                        id=workspace.name,
                        urn=self.make_container_urn_for_workspace(workspace),
                    ),
                    BrowsePathEntryClass(id=dataflow.name, urn=dataflow_key.as_urn()),
                ],
            )
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=ContainerClass(container=dataflow_key.as_urn()),
            )
        )
        if self.__config.extract_lineage:
            mcps.extend(
                self.extract_dataflow_entity_lineage(
                    dataflow, entity, ds_urn, bindings, workspace
                )
            )
        return mcps

    def to_datahub_dataflow(
        self,
        dataflow: powerbi_data_classes.Dataflow,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """A Dataflow Gen2 as a container in its workspace holding one
        dataset per load-enabled query."""
        dataflow_key = self.make_container_key_for_dataflow(dataflow)
        mcps: List[MetadataChangeProposalWrapper] = [
            wu.metadata
            for wu in gen_containers(
                container_key=dataflow_key,
                name=dataflow.name,
                description=dataflow.description,
                parent_container_key=self.workspace_key,
                sub_types=[BIContainerSubTypes.POWERBI_DATAFLOW],
                external_url=dataflow.webUrl,
                extra_properties={Constant.DATAFLOW_ID_PROPERTY: dataflow.id},
            )
            if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        ]
        bindings = (
            self._parseable_dataflow_bindings(dataflow)
            if self.__config.extract_lineage
            else {}
        )
        for entity in dataflow.entities():
            mcps.extend(
                self._dataflow_entity_mcps(
                    dataflow, entity, workspace, dataflow_key, bindings
                )
            )
        return mcps

    def _view_properties_mcp(
        self, ds_urn: str, expression: str, materialized: bool
    ) -> MetadataChangeProposalWrapper:
        # Convert PowerBI special characters (#(lf), (lf), #(tab)) to actual
        # characters. This is done after all parsing is complete to avoid
        # interfering with M-Query parsing.
        return MetadataChangeProposalWrapper(
            entityUrn=ds_urn,
            aspect=ViewPropertiesClass(
                materialized=materialized,
                viewLogic=native_sql_parser.remove_special_characters(expression),
                viewLanguage=Constant.M_QUERY_LANGUAGE,
            ),
        )

    def _dataset_identity_mcps(
        self,
        ds_urn: str,
        properties: DatasetPropertiesClass,
        subtype: str,
        browse_path: List[BrowsePathEntryClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """The aspects every Power BI dataset-like entity (semantic-model
        table, dataflow entity) carries, in one place so they cannot drift."""
        return [
            MetadataChangeProposalWrapper(entityUrn=ds_urn, aspect=aspect)
            for aspect in (
                properties,
                StatusClass(removed=False),
                SubTypesClass(typeNames=[subtype]),
                self._get_data_platform_instance_aspect(),
                BrowsePathsV2Class(path=browse_path),
            )
        ]

    def _directlake_lineage_with_model_internal(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """DirectLake OneLake lineage merged with the model-internal edges
        (calculated-table and measure references) of the same table."""
        mcps = self.extract_directlake_lineage(table, ds_urn, workspace)
        existing: Optional[UpstreamLineageClass] = next(
            (
                mcp.aspect
                for mcp in mcps
                if isinstance(mcp.aspect, UpstreamLineageClass)
            ),
            None,
        )
        upstreams: List[UpstreamClass] = (
            existing.upstreams if existing is not None else []
        )
        cll_lineage: List[FineGrainedLineage] = []
        self._add_model_internal_lineage(table, ds_urn, upstreams, cll_lineage)
        if existing is not None:
            existing.fineGrainedLineages = cll_lineage or None
        elif upstreams or cll_lineage:
            mcps.append(
                MetadataChangeProposalWrapper(
                    entityUrn=ds_urn,
                    aspect=UpstreamLineageClass(
                        upstreams=upstreams, fineGrainedLineages=cll_lineage or None
                    ),
                )
            )
        return mcps

    def _add_model_internal_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        upstream: List[UpstreamClass],
        cll_lineage: List[FineGrainedLineage],
    ) -> None:
        """Lineage between tables of the same semantic model, available only
        from TMDL definitions: a calculated table reads other model tables,
        and a measure reads columns (possibly on other tables). Both become
        same-platform edges; measure -> column edges are fine-grained.

        With `extract_dax_reference_lineage` the DAX of calculated columns and
        the measure -> measure references are followed as well. An edge is
        only emitted to a field the parsed model exposes; a reference that
        does not resolve is counted and reported, never guessed. A reference
        to a field of this same table becomes a fine-grained edge between two
        fields of this dataset without a coarse self-upstream."""
        if table.dataset is None:
            return
        resolver = _ModelReferenceResolver(
            table=table,
            ds_urn=ds_urn,
            upstream=upstream,
            table_urn=self.make_dataset_urn_for_table,
            reporter=self.__reporter,
            follow_dax_references=self.__config.extract_dax_reference_lineage,
        )

        if table.calculated_expression:
            for table_name in dax_table_references(table.calculated_expression):
                resolver.resolve_table_reference(table_name)

        if self.__config.extract_column_level_lineage is False:
            return

        for measure in table.measures or []:
            upstream_fields: List[str] = []
            for reference in measure.references:
                resolved = resolver.resolve_measure_reference(measure, reference)
                if resolved is not None:
                    upstream_fields.append(resolved)
            self._add_field_set_lineage(
                cll_lineage, ds_urn, measure.name, upstream_fields
            )

        if not self.__config.extract_dax_reference_lineage:
            return

        for column in table.columns or []:
            if (
                not powerbi_data_classes.is_calculated_column(column)
                or not column.references
            ):
                continue
            upstream_fields = []
            for reference in column.references:
                resolved = resolver.resolve_column_reference(column, reference)
                if resolved is not None:
                    upstream_fields.append(resolved)
            self._add_field_set_lineage(
                cll_lineage, ds_urn, column.name, upstream_fields
            )

    def _add_composite_model_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        upstream: List[UpstreamClass],
        cll_lineage: List[FineGrainedLineage],
    ) -> None:
        """A composite model's remote table reads one table of another
        semantic model: that table becomes the coarse upstream and each
        column the remote table mirrors gets a column-level edge. Only a
        source model and table ingested this run are linked; anything else
        is reported and gets no edge, so lineage never points at a dataset
        URN nothing emits. Columns calculated on the remote table are local
        DAX and are left to the DAX lineage."""
        reference = table.remote_model
        if reference is None or not self.__config.extract_composite_model_lineage:
            return
        source_table = self._resolve_remote_table(table, reference)
        if source_table is None:
            return

        source_urn = self.make_dataset_urn_for_table(source_table)
        first_sighting = self._first_composite_table(table)
        if source_urn == ds_urn:
            # The connection resolved to this very table (a model that names
            # itself, or URN options that fold two models into one name).
            if first_sighting:
                self.__reporter.composite_model_sources_ambiguous += 1
                self.__reporter.warning(
                    title=Constant.COMPOSITE_MODEL_SOURCE_AMBIGUOUS,
                    message="A composite model's remote table resolved to its own URN; no lineage is emitted for it. Check include_workspace_name_in_dataset_urn when two models share a name.",
                    context=self._composite_context(table, reference),
                )
            return
        _add_upstream(upstream, source_urn)
        if first_sighting:
            self.__reporter.composite_model_tables_linked += 1
        if not self.__config.extract_column_level_lineage:
            return

        unresolved = self._add_composite_column_lineage(
            table, ds_urn, source_table, source_urn, cll_lineage
        )
        if unresolved and first_sighting:
            self.__reporter.composite_model_columns_unresolved += len(unresolved)
            self.__reporter.warning(
                title=Constant.COMPOSITE_MODEL_COLUMN_NOT_RESOLVED,
                message="Some columns of a composite model's remote table have no column of that source name in the ingested source table; they get no column-level lineage.",
                context=f"{self._composite_context(table, reference)}, columns={unresolved}",
            )

    def _add_composite_column_lineage(
        self,
        table: powerbi_data_classes.Table,
        ds_urn: str,
        source_table: powerbi_data_classes.Table,
        source_urn: str,
        cll_lineage: List[FineGrainedLineage],
    ) -> List[str]:
        """Column-level edges from the source table's columns onto the
        mirrored columns, matched by the column's `sourceColumn` (the name in
        the source model; a column renamed on the composite model keeps it)
        with the display name as fallback. Returns the columns that matched
        nothing."""
        source_columns = _column_names(source_table)
        unresolved: List[str] = []
        for column in table.columns or []:
            if column.columnType == Constant.COLUMN_TYPE_CALCULATED:
                continue
            source_column = source_columns.get(
                (column.source_column or column.name).casefold()
            )
            if source_column is None:
                unresolved.append(column.name)
                continue
            self._add_field_set_lineage(
                cll_lineage,
                ds_urn,
                column.name,
                [builder.make_schema_field_urn(source_urn, source_column)],
            )
        return unresolved

    def _resolve_remote_table(
        self,
        table: powerbi_data_classes.Table,
        reference: powerbi_data_classes.RemoteModelReference,
    ) -> Optional[powerbi_data_classes.Table]:
        """The ingested source table of a remote table, or None. A problem
        with the source model is reported once per (workspace, model); a
        table missing from it once per remote table."""
        if table.remote_table_name is None:
            return None
        match = (
            self.__remote_model_lookup(reference)
            if self.__remote_model_lookup is not None
            else powerbi_data_classes.RemoteModelMatch()
        )
        first_model = self._first_composite_model(reference)
        context = self._composite_context(table, reference)
        if match.ambiguous:
            self._composite_source_problem(
                first_model,
                "composite_model_sources_ambiguous",
                Constant.COMPOSITE_MODEL_SOURCE_AMBIGUOUS,
                "A composite model's connection names a workspace and model that match more than one ingested semantic model; no lineage is emitted for its remote tables. Rename one of the models, or republish the composite model so its connection carries the dataset id.",
                context,
            )
            return None
        source = match.dataset
        if source is None:
            self._composite_source_problem(
                first_model,
                "composite_model_sources_not_ingested",
                Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED,
                "A composite model reads a semantic model that was not ingested in this run; its remote tables get no lineage. Include the source model's workspace in the workspace pattern and make sure the principal can read it.",
                context,
            )
            return None
        if not source.tables:
            self._composite_source_problem(
                first_model,
                "composite_model_sources_not_ingested",
                Constant.COMPOSITE_MODEL_SOURCE_NOT_INGESTED,
                "A composite model reads a semantic model whose definition was not ingested in this run (see the failure reported for its getDefinition call); its remote tables get no lineage.",
                context,
            )
            return None
        if first_model:
            self.__reporter.composite_model_sources_resolved += 1
        remote_name = table.remote_table_name.casefold()
        source_table = next(
            (t for t in source.tables if t.name.casefold() == remote_name), None
        )
        if source_table is None and self._first_composite_table(table):
            self.__reporter.composite_model_source_tables_not_found += 1
            self.__reporter.warning(
                title=Constant.COMPOSITE_MODEL_SOURCE_TABLE_NOT_FOUND,
                message="A composite model's remote table has no table of that name in the ingested source model (renamed or removed there, or its table part did not parse); it gets no lineage.",
                context=context,
            )
        return source_table

    def _composite_source_problem(
        self, first_model: bool, counter: str, title: str, message: str, context: str
    ) -> None:
        if not first_model:
            return
        setattr(self.__reporter, counter, getattr(self.__reporter, counter) + 1)
        self.__reporter.warning(title=title, message=message, context=context)

    def _first_composite_model(
        self, reference: powerbi_data_classes.RemoteModelReference
    ) -> bool:
        # Two connections to different dataset ids may share a display name.
        key = reference.dataset_id or remote_model_key(
            reference.workspace_name, reference.model_name
        )
        seen = self.__reporter._composite_models_seen
        if key in seen:
            return False
        seen.add(key)
        return True

    def _first_composite_table(self, table: powerbi_data_classes.Table) -> bool:
        seen = self.__reporter._composite_tables_seen
        key = (table.dataset.id if table.dataset is not None else "", table.name)
        if key in seen:
            return False
        seen.add(key)
        return True

    @staticmethod
    def _composite_context(
        table: powerbi_data_classes.Table,
        reference: powerbi_data_classes.RemoteModelReference,
    ) -> str:
        return (
            f"{table.full_name} -> workspace={reference.workspace_name}, "
            f"model={reference.model_name}, table={table.remote_table_name}"
        )

    @staticmethod
    def _add_field_set_lineage(
        cll_lineage: List[FineGrainedLineage],
        ds_urn: str,
        downstream_field: str,
        upstream_fields: List[str],
    ) -> None:
        """Append one FIELD_SET edge onto `downstream_field`, dropping
        duplicates and the field itself (a field never feeds itself)."""
        own_field = builder.make_schema_field_urn(ds_urn, downstream_field)
        upstreams = [
            field for field in deduplicate_list(upstream_fields) if field != own_field
        ]
        if not upstreams:
            return
        cll_lineage.append(
            FineGrainedLineage(
                downstreamType=FineGrainedLineageDownstreamType.FIELD,
                downstreams=[own_field],
                upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
                upstreams=upstreams,
            )
        )

    def _refresh_operation_mcps(
        self, dataset: powerbi_data_classes.PowerBIDataset, ds_urn: str
    ) -> List[MetadataChangeProposalWrapper]:
        """The latest completed and the latest failed refresh as Operations.

        Both are keyed on the refresh's own end time, so re-running ingestion
        does not create duplicate operation events.
        """
        if not self.__config.extract_refresh_history:
            return []
        if dataset.id not in self.__refresh_plans:
            self.__refresh_plans[dataset.id] = self._plan_refresh_operations(dataset)
        return [
            MetadataChangeProposalWrapper(
                entityUrn=ds_urn,
                aspect=self._refresh_operation(
                    plan.refresh, plan.end_ms, plan.last_updated_ms
                ),
            )
            for plan in self.__refresh_plans[dataset.id]
        ]

    def _plan_refresh_operations(
        self, dataset: powerbi_data_classes.PowerBIDataset
    ) -> List[_RefreshOperationPlan]:
        context = f"workspace={dataset.workspace_name}, dataset={dataset.name} id={dataset.id}"
        plans: List[_RefreshOperationPlan] = []
        completed_end_ms: Optional[int] = None
        completed = dataset.last_refresh
        if (
            completed is not None
            and completed.status == Constant.REFRESH_STATUS_COMPLETED
        ):
            completed_end_ms = self._refresh_end_ms(completed, context)
            if completed_end_ms is not None:
                plans.append(
                    _RefreshOperationPlan(completed, completed_end_ms, completed_end_ms)
                )
        failed = dataset.last_failed_refresh
        if failed is not None and failed.status == Constant.REFRESH_STATUS_FAILED:
            failed_end_ms = self._refresh_end_ms(failed, context)
            if failed_end_ms is not None:
                # A failed refresh leaves the data as it was. The dataset's
                # last-updated time is read off the newest Operation, so a
                # failure newer than the last success points back at it; an
                # older failure is not the newest Operation and keeps its own
                # time. With no completed refresh in the window the time is
                # unknown: 0 is read as "unknown" by both consumers
                # (auto_patch_last_modified.py skips a falsy
                # lastUpdatedTimestamp, and getDatasetLastUpdatedMs in
                # datahub-web-react/src/app/entityV2/shared/utils.ts maps 0 to
                # undefined), so the failure never surfaces as a data update.
                last_updated_ms = failed_end_ms
                if completed_end_ms is not None and failed_end_ms > completed_end_ms:
                    last_updated_ms = completed_end_ms
                elif completed_end_ms is None:
                    last_updated_ms = Constant.LAST_UPDATED_UNKNOWN
                    self.__reporter.refresh_failed_without_completed_baseline += 1
                    self.__reporter.warning(
                        title="Dataset Last Updated Unknown",
                        message="A failed refresh was emitted without a last-updated time because the fetched refresh history holds no completed refresh to anchor it on.",
                        context=context,
                    )
                plans.append(
                    _RefreshOperationPlan(failed, failed_end_ms, last_updated_ms)
                )
        return plans

    def _refresh_end_ms(
        self, refresh: powerbi_data_classes.DatasetRefresh, context: str
    ) -> Optional[int]:
        # A refresh that never finished has no end time; the operation would
        # otherwise have to be stamped with "now" and duplicate on every run.
        if not refresh.end_time:
            return None
        try:
            return int(date_parser.isoparse(refresh.end_time).timestamp() * 1000)
        except (ValueError, OverflowError):
            self.__reporter.refresh_history_unparseable += 1
            logger.debug(
                "Unparseable refresh endTime %r for %s", refresh.end_time, context
            )
            return None

    @staticmethod
    def _refresh_operation(
        refresh: powerbi_data_classes.DatasetRefresh,
        end_ms: int,
        last_updated_ms: int,
    ) -> OperationClass:
        custom_properties = {
            key: value
            for key, value in (
                (Constant.REFRESH_TYPE, refresh.refresh_type),
                (Constant.STATUS, refresh.status),
                (Constant.REQUEST_ID, refresh.request_id),
                (Constant.START_TIME, refresh.start_time),
            )
            if value
        }
        if refresh.status == Constant.REFRESH_STATUS_FAILED:
            custom_properties.update(
                _refresh_error_properties(refresh.service_exception_json)
            )
        return OperationClass(
            timestampMillis=end_ms,
            lastUpdatedTimestamp=last_updated_ms,
            operationType=OperationTypeClass.CUSTOM,
            customOperationType=Constant.CUSTOM_OPERATION_REFRESH,
            customProperties=custom_properties,
        )

    @staticmethod
    def _refresh_schedule_properties(
        schedule: Optional[powerbi_data_classes.DatasetRefreshSchedule],
    ) -> Dict[str, str]:
        if schedule is None:
            return {}
        prefix = Constant.REFRESH_SCHEDULE_PROPERTY_PREFIX
        properties = {
            f"{prefix}{Constant.DAYS}": ",".join(schedule.days),
            f"{prefix}{Constant.TIMES}": ",".join(schedule.times),
        }
        if schedule.local_time_zone_id:
            properties[f"{prefix}{Constant.LOCAL_TIME_ZONE_ID}"] = (
                schedule.local_time_zone_id
            )
        if schedule.enabled is not None:
            properties[f"{prefix}{Constant.ENABLED}"] = str(schedule.enabled).lower()
        return properties

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
        if self.__config.extract_model_properties:
            self._apply_field_model_properties(field, schema_field)
        return schema_field

    @staticmethod
    def _apply_field_model_properties(
        field: Union[powerbi_data_classes.Column, powerbi_data_classes.Measure],
        schema_field: SchemaFieldClass,
    ) -> None:
        """extract_model_properties: `isKey` and an explicit `isNullable` map
        onto the schema field; everything else goes into its jsonProps."""
        props = field.model_properties
        if props is None:
            return
        if props.is_key:
            schema_field.isPartOfKey = True
        if props.is_nullable is not None:
            schema_field.nullable = props.is_nullable
        schema_field.jsonProps = props.to_json_props(is_hidden=field.isHidden)

    def _table_custom_properties(
        self,
        table: powerbi_data_classes.Table,
        dataset: powerbi_data_classes.PowerBIDataset,
    ) -> Dict[str, str]:
        custom_properties: Dict[str, str] = {
            Constant.DATASET_ID: dataset.id,
            **self._refresh_schedule_properties(dataset.refresh_schedule),
        }
        if not self.__config.extract_model_properties:
            return custom_properties
        if table.storage_mode:
            custom_properties[Constant.STORAGE_MODE] = table.storage_mode
        if table.partition_count is not None:
            custom_properties[Constant.PARTITION_COUNT] = str(table.partition_count)
        policy = table.refresh_policy
        if policy is not None:
            custom_properties[Constant.INCREMENTAL_REFRESH] = Constant.TRUE_VALUE
            for key, value in (
                (
                    Constant.REFRESH_POLICY_ROLLING_WINDOW_GRANULARITY,
                    policy.rolling_window_granularity,
                ),
                (
                    Constant.REFRESH_POLICY_ROLLING_WINDOW_PERIODS,
                    policy.rolling_window_periods,
                ),
                (
                    Constant.REFRESH_POLICY_INCREMENTAL_GRANULARITY,
                    policy.incremental_granularity,
                ),
                (
                    Constant.REFRESH_POLICY_INCREMENTAL_PERIODS,
                    policy.incremental_periods,
                ),
            ):
                if value is not None:
                    custom_properties[key] = value
        if table.rls_filters:
            rls_filters = sorted(table.rls_filters, key=lambda f: f.role)
            custom_properties[Constant.RLS_ROLES] = json.dumps(
                [
                    {Constant.ROLE: f.role, Constant.FILTER: f.filter}
                    for f in rls_filters
                ]
            )
            custom_properties[Constant.RLS_ROLE_COUNT] = str(len(rls_filters))
        for name, value in sorted(dataset.parameters.items()):
            custom_properties[f"{Constant.PARAMETER_PREFIX}{name}"] = (
                Constant.MASKED_VALUE
                if _is_secret_parameter(name)
                # The API returns strings, but a non-string or oversized value
                # must not be able to break the aspect.
                else str(value)[:PARAMETER_VALUE_MAX_LENGTH]
            )
        return custom_properties

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
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi dataset to datahub dataset. Here we are mapping each table of PowerBi Dataset to Datahub dataset.
        In PowerBi Tile would be having single dataset, However corresponding Datahub's chart might have many input sources.
        """

        dataset_mcps: List[MetadataChangeProposalWrapper] = []

        if dataset is None:
            return dataset_mcps

        logger.debug(f"Processing dataset {dataset.name}")

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
            # Create a URN for dataset
            ds_urn = self.make_dataset_urn_for_table(table)

            logger.debug(f"dataset_urn={ds_urn}")
            if table.expression:
                dataset_mcps.append(
                    self._view_properties_mcp(
                        ds_urn, table.expression, materialized=False
                    )
                )
            if self.__config.extract_dataset_schema:
                dataset_mcps.extend(self.extract_dataset_schema(table, ds_urn))

            # normally, the person who configures the dataset will be the most accurate person for ownership
            if (
                self.__config.extract_ownership
                and self.__config.ownership.dataset_configured_by_as_owner
                and dataset.configuredBy
            ):
                user_urn = self.create_datahub_owner_urn(dataset.configuredBy)
                owner_class = OwnerClass(owner=user_urn, type=OwnershipTypeClass.NONE)
                dataset_mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=ds_urn,
                        aspect=OwnershipClass(owners=[owner_class]),
                    )
                )

            dataset_mcps.extend(
                self._dataset_identity_mcps(
                    ds_urn,
                    properties=DatasetPropertiesClass(
                        name=table.name,
                        # TMDL `///` table doc comments are more specific than
                        # the dataset description; the scan API never supplies
                        # them.
                        description=table.description or dataset.description,
                        externalUrl=dataset.webUrl,
                        customProperties=self._table_custom_properties(table, dataset),
                    ),
                    subtype=BIContainerSubTypes.POWERBI_DATASET_TABLE,
                    browse_path=[
                        BrowsePathEntryClass(
                            id=workspace.name,
                            urn=self.make_container_urn_for_workspace(workspace),
                        ),
                        BrowsePathEntryClass(
                            id=dataset.name or dataset.id,
                            urn=self.make_container_urn_for_dataset(dataset)
                            if self.__config.extract_datasets_to_containers
                            else None,
                        ),
                    ],
                )
            )

            if self.__config.extract_lineage is True:
                dataset_mcps.extend(self.extract_lineage(table, ds_urn, workspace))

            dataset_mcps.extend(self._refresh_operation_mcps(dataset, ds_urn))

            self.append_container_mcp(
                dataset_mcps,
                ds_urn,
                dataset,
            )

            self.append_tag_mcp(
                dataset_mcps,
                ds_urn,
                Constant.DATASET,
                dataset.tags,
            )
            self.extract_profile(dataset_mcps, workspace, dataset, table, ds_urn)

        return dataset_mcps

    def paginated_report_datasource_urns(
        self,
        report: powerbi_data_classes.Report,
    ) -> List[str]:
        """Resolve a paginated report's embedded datasources to upstream URNs.

        Produces coarse-grained (server[.database]) URNs; table-level lineage
        would require parsing the .rdl XML.
        """
        urns: List[str] = []
        for ds in report.datasources:
            if ds.powerbi_dataset_id is not None:
                # Bound to a shared Power BI dataset; lineage comes from
                # report.dataset (resolved in PowerBiAPI.get_reports), not here.
                continue

            # Normalise like dataset_type_mapping does (e.g. "Amazon Redshift"
            # -> "AmazonRedshift") so spaced API type names still resolve.
            pair = POWERBI_TYPE_TO_DATA_PLATFORM_PAIR.get(
                ds.datasource_type
            ) or POWERBI_TYPE_TO_DATA_PLATFORM_PAIR.get(
                ds.datasource_type.replace(" ", "")
            )
            if pair is None:
                self.__reporter.info(
                    title="Unmapped Paginated Report Datasource",
                    message=(
                        f"PowerBI datasource type {ds.datasource_type!r} has no "
                        "DataHub platform mapping; lineage skipped."
                    ),
                    context=f"report={report.name}, server={ds.server}",
                )
                continue

            # Respect a user-narrowed dataset_type_mapping, consistent with the
            # M-Query lineage path: a platform excluded from the mapping should
            # not produce lineage even though it is a known platform.
            if not self.__config.is_platform_in_dataset_type_mapping(
                ds.datasource_type
            ):
                logger.debug(
                    "Skipping paginated report datasource for platform %r: not "
                    "in dataset_type_mapping.",
                    ds.datasource_type,
                )
                continue

            platform_detail = (
                self.__dataplatform_instance_resolver.get_platform_instance(
                    PowerBIPlatformDetail(
                        data_platform_pair=pair,
                        data_platform_server=ds.server,
                    )
                )
            )

            name_parts = [p for p in [ds.server, ds.database] if p]
            urns.append(
                self.lineage_urn_to_lowercase(
                    builder.make_dataset_urn_with_platform_instance(
                        platform=pair.datahub_data_platform_name,
                        name=".".join(name_parts),
                        platform_instance=platform_detail.platform_instance,
                        env=platform_detail.env or self.__config.env,
                    )
                )
            )

        return urns

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
        dashboard: powerbi_data_classes.Dashboard,
        *,
        position: int,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi tile to datahub chart. `position` is the tile's 1-based
        place on the dashboard, which names a tile that has no title.
        """
        logger.info(
            f"Converting tile {tile.title or '(untitled)'}(id={tile.id}) to chart"
        )
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
        # A tile pinned from a report shows one of its visuals, so when the
        # report's definition told us which tables its pages read, those are
        # the tile's inputs rather than every table of the model.
        if tile.report is not None:
            ds_input = self._report_used_dataset_urns(tile.report) or ds_input
        title = tile.title or Constant.TILE_TITLE_FALLBACK.format(position=position)

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
            title=title,
            description=title,
            lastModified=ChangeAuditStamps(),
            inputs=ds_input,
            externalUrl=tile.report.webUrl if tile.report else None,
            customProperties=tile_custom_properties(tile),
        )

        info_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=chart_info_instance,
        )

        # removed status mcp
        status_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
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
        chart_key_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartUrn.from_string(chart_urn).to_key_aspect(),
        )

        # Browse path
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(
                    id=workspace.name,
                    urn=self.make_container_urn_for_workspace(workspace),
                ),
                BrowsePathEntryClass(
                    id=dashboard.displayName,
                    urn=self.make_dashboard_urn_for_dashboard(dashboard),
                ),
            ]
        )
        browse_path_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=browse_path,
        )

        # DataPlatformInstance aspect
        data_platform_instance_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=self._get_data_platform_instance_aspect(),
        )

        result_mcps = [
            info_mcp,
            status_mcp,
            subtype_mcp,
            chart_key_mcp,
            browse_path_mcp,
            data_platform_instance_mcp,
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

        dashboard_urn = self.make_dashboard_urn_for_dashboard(dashboard)

        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self._get_user_urns_for_ownership(
            dashboard.users, user_mcps
        )

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

        info_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(dashboard.id),
        )

        # Dashboard key
        dashboard_key_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
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
            owner_mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=ownership,
            )

        # Dashboard browsePaths
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(
                    id=workspace.name,
                    urn=self.make_container_urn_for_workspace(workspace),
                )
            ]
        )
        browse_path_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=browse_path,
        )

        # DataPlatformInstance aspect
        data_platform_instance_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=self._get_data_platform_instance_aspect(),
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
            data_platform_instance_mcp,
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
        dataset: Optional[powerbi_data_classes.PowerBIDataset] = None,
    ) -> None:
        if self.__config.extract_datasets_to_containers and isinstance(
            dataset, powerbi_data_classes.PowerBIDataset
        ):
            container_key = self.make_container_key_for_dataset(dataset)
        elif self.__config.extract_workspaces_to_containers and self.workspace_key:
            container_key = self.workspace_key
        else:
            return None

        container_urn = container_key.as_urn()
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
            env=self.__config.env,
            workspace_id_as_urn_part=self.__config.workspace_id_as_urn_part,
        )
        container_work_units = gen_containers(
            container_key=self.workspace_key,
            name=workspace.name,
            sub_types=[workspace.type],
            external_url=workspace.webUrl,
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
        dataset_key = self.make_container_key_for_dataset(dataset)
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
            tags_mcp = MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=self.transform_tags(tags),
            )
            list_of_mcps.append(tags_mcp)

    def _make_user_urn(self, user: powerbi_data_classes.User) -> str:
        """
        Single source of truth for user URN generation.
        Handles email/ID selection and suffix removal based on config.
        """
        user_id = user.get_urn_part(
            use_email=self.__config.ownership.use_powerbi_email,
            remove_email_suffix=self.__config.ownership.remove_email_suffix,
        )
        return builder.make_user_urn(user_id)

    def to_datahub_user(
        self, user: powerbi_data_classes.User
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Create user entity with Key and Info aspects.
        Only called when create_corp_user=True (opt-in).
        User accepts conflict risk - PowerBI data may overwrite existing profiles.
        """
        logger.debug(
            "Mapping user %s (id=%s) to datahub corpuser",
            user.displayName,
            user.id,
        )

        user_urn = self._make_user_urn(user)
        # Extract user_id from URN for Key aspect (must match URN identity)
        user_id = user_urn.split(":")[-1]

        user_key = CorpUserKeyClass(username=user_id)

        user_info = CorpUserInfoClass(
            active=True,
            displayName=user.displayName or user_id,  # Fallback to user_id if null
            email=user.emailAddress
            or None,  # PowerBI API may return "" for missing email
        )

        return [
            MetadataChangeProposalWrapper(entityUrn=user_urn, aspect=user_key),
            MetadataChangeProposalWrapper(entityUrn=user_urn, aspect=user_info),
            MetadataChangeProposalWrapper(
                entityUrn=user_urn,
                aspect=make_corp_user_status_aspect(CORP_USER_STATUS_ACTIVE),
            ),
        ]

    def _get_qualified_owners(
        self, users: List[powerbi_data_classes.User]
    ) -> List[powerbi_data_classes.User]:
        """
        Filter users based on principalType and owner_criteria.
        Returns users that qualify for ownership assignment.

        Filtering rules:
        - Must be non-None
        - principalType must be "User" (not Group, App, etc.)
        - If owner_criteria is set, user must have at least one matching access right
        - If owner_criteria is None or [], all User-type principals qualify
        """
        qualified_users = []
        for user in users:
            if not user:
                continue

            # Check principalType first
            if not user.principalType or user.principalType != "User":
                continue

            user_rights = [
                user.datasetUserAccessRight,
                user.reportUserAccessRight,
                user.dashboardUserAccessRight,
                user.groupUserAccessRight,
            ]

            # Check if user qualifies based on criteria
            # Note: Empty list [] is treated same as None (no criteria = all qualify)
            if not self.__config.ownership.owner_criteria:
                # No criteria (None or []) - all User-type principals qualify
                qualified_users.append(user)
            elif (
                len(set(user_rights) & set(self.__config.ownership.owner_criteria)) > 0
            ):
                # Has matching access rights
                qualified_users.append(user)

        return qualified_users

    def to_datahub_users(
        self, users: List[powerbi_data_classes.User]
    ) -> List[MetadataChangeProposalWrapper]:
        """Build user MCPs; gated by extract_ownership AND create_corp_user."""
        if not (
            self.__config.extract_ownership and self.__config.ownership.create_corp_user
        ):
            return []

        user_mcps = []
        for user in self._get_qualified_owners(users):
            user_mcps.extend(self.to_datahub_user(user))

        return user_mcps

    def to_datahub_user_urns(self, users: List[powerbi_data_classes.User]) -> List[str]:
        """
        Return user URNs for ownership references (soft references).
        Used when create_corp_user=False (default mode).
        No entity creation - follows Tableau/Looker pattern.
        """
        return [self._make_user_urn(user) for user in self._get_qualified_owners(users)]

    def _get_user_urns_for_ownership(
        self,
        users: List[powerbi_data_classes.User],
        user_mcps: List[MetadataChangeProposalWrapper],
    ) -> List[str]:
        """
        Get user URNs for ownership based on create_corp_user mode.

        When create_corp_user=True (opt-in): Extract URNs from created user MCPs
        When create_corp_user=False (default): Get URNs directly (soft references)
        """
        if not self.__config.extract_ownership:
            return []
        if self.__config.ownership.create_corp_user:
            return self.to_urn_set(user_mcps)
        else:
            return self.to_datahub_user_urns(users)

    def to_datahub_chart(
        self,
        tiles: List[powerbi_data_classes.Tile],
        workspace: powerbi_data_classes.Workspace,
        dashboard: powerbi_data_classes.Dashboard,
    ) -> Tuple[
        List[MetadataChangeProposalWrapper], List[MetadataChangeProposalWrapper]
    ]:
        ds_mcps = []
        chart_mcps = []

        # Return empty list if input list is empty
        if not tiles:
            return [], []

        logger.info(f"Converting tiles(count={len(tiles)}) to charts")

        for position, tile in enumerate(tiles, start=1):
            if tile is None:
                continue
            # First convert the dataset to MCP, because dataset mcp is used in input attribute of chart mcp
            dataset_mcps = self.to_datahub_dataset(tile.dataset, workspace)
            # Now convert tile to chart MCP
            chart_mcp = self.to_datahub_chart_mcp(
                tile, dataset_mcps, workspace, dashboard, position=position
            )

            ds_mcps.extend(dataset_mcps)
            chart_mcps.extend(chart_mcp)

        # Return dataset and chart MCPs

        return ds_mcps, chart_mcps

    def to_datahub_work_units(
        self,
        dashboard: powerbi_data_classes.Dashboard,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataWorkUnit]:
        mcps: List[MetadataChangeProposalWrapper] = []

        logger.info(
            f"Converting dashboard={dashboard.displayName} to datahub dashboard"
        )

        # Convert user to CorpUser
        user_mcps: List[MetadataChangeProposalWrapper] = self.to_datahub_users(
            dashboard.users
        )
        # Convert tiles to charts
        ds_mcps, chart_mcps = self.to_datahub_chart(
            dashboard.tiles, workspace, dashboard
        )

        # collect all downstream reports (dashboards)
        dashboard_edges = []
        for t in dashboard.tiles:
            if t.report:
                dashboard_urn = self.make_dashboard_urn_for_report(t.report)
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

        # Now add MCPs in sequence (excluding user MCPs which are handled separately)
        mcps.extend(ds_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(dashboard_mcps)

        # Convert MCP to work_units
        work_units: List[MetadataWorkUnit] = [
            wu.as_workunit() for wu in mcps if wu is not None
        ]

        # Handle user MCPs separately with is_primary_source=False
        # This prevents stateful ingestion from tracking/soft-deleting users
        if self.__config.ownership.create_corp_user:
            user_work_units = [
                wu.as_workunit(is_primary_source=False)
                for wu in user_mcps
                if wu is not None
            ]
            work_units.extend(user_work_units)

        # Return set of work_unit
        return deduplicate_list(work_units, lambda wu: wu.id)

    def _filter_properties(
        self, filters: List[powerbi_data_classes.ReportFilter]
    ) -> Dict[str, str]:
        """Report or page filters as `filters` (a JSON list) and `filterCount`
        custom properties; empty when the report declares none. The list is
        capped so a filter-heavy report cannot bloat the aspect; `filterCount`
        still reports the full number."""
        if not filters:
            return {}
        kept = filters[:MAX_FILTERS_IN_PROPERTY]
        properties = {
            Constant.FILTERS: json.dumps([f.to_dict() for f in kept]),
            Constant.FILTER_COUNT: str(len(filters)),
        }
        if len(kept) < len(filters):
            properties[Constant.FILTERS_TRUNCATED] = "true"
            self.__reporter.report_filters_truncated += len(filters) - len(kept)
        return properties

    def _page_input_fields(
        self,
        page: powerbi_data_classes.Page,
        report: powerbi_data_classes.Report,
        chart_urn: str,
    ) -> ResolvedInputFields:
        """Model columns and measures the page's visuals read, as inputFields
        pointing at the semantic-model tables' schema fields, plus the tables
        they belong to. This page pass is the only place feeding the page
        counters."""
        resolved = self._resolve_input_fields(
            page.field_references, report, chart_urn, f"page {page.name}"
        )
        self.__reporter.report_field_references_resolved += resolved.resolved
        self.__reporter.report_field_references_unresolved += resolved.unresolved
        return resolved

    def _report_used_dataset_urns(
        self, report: powerbi_data_classes.Report
    ) -> List[str]:
        """Tables the report's pages read, from their resolved field references,
        in first-seen order; empty when no definition was fetched or nothing
        resolved. Memoised per report id and counter-free: the page pass
        already counts these references, and every tile of the report asks."""
        cached = self.__report_used_dataset_urns.get(report.id)
        if cached is not None:
            return cached
        dataset_urns: List[str] = []
        if self.__config.extract_report_field_usage:
            for page in report.pages:
                resolved = self._resolve_input_fields(
                    page.field_references,
                    report,
                    self.make_chart_urn_for_page(page),
                    f"page {page.name}",
                )
                for dataset_urn in resolved.dataset_urns:
                    if dataset_urn not in dataset_urns:
                        dataset_urns.append(dataset_urn)
        self.__report_used_dataset_urns[report.id] = dataset_urns
        return dataset_urns

    def _resolve_input_fields(
        self,
        field_references: List[powerbi_data_classes.PageFieldReference],
        report: powerbi_data_classes.Report,
        chart_urn: str,
        context: str,
    ) -> ResolvedInputFields:
        """Field references matched against the report's semantic-model tables;
        the caller decides which counters the outcome feeds."""
        result = ResolvedInputFields()
        if not field_references or report.dataset is None:
            # Without the dataset (an unresolved cross-workspace reference) no
            # field can be matched; the references are neither resolved nor
            # counted as unresolved.
            return result
        tables_by_name = {t.name: t for t in report.dataset.tables}
        fields: List[InputFieldClass] = []
        for reference in field_references:
            table = tables_by_name.get(reference.table)
            if table is None:
                result.unresolved += 1
                logger.debug(
                    "Visual field %s[%s] on %s not found in dataset %s",
                    reference.table,
                    reference.field,
                    context,
                    report.dataset.id,
                )
                continue
            # The table-level edge is justified by the table match alone: a
            # reference whose field is missing from the schema (renamed
            # column, hierarchy level) still proves the chart reads the table.
            table_urn = self.make_dataset_urn_for_table(table)
            if table_urn not in result.dataset_urns:
                result.dataset_urns.append(table_urn)
            model_fields: List[
                Union[powerbi_data_classes.Column, powerbi_data_classes.Measure]
            ] = [*(table.columns or []), *(table.measures or [])]
            schema_field = next(
                (
                    model_field
                    for model_field in model_fields
                    if model_field.name == reference.field
                ),
                None,
            )
            if schema_field is None:
                result.unresolved += 1
                logger.debug(
                    "Visual field %s[%s] on %s not found in table schema",
                    reference.table,
                    reference.field,
                    context,
                )
                continue
            field_urn = builder.make_schema_field_urn(table_urn, reference.field)
            fields.append(
                InputFieldClass(
                    schemaFieldUrn=field_urn,
                    schemaField=self.to_datahub_schema_field(schema_field),
                )
            )
            result.resolved += 1
        if fields:
            result.mcp = MetadataChangeProposalWrapper(
                entityUrn=chart_urn, aspect=InputFieldsClass(fields=fields)
            )
        return result

    def _page_url(
        self, report: powerbi_data_classes.Report, page: powerbi_data_classes.Page
    ) -> Optional[str]:
        """The page's URL in the Power BI service: the report URL plus the
        page's name segment, inserted before any query string."""
        if not report.webUrl:
            return None
        base, separator, query = report.webUrl.partition(Constant.URL_QUERY_SEPARATOR)
        return f"{base.rstrip('/')}/{page.name}{separator}{query}"

    def _chart_common_mcps(
        self,
        chart_urn: str,
        subtype: str,
        workspace: powerbi_data_classes.Workspace,
        report: powerbi_data_classes.Report,
        input_fields_mcp: Optional[MetadataChangeProposalWrapper],
        page: Optional[powerbi_data_classes.Page] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        """The aspects every report chart (page or visual) carries besides
        its ChartInfo, in the order they are emitted: status, subtype, browse
        path (workspace > report, then the page for a visual), platform
        instance, inputFields when any field resolved, and the container."""
        path = [
            BrowsePathEntryClass(
                id=workspace.name,
                urn=self.make_container_urn_for_workspace(workspace),
            ),
            BrowsePathEntryClass(
                id=report.name,
                urn=self.make_dashboard_urn_for_report(report),
            ),
        ]
        if page is not None:
            path.append(
                BrowsePathEntryClass(
                    id=page.displayName or page.name,
                    urn=self.make_chart_urn_for_page(page),
                )
            )
        mcps = [
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn, aspect=StatusClass(removed=False)
            ),
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn, aspect=SubTypesClass(typeNames=[subtype])
            ),
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn, aspect=BrowsePathsV2Class(path=path)
            ),
            MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=self._get_data_platform_instance_aspect(),
            ),
        ]
        if input_fields_mcp is not None:
            mcps.append(input_fields_mcp)
        self.append_container_mcp(mcps, chart_urn)
        return mcps

    def make_chart_urn_for_page(self, page: powerbi_data_classes.Page) -> str:
        return builder.make_chart_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=page.get_urn_part(),
        )

    def _visual_properties(
        self, visual: powerbi_data_classes.Visual, page: powerbi_data_classes.Page
    ) -> Dict[str, str]:
        properties = {
            Constant.VISUAL_NAME: visual.name,
            Constant.PAGE_NAME: page.name,
        }
        if visual.visual_type:
            properties[Constant.VISUAL_TYPE] = visual.visual_type
        properties.update(self._filter_properties(visual.filters))
        return properties

    def visual_to_chart_mcps(
        self,
        visual: powerbi_data_classes.Visual,
        page: powerbi_data_classes.Page,
        report: powerbi_data_classes.Report,
        workspace: powerbi_data_classes.Workspace,
    ) -> List[MetadataChangeProposalWrapper]:
        """One visual of a report page as a Chart entity carrying exactly the
        fields that visual reads and its visual-level filters. Its table-level
        inputs are the tables those fields belong to; a visual that reads no
        resolvable field gets no table inputs rather than the page's whole
        model, so a text box never claims lineage from every table."""
        chart_urn = builder.make_chart_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=visual.get_urn_part(),
        )
        title = visual.title or Constant.VISUAL_TITLE_FALLBACK.format(
            visual_type=visual.visual_type or Constant.VISUAL,
            page=page.displayName or page.name,
        )
        resolved = self._resolve_input_fields(
            visual.field_references,
            report,
            chart_urn,
            f"visual {visual.name} of page {page.name}",
        )
        info_mcp = MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=ChartInfoClass(
                title=title,
                description=title,
                lastModified=ChangeAuditStamps(),
                chartUrl=self._page_url(report, page),
                type=VISUAL_TYPE_TO_CHART_TYPE.get(visual.visual_type or ""),
                inputs=resolved.dataset_urns,
                customProperties=self._visual_properties(visual, page),
            ),
        )
        reporter = self.__reporter
        reporter.report_visual_field_references_resolved += resolved.resolved
        reporter.report_visual_field_references_unresolved += resolved.unresolved
        reporter.report_visuals_emitted += 1
        if not visual.field_references:
            reporter.report_visuals_without_fields += 1
        elif resolved.mcp is None:
            reporter.report_visuals_fields_unresolved += 1
        return [
            info_mcp,
            *self._chart_common_mcps(
                chart_urn,
                BIAssetSubTypes.POWERBI_VISUAL,
                workspace,
                report,
                resolved.mcp,
                page=page,
            ),
        ]

    def pages_to_chart(
        self,
        pages: List[powerbi_data_classes.Page],
        workspace: powerbi_data_classes.Workspace,
        ds_mcps: List[MetadataChangeProposalWrapper],
        report: powerbi_data_classes.Report,
    ) -> List[MetadataChangeProposalWrapper]:
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
            chart_urn = self.make_chart_urn_for_page(page)

            logger.debug(f"{Constant.CHART_URN}={chart_urn}")

            ds_input: List[str] = self.to_urn_set(
                [x for x in ds_mcps if x.entityType == Constant.DATASET]
            )

            custom_properties = {Constant.ORDER: str(page.order)}
            if page.visual_count is not None:
                custom_properties[Constant.VISUAL_COUNT] = str(page.visual_count)
            if page.visual_types:
                custom_properties[Constant.VISUAL_TYPES] = ", ".join(page.visual_types)
            custom_properties.update(self._filter_properties(page.filters))

            resolved = self._page_input_fields(page, report, chart_urn)
            # When the report definition told us which tables the page's
            # visuals read, those are the page's inputs; without field usage
            # (or when nothing resolved) the whole model stays, as before.
            if self.__config.extract_report_field_usage and resolved.dataset_urns:
                ds_input = resolved.dataset_urns

            chart_info_instance = ChartInfoClass(
                title=page.displayName or "",
                description=page.displayName or "",
                lastModified=ChangeAuditStamps(),
                chartUrl=self._page_url(report, page),
                inputs=ds_input,
                customProperties=custom_properties,
            )

            info_mcp = MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=chart_info_instance,
            )

            list_of_mcps = [
                info_mcp,
                *self._chart_common_mcps(
                    chart_urn,
                    BIAssetSubTypes.POWERBI_PAGE,
                    workspace,
                    report,
                    resolved.mcp,
                ),
            ]

            if self.__config.extract_report_visuals:
                for visual in page.visuals:
                    list_of_mcps.extend(
                        self.visual_to_chart_mcps(visual, page, report, workspace)
                    )

            return list_of_mcps

        for page in pages:
            if page is None:
                continue
            # Now convert tile to chart MCP
            chart_mcp = to_chart_mcps(page, ds_mcps)
            chart_mcps.extend(chart_mcp)

        return chart_mcps

    def report_to_dashboard(
        self,
        workspace: powerbi_data_classes.Workspace,
        report: powerbi_data_classes.Report,
        chart_mcps: List[MetadataChangeProposalWrapper],
        user_mcps: List[MetadataChangeProposalWrapper],
        dataset_edges: List[EdgeClass],
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Map PowerBi report to Datahub dashboard
        """

        dashboard_urn = self.make_dashboard_urn_for_report(report)
        chart_urn_list: List[str] = self.to_urn_set(chart_mcps)
        user_urn_list: List[str] = self._get_user_urns_for_ownership(
            report.users, user_mcps
        )

        # DashboardInfo mcp
        dashboard_info_cls = DashboardInfoClass(
            description=report.description,
            title=report.name or "",
            charts=chart_urn_list,
            lastModified=ChangeAuditStamps(),
            dashboardUrl=report.webUrl,
            datasetEdges=dataset_edges,
            customProperties=self._filter_properties(report.filters),
        )

        info_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=dashboard_info_cls,
        )

        # removed status mcp
        removed_status_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=StatusClass(removed=False),
        )

        # dashboardKey mcp
        dashboard_key_cls = DashboardKeyClass(
            dashboardTool=self.__config.platform_name,
            dashboardId=Constant.DASHBOARD_ID.format(report.id),
        )

        # Dashboard key
        dashboard_key_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
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
            owner_mcp = MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=ownership,
            )

        # Report browsePaths
        browse_path = BrowsePathsV2Class(
            path=[
                BrowsePathEntryClass(
                    id=workspace.name,
                    urn=self.make_container_urn_for_workspace(workspace),
                ),
            ]
        )
        browse_path_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=browse_path,
        )

        sub_type_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[report.type.value]),
        )

        # DataPlatformInstance aspect
        data_platform_instance_mcp = MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=self._get_data_platform_instance_aspect(),
        )

        list_of_mcps = [
            browse_path_mcp,
            info_mcp,
            removed_status_mcp,
            dashboard_key_mcp,
            sub_type_mcp,
            data_platform_instance_mcp,
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
        mcps: List[MetadataChangeProposalWrapper] = []

        logger.debug(f"Converting report={report.name} to datahub dashboard")
        # Convert user to CorpUser
        user_mcps = self.to_datahub_users(report.users)
        # Convert pages to charts. A report has a single dataset and the same dataset used in pages to create visualization
        ds_mcps = self.to_datahub_dataset(report.dataset, workspace)
        chart_mcps = self.pages_to_chart(
            report.pages, workspace=workspace, ds_mcps=ds_mcps, report=report
        )

        # collect all upstream datasets; using a set to retain unique urns
        dataset_urns: Set[str] = {
            dataset.entityUrn
            for dataset in ds_mcps
            if dataset.entityType == DatasetUrn.ENTITY_TYPE and dataset.entityUrn
        }
        # Paginated (RDL) reports without a shared dataset get their lineage
        # solely from this fallback path.
        dataset_urns.update(self.paginated_report_datasource_urns(report))
        # Sorted so the emitted patch (and the golden files) are deterministic.
        dataset_edges = [
            EdgeClass(destinationUrn=dataset_urn)
            for dataset_urn in sorted(dataset_urns)
        ]

        # Let's convert report to datahub dashboard
        report_mcps = self.report_to_dashboard(
            workspace=workspace,
            report=report,
            chart_mcps=chart_mcps,
            user_mcps=user_mcps,
            dataset_edges=dataset_edges,
        )

        # Now add MCPs in sequence (excluding user MCPs which are handled separately)
        mcps.extend(ds_mcps)
        mcps.extend(chart_mcps)
        mcps.extend(report_mcps)

        # Convert primary MCPs to work units
        for mcp in mcps:
            yield mcp.as_workunit()

        # Handle user MCPs separately with is_primary_source=False
        # This prevents stateful ingestion from tracking/soft-deleting users
        if self.__config.ownership.create_corp_user:
            for mcp in user_mcps:
                yield mcp.as_workunit(is_primary_source=False)

    def make_container_key_for_dataset(
        self, dataset: powerbi_data_classes.PowerBIDataset
    ) -> ContainerKey:
        return dataset.get_dataset_key(
            self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            env=self.__config.env,
        )

    def make_container_urn_for_dataset(
        self, dataset: powerbi_data_classes.PowerBIDataset
    ) -> str:
        return self.make_container_key_for_dataset(dataset).as_urn()

    def make_container_key_for_dataflow(
        self, dataflow: powerbi_data_classes.Dataflow
    ) -> ContainerKey:
        return dataflow.get_dataflow_key(
            self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            env=self.__config.env,
        )

    def make_container_key_for_workspace(
        self, workspace: powerbi_data_classes.Workspace
    ) -> ContainerKey:
        return workspace.get_workspace_key(
            platform_name=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            env=self.__config.env,
            workspace_id_as_urn_part=self.__config.workspace_id_as_urn_part,
        )

    def make_container_urn_for_workspace(
        self, workspace: powerbi_data_classes.Workspace
    ) -> str:
        return self.make_container_key_for_workspace(workspace).as_urn()

    def make_dashboard_urn_for_report(self, report: powerbi_data_classes.Report) -> str:
        return builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=report.get_urn_part(),
        )

    def make_dashboard_urn_for_dashboard(
        self, dashboard: powerbi_data_classes.Dashboard
    ) -> str:
        return builder.make_dashboard_urn(
            platform=self.__config.platform_name,
            platform_instance=self.__config.platform_instance,
            name=dashboard.get_urn_part(),
        )


@platform_name("PowerBI")
@config_class(PowerBiDashboardSourceConfig)
@support_status(SupportStatus.GA)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.POWERBI_WORKSPACE,
        SourceCapabilityModifier.POWERBI_DATASET,
        SourceCapabilityModifier.POWERBI_DATAFLOW,
    ],
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Disabled by default, configured using `extract_ownership`. In "
    "`metadata_api_mode: workspace_apis` owners come from workspace roles.",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, configured using `extract_lineage`. In "
    "`metadata_api_mode: workspace_apis` lineage comes from the semantic-model "
    "definitions (TMDL partitions) without admin API access. With "
    "`extract_dataflows` (workspace_apis only) Dataflow Gen2 entities get upstream "
    "lineage from their M queries and downstream lineage to the semantic-model "
    "tables that read them.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, configured using `extract_column_level_lineage`. In "
    "`metadata_api_mode: workspace_apis`, `extract_report_field_usage` adds the "
    "model fields each report page reads as `inputFields` on the page chart, and "
    "`extract_report_visuals` emits every visual as its own chart with the fields "
    "it reads.",
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration profiling.enabled",
)
@capability(
    SourceCapability.OPERATION_CAPTURE,
    "Optionally enabled via `extract_refresh_history`: the latest completed and latest failed dataset refresh as Operation aspects, plus the refresh schedule as custom properties",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
@capability(
    SourceCapability.PROBE,
    "Check what a recipe would ingest before running it, with `datahub recipe probe`",
)
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
            ctx,
            config,
            self.reporter,
            self.dataplatform_instance_resolver,
            remote_model_lookup=self.powerbi_client.find_remote_model,
            powerbi_client=self.powerbi_client,
        )

        # Log info if user has opted-in to user creation (may overwrite LDAP/Okta users)
        if self.source_config.ownership.create_corp_user:
            logger.info(
                "PowerBI user creation is ENABLED (create_corp_user=True). "
                "User entities will be created with displayName and email from PowerBI. "
                "Note: This may overwrite existing user profiles from LDAP/Okta/SCIM."
            )

        # For modified_since, stale removal is handled per-workspace in get_workunits_internal().
        # For the default path, AutoStaleEntityRemovalProcessor handles it automatically via get_workunit_processors().
        if self.source_config.modified_since:
            from datahub.ingestion.source.state.entity_removal_state import (
                GenericCheckpointState,
            )

            self.stale_entity_removal_handler: Optional[StaleEntityRemovalHandler] = (
                StaleEntityRemovalHandler(
                    state_provider=self.state_provider,
                    report=self.reporter,
                    config=self.source_config,
                    state_type_class=GenericCheckpointState,
                    pipeline_name=ctx.pipeline_name,
                    run_id=ctx.run_id,
                    platform=self.platform,
                )
            )
        else:
            self.stale_entity_removal_handler = None

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        reporter = PowerBiDashboardSourceReport()
        try:
            PowerBiAPI(
                PowerBiDashboardSourceConfig.parse_obj_allow_extras(config_dict),
                reporter,
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
            return test_report
        # enrich_from_item_definitions only warns when the Fabric token cannot
        # be acquired (the scan still works); surface it here so a recipe
        # whose enrichment would silently do nothing is caught up front.
        token_warnings = [
            warning.message
            for warning in reporter.warnings
            if warning.title == Constant.FABRIC_TOKEN_NOT_ACQUIRED
        ]
        if token_warnings:
            test_report.capability_report = {
                SourceCapability.LINEAGE_FINE: CapabilityReport(
                    capable=False, failure_reason=token_warnings[0]
                )
            }
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
            verdict = workspace_verdict(
                self.source_config,
                WorkspaceFacts(workspace.name, workspace.id, workspace.type),
            )
            if verdict.excluded_by in (WORKSPACE_NAME_PATTERN, WORKSPACE_ID_PATTERN):
                self.reporter.filtered_workspace_names.append(
                    f"{workspace.id} - {workspace.name}"
                )
                continue
            if verdict.excluded_by == WORKSPACE_TYPE_FILTER:
                self.reporter.filtered_workspace_types.append(
                    f"{workspace.id} - {workspace.name} (type = {workspace.type})"
                )
                continue
            # A rule added to workspace_verdict needs its own report list.
            assert verdict.included, f"unreported exclusion {verdict.excluded_by}"
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

            base_url = self.source_config.environment.web_app_base_url
            if (
                self.source_config.app_url_pattern
                == PowerBiAppUrlPattern.REDIRECT_BASED
            ):
                app_url = f"{base_url}/Redirect?action=OpenApp&appId={workspace.app.id}"
            else:
                app_url = f"{base_url}/groups/{workspace.id}/apps/{workspace.app.id}"

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
                dashboardUrl=app_url,
                dashboards=assets_within_app,
            )

            # Browse path
            browse_path = BrowsePathsV2Class(
                path=[
                    BrowsePathEntryClass(
                        id=workspace.name,
                        urn=self.mapper.make_container_urn_for_workspace(workspace),
                    )
                ]
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

    def _workspace_workunits(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.get_phase1_workspace_workunits(workspace)
        try:
            self.powerbi_client.fill_regular_metadata_detail(workspace=workspace)
        except Exception as e:
            # In workspace_apis mode the rest of the workspace emits nothing
            # the run can fall back on, so a green run would let stale-entity
            # removal soft-delete all of its entities. admin_scan keeps its
            # long-standing warning, matching the Phase 1 handler.
            report_phase2 = (
                self.reporter.failure
                if self.source_config.metadata_api_mode
                == PowerBiMetadataApiMode.WORKSPACE_APIS
                else self.reporter.warning
            )
            report_phase2(
                title="Failed to Fetch Workspace Metadata",
                message="Phase 2 metadata fetch failed for this workspace; only its container and dataflows are emitted.",
                context=f"workspace={workspace.name} id={workspace.id}",
                exc=e,
            )
            return
        yield from self.get_workspace_workunit(workspace)

    def get_phase1_workspace_workunits(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        """Workunits that need nothing from Phase 2: the workspace container
        and its dataflows. Emitted even when Phase 2 fails, so a semantic
        model elsewhere can never point at a dataflow entity that was
        registered in Phase 1 but never emitted."""
        if self.source_config.extract_workspaces_to_containers:
            yield from self.mapper.generate_container_for_workspace(workspace)
        for dataflow in workspace.dataflows.values():
            yield from auto_workunit(
                self.mapper.to_datahub_dataflow(dataflow=dataflow, workspace=workspace)
            )

    def get_workspace_workunit(
        self, workspace: powerbi_data_classes.Workspace
    ) -> Iterable[MetadataWorkUnit]:
        yield from auto_workunit(self.emit_app(workspace=workspace))

        for dashboard in workspace.dashboards.values():
            try:
                # Increase dashboard and tiles count in report
                self.reporter.report_dashboards_scanned()
                self.reporter.report_charts_scanned(count=len(dashboard.tiles))
            except Exception as e:
                message = f"Error ({e}) occurred while loading dashboard {dashboard.displayName}(id={dashboard.id}) tiles."

                logger.exception(message, e)
                self.reporter.warning(dashboard.id, message, log=False)
            # Convert PowerBi Dashboard and child entities to Datahub work unit to ingest into Datahub
            workunits = self.mapper.to_datahub_work_units(dashboard, workspace)
            for workunit in workunits:
                wu = self._get_dashboard_patch_work_unit(workunit)
                if wu is not None:
                    yield wu

        for report in workspace.reports.values():
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

    def get_excluded_workunit_processors(self):
        # For modified_since, stale removal is handled per-workspace in get_workunits_internal()
        if self.source_config.modified_since:
            return [AutoStaleEntityRemovalProcessor]
        return []

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        # As modified_workspaces is not idempotent, workunit processors are run per workspace_id
        # to create a separate checkpoint for each workspace.
        if self.source_config.modified_since:
            return []  # All processing handled per-workspace in get_workunits_internal
        return super().get_workunit_processors()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Datahub Ingestion framework invokes this method
        """
        logger.info("PowerBi plugin execution is started")
        # Validate dataset type mapping
        self.validate_dataset_type_mapping()
        # Fetch PowerBi workspace for given workspace identifier

        allowed_workspaces = self.get_allowed_workspaces()
        workspace_ids: List[str] = [w.id for w in allowed_workspaces]

        # Workspaces (their raw scan_result payloads especially) are the dominant
        # memory consumer and must stay available between Phase 1 (scan) and
        # Phase 2 (per-workspace detail). Spilling them to a file-backed dict
        # instead of holding them all resident is what prevents OOMs on large
        # tenants. Cache size holds a scan batch plus buffer to avoid thrashing.
        ws_cache_size = min(max(self.source_config.scan_batch_size * 2, 10), 200)
        with FileBackedDict[powerbi_data_classes.Workspace](
            tablename="workspaces",
            cache_max_size=ws_cache_size,
            cache_eviction_batch_size=max(ws_cache_size // 2, 1),
        ) as workspaces:
            for workspace in allowed_workspaces:
                workspaces[workspace.id] = workspace
            del allowed_workspaces

            # Phase 1: fetch scan results per batch, filling out
            # self.powerbi_client.dataset_registry so cross-workspace references
            # resolve in Phase 2.
            excluded_workspace_ids: Set[str] = set()
            for batch_ids in more_itertools.chunked(
                workspace_ids, self.source_config.scan_batch_size
            ):
                batch_workspaces = [workspaces[wid] for wid in batch_ids]
                logger.info(
                    f"Fetching initial metadata for workspaces: {[w.format_name_for_logger() for w in batch_workspaces]}"
                )
                try:
                    if (
                        self.source_config.metadata_api_mode
                        == PowerBiMetadataApiMode.WORKSPACE_APIS
                    ):
                        excluded_workspace_ids.update(
                            self.powerbi_client.fill_metadata_from_definitions(
                                batch_workspaces
                            )
                        )
                    else:
                        excluded_workspace_ids.update(
                            self.powerbi_client.fill_metadata_from_scan_result(
                                batch_workspaces
                            )
                        )
                except Exception as e:
                    # Phase 1 must not abort the whole ingestion: an unhandled
                    # exception here would mean zero workunits emitted from any
                    # batch. Record one "Incomplete Scan Metadata" entry per
                    # affected workspace (the traceback rides along via exc=)
                    # so the gap is discoverable in the source report, then
                    # fall through to Phase 2 with empty scan_result. In
                    # workspace_apis mode the per-workspace endpoints are the
                    # only source of datasets, so this is a failure: a green
                    # run would let stale-entity removal soft-delete the tables.
                    report_phase1 = (
                        self.reporter.failure
                        if self.source_config.metadata_api_mode
                        == PowerBiMetadataApiMode.WORKSPACE_APIS
                        else self.reporter.warning
                    )
                    for ws in batch_workspaces:
                        report_phase1(
                            title="Incomplete Scan Metadata",
                            message=(
                                "Phase 1 (scan or definition fetch) failed for "
                                "this batch; the workspace will be ingested "
                                "without datasets, schema or lineage."
                            ),
                            context=f"workspace={ws.name} id={ws.id}",
                            exc=e,
                        )
                # fill_metadata_from_scan_result mutates the workspaces in place;
                # write them back or the mutations are lost on cache eviction.
                for ws in batch_workspaces:
                    workspaces[ws.id] = ws

            if self.source_config.item_definitions_available:
                self.powerbi_client.report_if_no_definitions_fetched()

            # Phase 2: fetch the rest of the metadata per workspace. We skip
            # workspaces the scan excluded (inactive or wrong type) to avoid
            # redundant per-workspace API calls.
            for workspace_id in workspace_ids:
                if workspace_id in excluded_workspace_ids:
                    continue
                workspace = workspaces[workspace_id]
                logger.info(f"Processing workspace id: {workspace.id}")

                if self.source_config.modified_since:
                    # Per-workspace checkpointing: each workspace gets its own checkpoint job_id.
                    # Because job_id is used as a dictionary key, we have to set a new job_id.
                    # Refer to https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/state/stateful_ingestion_base.py#L390
                    assert self.stale_entity_removal_handler is not None
                    self.stale_entity_removal_handler.set_job_id(workspace.id)
                    self.state_provider.register_stateful_ingestion_usecase_handler(
                        self.stale_entity_removal_handler
                    )

                    yield from self._apply_workunit_processors(
                        [
                            *super().get_workunit_processors(),
                            # stale_entity_removal is excluded from super() via get_excluded_workunit_processors(),
                            # so we apply it manually here for per-workspace checkpointing.
                            functools.partial(
                                auto_stale_entity_removal,
                                self.stale_entity_removal_handler,
                            ),
                        ],
                        self._workspace_workunits(workspace),
                    )
                else:
                    # Maintain backward compatibility
                    yield from self._workspace_workunits(workspace)

                # Fully emitted and never revisited; drop it to free its
                # scan_result and parsed children.
                del workspaces[workspace_id]

    def get_report(self) -> SourceReport:
        return self.reporter

    def close(self) -> None:
        # Best-effort temp-file cleanup must not skip the stateful-ingestion
        # commit in super().close(); a cleanup failure would otherwise corrupt
        # incremental state on the next run.
        try:
            self.powerbi_client.close()
        finally:
            super().close()
