import itertools
import json
import logging
from typing import ClassVar, Dict, Iterable, Iterator, List, Optional, Set, Type, Union

import requests

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import (
    get_fine_grained_lineage_key,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import WorkunitProcessor
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sap_datasphere.analytic_model import (
    extract_projection_source_columns,
    parse_business_layer,
)
from datahub.ingestion.source.sap_datasphere.client import SapDatasphereClient
from datahub.ingestion.source.sap_datasphere.config import (
    SapDatasphereConfig,
    SpaceContainerKey,
)
from datahub.ingestion.source.sap_datasphere.constants import (
    CATALOG_FIELD_HAS_PARAMETERS,
    CATALOG_FIELD_LABEL,
    CATALOG_FIELD_METADATA_URL,
    CATALOG_FIELD_NAME,
    CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES,
    CSN_ANN_AGGREGATION_DEFAULT,
    CSN_ANN_MEASURE_TYPE,
    CSN_KEY_BUSINESS_LAYER,
    CSN_KEY_DEFINITIONS,
    CSN_KEY_ELEMENTS,
    CSN_KEY_QUERY,
    CSN_KEY_SQL_EDITOR_QUERY,
    FIELD_TECHNICAL_NAME,
    GENERIC_SCHEME_PLATFORMS,
    MANAGED_CONNECTION_KEY,
    OBJECT_TYPE_ANALYTIC_MODELS,
    OBJECT_TYPE_DATA_FLOWS,
    OBJECT_TYPE_LOCAL_TABLES,
    OBJECT_TYPE_REMOTE_TABLES,
    OBJECT_TYPE_REPLICATION_FLOWS,
    OBJECT_TYPE_TASK_CHAINS,
    OBJECT_TYPE_TRANSFORMATION_FLOWS,
    OBJECT_TYPE_VIEWS,
    PLATFORM,
    PROP_EXPOSED_FOR_CONSUMPTION,
    PROP_LOCAL_TABLE,
    PROP_SAP_CALENDAR_TYPE,
    PROP_SAP_DATASPHERE_ASSET,
    PROP_SAP_DATASPHERE_SPACE,
    PROP_SAP_DIMENSION_TYPE,
    PROP_SAP_IS_DIMENSION,
    PROP_SAP_IS_MEASURE,
    PROP_SAP_SEMANTIC,
    PROP_SAP_VARIABLES,
    PROP_SPACE_NAME,
    PROP_VALUE_FALSE,
    PROP_VALUE_TRUE,
    SCALE_WARNING_URN_THRESHOLD,
    SCHEMA_FIELD_URN_PREFIX,
    SEMANTIC_CURRENCY,
    SEMANTIC_UNIT,
    VIEW_LANGUAGE_CSN,
    VIEW_LANGUAGE_SQL,
)
from datahub.ingestion.source.sap_datasphere.csn_parser import (
    parse_csn_elements_to_schema_fields,
)
from datahub.ingestion.source.sap_datasphere.edmx_parser import EdmxParser
from datahub.ingestion.source.sap_datasphere.flows import parse_flow
from datahub.ingestion.source.sap_datasphere.graph_resolver import (
    ExternalUrnGraphResolver,
)
from datahub.ingestion.source.sap_datasphere.lineage import (
    CsnLineageExtractor,
    is_qualified,
    parse_remote_table_source,
)
from datahub.ingestion.source.sap_datasphere.models import (
    AssetCsn,
    ColumnLineageContext,
    ColumnLineagePair,
    CsnSchemaResult,
    EdmxFetchReason,
    EdmxParseResult,
    FlowColumnMapping,
    FlowEndpoint,
    FlowTask,
    JsonDict,
    ParsedFlow,
    ResolvedPlatform,
    ResolveSkipReason,
    SourceColumnRef,
    TransformOp,
    UnknownColumnType,
    UpstreamRef,
    dedup_preserving_order,
)
from datahub.ingestion.source.sap_datasphere.platform_mapping import (
    PlatformMappingResolver,
)
from datahub.ingestion.source.sap_datasphere.report import SapDatasphereReport
from datahub.ingestion.source.sap_datasphere.tags import (
    DIMENSION_TAG_URN,
    MEASURE_TAG_URN,
    SAP_CALENDAR_TAG_URNS,
    SAP_CURRENCY_TAG_URN,
    SAP_UNIT_TAG_URN,
    get_predefined_tag_workunits,
    sap_dimension_type_tag_urn,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.workunit_processors.auto_lowercase_urns import (
    AutoLowercaseUrnsProcessor,
)
from datahub.metadata.schema_classes import (
    BrowsePathsV2Class,
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    NumberTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.container import Container
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

_FLOW_SCANNED_ATTR: Dict[str, str] = {
    OBJECT_TYPE_DATA_FLOWS: "data_flows_scanned",
    OBJECT_TYPE_REPLICATION_FLOWS: "replication_flows_scanned",
    OBJECT_TYPE_TRANSFORMATION_FLOWS: "transformation_flows_scanned",
    OBJECT_TYPE_TASK_CHAINS: "task_chains_scanned",
}
_FLOW_EMITTED_ATTR: Dict[str, str] = {
    OBJECT_TYPE_DATA_FLOWS: "data_flows_emitted",
    OBJECT_TYPE_REPLICATION_FLOWS: "replication_flows_emitted",
    OBJECT_TYPE_TRANSFORMATION_FLOWS: "transformation_flows_emitted",
    OBJECT_TYPE_TASK_CHAINS: "task_chains_emitted",
}


def _chunked(iterable: Iterable[Dict], size: int) -> Iterator[List[Dict]]:
    # Lazy chunking keeps peak memory bounded: only the current chunk is materialized.
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            return
        yield chunk


# Keeps a DataFlow and its tasks from sharing one label (a "Replication Flow"
# pipeline holds "Replication Task" jobs).
_JOB_SUBTYPE_BY_FLOW: Dict[DataFlowSubTypes, DataJobSubTypes] = {
    DataFlowSubTypes.SAP_DATA_FLOW: DataJobSubTypes.SAP_DATA_FLOW_TASK,
    DataFlowSubTypes.SAP_REPLICATION_FLOW: DataJobSubTypes.SAP_REPLICATION_TASK,
    DataFlowSubTypes.SAP_TRANSFORMATION_FLOW: DataJobSubTypes.SAP_TRANSFORMATION_TASK,
    DataFlowSubTypes.SAP_TASK_CHAIN: DataJobSubTypes.SAP_TASK_CHAIN_STEP,
}


@platform_name("SAP Datasphere")
@config_class(SapDatasphereConfig)
@support_status(SupportStatus.TESTING)
@capability(
    SourceCapability.TEST_CONNECTION, "Validates OAuth credentials and tenant URL"
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Per-connection platform_instance via connection_to_platform_map",
)
@capability(SourceCapability.CONTAINERS, "Spaces emitted as containers")
@capability(SourceCapability.SCHEMA_METADATA, "Columns from OData EDMX")
@capability(
    SourceCapability.DESCRIPTIONS,
    "Field descriptions from EDMX Common.Label annotations",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    (
        "Table-level lineage from CSN query refs and @remote.source annotations. "
        "Enable via `include_lineage: true`"
    ),
    supported=True,
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage from CSN columns[] expressions. "
    "Enable via `include_lineage: true`",
    supported=True,
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Soft-delete via stateful ingestion stale-entity removal",
    supported=True,
)
@capability(
    SourceCapability.TAGS,
    (
        "CDS semantic annotations (Dimension/Measure/Calendar/Currency/Unit/"
        "DimensionType) emitted as DataHub tags on the relevant schema fields "
        "and datasets. Toggle via `emit_sap_semantics_as_tags` (default True)."
    ),
    supported=True,
)
class SapDatasphereSource(StatefulIngestionSourceBase, TestableSource):
    config: SapDatasphereConfig
    report: SapDatasphereReport
    platform: ClassVar[str] = PLATFORM

    def __init__(self, ctx: PipelineContext, config: SapDatasphereConfig) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = SapDatasphereReport()
        self._client = SapDatasphereClient(config, report=self.report)
        self._lineage_extractor = CsnLineageExtractor()
        # Built lazily per space — each space has its own connections list.
        self._resolvers: Dict[str, PlatformMappingResolver] = {}
        self._scale_warning_emitted = False
        self._datasets_emitted = 0
        self._builtin_defaults_warning_emitted_for: Dict[str, bool] = {}
        self._sap_tags_emitted = False
        # Merged per target and emitted once at end of run: each flow emits a
        # *full* UpstreamLineage aspect, so without aggregation the last write
        # would drop earlier flows' edges when multiple flows share a target.
        self._flow_downstream_lineage: Dict[str, UpstreamLineageClass] = {}
        # Datasphere datasets we actually emitted. Writing an UpstreamLineage
        # aspect *materializes* its target, so a Datasphere flow target we never
        # scanned is skipped at flush time to avoid a bare phantom under its space.
        self._emitted_dataset_urns: Set[str] = set()
        # Lazily built when the first external flow endpoint needs graph
        # resolution; stays None when the feature is off or no graph is available.
        self._graph_resolver: Optional[ExternalUrnGraphResolver] = None
        self._graph_resolver_unavailable_warned = False
        # Source object (space-qualified name) -> {column: typed SchemaField}, used
        # to copy scalar types into analytic-model elements (which carry none). One
        # CSN fetch per distinct source object, reused across analytic models.
        self._source_field_type_cache: Dict[str, Dict[str, SchemaFieldClass]] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SapDatasphereSource":
        config = SapDatasphereConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_excluded_workunit_processors(
        self,
    ) -> List[Union[str, Type[WorkunitProcessor]]]:
        # This connector applies URN casing itself, per platform (see
        # _maybe_lower_external), so the global AutoLowercaseUrnsProcessor is
        # excluded: it would re-lowercase every URN and defeat the per-connection
        # override that lets e.g. BigQuery endpoints stitch to the BigQuery
        # connector. AutoIncrementalLineageProcessor is kept so incremental_lineage
        # can convert our full-aspect lineage MCPs into merge-not-overwrite patches.
        return [AutoLowercaseUrnsProcessor]

    def _safe_list_spaces(self) -> Iterator[Dict]:
        # Soften only transport errors. Auth/config failures raise ValueError and
        # must propagate: a total auth outage that "succeeded" with zero spaces
        # would let stateful ingestion soft-delete every prior entity.
        try:
            yield from self._client.list_spaces()
        except requests.RequestException as e:
            # Root enumeration: record a failure (not a warning) so a total
            # outage marks the run failed rather than a silent success with
            # nothing ingested. Per-space outages stay warnings.
            self.report.failure(
                title="Failed to list spaces",
                message=(
                    "Could not enumerate SAP Datasphere spaces; "
                    "no containers or assets will be emitted"
                ),
                context=str(e),
            )

    def _safe_list_assets(self, space_name: str) -> Iterator[Dict]:
        # Soften transport errors into a warning so one space's outage doesn't
        # abort the run.
        try:
            yield from self._client.list_assets(space_name)
        except requests.RequestException as e:
            self.report.warning(
                title="Failed to list assets in space",
                message=(
                    f"Could not enumerate assets in space {space_name}; "
                    f"datasets in this space will be missing from this run"
                ),
                context=str(e),
            )

    def _safe_list_objects(
        self, space_name: str, object_type: str, *, entity_label: str, impact: str
    ) -> Optional[List[JsonDict]]:
        # Returns None when the listing failed so a per-space outage warns once
        # and the caller skips the type rather than aborting the run.
        try:
            return list(self._client.list_objects(space_name, object_type))
        except requests.RequestException as e:
            self.report.warning(
                title=f"Failed to list {entity_label} in space",
                message=(
                    f"Could not enumerate {entity_label} in space {space_name}; "
                    f"{impact}"
                ),
                context=str(e),
            )
            return None

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Emit standalone Tag entities once per run so the predefined SAP tag URNs
        # get display names + descriptions in the UI.
        if self.config.emit_sap_semantics_as_tags and not self._sap_tags_emitted:
            yield from get_predefined_tag_workunits()
            self._sap_tags_emitted = True

        for space in self._safe_list_spaces():
            if not isinstance(space, dict) or not space.get(CATALOG_FIELD_NAME):
                self.report.warning(
                    title="Skipped malformed Datasphere space record",
                    message="Space record from catalog API is missing the 'name' field; skipping.",
                    context=str(space),
                )
                continue
            space_name: str = space[CATALOG_FIELD_NAME]
            space_label: str = space.get(CATALOG_FIELD_LABEL) or space_name
            self.report.spaces_scanned += 1
            if not self.config.space_pattern.allowed(space_name):
                self.report.spaces_filtered += 1
                continue

            # Warm the resolver cache serially AND outside the per-space handler
            # below. _get_resolver softens transport errors internally but lets an
            # auth/config ValueError propagate: on a connections-endpoint auth
            # outage every asset would otherwise resolve to unknown_connection and
            # let stateful ingestion soft-delete prior entities. Catching it in the
            # per-space `except` would silently mark such a run green, so the
            # warm-up must abort the whole run instead.
            self._get_resolver(space_name)

            try:
                yield from self._emit_space(space_name, space_label)
                yield from self._emit_assets_in_space(space_name)

                if self.config.include_local_tables:
                    yield from self._emit_local_tables_for_space(space_name)

                if self.config.include_remote_tables:
                    yield from self._emit_remote_tables_for_space(space_name)

                yield from self._emit_flows_for_space(space_name)
            except Exception as e:  # per-space isolation
                self.report.warning(
                    title="Failed to process Datasphere space",
                    message="Encountered unexpected error iterating space; continuing with next space.",
                    context=f"{type(e).__name__}: {e}",
                )
                continue

        # Flow targets can be shared across flows/spaces, so their lineage is
        # merged during the loop and emitted once here to avoid full-aspect
        # overwrites between flows writing the same target.
        yield from self._emit_flow_downstream_lineage()

    def _emit_assets_in_space(self, space_name: str) -> Iterable[MetadataWorkUnit]:
        def _emit_asset_with_isolation(
            asset: JsonDict,
        ) -> Iterable[MetadataWorkUnit]:
            asset_name = (
                asset.get(CATALOG_FIELD_NAME, "<unknown>")
                if isinstance(asset, dict)
                else "<unknown>"
            )
            try:
                yield from self._emit_asset(space_name, asset)
            except requests.RequestException as e:
                self.report.warning(
                    title="Failed to emit asset",
                    message="Skipped asset due to network error.",
                    context=f"{space_name}.{asset_name}: {e}",
                )
            except Exception as e:  # per-entity isolation
                self.report.warning(
                    title="Failed to emit Datasphere asset",
                    message="Skipped asset due to unexpected error.",
                    context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
                )

        if self.config.max_workers_assets > 1:
            # Bounded chunks cap peak memory at ~asset_batch_size live tasks
            # (ThreadedIteratorExecutor otherwise submits every task up front).
            for chunk in _chunked(
                self._safe_list_assets(space_name), self.config.asset_batch_size
            ):
                yield from ThreadedIteratorExecutor.process(
                    worker_func=_emit_asset_with_isolation,
                    args_list=((asset,) for asset in chunk),
                    max_workers=self.config.max_workers_assets,
                )
        else:
            for asset in self._safe_list_assets(space_name):
                yield from _emit_asset_with_isolation(asset)

    def _emit_local_tables_for_space(
        self, space_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Local Table stubs so views' phantom lineage edges become navigable nodes."""
        local_tables = self._safe_list_objects(
            space_name,
            OBJECT_TYPE_LOCAL_TABLES,
            entity_label="Local Tables",
            impact="phantom-lineage targets in this space will remain stubs.",
        )
        if not local_tables:
            return

        # Local Tables always live in the tenant's own HANA Cloud (_managed).
        resolved = self._resolve_managed_or_warn(
            space_name,
            "Local Tables",
            extra_hint=(
                " Configure connection_to_platform_map to enable Local Table discovery."
            ),
        )
        if resolved is None:
            return

        space_key = self._space_key(space_name)

        for technical_name in self._iter_allowed_technical_names(local_tables):
            yield from self._isolate(
                f"{space_name}.{OBJECT_TYPE_LOCAL_TABLES}.{technical_name}",
                self._emit_one_local_table(
                    space_name, technical_name, resolved, space_key
                ),
            )

    def _emit_one_local_table(
        self,
        space_name: str,
        technical_name: str,
        resolved: ResolvedPlatform,
        space_key: SpaceContainerKey,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = self._build_dataset_name(space_name, technical_name)

        # Schema fields on both sides (View + Local Table) let the UI draw
        # column-level lineage edges between them.
        schema_fields = None
        csn_obj = self._client.fetch_object_definition(
            space_name, OBJECT_TYPE_LOCAL_TABLES, technical_name
        )
        if csn_obj is not None:
            definition = self._csn_definition(csn_obj, technical_name)
            elements = (
                definition.get(CSN_KEY_ELEMENTS) if definition is not None else None
            )
            if isinstance(elements, dict):
                # Honor column_pattern here too, for consistency with views /
                # remote tables.
                schema_fields = self._schema_from_elements(
                    space_name, technical_name, elements
                )
            else:
                # 200 OK but not a parseable CSN shape — record it so a parse
                # miss isn't mistaken for a genuine no-schema base table.
                self._report_csn_unparseable(
                    self.report.assets_csn_unparseable,
                    space_name,
                    technical_name,
                    title="Unparseable Local Table CSN",
                    message=(
                        "Fetched the Local Table definition but it did not "
                        "contain a parseable elements map; emitting the table "
                        "without column schema."
                    ),
                )

        # Local Tables aren't exposed via the OData Consumption API, so SAP
        # provides no description; emitted only to close phantom-lineage gaps.
        dataset = Dataset(
            platform=resolved.platform,
            name=dataset_name,
            platform_instance=resolved.platform_instance,
            env=resolved.env,
            display_name=technical_name,
            subtype=DatasetSubTypes.SAP_LOCAL_TABLE,
            parent_container=space_key,
            custom_properties={
                PROP_SPACE_NAME: space_name,
                PROP_SAP_DATASPHERE_SPACE: space_name,
                PROP_SAP_DATASPHERE_ASSET: technical_name,
                PROP_EXPOSED_FOR_CONSUMPTION: PROP_VALUE_FALSE,
                PROP_LOCAL_TABLE: PROP_VALUE_TRUE,
            },
            schema=schema_fields,
        )
        self._emitted_dataset_urns.add(self._dataset_urn(resolved, dataset_name))
        yield from dataset.as_workunits()
        self.report.local_tables_emitted += 1

    def _enabled_flow_types(self) -> List[str]:
        types: List[str] = []
        if self.config.include_data_flows:
            types.append(OBJECT_TYPE_DATA_FLOWS)
        if self.config.include_replication_flows:
            types.append(OBJECT_TYPE_REPLICATION_FLOWS)
        if self.config.include_transformation_flows:
            types.append(OBJECT_TYPE_TRANSFORMATION_FLOWS)
        if self.config.include_task_chains:
            types.append(OBJECT_TYPE_TASK_CHAINS)
        return types

    def _emit_flows_for_space(self, space_name: str) -> Iterable[MetadataWorkUnit]:
        """Emit each enabled flow as a DataFlow plus one DataJob per target (a replication flow's N pairs become N tasks)."""
        flow_types = self._enabled_flow_types()
        if not flow_types:
            return

        for object_type in flow_types:
            entries = self._safe_list_objects(
                space_name,
                object_type,
                entity_label="flows",
                impact=f"{object_type} flow lineage from this space will be missing.",
            )
            if entries is None:
                continue
            for technical_name in self._iter_allowed_technical_names(entries):
                self._bump_report(_FLOW_SCANNED_ATTR[object_type])
                payload = self._client.fetch_flow_definition(
                    space_name, object_type, technical_name
                )
                if payload is None:
                    continue
                parsed = parse_flow(payload, object_type, technical_name)
                if parsed is None:
                    self.report.flows_unparseable.append(
                        f"{space_name}.{object_type}.{technical_name}"
                    )
                    continue
                if parsed.cll_suppressed_multi_input:
                    self.report.flow_column_lineage_suppressed_multi_input += 1
                if parsed.dropped_node_count:
                    self.report.flow_nodes_dropped += parsed.dropped_node_count
                dataflow = self._build_flow_dataflow(space_name, parsed)
                yield from dataflow.as_workunits()
                yield from self._isolate(
                    f"{space_name}.{object_type}.{technical_name}",
                    self._emit_flow_jobs(space_name, dataflow, parsed),
                )
                self._bump_report(_FLOW_EMITTED_ATTR[object_type])

    def _bump_report(self, attr: str) -> None:
        setattr(self.report, attr, getattr(self.report, attr) + 1)

    def _iter_allowed_technical_names(
        self, entries: Iterable[JsonDict]
    ) -> Iterator[str]:
        # Callers own their per-type scanned/emitted counters; this only skips
        # malformed records and applies asset_pattern.
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            technical_name = entry.get(FIELD_TECHNICAL_NAME)
            if not isinstance(technical_name, str) or not technical_name:
                continue
            if not self.config.asset_pattern.allowed(technical_name):
                continue
            yield technical_name

    def _resolve_managed_or_warn(
        self, space_name: str, entity_label: str, extra_hint: str = ""
    ) -> Optional[ResolvedPlatform]:
        # Local and Remote Tables both live in the tenant's managed HANA Cloud,
        # so both resolve the _managed connection and warn identically.
        result = self._get_resolver(space_name).resolve(MANAGED_CONNECTION_KEY)
        if result.platform is None:
            self.report.warning(
                title=f"Cannot emit {entity_label} — _managed connection unresolvable",
                message=(
                    f"Space {space_name} has {entity_label} but the _managed "
                    f"connection's platform mapping is missing/disabled "
                    f"(reason={result.skip_reason}).{extra_hint}"
                ),
            )
        return result.platform

    def _build_flow_dataflow(self, space_name: str, parsed: ParsedFlow) -> DataFlow:
        # Named <space>.<flow> for cross-space uniqueness.
        return DataFlow(
            platform=PLATFORM,
            name=self._maybe_lower(f"{space_name}.{parsed.technical_name}"),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=parsed.technical_name,
            subtype=parsed.subtype,
            parent_container=self._space_key(space_name),
        )

    def _emit_flow_jobs(
        self, space_name: str, dataflow: DataFlow, parsed: ParsedFlow
    ) -> Iterable[MetadataWorkUnit]:
        # Keyed separately by side: a replication task often maps a same-named
        # source to target (CUSTOMER -> CUSTOMER), so one name->urn map would let
        # the output clobber the input and corrupt per-task upstream pairing.
        input_urn_by_object: Dict[str, str] = {}
        output_urn_by_object: Dict[str, str] = {}
        inlets: List[Union[str, DatasetUrn]] = []
        for endpoint in parsed.inputs:
            urn = self._resolve_flow_endpoint_urn(space_name, endpoint)
            if urn is not None:
                input_urn_by_object[endpoint.object_name] = urn
                inlets.append(urn)
        for endpoint in parsed.outputs:
            urn = self._resolve_flow_endpoint_urn(space_name, endpoint)
            if urn is not None:
                output_urn_by_object[endpoint.object_name] = urn

        for task in self._build_flow_tasks(
            parsed, input_urn_by_object, output_urn_by_object, inlets
        ):
            job = DataJob(
                # Named after the target it produces (unique within the DataFlow).
                name=self._maybe_lower(task.target_object),
                flow=dataflow,
                display_name=task.target_object,
                subtype=_JOB_SUBTYPE_BY_FLOW[parsed.subtype],
                custom_properties={
                    PROP_SAP_DATASPHERE_SPACE: space_name,
                    PROP_SAP_DATASPHERE_ASSET: parsed.technical_name,
                },
                inlets=list(task.upstream_urns) or None,
                outlets=[task.target_urn],
                fine_grained_lineages=task.fine_grained or None,
            )
            yield from self._flow_scoped_job_workunits(job)

            # Also surface the lineage as a dataset-to-dataset edge so it shows on
            # the downstream's Lineage tab (the DataJob IO aspect only drives the
            # job's own view). Accumulated and merged since flows can share a
            # target and each emits a full UpstreamLineage aspect.
            if task.upstream_urns:
                self._merge_flow_downstream_lineage(
                    task.target_urn,
                    UpstreamLineageClass(
                        upstreams=[
                            UpstreamClass(
                                dataset=upstream_urn,
                                type=DatasetLineageTypeClass.TRANSFORMED,
                            )
                            for upstream_urn in task.upstream_urns
                        ],
                        fineGrainedLineages=task.fine_grained or None,
                    ),
                )

    @staticmethod
    def _flow_scoped_job_workunits(job: DataJob) -> Iterable[MetadataWorkUnit]:
        # browseV2 only resolves a browse-path segment to a display name when its
        # id is a URN, but the SDK keys the parent-flow segment on the flow id
        # ("<space>.<flow>"). Rewrite entity-backed segment ids to their URN so
        # the sidebar shows the flow's display name instead of the raw prefixed id.
        for wu in job.as_workunits():
            browse_path = wu.get_aspect_of_type(BrowsePathsV2Class)
            if browse_path is not None:
                for entry in browse_path.path:
                    if entry.urn and entry.id != entry.urn:
                        entry.id = entry.urn
            yield wu

    def _merge_flow_downstream_lineage(
        self, downstream_urn: str, lineage: UpstreamLineageClass
    ) -> None:
        existing = self._flow_downstream_lineage.get(downstream_urn)
        if existing is None:
            self._flow_downstream_lineage[downstream_urn] = lineage
            return

        seen_upstreams = {u.dataset for u in existing.upstreams}
        for upstream in lineage.upstreams:
            if upstream.dataset not in seen_upstreams:
                existing.upstreams.append(upstream)
                seen_upstreams.add(upstream.dataset)

        incoming_fine_grained = lineage.fineGrainedLineages or []
        if incoming_fine_grained:
            merged = list(existing.fineGrainedLineages or [])
            seen_keys = {get_fine_grained_lineage_key(f) for f in merged}
            for fine_grained in incoming_fine_grained:
                key = get_fine_grained_lineage_key(fine_grained)
                if key not in seen_keys:
                    merged.append(fine_grained)
                    seen_keys.add(key)
            existing.fineGrainedLineages = merged

    def _emit_flow_downstream_lineage(self) -> Iterable[MetadataWorkUnit]:
        """Emit merged flow lineage once per target, skipping Datasphere targets we never scanned (writing the aspect would materialize them as bare phantoms)."""
        for downstream_urn, lineage in self._flow_downstream_lineage.items():
            if self._is_unscanned_datasphere_target(downstream_urn):
                self.report.flow_targets_skipped_unscanned.append(downstream_urn)
                continue
            yield MetadataChangeProposalWrapper(
                entityUrn=downstream_urn, aspect=lineage
            ).as_workunit(is_primary_source=False)
        self._flow_downstream_lineage.clear()

    def _is_unscanned_datasphere_target(self, downstream_urn: str) -> bool:
        # Only a Datasphere target we never emitted would become a phantom;
        # external targets belong to another platform.
        return (
            DatasetUrn.from_string(downstream_urn).platform
            == make_data_platform_urn(PLATFORM)
            and downstream_urn not in self._emitted_dataset_urns
        )

    def _resolve_flow_endpoint_urn(
        self, space_name: str, endpoint: FlowEndpoint
    ) -> Optional[str]:
        resolver = self._get_resolver(space_name)
        if endpoint.is_local:
            resolved = resolver.resolve(MANAGED_CONNECTION_KEY).platform
            # A qualifiedName-derived object_name may already be "SPACE.OBJECT";
            # don't prefix the space twice. Only guards the current space — a
            # cross-space "OTHER_SPACE.OBJECT" would be mis-prefixed, but managed
            # objects are space-scoped so this isn't seen in practice.
            prefix = f"{space_name}."
            if endpoint.object_name.startswith(prefix):
                name = self._maybe_lower(endpoint.object_name)
            else:
                name = self._build_dataset_name(space_name, endpoint.object_name)
        else:
            resolved = resolver.resolve_external(
                endpoint.connection, endpoint.connection_type
            ).platform
            name = self._maybe_lower_external(
                resolved,
                self._qualify_external_name(
                    resolved, endpoint.container, endpoint.object_name
                ),
            )
        if resolved is None:
            self.report.flow_endpoints_unresolved.append(
                f"{space_name}.{endpoint.object_name} "
                f"(connection={endpoint.connection}, type={endpoint.connection_type})"
            )
            return None
        if not endpoint.is_local:
            name = self._graph_resolve_external_name(resolved, name)
        return self._dataset_urn(resolved, name)

    def _graph_resolve_external_name(
        self, resolved: ResolvedPlatform, name: str
    ) -> str:
        # Rewrite a logical external endpoint name to the real physical URN name
        # in the graph so the edge stitches. Best-effort: a miss keeps the candidate.
        graph_resolver = self._get_graph_resolver()
        if graph_resolver is None:
            return name
        real = graph_resolver.resolve_name(
            resolved.platform, resolved.platform_instance, resolved.env, name
        )
        if real is None:
            self.report.external_lineage_graph_unresolved.append(
                f"{resolved.platform}:{name}"
            )
            return name
        if real != name:
            self.report.external_lineage_graph_resolved += 1
        return real

    def _get_graph_resolver(self) -> Optional[ExternalUrnGraphResolver]:
        if not self.config.resolve_external_urns_via_graph:
            return None
        if self._graph_resolver is None:
            if self.ctx.graph is None:
                if not self._graph_resolver_unavailable_warned:
                    self.report.warning(
                        title="Graph unavailable for external URN resolution",
                        message=(
                            "resolve_external_urns_via_graph is enabled but no "
                            "DataHub graph is configured (needs a REST sink or a "
                            "datahub_api block); external flow URNs are left as their "
                            "raw candidate names."
                        ),
                    )
                    self._graph_resolver_unavailable_warned = True
                return None
            self._graph_resolver = ExternalUrnGraphResolver(self.ctx.graph, self.report)
        return self._graph_resolver

    def _build_flow_tasks(
        self,
        parsed: ParsedFlow,
        input_urn_by_object: Dict[str, str],
        output_urn_by_object: Dict[str, str],
        inlets: List[Union[str, DatasetUrn]],
    ) -> List[FlowTask]:
        """Split a flow into one target-anchored task per output, attributing each target's upstreams from its column mappings (falling back to paired/all inputs)."""
        mappings_by_downstream: Dict[str, List[FlowColumnMapping]] = {}
        for mapping in parsed.column_mappings:
            mappings_by_downstream.setdefault(mapping.downstream_object, []).append(
                mapping
            )

        # Per-target source pairing (replication flows only) so a pure-copy task's
        # target is never attributed to sibling tasks' sources.
        paired_upstreams_by_downstream: Dict[str, List[str]] = {}
        for table_mapping in parsed.table_mappings:
            upstream_urn = input_urn_by_object.get(table_mapping.upstream_object)
            paired_downstream_urn = output_urn_by_object.get(
                table_mapping.downstream_object
            )
            if upstream_urn is None or paired_downstream_urn is None:
                continue
            bucket = paired_upstreams_by_downstream.setdefault(
                paired_downstream_urn, []
            )
            if upstream_urn not in bucket:
                bucket.append(upstream_urn)

        tasks: List[FlowTask] = []
        for object_name, downstream_urn in output_urn_by_object.items():
            upstream_urns: List[str] = []
            fine_grained: List[FineGrainedLineageClass] = []
            for mapping in mappings_by_downstream.get(object_name, []):
                upstream_urn = input_urn_by_object.get(mapping.upstream_object)
                if upstream_urn is None:
                    continue
                if upstream_urn not in upstream_urns:
                    upstream_urns.append(upstream_urn)
                fine_grained.append(
                    self._field_edge(
                        upstream_urn,
                        mapping.upstream_col,
                        downstream_urn,
                        mapping.downstream_col,
                    )
                )
            if not upstream_urns:
                if parsed.table_mappings:
                    upstream_urns = list(
                        paired_upstreams_by_downstream.get(downstream_urn, [])
                    )
                else:
                    upstream_urns = [str(urn) for urn in inlets]
            # Guard against a target that also appears as its own input.
            upstream_urns = [urn for urn in upstream_urns if urn != downstream_urn]
            tasks.append(
                FlowTask(
                    target_object=object_name,
                    target_urn=downstream_urn,
                    upstream_urns=upstream_urns,
                    fine_grained=fine_grained,
                )
            )
        return tasks

    def _emit_remote_tables_for_space(
        self, space_name: str
    ) -> Iterable[MetadataWorkUnit]:
        entries = self._safe_list_objects(
            space_name,
            OBJECT_TYPE_REMOTE_TABLES,
            entity_label="Remote Tables",
            impact="federated lineage in this space will be missing.",
        )
        if not entries:
            return

        # The remote-table proxy object itself lives in the tenant's managed HANA.
        local_resolved = self._resolve_managed_or_warn(space_name, "Remote Tables")
        if local_resolved is None:
            return

        resolver = self._get_resolver(space_name)
        space_key = self._space_key(space_name)
        for technical_name in self._iter_allowed_technical_names(entries):
            self.report.remote_tables_scanned += 1
            yield from self._isolate(
                f"{space_name}.{OBJECT_TYPE_REMOTE_TABLES}.{technical_name}",
                self._emit_one_remote_table(
                    space_name, technical_name, local_resolved, resolver, space_key
                ),
            )

    def _emit_one_remote_table(
        self,
        space_name: str,
        technical_name: str,
        local_resolved: ResolvedPlatform,
        resolver: PlatformMappingResolver,
        space_key: SpaceContainerKey,
    ) -> Iterable[MetadataWorkUnit]:
        csn_obj = self._client.fetch_object_definition(
            space_name, OBJECT_TYPE_REMOTE_TABLES, technical_name
        )
        definition = self._csn_definition(csn_obj, technical_name)

        schema_fields = None
        upstreams_aspect: Optional[UpstreamLineageClass] = None
        downstream_urn = self._dataset_urn(
            local_resolved, self._build_dataset_name(space_name, technical_name)
        )
        if definition is not None:
            schema_fields = self._remote_table_schema(
                space_name, technical_name, definition
            )
            upstreams_aspect = self._remote_table_upstream(
                space_name,
                technical_name,
                definition,
                resolver,
                schema_fields,
                downstream_urn,
            )
        elif csn_obj is not None:
            # 200 OK but no CSN definition: federated lineage (the point of a
            # remote table) is silently absent, so record it rather than emit an
            # indistinguishable stub. (csn_obj is None means the fetch failed and
            # is already tracked.)
            self._report_csn_unparseable(
                self.report.remote_tables_csn_unparseable,
                space_name,
                technical_name,
                title="Remote Table emitted without schema or lineage",
                message=(
                    "Fetched the Remote Table definition but it contained no "
                    "parseable CSN definition for this table; emitting a bare "
                    "stub with neither column schema nor federated upstream "
                    "lineage."
                ),
            )

        dataset = Dataset(
            platform=local_resolved.platform,
            name=self._build_dataset_name(space_name, technical_name),
            platform_instance=local_resolved.platform_instance,
            env=local_resolved.env,
            display_name=technical_name,
            subtype=DatasetSubTypes.SAP_REMOTE_TABLE,
            parent_container=space_key,
            custom_properties={
                PROP_SPACE_NAME: space_name,
                PROP_SAP_DATASPHERE_SPACE: space_name,
                PROP_SAP_DATASPHERE_ASSET: technical_name,
            },
            schema=schema_fields,
            upstreams=upstreams_aspect,
        )
        self._emitted_dataset_urns.add(downstream_urn)
        yield from dataset.as_workunits()
        self.report.remote_tables_emitted += 1

    def _csn_definition(
        self, csn_obj: Optional[JsonDict], name: str
    ) -> Optional[JsonDict]:
        if not isinstance(csn_obj, dict):
            return None
        defs = csn_obj.get(CSN_KEY_DEFINITIONS)
        definition = defs.get(name) if isinstance(defs, dict) else None
        return definition if isinstance(definition, dict) else None

    def _schema_from_elements(
        self, space_name: str, technical_name: str, elements: JsonDict
    ) -> Optional[List[SchemaFieldClass]]:
        csn_schema = parse_csn_elements_to_schema_fields(elements)
        self._report_csn_parse_concerns(space_name, technical_name, csn_schema)
        return self._apply_column_pattern(csn_schema.fields) or None

    def _report_csn_unparseable(
        self,
        bucket: LossyList[str],
        space_name: str,
        name: str,
        *,
        title: str,
        message: str,
    ) -> None:
        # Record a parse miss so it isn't mistaken for a genuine no-schema asset.
        bucket.append(f"{space_name}.{name}")
        self.report.warning(
            title=title, message=message, context=f"{space_name}.{name}"
        )

    def _remote_table_schema(
        self, space_name: str, technical_name: str, definition: JsonDict
    ) -> Optional[List[SchemaFieldClass]]:
        elements = definition.get(CSN_KEY_ELEMENTS)
        if not isinstance(elements, dict) or not elements:
            return None
        return self._schema_from_elements(space_name, technical_name, elements)

    def _remote_table_upstream(
        self,
        space_name: str,
        technical_name: str,
        definition: JsonDict,
        resolver: PlatformMappingResolver,
        schema_fields: Optional[List[SchemaFieldClass]],
        downstream_urn: str,
    ) -> Optional[UpstreamLineageClass]:
        remote = parse_remote_table_source(definition)
        if remote is None:
            # CSN parsed but had no @DataWarehouse.remote.* annotations, so no
            # federated origin can be derived; record rather than silently emit a
            # lineage-less stub.
            self.report.remote_tables_missing_remote_annotation.append(
                f"{space_name}.{technical_name}"
            )
            self.report.warning(
                title="Remote Table has no federated source annotation",
                message=(
                    "The Remote Table CSN parsed but carried no "
                    "@DataWarehouse.remote.* annotations, so no federated upstream "
                    "lineage could be derived; emitting without upstream lineage."
                ),
                context=f"{space_name}.{technical_name}",
            )
            return None
        resolved = resolver.resolve_external(remote.connection, None).platform
        if resolved is None:
            self.report.remote_table_source_unresolved.append(
                f"{space_name}.{technical_name} (connection={remote.connection})"
            )
            return None
        upstream_urn = self._dataset_urn(
            resolved, self._maybe_lower_external(resolved, remote.qualified_name)
        )
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.COPY,
                )
            ],
            fineGrainedLineages=self._remote_table_fine_grained(
                resolved, upstream_urn, downstream_urn, schema_fields
            )
            or None,
        )

    def _remote_table_fine_grained(
        self,
        resolved: ResolvedPlatform,
        upstream_urn: str,
        downstream_urn: str,
        schema_fields: Optional[List[SchemaFieldClass]],
    ) -> List[FineGrainedLineageClass]:
        # A Remote Table mirrors its external table 1:1 by column name. The
        # upstream column takes the same per-platform casing so it stitches to the
        # native connector's schema-field URNs.
        if not schema_fields:
            return []
        fine_grained: List[FineGrainedLineageClass] = []
        for field in schema_fields:
            fine_grained.append(
                self._field_edge(
                    upstream_urn,
                    self._maybe_lower_external(resolved, field.fieldPath),
                    downstream_urn,
                    field.fieldPath,
                )
            )
        return fine_grained

    def _maybe_lower(self, name: str) -> str:
        return name.lower() if self.config.convert_urns_to_lowercase else name

    def _maybe_lower_external(
        self, resolved: Optional[ResolvedPlatform], name: str
    ) -> str:
        # External URNs must match the case the sibling native connector emits,
        # which differs per platform, so the resolved mapping's per-platform casing
        # decides this independent of the top-level flag (used only as fallback).
        lowercase = (
            resolved.convert_urns_to_lowercase
            if resolved is not None
            else self.config.convert_urns_to_lowercase
        )
        return name.lower() if lowercase else name

    def _qualify_external_name(
        self,
        resolved: Optional[ResolvedPlatform],
        container: Optional[str],
        object_name: str,
    ) -> str:
        # A flow endpoint object arrives bare; prepend an optional configured
        # `database` (e.g. the BigQuery project the API omits) then the container
        # schema so the URN matches the sibling connector's
        # `[database.]schema.table` naming. Missing segments are skipped.
        parts: List[str] = []
        if resolved is not None and resolved.database:
            parts.append(resolved.database)
        schema = (container or "").strip("/")
        if schema:
            parts.append(schema)
        parts.append(object_name)
        return ".".join(parts)

    @staticmethod
    def _dataset_urn(resolved: ResolvedPlatform, name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=resolved.platform,
            name=name,
            platform_instance=resolved.platform_instance,
            env=resolved.env,
        )

    @staticmethod
    def _field_edge(
        upstream_urn: str,
        upstream_col: str,
        downstream_urn: str,
        downstream_col: str,
        transform_op: Optional[TransformOp] = None,
    ) -> FineGrainedLineageClass:
        return FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[make_schema_field_urn(upstream_urn, upstream_col)],
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[make_schema_field_urn(downstream_urn, downstream_col)],
            transformOperation=transform_op,
        )

    def _isolate(
        self, label: str, workunits: Iterable[MetadataWorkUnit]
    ) -> Iterable[MetadataWorkUnit]:
        # Per-item guard so one bad object is reported and skipped instead of
        # aborting the whole space.
        try:
            yield from workunits
        except requests.RequestException as e:
            self.report.warning(
                title="Failed to emit object",
                message="Skipped object due to network error.",
                context=f"{label}: {e}",
            )
        except Exception as e:
            self.report.warning(
                title="Failed to emit object",
                message="Skipped object due to unexpected error.",
                context=f"{label}: {type(e).__name__}: {e}",
            )

    def _space_key(self, space_name: str) -> SpaceContainerKey:
        return SpaceContainerKey(
            platform=PLATFORM,
            instance=self.config.platform_instance,
            space=self._maybe_lower(space_name),
        )

    def _get_resolver(self, space: str) -> PlatformMappingResolver:
        if space not in self._resolvers:
            try:
                connections = self._client.list_connections(space)
            except requests.RequestException as e:
                # Soften only transport errors. An auth/config failure raises
                # ValueError and must propagate: swallowing it would resolve every
                # asset to unknown_connection and let stateful ingestion
                # soft-delete prior entities on a pure auth outage.
                self.report.warning(
                    title="Failed to fetch Datasphere connections",
                    message=(
                        f"Could not list connections for space {space}; "
                        f"federated assets in this space will not resolve correctly"
                    ),
                    context=str(e),
                )
                connections = []
            # Literal keys required: TypedDict.get only type-narrows with a literal.
            by_name = {c.get("name", ""): c for c in connections if c.get("name")}
            self._resolvers[space] = PlatformMappingResolver(
                self.config, by_name, report=self.report
            )
        return self._resolvers[space]

    def _emit_space(
        self, space_name: str, space_label: str
    ) -> Iterable[MetadataWorkUnit]:
        # The stale-entity workunit_processor auto-adds every primary workunit to
        # state, so we must not add URNs manually (double-count + races with
        # parallel asset workers).
        key = self._space_key(space_name)
        # SAP provides only the space label; no description is available.
        container = Container(
            key,
            display_name=space_label,
            subtype=DatasetContainerSubTypes.SAP_DATASPHERE_SPACE,
        )
        yield from container.as_workunits()

    def _record_resolve_skip(
        self,
        space_name: str,
        asset_name: str,
        connection_name: Optional[str],
        skip_reason: Optional[ResolveSkipReason],
    ) -> None:
        qualified = f"{space_name}.{asset_name} (connection={connection_name})"
        if skip_reason == ResolveSkipReason.UNKNOWN_CONNECTION:
            self.report.assets_skipped_unknown_connection.append(qualified)
        elif skip_reason == ResolveSkipReason.DISABLED:
            self.report.assets_skipped_disabled.append(qualified)
        else:
            # UNKNOWN_TYPEID + a catch-all so an asset is never dropped silently.
            self.report.assets_skipped_unknown_typeid.append(qualified)

    def _base_asset_custom_properties(
        self, space_name: str, asset_name: str, asset: JsonDict, metadata_url: str
    ) -> Dict[str, str]:
        # entity_custom_props from the EDMX parse are merged on top by the caller.
        return {
            PROP_SPACE_NAME: space_name,
            CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES: str(
                asset.get(CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES, False)
            ).lower(),
            CATALOG_FIELD_HAS_PARAMETERS: str(
                asset.get(CATALOG_FIELD_HAS_PARAMETERS, False)
            ).lower(),
            PROP_EXPOSED_FOR_CONSUMPTION: str(bool(metadata_url)).lower(),
            PROP_SAP_DATASPHERE_SPACE: space_name,
            PROP_SAP_DATASPHERE_ASSET: asset_name,
        }

    def _resolve_asset_schema_fields(
        self,
        space_name: str,
        asset_name: str,
        parse_result: Optional[EdmxParseResult],
        csn_def: Optional[JsonDict],
    ) -> Optional[List[SchemaFieldClass]]:
        # Prefer the relational EDMX schema; fall back to the CSN elements map for
        # analytic models, which expose no relational metadata URL for EDMX.
        if parse_result is not None and parse_result.fields:
            self.report.assets_schema_fetched += 1
            return self._decorate_fields(parse_result)
        if csn_def is not None:
            return self._schema_fields_from_csn(space_name, asset_name, csn_def)
        return None

    def _fetch_asset_csn(
        self, space_name: str, asset: JsonDict, asset_name: str
    ) -> AssetCsn:
        # Fetch the View / Analytic Model CSN (for lineage, view definitions, or
        # @remote.source detection) and resolve the routing connection.
        csn_obj: Optional[JsonDict] = None
        csn_def: Optional[JsonDict] = None
        if self.config.include_lineage or self.config.include_view_definitions:
            object_type = (
                OBJECT_TYPE_ANALYTIC_MODELS
                if asset.get(CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES)
                else OBJECT_TYPE_VIEWS
            )
            csn_obj = self._client.fetch_object_definition(
                space_name, object_type, asset_name
            )
            if csn_obj is not None:
                csn_def = self._csn_definition(csn_obj, asset_name)
                if csn_def is None:
                    # 200 OK but no parseable definition. An analytic model has no
                    # OData $metadata fallback, so record the miss rather than let
                    # a healthy scanned==emitted count hide a schema-less,
                    # lineage-less stub.
                    self._report_csn_unparseable(
                        self.report.assets_csn_unparseable,
                        space_name,
                        asset_name,
                        title="Unparseable asset CSN",
                        message=(
                            "Fetched the object definition but it contained no "
                            "parseable definition for this asset; emitting without "
                            "CSN-derived schema or lineage."
                        ),
                    )

        # Default to the managed HANA connection; override if CSN declares a
        # federated remote source.
        connection_name = MANAGED_CONNECTION_KEY
        if csn_def is not None:
            try:
                remote = self._lineage_extractor.remote_source(csn_def)
            except Exception as e:
                self.report.warning(
                    title="Failed to read CSN @remote.source",
                    message=(
                        "Could not determine remote source from CSN; defaulting to "
                        "the managed HANA connection. Federated routing for this "
                        "asset may be wrong."
                    ),
                    context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
                )
                remote = None
            if remote:
                connection_name = remote

        return AssetCsn(
            connection_name=connection_name, csn_obj=csn_obj, csn_def=csn_def
        )

    def _emit_asset(
        self, space_name: str, asset: JsonDict
    ) -> Iterable[MetadataWorkUnit]:
        asset_name_opt = asset.get(CATALOG_FIELD_NAME)
        if not asset_name_opt:
            # A record missing 'name' is a data problem; report it specifically
            # rather than letting a KeyError surface as the generic warning.
            self.report.warning(
                title="Skipped malformed Datasphere asset record",
                message="Asset record from catalog API is missing the 'name' field; skipping.",
                context=f"space={space_name}, record={asset}",
            )
            return
        asset_name: str = asset_name_opt
        asset_label: str = asset.get(CATALOG_FIELD_LABEL) or asset_name
        self.report.assets_scanned += 1

        if not self.config.asset_pattern.allowed(asset_name):
            self.report.assets_filtered += 1
            return

        metadata_url: str = asset.get(CATALOG_FIELD_METADATA_URL) or ""
        if self.config.expose_for_consumption_only and not metadata_url:
            self.report.assets_filtered += 1
            return

        asset_csn = self._fetch_asset_csn(space_name, asset, asset_name)
        csn_obj = asset_csn.csn_obj
        csn_def = asset_csn.csn_def
        connection_name = asset_csn.connection_name

        resolver = self._get_resolver(space_name)
        result = resolver.resolve(connection_name)
        resolved = result.platform
        skip_reason = result.skip_reason
        if resolved is None:
            self._record_resolve_skip(
                space_name, asset_name, connection_name, skip_reason
            )
            return

        self._maybe_warn_builtin_defaults_missing_instance(resolved)

        parse_result: Optional[EdmxParseResult] = None
        if metadata_url:
            parse_result = self._parse_schema(space_name, asset_name, metadata_url)

        description: Optional[str] = None
        custom_properties = self._base_asset_custom_properties(
            space_name, asset_name, asset, metadata_url
        )
        if parse_result is not None:
            if parse_result.entity_label:
                description = parse_result.entity_label
            custom_properties.update(parse_result.entity_custom_props)

        if asset.get(CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES):
            sub_type: str = DatasetSubTypes.SAP_ANALYTICAL_MODEL
        else:
            sub_type = DatasetSubTypes.VIEW

        schema_fields = self._resolve_asset_schema_fields(
            space_name, asset_name, parse_result, csn_def
        )

        dataset_name = self._build_dataset_name(space_name, asset_name)

        upstreams_aspect: Optional[UpstreamLineageClass] = None
        if csn_def is not None and self.config.include_lineage:
            # Guard non-walker failures (URN construction, aspect assembly) so a
            # single bad asset doesn't crash the emit.
            try:
                upstreams_aspect = self._extract_lineage_aspect(
                    csn_def, resolved, space_name, asset_name, dataset_name
                )
            except Exception as e:
                self.report.warning(
                    title="Failed to build lineage aspect",
                    message=(
                        "Could not assemble UpstreamLineage; the dataset will be "
                        "emitted without lineage."
                    ),
                    context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
                )
                upstreams_aspect = None

        # Analytic-model star-schema lineage + measure/dimension tags + variables
        # (no-op for plain views, which have no businessLayerDefinitions).
        upstreams_aspect = self._apply_business_layer_guarded(
            csn_obj,
            asset_name,
            schema_fields,
            custom_properties,
            upstreams_aspect,
            space_name,
        )

        view_properties = self._build_view_properties(csn_def)

        # 2-tier model: parent directly to the Space container; the object kind
        # survives as the dataset subtype (a UI filter facet).
        dataset_parent: ContainerKey = self._space_key(space_name)

        dataset_tags = self._entity_tag_urns(custom_properties)

        dataset = Dataset(
            platform=resolved.platform,
            name=dataset_name,
            platform_instance=resolved.platform_instance,
            env=resolved.env,
            description=description,
            display_name=asset_label,
            custom_properties=custom_properties,
            parent_container=dataset_parent,
            subtype=sub_type,
            schema=schema_fields,
            tags=dataset_tags or None,
            upstreams=upstreams_aspect,
        )
        dataset_urn = self._dataset_urn(resolved, dataset_name)
        self._emitted_dataset_urns.add(dataset_urn)
        yield from dataset.as_workunits()
        if view_properties is not None:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties,
            ).as_workunit()
        # State is added by the workunit_processor (see _emit_space). The counter
        # isn't strictly atomic under threading but is fine for a coarse threshold.
        self._datasets_emitted += 1
        self._check_scale_warning()

    def _build_view_properties(
        self, csn_def: Optional[JsonDict]
    ) -> Optional[ViewPropertiesClass]:
        # SQL views store the modeler's raw SQL in @DataWarehouse.sqlEditor.query;
        # that wins. Graphical/modeled views instead emit their CSN/CQN query tree.
        if not self.config.include_view_definitions or csn_def is None:
            return None
        sql = csn_def.get(CSN_KEY_SQL_EDITOR_QUERY)
        if isinstance(sql, str) and sql.strip():
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=sql,
                viewLanguage=VIEW_LANGUAGE_SQL,
            )
        query = csn_def.get(CSN_KEY_QUERY)
        if isinstance(query, dict):
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=json.dumps(query, indent=2, sort_keys=False),
                viewLanguage=VIEW_LANGUAGE_CSN,
            )
        return None

    def _build_dataset_name(
        self,
        space_name: str,
        asset_name: str,
    ) -> str:
        name = f"{space_name}.{asset_name}"
        return self._maybe_lower(name)

    def _qualified_upstream_urn(self, qualified_key: str) -> str:
        # The key already carries its own space, so use it as the URN name
        # directly — re-prefixing would corrupt cross-space references.
        name = self._maybe_lower(qualified_key)
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def _business_layer_upstream_urn(self, space_name: str, key: str) -> str:
        # Bare same-space BL keys need the AM's space; dotted keys are already
        # space-qualified (same heuristic as query-FROM lineage).
        return self._qualified_upstream_urn(
            key if is_qualified(key) else f"{space_name}.{key}"
        )

    def _apply_business_layer_guarded(
        self,
        csn_obj: Optional[Dict],
        technical_name: str,
        schema_fields: Optional[List[SchemaFieldClass]],
        custom_properties: Dict[str, str],
        query_upstreams: Optional[UpstreamLineageClass],
        space_name: str,
    ) -> Optional[UpstreamLineageClass]:
        # Degrade a star-schema assembly failure to the already-computed
        # query_upstreams rather than dropping the whole analytic-model dataset.
        try:
            return self._apply_business_layer(
                csn_obj,
                technical_name,
                schema_fields,
                custom_properties,
                query_upstreams,
                space_name,
            )
        except Exception as e:
            self.report.warning(
                title="Failed to apply analytic-model business layer",
                message=(
                    "Could not assemble star-schema lineage/measure tags; emitting "
                    "with prior lineage only."
                ),
                context=f"{space_name}.{technical_name}: {type(e).__name__}: {e}",
            )
            return query_upstreams

    def _apply_business_layer(
        self,
        csn_obj: Optional[Dict],
        technical_name: str,
        schema_fields: Optional[List[SchemaFieldClass]],
        custom_properties: Dict[str, str],
        query_upstreams: Optional[UpstreamLineageClass],
        space_name: str,
    ) -> Optional[UpstreamLineageClass]:
        """Wire an analytic model's businessLayerDefinitions in as the authoritative table-level lineage, replacing the query-FROM upstreams (which may double-prefix the fact's space)."""
        bld = (csn_obj or {}).get(CSN_KEY_BUSINESS_LAYER)
        if not isinstance(bld, dict):
            return query_upstreams
        bl = parse_business_layer(bld, technical_name)

        if bl.variable_names:
            custom_properties[PROP_SAP_VARIABLES] = ",".join(bl.variable_names)

        # Cube elements usually lack @Analytics flags, so the business layer is
        # the primary measure/dimension signal.
        if self.config.emit_sap_semantics_as_tags and schema_fields:
            measures = set(bl.measure_names)
            attributes = set(bl.attribute_names)
            for field in schema_fields:
                if field.fieldPath in measures:
                    self._add_field_tag(field, MEASURE_TAG_URN)
                if field.fieldPath in attributes:
                    self._add_field_tag(field, DIMENSION_TAG_URN)

        if not bl.upstream_keys:
            return query_upstreams

        # One URN per key — shared by table upstreams and the FGL retention filter.
        bl_upstream_urns = dedup_preserving_order(
            self._business_layer_upstream_urn(space_name, key)
            for key in bl.upstream_keys
        )
        upstreams = [
            UpstreamClass(dataset=urn, type=DatasetLineageTypeClass.VIEW)
            for urn in bl_upstream_urns
        ]

        fine_grained = None
        if query_upstreams is not None and query_upstreams.fineGrainedLineages:
            kept = [
                fgl
                for fgl in query_upstreams.fineGrainedLineages
                if fgl.upstreams
                and all(
                    self._schema_field_parent(u) in bl_upstream_urns
                    for u in fgl.upstreams
                )
            ]
            fine_grained = kept or None

        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained,
        )

    @staticmethod
    def _add_field_tag(field: SchemaFieldClass, tag_urn: str) -> None:
        if field.globalTags is None:
            field.globalTags = GlobalTagsClass(tags=[])
        existing = {t.tag for t in field.globalTags.tags}
        if tag_urn not in existing:
            field.globalTags.tags.append(TagAssociationClass(tag=tag_urn))

    @staticmethod
    def _schema_field_parent(schema_field_urn: str) -> str:
        prefix = SCHEMA_FIELD_URN_PREFIX
        if schema_field_urn.startswith(prefix):
            inner = schema_field_urn[len(prefix) : schema_field_urn.rfind(")")]
            return inner.rsplit(",", 1)[0]
        return schema_field_urn

    def _extract_lineage_aspect(
        self,
        csn_def: dict,
        resolved: ResolvedPlatform,
        space_name: str,
        asset_name: str,
        dataset_name: str,
    ) -> Optional[UpstreamLineageClass]:
        # Each extractor is guarded independently so a parsing failure on one
        # side (table-level vs column-level) still allows the other to emit.
        upstream_refs: List[UpstreamRef] = []
        column_pairs: List[ColumnLineagePair] = []
        try:
            upstream_refs = self._lineage_extractor.extract_upstream_refs(csn_def)
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN lineage",
                message=(
                    "Could not parse upstream references from CSN; the dataset "
                    "will be emitted without upstreamLineage."
                ),
                context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
            )
        try:
            association_targets = self._lineage_extractor.extract_association_targets(
                csn_def
            )
            if association_targets:
                self.report.association_upstreams_emitted += len(association_targets)
                # A ref in both FROM and an association resolves to the same URN,
                # so dedup by name (qualified refs already in upstream_refs win).
                existing = {ref.name for ref in upstream_refs}
                upstream_refs.extend(
                    ref for ref in association_targets if ref.name not in existing
                )
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN association lineage",
                message=(
                    "Could not parse association-based lineage from CSN; the "
                    "dataset will be emitted without association upstreams."
                ),
                context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
            )
        try:
            column_pairs = self._lineage_extractor.extract_column_lineage(csn_def)
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN column lineage",
                message=(
                    "Could not parse column-level lineage from CSN; the dataset "
                    "will be emitted with table-level lineage only."
                ),
                context=f"{space_name}.{asset_name}: {type(e).__name__}: {e}",
            )
        if not upstream_refs and not column_pairs:
            return None
        downstream_dataset_urn = self._dataset_urn(resolved, dataset_name)
        column_lineage = (
            ColumnLineageContext(
                pairs=column_pairs,
                downstream_dataset_urn=downstream_dataset_urn,
            )
            if column_pairs
            else None
        )
        return self._build_upstream_lineage(
            resolved,
            space_name,
            upstream_refs,
            column_lineage=column_lineage,
        )

    def _build_upstream_lineage(
        self,
        resolved: ResolvedPlatform,
        space_name: str,
        upstream_refs: List[UpstreamRef],
        column_lineage: Optional[ColumnLineageContext] = None,
    ) -> Optional[UpstreamLineageClass]:
        # Intra-Datasphere lineage is emitted under the same resolved platform so
        # all URNs in the lineage graph are consistent.
        upstreams: List[UpstreamClass] = []
        upstream_urn_by_name: Dict[str, str] = {}
        for ref in upstream_refs:
            if ref.qualified:
                # Already space-qualified (cross-space or built-in association
                # target): use as-is on the sap-datasphere platform.
                upstream_urn = self._qualified_upstream_urn(ref.name)
            else:
                upstream_name = self._maybe_lower(f"{space_name}.{ref.name}")
                upstream_urn = self._dataset_urn(resolved, upstream_name)
            upstream_urn_by_name[ref.name] = upstream_urn
            upstreams.append(
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.VIEW,
                )
            )

        fine_grained: List[FineGrainedLineageClass] = (
            self._build_fine_grained_lineages(column_lineage, upstream_urn_by_name)
            if column_lineage is not None
            else []
        )

        if not upstreams and not fine_grained:
            return None
        return UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained or None,
        )

    def _build_fine_grained_lineages(
        self,
        column_lineage: ColumnLineageContext,
        upstream_urn_by_name: Dict[str, str],
    ) -> List[FineGrainedLineageClass]:
        fine_grained: List[FineGrainedLineageClass] = []
        downstream_dataset_urn = column_lineage.downstream_dataset_urn
        for pair in column_lineage.pairs:
            # Surface walker-level unresolvable refs so operators can debug
            # silent drops.
            for unresolved_ref in pair.unresolved_refs:
                self.report.column_lineage_unresolved.append(
                    f"{downstream_dataset_urn}#{pair.downstream_col}: {unresolved_ref}"
                )
            upstream_field_urns: List[str] = []
            for ref in pair.upstream_refs:
                if ref.col == "*":
                    self.report.column_lineage_unresolved.append(
                        f"{downstream_dataset_urn}#{pair.downstream_col}: "
                        f"<wildcard upstream {ref.qname}.*>"
                    )
                    continue
                resolved_upstream_urn = upstream_urn_by_name.get(ref.qname)
                if resolved_upstream_urn is None:
                    self.report.column_lineage_unresolved.append(
                        f"{downstream_dataset_urn}#{pair.downstream_col}: "
                        f"<missing upstream qname {ref.qname!r} "
                        f"for col {ref.col!r}>"
                    )
                    continue
                upstream_field_urns.append(
                    make_schema_field_urn(resolved_upstream_urn, ref.col)
                )
            if not upstream_field_urns:
                continue
            downstream_field_urn = make_schema_field_urn(
                downstream_dataset_urn, pair.downstream_col
            )
            fine_grained.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=upstream_field_urns,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[downstream_field_urn],
                    transformOperation=pair.transform_op,
                )
            )
        return fine_grained

    def _parse_schema(
        self, space_name: str, asset_name: str, metadata_url: str
    ) -> Optional[EdmxParseResult]:
        fetch = self._client.fetch_edmx(metadata_url)
        if fetch.reason is EdmxFetchReason.NOT_FOUND:
            # Benign: the asset is legitimately not exposed for OData consumption,
            # so skip quietly (no warning, not tracked as a failure).
            logger.debug(
                "Asset %s.%s is not exposed for OData consumption (HTTP 404); "
                "emitting without EDMX-derived schema.",
                space_name,
                asset_name,
            )
            return None
        if fetch.reason is EdmxFetchReason.FORBIDDEN:
            # fetch_edmx already warned about permissions; don't double-warn, but
            # still track the asset's missing schema for the report summary.
            self.report.assets_schema_failed.append(asset_name)
            return None
        if fetch.reason is EdmxFetchReason.ERROR or fetch.xml is None:
            self.report.assets_schema_failed.append(asset_name)
            self.report.warning(
                title="EDMX schema fetch failed",
                message="Could not fetch schema metadata for asset.",
                context=f"{asset_name}: {metadata_url}",
            )
            return None
        result = EdmxParser.parse(fetch.xml)
        if result.error is not None:
            self.report.assets_schema_failed.append(asset_name)
            self.report.warning(
                title="EDMX schema parse failed",
                message="Could not parse schema metadata for asset.",
                context=f"{asset_name}: {result.error} ({metadata_url})",
            )
            return None
        if result.unknown_edm_types:
            self.report.warning(
                title="Unknown EDMX field type(s)",
                message=(
                    "Field(s) have EDMX types not in the connector's _EDM_TYPE_MAP; "
                    "schema for those columns will use NullType. Consider adding the "
                    "type to the connector."
                ),
                context=(
                    f"{space_name}.{asset_name}: "
                    + ", ".join(
                        f"{unknown.column}:{unknown.type}"
                        for unknown in result.unknown_edm_types
                    )
                ),
            )
            self.report.assets_with_unknown_edm_types.append(
                f"{space_name}.{asset_name}"
            )
        return result

    def _report_unknown_cds_types(
        self,
        space_name: str,
        asset_name: str,
        unknown_cds_types: List[UnknownColumnType],
    ) -> None:
        if not unknown_cds_types:
            return
        self.report.warning(
            title="Unknown CDS field type(s)",
            message=(
                "Field(s) have CDS types not in the connector's CSN _TYPE_MAP; those "
                "columns fall back to NullType (unclassified). Consider adding the "
                "type to the connector."
            ),
            context=(
                f"{space_name}.{asset_name}: "
                + ", ".join(
                    f"{unknown.column}:{unknown.type}" for unknown in unknown_cds_types
                )
            ),
        )
        self.report.assets_with_unknown_cds_types.append(f"{space_name}.{asset_name}")

    def _report_csn_parse_concerns(
        self,
        space_name: str,
        asset_name: str,
        csn_schema: CsnSchemaResult,
    ) -> None:
        # Surface both parse concerns: columns with an unmapped CDS type, and
        # columns the API emitted with no type at all.
        self._report_unknown_cds_types(space_name, asset_name, csn_schema.unknown_types)
        self._report_missing_cds_types(
            space_name, asset_name, csn_schema.columns_missing_type
        )

    def _report_missing_cds_types(
        self, space_name: str, asset_name: str, missing: List[str]
    ) -> None:
        if not missing:
            return
        self.report.warning(
            title="CSN column(s) missing a type",
            message=(
                "Column(s) had no type in the CSN and could not be resolved from a "
                "source object or inferred as a measure; they degrade to NullType "
                "(native UNKNOWN). Expected for calculated/derived analytic-model "
                "columns."
            ),
            context=f"{space_name}.{asset_name}: " + ", ".join(missing),
        )
        self.report.assets_with_missing_cds_types.append(f"{space_name}.{asset_name}")

    def _resolve_missing_element_types(
        self,
        space_name: str,
        asset_name: str,
        csn_def: JsonDict,
        fields: List[SchemaFieldClass],
        missing: List[str],
    ) -> List[str]:
        # Analytic-model elements carry no inline type. Recover each typeless
        # column's type by following the projection to its source object's column
        # (accurate), then fall back to a numeric guess for derived measures that
        # have no single source column. Returns the columns still unresolved.
        try:
            source_columns = extract_projection_source_columns(csn_def)
        except Exception as e:
            logger.debug(
                "Could not map %s.%s projection: %s", space_name, asset_name, e
            )
            return missing
        elements = csn_def.get(CSN_KEY_ELEMENTS)
        elements = elements if isinstance(elements, dict) else {}
        field_by_path = {f.fieldPath: f for f in fields}
        still_missing: List[str] = []
        for column in missing:
            field = field_by_path.get(column)
            if field is None:
                continue
            if self._type_from_source(space_name, column, source_columns, field):
                self.report.analytic_model_columns_typed_from_source += 1
            elif self._infer_measure_type(elements.get(column), field):
                self.report.analytic_model_columns_typed_by_measure_heuristic += 1
            else:
                still_missing.append(column)
        return still_missing

    def _type_from_source(
        self,
        space_name: str,
        column: str,
        source_columns: Dict[str, SourceColumnRef],
        field: SchemaFieldClass,
    ) -> bool:
        ref = source_columns.get(column)
        if ref is None:
            return False
        type_map = self._source_object_field_types(space_name, ref.source_object)
        source_field = type_map.get(ref.column)
        if source_field is None:
            return False
        field.type = source_field.type
        field.nativeDataType = source_field.nativeDataType
        return True

    @staticmethod
    def _infer_measure_type(element: object, field: SchemaFieldClass) -> bool:
        if not isinstance(element, dict):
            return False
        if (
            CSN_ANN_MEASURE_TYPE not in element
            and CSN_ANN_AGGREGATION_DEFAULT not in element
        ):
            return False
        field.type = SchemaFieldDataTypeClass(type=NumberTypeClass())
        field.nativeDataType = "DECIMAL"
        return True

    def _source_object_field_types(
        self, default_space: str, source_object: str
    ) -> Dict[str, SchemaFieldClass]:
        cached = self._source_field_type_cache.get(source_object)
        if cached is not None:
            return cached
        # A projection ref is space-qualified (``SPACE.name``); a bare name is a
        # same-space source. NOTE: only VIEWS is probed (its 404 retry also covers
        # analytic models) — a local/remote-table source is not resolved and its
        # columns fall back to the measure heuristic or stay NullType.
        space, _, name = source_object.partition(".")
        if not name:
            space, name = default_space, source_object
        field_map: Dict[str, SchemaFieldClass] = {}
        csn_obj = self._client.fetch_object_definition(space, OBJECT_TYPE_VIEWS, name)
        definition = self._csn_definition(csn_obj, name)
        elements = definition.get(CSN_KEY_ELEMENTS) if definition is not None else None
        if isinstance(elements, dict):
            result = parse_csn_elements_to_schema_fields(elements)
            # Only propagate types we actually know: a typeless source column (e.g.
            # a nested analytic model) must not overwrite with a bogus NullType.
            typeless = set(result.columns_missing_type)
            field_map = {
                f.fieldPath: f for f in result.fields if f.fieldPath not in typeless
            }
        self._source_field_type_cache[source_object] = field_map
        return field_map

    def _apply_column_pattern(
        self, fields: List[SchemaFieldClass]
    ) -> List[SchemaFieldClass]:
        # Single place that honors column_pattern so every schema path filters
        # consistently.
        column_pattern = self.config.column_pattern
        kept: List[SchemaFieldClass] = []
        for f in fields:
            if not column_pattern.allowed(f.fieldPath):
                self.report.columns_filtered += 1
                continue
            kept.append(f)
        return kept

    def _schema_fields_from_csn(
        self, space_name: str, asset_name: str, csn_def: JsonDict
    ) -> Optional[List[SchemaFieldClass]]:
        # Analytic models expose no OData $metadata, so the EDMX path yields
        # nothing; their CSN still carries a full elements map.
        elements = csn_def.get(CSN_KEY_ELEMENTS)
        if not isinstance(elements, dict) or not elements:
            return None
        csn_schema = parse_csn_elements_to_schema_fields(elements)
        self._report_unknown_cds_types(space_name, asset_name, csn_schema.unknown_types)
        # Analytic-model elements have no inline type; recover it from the
        # projection's source columns (or a measure guess) before warning, so the
        # warning fires only on the genuinely unresolvable remainder.
        missing = csn_schema.columns_missing_type
        if missing:
            missing = self._resolve_missing_element_types(
                space_name, asset_name, csn_def, csn_schema.fields, missing
            )
        self._report_missing_cds_types(space_name, asset_name, missing)
        filtered = self._apply_column_pattern(csn_schema.fields)
        if not filtered:
            return None
        self.report.assets_schema_from_csn += 1
        return filtered

    def _decorate_fields(self, result: EdmxParseResult) -> List[SchemaFieldClass]:
        decorated: List[SchemaFieldClass] = []
        # Reuse the shared column_pattern filter/counter so the EDMX path can't
        # drift from the other schema paths.
        for f in self._apply_column_pattern(result.fields):
            field_props = result.field_custom_props.get(f.fieldPath, {})
            # CDS annotations become field tags only; the description stays the
            # SAP Common.Label rather than a synthesized annotation dump.
            self._apply_field_tags(f, field_props)
            decorated.append(f)
        return decorated

    def _apply_field_tags(
        self, field: SchemaFieldClass, field_props: Dict[str, str]
    ) -> None:
        if not self.config.emit_sap_semantics_as_tags or not field_props:
            return
        tag_urns = self._tag_urns_for_field_props(field_props)
        if tag_urns:
            field.globalTags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=u) for u in tag_urns]
            )

    def _entity_tag_urns(self, entity_props: Dict[str, str]) -> List[str]:
        # sap_dimension_type builds a namespaced tag URN on the fly because CDS
        # allows arbitrary values here.
        if not self.config.emit_sap_semantics_as_tags:
            return []
        tag_urns: List[str] = []
        if entity_props.get(PROP_SAP_IS_DIMENSION) == PROP_VALUE_TRUE:
            tag_urns.append(DIMENSION_TAG_URN)
        if entity_props.get(PROP_SAP_IS_MEASURE) == PROP_VALUE_TRUE:
            tag_urns.append(MEASURE_TAG_URN)
        sap_dim_type = entity_props.get(PROP_SAP_DIMENSION_TYPE)
        if sap_dim_type:
            tag_urns.append(sap_dimension_type_tag_urn(sap_dim_type))
        return tag_urns

    @staticmethod
    def _tag_urns_for_field_props(field_props: Dict[str, str]) -> List[str]:
        tag_urns: List[str] = []
        if field_props.get(PROP_SAP_IS_DIMENSION) == PROP_VALUE_TRUE:
            tag_urns.append(DIMENSION_TAG_URN)
        if field_props.get(PROP_SAP_IS_MEASURE) == PROP_VALUE_TRUE:
            tag_urns.append(MEASURE_TAG_URN)
        sap_semantic = field_props.get(PROP_SAP_SEMANTIC)
        if sap_semantic == SEMANTIC_CURRENCY:
            tag_urns.append(SAP_CURRENCY_TAG_URN)
        elif sap_semantic == SEMANTIC_UNIT:
            tag_urns.append(SAP_UNIT_TAG_URN)
        sap_calendar_type = field_props.get(PROP_SAP_CALENDAR_TYPE)
        if sap_calendar_type and sap_calendar_type in SAP_CALENDAR_TAG_URNS:
            tag_urns.append(SAP_CALENDAR_TAG_URNS[sap_calendar_type])
        return tag_urns

    def _maybe_warn_builtin_defaults_missing_instance(
        self, resolved: ResolvedPlatform
    ) -> None:
        # Without a platform_instance the emitted URN uses the generic s3:// /
        # gs:// scheme and won't merge with URNs from the operator's dedicated
        # S3/GCS connector; warn once so they can override the mapping.
        if resolved.platform_instance is not None:
            return
        if resolved.platform not in GENERIC_SCHEME_PLATFORMS:
            return
        if self._builtin_defaults_warning_emitted_for.get(resolved.platform):
            return
        self._builtin_defaults_warning_emitted_for[resolved.platform] = True
        self.report.warning(
            title=(
                f"{resolved.platform.upper()} default mapping lacks platform_instance"
            ),
            message=(
                f"Connection routed to platform={resolved.platform!r} via the "
                f"built-in defaults table, but no platform_instance is set. The "
                f"emitted dataset URN will use the generic {resolved.platform}:// "
                f"scheme and may not merge with URNs from your dedicated "
                f"{resolved.platform.upper()} DataHub connector. Override via "
                f"platform_type_defaults or connection_to_platform_map."
            ),
        )

    def _check_scale_warning(self) -> None:
        # At default GMS/Kafka payload limits the soft-delete checkpoint MCP
        # approaches its ~80K-URN ceiling around here; warn early so operators
        # can switch to manual cleanup or partition the run.
        if (
            not self._scale_warning_emitted
            and self._datasets_emitted >= SCALE_WARNING_URN_THRESHOLD
            and self.config.stateful_ingestion is not None
            and self.config.stateful_ingestion.enabled
        ):
            self._scale_warning_emitted = True
            self.report.warning(
                title="Approaching stateful-ingestion scaling ceiling",
                message=(
                    f"Emitted {self._datasets_emitted} datasets so far with "
                    f"stateful_ingestion enabled. At default GMS payload limits "
                    f"the soft-delete checkpoint can hold ~80,000 URNs. "
                    f"Consider disabling stateful_ingestion or partitioning by "
                    f"space_pattern. See connector docs section "
                    f"'Stateful ingestion + large catalogs'."
                ),
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            config = SapDatasphereConfig.parse_obj_allow_extras(config_dict)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Configuration error: {e}",
            )
            return test_report

        try:
            client = SapDatasphereClient(config)
            try:
                client._ensure_auth()
                test_report.basic_connectivity = CapabilityReport(capable=True)
            except Exception as e:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason=f"Authentication failed: {e}",
                )
                return test_report

            capability_report: Dict[Union[SourceCapability, str], CapabilityReport] = {}
            try:
                # Touch the spaces endpoint — first item is the lightest probe.
                next(iter(client.list_spaces()), None)
                capability_report[SourceCapability.CONTAINERS] = CapabilityReport(
                    capable=True
                )
            except Exception as e:
                capability_report[SourceCapability.CONTAINERS] = CapabilityReport(
                    capable=False,
                    failure_reason=f"Could not list spaces: {e}",
                )
            test_report.capability_report = capability_report
        except Exception as e:
            logger.exception("Unexpected failure during test_connection")
            test_report.internal_failure = True
            test_report.internal_failure_reason = f"{e}"
            if test_report.basic_connectivity is None:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=f"{e}"
                )
        return test_report

    def get_report(self) -> SapDatasphereReport:
        return self.report
