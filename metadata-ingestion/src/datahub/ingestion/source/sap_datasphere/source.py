import itertools
import json
import logging
from typing import ClassVar, Dict, Iterable, Iterator, List, Optional, Union

import requests

from datahub.emitter.mce_builder import (
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
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sap_datasphere.analytic_model import (
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
    CSN_KEY_BUSINESS_LAYER,
    CSN_KEY_DEFINITIONS,
    CSN_KEY_ELEMENTS,
    CSN_KEY_QUERY,
    CSN_KEY_SQL_EDITOR_QUERY,
    FIELD_TECHNICAL_NAME,
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
from datahub.ingestion.source.sap_datasphere.lineage import (
    CsnLineageExtractor,
    parse_remote_table_source,
)
from datahub.ingestion.source.sap_datasphere.models import (
    ColumnLineageContext,
    ColumnLineagePair,
    EdmxFetchReason,
    EdmxParseResult,
    FlowColumnMapping,
    FlowEndpoint,
    ParsedFlow,
    ResolvedPlatform,
    ResolveSkipReason,
    UnknownColumnType,
    UpstreamRef,
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
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    SchemaFieldClass,
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
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

# Per-flow-object-type report counter attribute names, keyed by dwaas-core type.
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
    # Lazy chunking keeps peak memory bounded: only the current chunk is
    # materialized, even when the source iterable is a paginated listing.
    iterator = iter(iterable)
    while True:
        chunk = list(itertools.islice(iterator, size))
        if not chunk:
            return
        yield chunk


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
        # Flow target -> merged UpstreamLineage. Accumulated across every flow in
        # the run so that multiple flows writing to the same target (e.g. an
        # initial-load flow plus a delta flow) don't clobber each other: each
        # flow emits a *full* UpstreamLineage aspect, and without aggregation the
        # last write would win and drop the earlier flow's edges. Emitted once
        # per target at the end of the run (see _emit_flow_downstream_lineage).
        self._flow_downstream_lineage: Dict[str, UpstreamLineageClass] = {}

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SapDatasphereSource":
        config = SapDatasphereConfig.model_validate(config_dict)
        return cls(ctx, config)

    def _safe_list_spaces(self) -> Iterator[Dict]:
        # Soften only transport errors. Auth/config failures raise ValueError and
        # must propagate: a total auth outage that "succeeded" with zero spaces
        # would let stateful ingestion soft-delete every prior entity.
        try:
            yield from self._client.list_spaces()
        except requests.RequestException as e:
            self.report.warning(
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Emit standalone Tag entities once per run so the predefined SAP tag URNs
        # get display names + descriptions in the UI.
        if self.config.emit_sap_semantics_as_tags and not self._sap_tags_emitted:
            yield from get_predefined_tag_workunits()
            self._sap_tags_emitted = True

        for space in self._safe_list_spaces():
            try:
                space_name = space.get(CATALOG_FIELD_NAME)
                if not space_name:
                    self.report.warning(
                        title="Skipped malformed Datasphere space record",
                        message="Space record from catalog API is missing the 'name' field; skipping.",
                        context=str(space),
                    )
                    continue
                space_label: str = space.get(CATALOG_FIELD_LABEL) or space_name
                self.report.spaces_scanned += 1
                if not self.config.space_pattern.allowed(space_name):
                    self.report.spaces_filtered += 1
                    continue
                yield from self._emit_space(space_name, space_label)

                # Warm the resolver cache serially before parallel workers hit it,
                # so two threads don't race to build it (and double-fire the
                # connections API call).
                self._get_resolver(space_name)

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

        # Flow targets can be shared across flows (and spaces), so their lineage
        # is accumulated during the space loop and emitted once here — merged —
        # to avoid full-aspect overwrites between flows writing the same target.
        yield from self._emit_flow_downstream_lineage()

    def _emit_assets_in_space(self, space_name: str) -> Iterable[MetadataWorkUnit]:
        def _emit_asset_with_isolation(
            asset: Dict,
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
                    message=(
                        f"Skipped asset {space_name}.{asset_name} due to network error"
                    ),
                    context=str(e),
                )
            except Exception as e:  # per-entity isolation
                self.report.warning(
                    title="Failed to emit Datasphere asset",
                    message=(
                        f"Skipped asset {space_name}.{asset_name} due to "
                        f"unexpected error"
                    ),
                    context=f"{type(e).__name__}: {e}",
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
        """Emit Dataset stubs for Local Tables (base tables not exposed via OData).

        They show up as upstream lineage targets of views, so emitting them turns
        phantom lineage edges into navigable nodes. Column schema is parsed from
        the per-table CSN when available so column-level edges render.
        """
        try:
            local_tables = list(self._client.list_local_tables(space_name))
        except requests.RequestException as e:
            self.report.warning(
                title="Failed to list Local Tables in space",
                message=(
                    f"Could not enumerate Local Tables in space {space_name}; "
                    f"phantom-lineage targets in this space will remain stubs."
                ),
                context=str(e),
            )
            return

        if not local_tables:
            return

        # Local Tables always live in the tenant's own HANA Cloud (_managed).
        resolver = self._get_resolver(space_name)
        result = resolver.resolve(MANAGED_CONNECTION_KEY)
        resolved = result.platform
        skip_reason = result.skip_reason
        if resolved is None:
            self.report.warning(
                title="Cannot emit Local Tables — _managed connection unresolvable",
                message=(
                    f"Space {space_name} has {len(local_tables)} Local Tables but "
                    f"the _managed connection's platform mapping is missing/disabled "
                    f"(reason={skip_reason}). Configure connection_to_platform_map "
                    f"to enable Local Table discovery."
                ),
            )
            return

        space_key = self._space_key(space_name)

        for entry in local_tables:
            if not isinstance(entry, dict):
                continue
            technical_name = entry.get("technicalName")
            if not isinstance(technical_name, str) or not technical_name:
                continue

            if not self.config.asset_pattern.allowed(technical_name):
                continue

            dataset_name = self._build_dataset_name(space_name, technical_name)

            # Schema fields on both sides (View + Local Table) let the UI draw
            # column-level lineage edges between them.
            schema_fields = None
            csn_obj = self._client.fetch_object_definition(
                space_name, OBJECT_TYPE_LOCAL_TABLES, technical_name
            )
            if csn_obj is not None:
                definition = csn_obj.get(CSN_KEY_DEFINITIONS, {}).get(technical_name)
                elements = (
                    definition.get(CSN_KEY_ELEMENTS)
                    if isinstance(definition, dict)
                    else None
                )
                if isinstance(elements, dict):
                    csn_schema = parse_csn_elements_to_schema_fields(elements)
                    schema_fields = csn_schema.fields
                    self._report_unknown_cds_types(
                        space_name, technical_name, csn_schema.unknown_types
                    )
                else:
                    # 200 OK but not a parseable CSN shape — record it so a parse
                    # miss isn't mistaken for a genuine no-schema base table.
                    self.report.assets_csn_unparseable.append(
                        f"{space_name}.{technical_name}"
                    )
                    self.report.warning(
                        title="Unparseable Local Table CSN",
                        message=(
                            "Fetched the Local Table definition but it did not "
                            "contain a parseable elements map; emitting the table "
                            "without column schema."
                        ),
                        context=f"{space_name}.{technical_name}",
                    )

            if schema_fields:
                description = f"Local Table from SAP Datasphere space {space_name}."
            else:
                description = (
                    f"Local Table from SAP Datasphere space {space_name}. "
                    f"This is a base table not exposed via the OData Consumption "
                    f"API — schema details are unavailable. Emitted to close "
                    f"phantom-lineage gaps from views referencing this table."
                )

            dataset = Dataset(
                platform=resolved.platform,
                name=dataset_name,
                platform_instance=resolved.platform_instance,
                env=resolved.env,
                display_name=technical_name,
                description=description,
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
        """Emit each enabled flow object (data / replication / transformation flow,
        task chain) as a DataJob under a single per-space DataFlow, wiring its IO
        objects as inlets/outlets and its column mappings as fine-grained lineage.
        """
        flow_types = self._enabled_flow_types()
        if not flow_types:
            return

        # The DataFlow is emitted lazily on the first parseable flow so a space
        # with none doesn't get an empty pipeline node.
        dataflow: Optional[DataFlow] = None
        for object_type in flow_types:
            try:
                entries = list(self._client.list_objects(space_name, object_type))
            except requests.RequestException as e:
                self.report.warning(
                    title="Failed to list flows in space",
                    message=(
                        f"Could not enumerate {object_type} in space {space_name}; "
                        f"flow lineage from this type will be missing."
                    ),
                    context=str(e),
                )
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                technical_name = entry.get(FIELD_TECHNICAL_NAME)
                if not isinstance(technical_name, str) or not technical_name:
                    continue
                if not self.config.asset_pattern.allowed(technical_name):
                    continue
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
                if dataflow is None:
                    dataflow = self._build_space_dataflow(space_name)
                    yield from dataflow.as_workunits()
                yield from self._emit_flow_job(space_name, dataflow, parsed)
                self._bump_report(_FLOW_EMITTED_ATTR[object_type])

    def _bump_report(self, attr: str) -> None:
        setattr(self.report, attr, getattr(self.report, attr) + 1)

    def _build_space_dataflow(self, space_name: str) -> DataFlow:
        return DataFlow(
            platform=PLATFORM,
            name=self._maybe_lower(space_name),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            display_name=space_name,
            description=f"SAP Datasphere flows in space {space_name}.",
            subtype=DataFlowSubTypes.SAP_DATASPHERE_SPACE_FLOWS,
            parent_container=self._space_key(space_name),
        )

    def _emit_flow_job(
        self, space_name: str, dataflow: DataFlow, parsed: ParsedFlow
    ) -> Iterable[MetadataWorkUnit]:
        urn_by_object: Dict[str, str] = {}
        inlets: List[Union[str, DatasetUrn]] = []
        outlets: List[Union[str, DatasetUrn]] = []
        output_urn_by_object: Dict[str, str] = {}
        for endpoint in parsed.inputs:
            urn = self._resolve_flow_endpoint_urn(space_name, endpoint)
            if urn is not None:
                urn_by_object[endpoint.object_name] = urn
                inlets.append(urn)
        for endpoint in parsed.outputs:
            urn = self._resolve_flow_endpoint_urn(space_name, endpoint)
            if urn is not None:
                urn_by_object[endpoint.object_name] = urn
                output_urn_by_object[endpoint.object_name] = urn
                outlets.append(urn)

        fine_grained = self._build_flow_fine_grained(parsed, urn_by_object)
        job = DataJob(
            name=self._maybe_lower(f"{space_name}.{parsed.technical_name}"),
            flow=dataflow,
            display_name=parsed.technical_name,
            subtype=parsed.subtype,
            custom_properties={
                PROP_SAP_DATASPHERE_SPACE: space_name,
                PROP_SAP_DATASPHERE_ASSET: parsed.technical_name,
            },
            inlets=inlets or None,
            outlets=outlets or None,
            fine_grained_lineages=fine_grained or None,
        )
        yield from job.as_workunits()

        # Also surface the flow's lineage as a dataset-to-dataset edge on each
        # target, so it shows on the downstream's Lineage tab (the DataJob IO
        # aspect alone only drives the job's own lineage view). Accumulate rather
        # than emit inline: several flows can share a target, and each edge is a
        # *full* UpstreamLineage aspect, so we merge them and emit once per target
        # at the end of the run (see _emit_flow_downstream_lineage).
        for downstream_urn, lineage in self._build_flow_downstream_lineage(
            parsed, urn_by_object, output_urn_by_object, inlets
        ).items():
            self._merge_flow_downstream_lineage(downstream_urn, lineage)

    def _merge_flow_downstream_lineage(
        self, downstream_urn: str, lineage: UpstreamLineageClass
    ) -> None:
        """Union a flow's downstream lineage into the per-target accumulator.

        Upstream datasets are deduped by URN and fine-grained edges by their
        (upstreams, downstreams, transformOperation) identity so two flows
        contributing the same edge to a shared target don't double-count it.
        """
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
        """Emit the merged flow lineage once per target after all flows are seen.

        is_primary_source=False keeps AutoStatusAspectProcessor from synthesizing
        a Status(removed=false) for the target: a flow target is typically a
        Local/Remote table owned by another platform (or not ingested here), and
        materializing a phantom entity would pollute search and stale-entity
        state. This mirrors the airbyte connector's downstream lineage emission.
        """
        for downstream_urn, lineage in self._flow_downstream_lineage.items():
            yield MetadataChangeProposalWrapper(
                entityUrn=downstream_urn, aspect=lineage
            ).as_workunit(is_primary_source=False)
        self._flow_downstream_lineage.clear()

    def _resolve_flow_endpoint_urn(
        self, space_name: str, endpoint: FlowEndpoint
    ) -> Optional[str]:
        resolver = self._get_resolver(space_name)
        if endpoint.is_local:
            resolved = resolver.resolve(MANAGED_CONNECTION_KEY).platform
            name = self._build_dataset_name(space_name, endpoint.object_name)
        else:
            resolved = resolver.resolve_external(
                endpoint.connection, endpoint.connection_type
            ).platform
            name = self._maybe_lower(endpoint.object_name)
        if resolved is None:
            self.report.flow_endpoints_unresolved.append(
                f"{space_name}.{endpoint.object_name} "
                f"(connection={endpoint.connection}, type={endpoint.connection_type})"
            )
            return None
        return make_dataset_urn_with_platform_instance(
            platform=resolved.platform,
            name=name,
            platform_instance=resolved.platform_instance,
            env=resolved.env,
        )

    def _build_flow_fine_grained(
        self, parsed: ParsedFlow, urn_by_object: Dict[str, str]
    ) -> List[FineGrainedLineageClass]:
        fine_grained: List[FineGrainedLineageClass] = []
        for mapping in parsed.column_mappings:
            upstream_dataset_urn = urn_by_object.get(mapping.upstream_object)
            downstream_dataset_urn = urn_by_object.get(mapping.downstream_object)
            if upstream_dataset_urn is None or downstream_dataset_urn is None:
                continue
            fine_grained.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[
                        make_schema_field_urn(
                            upstream_dataset_urn, mapping.upstream_col
                        )
                    ],
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[
                        make_schema_field_urn(
                            downstream_dataset_urn, mapping.downstream_col
                        )
                    ],
                )
            )
        return fine_grained

    def _build_flow_downstream_lineage(
        self,
        parsed: ParsedFlow,
        urn_by_object: Dict[str, str],
        output_urn_by_object: Dict[str, str],
        inlets: List[Union[str, DatasetUrn]],
    ) -> Dict[str, UpstreamLineageClass]:
        """Build one UpstreamLineage per flow target (keyed by its dataset URN).

        Column edges are attributed to the specific target they land on from the
        flow's column mappings. A target with no attributable column mapping
        falls back to a coarse table-level edge from every resolved input (the
        same set the DataJob carries as inlets), so multi-input flows still get a
        dataset-to-dataset edge even when column attribution was suppressed.
        """
        mappings_by_downstream: Dict[str, List[FlowColumnMapping]] = {}
        for mapping in parsed.column_mappings:
            mappings_by_downstream.setdefault(mapping.downstream_object, []).append(
                mapping
            )

        result: Dict[str, UpstreamLineageClass] = {}
        for object_name, downstream_urn in output_urn_by_object.items():
            upstream_urns: List[str] = []
            fine_grained: List[FineGrainedLineageClass] = []
            for mapping in mappings_by_downstream.get(object_name, []):
                upstream_urn = urn_by_object.get(mapping.upstream_object)
                if upstream_urn is None:
                    continue
                if upstream_urn not in upstream_urns:
                    upstream_urns.append(upstream_urn)
                fine_grained.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[
                            make_schema_field_urn(upstream_urn, mapping.upstream_col)
                        ],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[
                            make_schema_field_urn(
                                downstream_urn, mapping.downstream_col
                            )
                        ],
                    )
                )
            if not upstream_urns:
                upstream_urns = [str(urn) for urn in inlets]
            # Guard against a target that also appears as its own input.
            upstream_urns = [urn for urn in upstream_urns if urn != downstream_urn]
            if not upstream_urns:
                continue
            result[downstream_urn] = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=upstream_urn,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    for upstream_urn in upstream_urns
                ],
                fineGrainedLineages=fine_grained or None,
            )
        return result

    def _emit_remote_tables_for_space(
        self, space_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit federated Remote Tables as Datasets on the sap_datasphere platform,
        with upstream lineage to their external source object parsed from the CSN
        ``@DataWarehouse.remote.*`` annotations."""
        try:
            entries = list(
                self._client.list_objects(space_name, OBJECT_TYPE_REMOTE_TABLES)
            )
        except requests.RequestException as e:
            self.report.warning(
                title="Failed to list Remote Tables in space",
                message=(
                    f"Could not enumerate Remote Tables in space {space_name}; "
                    f"federated lineage in this space will be missing."
                ),
                context=str(e),
            )
            return

        if not entries:
            return

        resolver = self._get_resolver(space_name)
        # The remote-table proxy object itself lives in the tenant's managed HANA.
        result = resolver.resolve(MANAGED_CONNECTION_KEY)
        local_resolved = result.platform
        skip_reason = result.skip_reason
        if local_resolved is None:
            self.report.warning(
                title="Cannot emit Remote Tables — _managed connection unresolvable",
                message=(
                    f"Space {space_name} has Remote Tables but the _managed "
                    f"connection's platform mapping is missing/disabled "
                    f"(reason={skip_reason})."
                ),
            )
            return

        space_key = self._space_key(space_name)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            technical_name = entry.get(FIELD_TECHNICAL_NAME)
            if not isinstance(technical_name, str) or not technical_name:
                continue
            if not self.config.asset_pattern.allowed(technical_name):
                continue
            self.report.remote_tables_scanned += 1
            yield from self._emit_one_remote_table(
                space_name, technical_name, local_resolved, resolver, space_key
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
        definition = (
            csn_obj.get(CSN_KEY_DEFINITIONS, {}).get(technical_name)
            if isinstance(csn_obj, dict)
            else None
        )

        schema_fields = None
        upstreams_aspect: Optional[UpstreamLineageClass] = None
        if isinstance(definition, dict):
            schema_fields = self._remote_table_schema(
                space_name, technical_name, definition
            )
            upstreams_aspect = self._remote_table_upstream(
                space_name, technical_name, definition, resolver
            )

        dataset = Dataset(
            platform=local_resolved.platform,
            name=self._build_dataset_name(space_name, technical_name),
            platform_instance=local_resolved.platform_instance,
            env=local_resolved.env,
            display_name=technical_name,
            description=f"Remote Table from SAP Datasphere space {space_name}.",
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
        yield from dataset.as_workunits()
        self.report.remote_tables_emitted += 1

    def _remote_table_schema(
        self, space_name: str, technical_name: str, definition: Dict
    ) -> Optional[List[SchemaFieldClass]]:
        elements = definition.get(CSN_KEY_ELEMENTS)
        if not isinstance(elements, dict) or not elements:
            return None
        csn_schema = parse_csn_elements_to_schema_fields(elements)
        self._report_unknown_cds_types(
            space_name, technical_name, csn_schema.unknown_types
        )
        filtered = [
            f
            for f in csn_schema.fields
            if self.config.column_pattern.allowed(f.fieldPath)
        ]
        return filtered or None

    def _remote_table_upstream(
        self,
        space_name: str,
        technical_name: str,
        definition: Dict,
        resolver: PlatformMappingResolver,
    ) -> Optional[UpstreamLineageClass]:
        remote = parse_remote_table_source(definition)
        if remote is None:
            return None
        resolved = resolver.resolve_external(remote.connection, None).platform
        if resolved is None:
            self.report.remote_table_source_unresolved.append(
                f"{space_name}.{technical_name} (connection={remote.connection})"
            )
            return None
        upstream_urn = make_dataset_urn_with_platform_instance(
            platform=resolved.platform,
            name=self._maybe_lower(remote.qualified_name),
            platform_instance=resolved.platform_instance,
            env=resolved.env,
        )
        return UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.COPY,
                )
            ]
        )

    def _maybe_lower(self, name: str) -> str:
        return name.lower() if self.config.convert_urns_to_lowercase else name

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
            except Exception as e:
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
        # state, so we must not add URNs manually (would double-count and race
        # with the parallel asset workers).
        key = self._space_key(space_name)
        container = Container(
            key,
            display_name=space_label,
            description=f"SAP Datasphere Space: {space_name}",
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

    def _emit_asset(self, space_name: str, asset: dict) -> Iterable[MetadataWorkUnit]:
        asset_name_opt = asset.get(CATALOG_FIELD_NAME)
        if not asset_name_opt:
            # Mirror the malformed-space-record guard: a record missing 'name'
            # is a data problem, not a code bug — report it specifically rather
            # than letting a KeyError surface as the generic isolation warning.
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

        # Fetch CSN for lineage, view definitions, or @remote.source detection.
        csn_def: Optional[Dict] = None
        csn_obj: Optional[Dict] = None
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
                csn_def = csn_obj.get(CSN_KEY_DEFINITIONS, {}).get(asset_name)

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
                        f"Could not determine remote source from CSN for "
                        f"{space_name}.{asset_name}; defaulting to the managed HANA "
                        f"connection. Federated routing for this asset may be wrong."
                    ),
                    context=f"{type(e).__name__}: {e}",
                )
                remote = None
            if remote:
                connection_name = remote

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
        custom_properties = {
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
        if parse_result is not None:
            if parse_result.entity_label:
                description = parse_result.entity_label
            custom_properties.update(parse_result.entity_custom_props)

        if asset.get(CATALOG_FLAG_SUPPORTS_ANALYTICAL_QUERIES):
            sub_type: str = DatasetSubTypes.SAP_ANALYTICAL_MODEL
        else:
            sub_type = DatasetSubTypes.VIEW

        schema_fields = None
        if parse_result is not None and parse_result.fields:
            self.report.assets_schema_fetched += 1
            schema_fields = self._decorate_fields(parse_result)
        elif csn_def is not None:
            # Analytic models expose no relational metadata URL, so EDMX yields
            # nothing; recover their schema from the CSN elements map instead.
            schema_fields = self._schema_fields_from_csn(
                space_name, asset_name, csn_def
            )

        dataset_name = self._build_dataset_name(space_name, asset_name)

        upstreams_aspect: Optional[UpstreamLineageClass] = None
        if csn_def is not None and self.config.include_lineage:
            # Outer guard for non-walker failures (URN construction, aspect
            # assembly) so a single bad asset doesn't crash the emit.
            try:
                upstreams_aspect = self._extract_lineage_aspect(
                    csn_def, resolved, space_name, asset_name, dataset_name
                )
            except Exception as e:
                self.report.warning(
                    title="Failed to build lineage aspect",
                    message=(
                        f"Could not assemble UpstreamLineage for "
                        f"{space_name}.{asset_name}; the dataset will be emitted "
                        f"without lineage."
                    ),
                    context=f"{type(e).__name__}: {e}",
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
        yield from dataset.as_workunits()
        if view_properties is not None:
            view_dataset_urn = make_dataset_urn_with_platform_instance(
                platform=resolved.platform,
                name=dataset_name,
                platform_instance=resolved.platform_instance,
                env=resolved.env,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=view_dataset_urn,
                aspect=view_properties,
            ).as_workunit()
        # State is added by the workunit_processor (see _emit_space). The counter
        # isn't strictly atomic under threading but is fine for a coarse threshold.
        self._datasets_emitted += 1
        self._check_scale_warning()

    def _build_view_properties(
        self, csn_def: Optional[Dict]
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
        # The key already carries its own (possibly different) space, so it is
        # used as the URN name directly — re-prefixing would corrupt cross-space
        # references.
        name = self._maybe_lower(qualified_key)
        return make_dataset_urn_with_platform_instance(
            platform=PLATFORM,
            name=name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
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
        # _apply_business_layer mutates schema_fields/custom_properties in place;
        # on a mid-way exception those may be partial but the dataset still emits.
        try:
            return self._apply_business_layer(
                csn_obj,
                technical_name,
                schema_fields,
                custom_properties,
                query_upstreams,
            )
        except Exception as e:
            self.report.warning(
                title="Failed to apply analytic-model business layer",
                message=(
                    f"Could not assemble star-schema lineage/measure tags for "
                    f"{space_name}.{technical_name}; emitting with prior lineage "
                    f"only."
                ),
                context=f"{type(e).__name__}: {e}",
            )
            return query_upstreams

    def _apply_business_layer(
        self,
        csn_obj: Optional[Dict],
        technical_name: str,
        schema_fields: Optional[List[SchemaFieldClass]],
        custom_properties: Dict[str, str],
        query_upstreams: Optional[UpstreamLineageClass],
    ) -> Optional[UpstreamLineageClass]:
        """Wire an analytic model's ``businessLayerDefinitions`` (fact + dimension
        sources, measures, attributes, variables) into emission.

        These live in a sibling block of the CSN ``query`` the generic walker
        reads. When present, the business layer is the AUTHORITATIVE table-level
        lineage, replacing the query-FROM upstreams (the query-FROM path may
        double-prefix the fact's space). Query-derived fine-grained lineage is
        kept only when every upstream schemaField URN points at a business-layer
        dataset URN; otherwise it would dangle off the double-prefixed fact and
        is dropped. Returns ``query_upstreams`` unchanged for plain views.
        """
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

        bl_upstream_urns = {
            self._qualified_upstream_urn(key) for key in bl.upstream_keys
        }
        upstreams = [
            UpstreamClass(
                dataset=self._qualified_upstream_urn(key),
                type=DatasetLineageTypeClass.VIEW,
            )
            for key in bl.upstream_keys
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
            upstream_refs = [
                UpstreamRef(name=ref, qualified=False)
                for ref in self._lineage_extractor.extract_upstream_refs(csn_def)
            ]
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN lineage",
                message=(
                    f"Could not parse upstream references from CSN for "
                    f"{space_name}.{asset_name}; the dataset will be emitted "
                    f"without upstreamLineage."
                ),
                context=f"{type(e).__name__}: {e}",
            )
        try:
            association_targets = self._lineage_extractor.extract_association_targets(
                csn_def
            )
            if association_targets:
                self.report.association_upstreams_emitted += len(association_targets)
                # A ref appearing in both FROM and an association resolves to the
                # same URN key, so dedup by name (qualified refs win — they carry
                # the correct URN-build flag).
                existing = {ref.name for ref in upstream_refs}
                upstream_refs.extend(
                    ref for ref in association_targets if ref.name not in existing
                )
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN association lineage",
                message=(
                    f"Could not parse association-based lineage from CSN for "
                    f"{space_name}.{asset_name}; the dataset will be emitted "
                    f"without association upstreams."
                ),
                context=f"{type(e).__name__}: {e}",
            )
        try:
            column_pairs = self._lineage_extractor.extract_column_lineage(csn_def)
        except Exception as e:
            self.report.warning(
                title="Failed to extract CSN column lineage",
                message=(
                    f"Could not parse column-level lineage from CSN for "
                    f"{space_name}.{asset_name}; the dataset will be emitted "
                    f"with table-level lineage only."
                ),
                context=f"{type(e).__name__}: {e}",
            )
        if not upstream_refs and not column_pairs:
            return None
        downstream_dataset_urn = make_dataset_urn_with_platform_instance(
            platform=resolved.platform,
            name=dataset_name,
            platform_instance=resolved.platform_instance,
            env=resolved.env,
        )
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
                # target): use as-is on the sap-datasphere platform, mirroring the
                # business-layer upstream path.
                upstream_urn = self._qualified_upstream_urn(ref.name)
            else:
                upstream_name = self._maybe_lower(f"{space_name}.{ref.name}")
                upstream_urn = make_dataset_urn_with_platform_instance(
                    platform=resolved.platform,
                    name=upstream_name,
                    platform_instance=resolved.platform_instance,
                    env=resolved.env,
                )
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
            # Surface walker-level unresolvable refs (unknown alias, 3-segment
            # ref, unnamed expression, malformed CSN markers) so operators can
            # debug silent drops.
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
            # Benign: the asset is legitimately not exposed for OData
            # consumption. Skip quietly — no report.warning and not tracked in
            # assets_schema_failed (there is no failure to surface).
            logger.debug(
                "Asset %s.%s is not exposed for OData consumption (HTTP 404); "
                "emitting without EDMX-derived schema.",
                space_name,
                asset_name,
            )
            return None
        if fetch.reason is EdmxFetchReason.FORBIDDEN:
            # fetch_edmx already emitted a permission-oriented warning; do NOT
            # double-warn here. Still track the asset so its missing schema is
            # visible in the report summary.
            self.report.assets_schema_failed.append(asset_name)
            return None
        if fetch.reason is EdmxFetchReason.ERROR or fetch.xml is None:
            self.report.assets_schema_failed.append(asset_name)
            self.report.warning(
                title="EDMX schema fetch failed",
                message=f"Could not fetch schema metadata for asset {asset_name}",
                context=metadata_url,
            )
            return None
        result = EdmxParser.parse(fetch.xml)
        if result.error is not None:
            self.report.assets_schema_failed.append(asset_name)
            self.report.warning(
                title="EDMX schema parse failed",
                message=f"Could not parse schema metadata for asset {asset_name}: {result.error}",
                context=metadata_url,
            )
            return None
        if result.unknown_edm_types:
            self.report.warning(
                title="Unknown EDMX field type(s)",
                message=(
                    f"{len(result.unknown_edm_types)} field(s) on "
                    f"{space_name}.{asset_name} have EDMX types not in the connector's "
                    f"_EDM_TYPE_MAP; schema for those columns will use NullType. "
                    f"Consider adding the type to the connector."
                ),
                context=", ".join(
                    f"{unknown.column}:{unknown.type}"
                    for unknown in result.unknown_edm_types
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
                f"{len(unknown_cds_types)} field(s) on {space_name}.{asset_name} "
                f"have CDS types not in the connector's CSN _TYPE_MAP; those "
                f"columns fall back to StringType. Consider adding the type to "
                f"the connector."
            ),
            context=", ".join(
                f"{unknown.column}:{unknown.type}" for unknown in unknown_cds_types
            ),
        )
        self.report.assets_with_unknown_cds_types.append(f"{space_name}.{asset_name}")

    def _schema_fields_from_csn(
        self, space_name: str, asset_name: str, csn_def: Dict
    ) -> Optional[List[SchemaFieldClass]]:
        # Analytic models expose no OData $metadata, so the EDMX path yields
        # nothing; their CSN still carries a full elements map. column_pattern is
        # applied here for parity with the EDMX _decorate_fields path.
        elements = csn_def.get(CSN_KEY_ELEMENTS)
        if not isinstance(elements, dict) or not elements:
            return None
        csn_schema = parse_csn_elements_to_schema_fields(elements)
        self._report_unknown_cds_types(space_name, asset_name, csn_schema.unknown_types)
        column_pattern = self.config.column_pattern
        filtered = []
        for f in csn_schema.fields:
            if not column_pattern.allowed(f.fieldPath):
                self.report.columns_filtered += 1
                continue
            filtered.append(f)
        if not filtered:
            return None
        self.report.assets_schema_from_csn += 1
        return filtered

    def _decorate_fields(self, result: EdmxParseResult) -> List[SchemaFieldClass]:
        column_pattern = self.config.column_pattern
        decorated: List[SchemaFieldClass] = []
        for f in result.fields:
            if not column_pattern.allowed(f.fieldPath):
                self.report.columns_filtered += 1
                continue
            field_props = result.field_custom_props.get(f.fieldPath, {})
            if field_props:
                prop_str = ", ".join(f"{k}={v}" for k, v in field_props.items())
                f.description = (
                    f"{f.description} [{prop_str}]"
                    if f.description
                    else f"[{prop_str}]"
                )
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
        if resolved.platform not in {"s3", "gcs"}:
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
            and self._datasets_emitted >= 50000
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
