"""DataHub source for SAP Datasphere.

Architectural overview and configuration guide live in the customer-facing docs
at ``docs/sources/sap-datasphere/sap-datasphere_pre.md``. This module docstring
is the developer-facing summary.

URN platform model: All Datasphere-managed assets (Views, Analytical Models,
Local Tables) emit on the ``sap-datasphere`` platform. Federated Remote Tables
emit on the storage platform they federate from (``snowflake`` / ``s3`` /
``bigquery`` / ``hana`` / ...) via ``connection_to_platform_map``, so they
merge automatically with native warehouse connectors' URNs. Space containers
also use ``sap-datasphere`` (2-tier Space -> object model, no folder layer).
Resolution is centralized in ``PlatformMappingResolver``; see
``sap-datasphere_pre.md`` for the full explanation and a worked example.

Endpoints used (all SAP-supported public APIs):
  * Catalog API: /api/v1/datasphere/consumption/catalog/spaces,
                 .../spaces('<S>')/assets
  * Connections API: /api/v1/datasphere/spaces/<S>/connections
  * EDMX consumption metadata:
                 /api/v1/datasphere/consumption/relational/<S>/<A>/$metadata
  * Per-object-type CSN read (lineage extraction; same surface the official
    ``datasphere`` CLI uses):
                 /dwaas-core/api/v1/spaces/<S>/{views,analyticmodels,localtables}/<name>
                 with Accept: application/vnd.sap.datasphere.object.content+json
                 Called ONLY when ``include_lineage=true``.

Supported authentication (priority order):
  * Raw bearer token (config.token)
  * XSUAA refresh-token grant (config.refresh_token + client_id + xsuaa_url)
  * XSUAA client-credentials grant (config.client_id + client_secret + xsuaa_url)

Example recipe: ``docs/sources/sap-datasphere/sap-datasphere_recipe.yml``.
"""

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
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sap_datasphere.analytic_model import (
    parse_business_layer,
)
from datahub.ingestion.source.sap_datasphere.client import (
    EdmxFetchReason,
    SapDatasphereClient,
)
from datahub.ingestion.source.sap_datasphere.config import (
    MANAGED_CONNECTION_KEY,
    SapDatasphereConfig,
    SpaceContainerKey,
)
from datahub.ingestion.source.sap_datasphere.csn_parser import (
    parse_csn_elements_to_schema_fields,
)
from datahub.ingestion.source.sap_datasphere.edmx_parser import (
    EdmxParser,
    EdmxParseResult,
)
from datahub.ingestion.source.sap_datasphere.lineage import (
    ColumnLineageContext,
    ColumnLineagePair,
    CsnLineageExtractor,
)
from datahub.ingestion.source.sap_datasphere.platform_mapping import (
    SAP_DATASPHERE_PLATFORM as PLATFORM,
    PlatformMappingResolver,
    ResolvedPlatform,
    ResolveSkipReason,
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
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
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
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)


def _chunked(iterable: Iterable[Dict], size: int) -> Iterator[List[Dict]]:
    """Yield successive lists of up to ``size`` items from ``iterable`` (lazy).

    Pulling one ``size``-bounded chunk at a time keeps peak memory bounded when
    the source iterable is itself lazy (e.g. paginated asset listing): only the
    current chunk is materialized at any moment.
    """
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
        self.stale_entity_removal_handler = StaleEntityRemovalHandler.create(
            self, config, ctx
        )
        self._lineage_extractor = CsnLineageExtractor()
        # Resolvers are built lazily per space because each space has its own
        # connections list from the Datasphere connections API.
        self._resolvers: Dict[str, PlatformMappingResolver] = {}
        # Scaling counters — used to warn operators when the stateful-ingestion
        # checkpoint is approaching the GMS payload ceiling (see
        # docs/sources/sap-datasphere/sap-datasphere_pre.md). Not strictly atomic
        # under threading but ok for a coarse threshold check.
        self._scale_warning_emitted = False
        self._datasets_emitted = 0
        # M5: dedupe the built-in-defaults warning to once per platform.
        self._builtin_defaults_warning_emitted_for: Dict[str, bool] = {}
        # Tracks whether we've yielded the standalone TagProperties MCPs for the
        # predefined SAP tag URNs yet (display name + description). Emitted once
        # per ingestion run; resets per source instance.
        self._sap_tags_emitted = False

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "SapDatasphereSource":
        config = SapDatasphereConfig.model_validate(config_dict)
        return cls(ctx, config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            self.stale_entity_removal_handler.workunit_processor,
        ]

    def _safe_list_spaces(self) -> Iterator[Dict]:
        """Yield spaces from the catalog API, converting network errors into a
        report warning instead of propagating. Allows ``get_workunits_internal``
        to start emitting workunits without first buffering every space response."""
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
        """Yield assets for a space, converting network errors into a report
        warning instead of propagating. Generator semantics mean the warning is
        emitted lazily on iteration, which matches the previous behaviour where a
        ``RequestException`` could be raised by either the initial call or
        pagination."""
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
        # Emit standalone Tag entities once per run so the predefined SAP tag
        # URNs (Dimension/Measure/sap:calendar:*/sap:semantic:*) have proper
        # display names + descriptions in the DataHub UI. Mirrors Looker's
        # tag-emission pattern (looker_common.py:850-855).
        if self.config.emit_sap_semantics_as_tags and not self._sap_tags_emitted:
            yield from get_predefined_tag_workunits()
            self._sap_tags_emitted = True

        for space in self._safe_list_spaces():
            try:
                space_name = space.get("name")
                if not space_name:
                    self.report.warning(
                        title="Skipped malformed Datasphere space record",
                        message="Space record from catalog API is missing the 'name' field; skipping.",
                        context=str(space),
                    )
                    continue
                space_label: str = space.get("label") or space_name
                self.report.spaces_scanned += 1
                if not self.config.space_pattern.allowed(space_name):
                    self.report.spaces_filtered += 1
                    continue
                yield from self._emit_space(space_name, space_label)

                # Warm the per-space resolver cache BEFORE workers hit it in
                # parallel. _get_resolver mutates self._resolvers; doing this
                # serially up-front avoids two threads racing to build the same
                # resolver (and double-firing the connections API call).
                self._get_resolver(space_name)

                yield from self._emit_assets_in_space(space_name)

                # PR-1: discover Local Tables via the supported /dwaas-core/
                # endpoint. Gated on `include_local_tables` (off by default).
                if self.config.include_local_tables:
                    yield from self._emit_local_tables_for_space(space_name)
            except Exception as e:  # per-space isolation
                # Catches malformed space records (missing 'name', etc.) so one bad
                # space doesn't abort the entire ingestion.
                self.report.warning(
                    title="Failed to process Datasphere space",
                    message="Encountered unexpected error iterating space; continuing with next space.",
                    context=f"{type(e).__name__}: {e}",
                )
                continue

    def _emit_assets_in_space(self, space_name: str) -> Iterable[MetadataWorkUnit]:
        """Drive per-asset emission, optionally in parallel.

        With ``max_workers_assets > 1`` (the default) assets in a space are
        processed concurrently via :class:`ThreadedIteratorExecutor`. The
        executor preserves per-asset exception isolation by running each asset
        through ``_emit_asset_with_isolation``, which converts any exception
        into a report warning rather than letting it bubble up and abort the
        whole space.
        """

        def _emit_asset_with_isolation(
            asset: Dict,
        ) -> Iterable[MetadataWorkUnit]:
            asset_name = (
                asset.get("name", "<unknown>")
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
                # Catches non-network errors (KeyError/ValueError/TypeError from
                # malformed API records, URN construction, CSN parsing, etc.) so
                # one bad asset doesn't abort the whole space.
                self.report.warning(
                    title="Failed to emit Datasphere asset",
                    message=(
                        f"Skipped asset {space_name}.{asset_name} due to "
                        f"unexpected error"
                    ),
                    context=f"{type(e).__name__}: {e}",
                )

        if self.config.max_workers_assets > 1:
            # Process the asset stream in bounded chunks so peak memory stays
            # ~asset_batch_size live futures/asset-dicts instead of growing with
            # the number of assets in the space (ThreadedIteratorExecutor submits
            # every task up front). Output is identical; only memory is bounded.
            for chunk in _chunked(
                self._safe_list_assets(space_name), self.config.asset_batch_size
            ):
                yield from ThreadedIteratorExecutor.process(
                    worker_func=_emit_asset_with_isolation,
                    args_list=((asset,) for asset in chunk),
                    max_workers=self.config.max_workers_assets,
                )
        else:
            # Serial path — preserve previous behaviour for max_workers_assets=1.
            for asset in self._safe_list_assets(space_name):
                yield from _emit_asset_with_isolation(asset)

    def _emit_local_tables_for_space(
        self, space_name: str
    ) -> Iterable[MetadataWorkUnit]:
        """Emit minimal Dataset stubs for Local Tables in a Space.

        These are base tables not exposed via the OData Consumption API — they
        show up as upstream lineage targets of views. Emitting them as Dataset
        entities turns phantom lineage edges into real, navigable nodes in
        DataHub's lineage graph.

        Stubs carry: platform (resolved storage platform — defaults to ``hana``
        via the ``_managed`` connection), subtype ``Local Table``, container
        membership (directly under the Space container), and column-level
        schema parsed from the per-table CSN (so column-level lineage edges
        from Views render in the DataHub UI).

        The endpoints hit (``/dwaas-core/api/v1/spaces/X/localtables`` list and
        ``/dwaas-core/api/v1/spaces/X/localtables/{name}`` detail) are the same
        surface the official ``datasphere`` CLI uses; SAP marks them as the
        public API in KBA #3517441 — no policy caveat applies here.
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

        # Resolve the storage platform via the _managed connection (Local Tables
        # always live in the Datasphere tenant's own HANA Cloud).
        resolver = self._get_resolver(space_name)
        resolved, skip_reason = resolver.resolve(MANAGED_CONNECTION_KEY)
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

        # Local Tables parent directly to the Space container (2-tier model);
        # the "Local Table" dataset subtype is the UI filter facet, so there's
        # no synthetic folder layer.
        space_key = self._space_key(space_name)

        for entry in local_tables:
            if not isinstance(entry, dict):
                continue
            technical_name = entry.get("technicalName")
            if not isinstance(technical_name, str) or not technical_name:
                continue

            if not self.config.asset_pattern.allowed(technical_name):
                # Honour the same asset_pattern filter used for exposed views.
                continue

            dataset_name = self._build_dataset_name(space_name, technical_name)

            # Fetch the per-table CSN to attach column metadata. With schema
            # fields on both sides (View + Local Table), the DataHub UI can
            # draw column-level lineage edges between them. Endpoint per SAP
            # KBA #3517441: GET /dwaas-core/api/v1/spaces/{space}/localtables/{name}
            schema_fields = None
            csn_obj = self._client.fetch_object_definition(
                space_name, "localtables", technical_name
            )
            if csn_obj is not None:
                definition = csn_obj.get("definitions", {}).get(technical_name)
                if isinstance(definition, dict):
                    elements = definition.get("elements")
                    if isinstance(elements, dict):
                        schema_fields = parse_csn_elements_to_schema_fields(elements)

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
                    "spaceName": space_name,
                    "sap_datasphere_space": space_name,
                    "sap_datasphere_asset": technical_name,
                    "exposed_for_consumption": "false",
                    "local_table": "true",
                },
                schema=schema_fields,
            )
            yield from dataset.as_workunits()
            self.report.local_tables_emitted += 1

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
            by_name = {c.get("name", ""): c for c in connections if c.get("name")}
            self._resolvers[space] = PlatformMappingResolver(
                self.config, by_name, report=self.report
            )
        return self._resolvers[space]

    def _emit_space(
        self, space_name: str, space_label: str
    ) -> Iterable[MetadataWorkUnit]:
        # NOTE: the workunit_processor returned by ``StaleEntityRemovalHandler``
        # automatically calls ``add_entity_to_state`` for every primary-source
        # workunit (see auto_stale_entity_removal in the handler), so we don't
        # add the URN to state manually here. Manual calls would double-count
        # and also race with worker threads under FIX 1's parallelism.
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
        """Record (in the report) an asset skipped because its connection could
        not be resolved to a concrete platform."""
        qualified = f"{space_name}.{asset_name} (connection={connection_name})"
        if skip_reason == ResolveSkipReason.UNKNOWN_CONNECTION:
            self.report.assets_skipped_unknown_connection.append(qualified)
        elif skip_reason == ResolveSkipReason.DISABLED:
            self.report.assets_skipped_disabled.append(qualified)
        else:
            # UNKNOWN_TYPEID, plus a defensive catch-all for any future skip
            # reason so the asset is never dropped silently from the report.
            self.report.assets_skipped_unknown_typeid.append(qualified)

    def _emit_asset(self, space_name: str, asset: dict) -> Iterable[MetadataWorkUnit]:
        asset_name_opt = asset.get("name")
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
        asset_label: str = asset.get("label") or asset_name
        self.report.assets_scanned += 1

        if not self.config.asset_pattern.allowed(asset_name):
            self.report.assets_filtered += 1
            return

        metadata_url: str = asset.get("assetRelationalMetadataUrl") or ""
        if self.config.expose_for_consumption_only and not metadata_url:
            self.report.assets_filtered += 1
            return

        # Step 1: Optionally fetch CSN (when lineage is enabled OR to detect a
        # federated remote source via @remote.source).
        csn_def: Optional[Dict] = None
        csn_obj: Optional[Dict] = None
        if self.config.include_lineage or self.config.include_view_definitions:
            # Route to the supported per-object-type endpoint based on the
            # asset's analytical-queries flag from the catalog API.
            object_type = (
                "analyticmodels" if asset.get("supportsAnalyticalQueries") else "views"
            )
            csn_obj = self._client.fetch_object_definition(
                space_name, object_type, asset_name
            )
            if csn_obj is not None:
                csn_def = csn_obj.get("definitions", {}).get(asset_name)

        # Step 2: Determine the source connection.  Default to `_managed` (the
        # Datasphere tenant's own HANA Cloud); override if CSN declares a remote source.
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

        # Step 3: Resolve to a concrete platform/instance/env via the per-space resolver.
        resolver = self._get_resolver(space_name)
        resolved, skip_reason = resolver.resolve(connection_name)
        if resolved is None:
            self._record_resolve_skip(
                space_name, asset_name, connection_name, skip_reason
            )
            return

        # M5: warn once per platform when a built-in default (S3/GCS) lacks
        # platform_instance — the resulting URN won't merge with the user's
        # dedicated storage-platform connector. Emitted at most once per platform
        # per run regardless of how many assets are routed through.
        self._maybe_warn_builtin_defaults_missing_instance(resolved)

        # Step 4: Parse EDMX schema (existing behaviour — only for exposed assets).
        parse_result: Optional[EdmxParseResult] = None
        if metadata_url:
            parse_result = self._parse_schema(space_name, asset_name, metadata_url)

        description: Optional[str] = None
        custom_properties = {
            "spaceName": space_name,
            "supportsAnalyticalQueries": str(
                asset.get("supportsAnalyticalQueries", False)
            ).lower(),
            "hasParameters": str(asset.get("hasParameters", False)).lower(),
            "exposed_for_consumption": str(bool(metadata_url)).lower(),
            "sap_datasphere_space": space_name,
            "sap_datasphere_asset": asset_name,
        }
        if parse_result is not None:
            if parse_result.entity_label:
                description = parse_result.entity_label
            custom_properties.update(parse_result.entity_custom_props)

        # M2: use enum-backed subtypes (string values match what was emitted
        # before so golden files are unaffected).
        if asset.get("supportsAnalyticalQueries"):
            sub_type: str = DatasetSubTypes.SAP_ANALYTICAL_MODEL
        else:
            sub_type = DatasetSubTypes.VIEW

        schema_fields = None
        if parse_result is not None and parse_result.fields:
            self.report.assets_schema_fetched += 1
            schema_fields = self._decorate_fields(parse_result)

        dataset_name = self._build_dataset_name(space_name, asset_name)

        upstreams_aspect: Optional[UpstreamLineageClass] = None
        if csn_def is not None and self.config.include_lineage:
            # The walker itself has per-extractor guards (extract_upstream_refs
            # and extract_column_lineage each catch); this outer guard catches
            # non-walker failure modes — URN construction, _build_upstream_lineage
            # raising, or any other surprise in the aspect-assembly path — so a
            # single bad asset doesn't crash the per-asset emit.
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

        # Analytic-model star-schema lineage + measure/dimension tags +
        # variables from the businessLayerDefinitions block (no-op for plain
        # views, whose csn_obj has no businessLayerDefinitions).
        upstreams_aspect = self._apply_business_layer_guarded(
            csn_obj,
            asset_name,
            schema_fields,
            custom_properties,
            upstreams_aspect,
            space_name,
        )

        # View Definition: surface the CSN query tree as the viewProperties
        # aspect for Views and Analytic Models. Local Tables are base tables
        # (no `query` in their CSN) and are naturally excluded.
        view_properties = self._build_view_properties(csn_def)

        # 2-tier model: parent the dataset directly to the Space container. The
        # object kind (View / Analytical Model) survives as the dataset subtype,
        # which the DataHub UI exposes as a filter facet.
        dataset_parent: ContainerKey = self._space_key(space_name)

        # Build entity-level tags from the parsed entity-scope CDS annotations
        # (Analytics.Dimension/Measure/DimensionType). Field-level tags are
        # attached separately in ``_decorate_fields``.
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
        # NOTE: see _emit_space — we rely on the workunit_processor's automatic
        # add_entity_to_state, both to avoid double-counting and to keep
        # FIX 1's parallel workers thread-safe.
        # Not strictly atomic under threading but ok for a coarse threshold check.
        self._datasets_emitted += 1
        self._check_scale_warning()

    def _build_view_properties(
        self, csn_def: Optional[Dict]
    ) -> Optional[ViewPropertiesClass]:
        """Build a viewProperties aspect from an asset's CSN definition.

        Returns the aspect for Views and Analytic Models, or None when
        view-definition emission is disabled, no CSN was fetched, or the CSN
        has neither raw SQL nor a ``query`` (e.g. base tables).

        SQL views store the raw SQL the modeler wrote in the
        ``@DataWarehouse.sqlEditor.query`` annotation (confirmed against a real
        deployed SQL view); that friendly definition wins and is emitted with
        ``viewLanguage="SQL"``. Graphical/modeled views have no such
        annotation — their definition is the CSN/CQN ``query`` tree, emitted
        with ``viewLanguage="CSN"``.
        """
        if not self.config.include_view_definitions or csn_def is None:
            return None
        sql = csn_def.get("@DataWarehouse.sqlEditor.query")
        if isinstance(sql, str) and sql.strip():
            # SQL view — surface the actual SQL the modeler wrote.
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=sql,
                viewLanguage="SQL",
            )
        query = csn_def.get("query")
        if isinstance(query, dict):
            # Graphical/modeled view — definition is the CSN/CQN query tree.
            return ViewPropertiesClass(
                materialized=False,
                viewLogic=json.dumps(query, indent=2, sort_keys=False),
                viewLanguage="CSN",
            )
        return None

    def _build_dataset_name(
        self,
        space_name: str,
        asset_name: str,
    ) -> str:
        """Construct the dataset-name portion of the URN.

        Format is always ``<space>.<asset>`` (lowercased per
        ``config.convert_urns_to_lowercase``). The platform/platform_instance/env
        that complete the URN are applied separately by the caller from the
        asset's resolved mapping.
        """
        name = f"{space_name}.{asset_name}"
        return self._maybe_lower(name)

    def _qualified_upstream_urn(self, qualified_key: str) -> str:
        """Build a sap-datasphere upstream URN from a fully-qualified
        ``<space>.<object>`` key (e.g. an analytic model's fact/dimension
        ``dataEntity.key``). The key already contains its (possibly different)
        space, so it is used as the URN name directly — NOT re-prefixed with the
        connector's current space (which would corrupt cross-space references)."""
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
        """Call ``_apply_business_layer`` with a degradation guard.

        Mirrors the query-lineage try/except in ``_emit_asset``: a star-schema
        assembly failure (e.g. a malformed ``dataEntity.key`` breaking URN
        construction) must degrade to the already-computed ``query_upstreams``
        rather than propagate to the per-asset isolation handler and drop the
        whole analytic-model dataset.

        NOTE: ``_apply_business_layer`` mutates ``schema_fields`` (measure/
        dimension tags) and ``custom_properties`` (variables) in place; on a
        mid-way exception those mutations may be partial, but the dataset is
        still emitted, which is the primary requirement.
        """
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
        """Wire an analytic model's ``businessLayerDefinitions`` into emission.

        Analytic models are star schemas whose fact + dimension sources live in
        the ``businessLayerDefinitions`` block (a sibling of ``definitions`` in
        the CSN), NOT in the CSN ``query`` the generic walker reads. This helper:

        1. Lineage — builds a table-level ``UpstreamClass`` for the fact AND each
           dimension source via ``_qualified_upstream_urn`` (cross-space safe:
           the source key already carries its own space). When the business
           layer yields upstreams these become the AUTHORITATIVE table-level
           lineage, REPLACING the query-FROM-derived table-level upstreams. The
           query-FROM path may double-prefix the fact's space (e.g.
           ``s1.finance_data.sales_all_ge``), so its table-level upstreams are
           dropped. Column-level fine-grained lineage from the query is kept only
           when every one of its upstream schemaField URNs points at a
           business-layer dataset URN; otherwise it is dropped (it would dangle
           off the double-prefixed fact).
        2. Tags — tags each schema field whose ``fieldPath`` is a measure with
           ``MEASURE_TAG_URN`` and each attribute with ``DIMENSION_TAG_URN``
           (cube elements usually lack ``@Analytics`` flags, so the business
           layer is the primary signal). Existing tags are not duplicated.
        3. Variables — surfaces ``businessLayerDefinitions`` variables as the
           ``sap_variables`` custom property.

        Returns the (possibly replaced) ``UpstreamLineageClass`` to use for the
        dataset; for non-analytic-model objects (no ``businessLayerDefinitions``)
        it returns ``query_upstreams`` unchanged so plain views are unaffected.
        """
        bld = (csn_obj or {}).get("businessLayerDefinitions")
        if not isinstance(bld, dict):
            return query_upstreams
        bl = parse_business_layer(bld, technical_name)

        # 3. Variables.
        if bl.variable_names:
            custom_properties["sap_variables"] = ",".join(bl.variable_names)

        # 2. Measure / dimension field tags (supplement any element-flag tags).
        if self.config.emit_sap_semantics_as_tags and schema_fields:
            measures = set(bl.measure_names)
            attributes = set(bl.attribute_names)
            for field in schema_fields:
                if field.fieldPath in measures:
                    self._add_field_tag(field, MEASURE_TAG_URN)
                if field.fieldPath in attributes:
                    self._add_field_tag(field, DIMENSION_TAG_URN)

        # 1. Star-schema lineage: business layer is authoritative for the
        # table-level upstreams; query-FROM table-level upstreams are suppressed.
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

        # Keep fine-grained lineage only when ALL its upstream schemaField URNs
        # reference a business-layer dataset URN (else it dangles off the
        # double-prefixed fact and is dropped).
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
        """Add a tag URN to a schema field's ``globalTags`` without duplicating
        a tag that is already present."""
        if field.globalTags is None:
            field.globalTags = GlobalTagsClass(tags=[])
        existing = {t.tag for t in field.globalTags.tags}
        if tag_urn not in existing:
            field.globalTags.tags.append(TagAssociationClass(tag=tag_urn))

    @staticmethod
    def _schema_field_parent(schema_field_urn: str) -> str:
        """Return the parent dataset URN of a ``urn:li:schemaField:(<dataset>,<col>)``
        URN, or the input unchanged when it is not a schemaField URN."""
        prefix = "urn:li:schemaField:("
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
        """Extract table-level upstream refs and column-level lineage pairs from CSN
        and assemble them into a single ``UpstreamLineageClass`` aspect.

        Each extractor is guarded independently so a parsing failure on one side
        (table-level vs column-level) still allows the other to be emitted.
        """
        upstream_refs: List[str] = []
        column_pairs: List[ColumnLineagePair] = []
        try:
            upstream_refs = self._lineage_extractor.extract_upstream_refs(csn_def)
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
                pairs=tuple(column_pairs),
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
        upstream_refs: List[str],
        column_lineage: Optional[ColumnLineageContext] = None,
    ) -> Optional[UpstreamLineageClass]:
        """Construct an UpstreamLineageClass aspect.

        Intra-Datasphere lineage is emitted under the same resolved platform so that
        all URNs in the lineage graph are consistent. When ``column_lineage`` is
        provided, ``FineGrainedLineageClass`` entries are appended for each
        downstream column with at least one resolvable upstream column.
        """
        upstreams: List[UpstreamClass] = []
        upstream_urn_by_name: Dict[str, str] = {}
        for ref in upstream_refs:
            upstream_name = self._maybe_lower(f"{space_name}.{ref}")
            upstream_urn = make_dataset_urn_with_platform_instance(
                platform=resolved.platform,
                name=upstream_name,
                platform_instance=resolved.platform_instance,
                env=resolved.env,
            )
            upstream_urn_by_name[ref] = upstream_urn
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
        """Build the FineGrainedLineage entries for column-level lineage.

        For each ``ColumnLineagePair``:
          - Resolve each upstream ref to a schemaField URN via ``upstream_urn_by_name``
          - Skip wildcard ``*`` upstream refs (record to ``report.column_lineage_unresolved``)
          - Skip refs whose upstream qname isn't in the URN map (record to report)
          - Append walker-collected ``unresolved_refs`` (record each to report)
          - If any upstream field URNs survive, emit one ``FineGrainedLineage`` entry
        """
        fine_grained: List[FineGrainedLineageClass] = []
        downstream_dataset_urn = column_lineage.downstream_dataset_urn
        for pair in column_lineage.pairs:
            # Surface walker-level unresolvable refs (unknown alias,
            # 3-segment ref, unnamed expression, malformed CSN markers)
            # via the report so operators can debug silent drops.
            for unresolved_ref in pair.unresolved_refs:
                self.report.column_lineage_unresolved.append(
                    f"{downstream_dataset_urn}#{pair.downstream_col}: {unresolved_ref}"
                )
            upstream_field_urns: List[str] = []
            for upstream_qname, upstream_col in pair.upstream_refs:
                if upstream_col == "*":
                    self.report.column_lineage_unresolved.append(
                        f"{downstream_dataset_urn}#{pair.downstream_col}: "
                        f"<wildcard upstream {upstream_qname}.*>"
                    )
                    continue
                resolved_upstream_urn = upstream_urn_by_name.get(upstream_qname)
                if resolved_upstream_urn is None:
                    self.report.column_lineage_unresolved.append(
                        f"{downstream_dataset_urn}#{pair.downstream_col}: "
                        f"<missing upstream qname {upstream_qname!r} "
                        f"for col {upstream_col!r}>"
                    )
                    continue
                upstream_field_urns.append(
                    make_schema_field_urn(resolved_upstream_urn, upstream_col)
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
                    f"{name}:{edm_type}" for edm_type, name in result.unknown_edm_types
                ),
            )
            self.report.assets_with_unknown_edm_types.append(
                f"{space_name}.{asset_name}"
            )
        return result

    def _decorate_fields(self, result: EdmxParseResult) -> List[SchemaFieldClass]:
        # M4: filter columns by config.column_pattern (defaults to allow-all so
        # behaviour is unchanged unless the user opts in). Filtering is applied
        # to the EDMX property name (fieldPath) before any decoration.
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
            # Also surface field-level CDS semantic annotations as DataHub tags.
            # The same signal is already encoded in the description-suffix above
            # (additive / backward-compatible); tags make it searchable and
            # filterable in DataHub Search.
            self._apply_field_tags(f, field_props)
            decorated.append(f)
        return decorated

    def _apply_field_tags(
        self, field: SchemaFieldClass, field_props: Dict[str, str]
    ) -> None:
        """Attach ``globalTags`` to a schema field from its ``sap_*`` custom
        properties, honouring ``emit_sap_semantics_as_tags``.

        Used by ``_decorate_fields`` (EDMX-sourced) to derive field tags from
        the ``sap_*`` vocabulary.
        """
        if not self.config.emit_sap_semantics_as_tags or not field_props:
            return
        tag_urns = self._tag_urns_for_field_props(field_props)
        if tag_urns:
            field.globalTags = GlobalTagsClass(
                tags=[TagAssociationClass(tag=u) for u in tag_urns]
            )

    def _entity_tag_urns(self, entity_props: Dict[str, str]) -> List[str]:
        """Build dataset-level tag URNs from entity-scope custom properties.

        ``Analytics.Dimension``/``Analytics.Measure`` on the entity (as opposed
        to on a property) are surfaced as dataset tags, so an analytical model
        that's tagged "Measure" at the entity level pivots cross-connector with
        the same flat-namespace URN. ``Analytics.DimensionType`` builds a
        namespaced ``sap:dimension_type:<value>`` tag URN on the fly because
        CDS allows arbitrary values here.
        """
        if not self.config.emit_sap_semantics_as_tags:
            return []
        tag_urns: List[str] = []
        if entity_props.get("sap_is_dimension") == "true":
            tag_urns.append(DIMENSION_TAG_URN)
        if entity_props.get("sap_is_measure") == "true":
            tag_urns.append(MEASURE_TAG_URN)
        sap_dim_type = entity_props.get("sap_dimension_type")
        if sap_dim_type:
            tag_urns.append(sap_dimension_type_tag_urn(sap_dim_type))
        return tag_urns

    @staticmethod
    def _tag_urns_for_field_props(field_props: Dict[str, str]) -> List[str]:
        """Map a field's parsed ``sap_*`` custom properties to the DataHub tag
        URNs they correspond to. Returns an empty list when none apply.

        Kept separate from ``_decorate_fields`` so the mapping logic is unit-
        testable without standing up the full source.
        """
        tag_urns: List[str] = []
        if field_props.get("sap_is_dimension") == "true":
            tag_urns.append(DIMENSION_TAG_URN)
        if field_props.get("sap_is_measure") == "true":
            tag_urns.append(MEASURE_TAG_URN)
        sap_semantic = field_props.get("sap_semantic")
        if sap_semantic == "currency":
            tag_urns.append(SAP_CURRENCY_TAG_URN)
        elif sap_semantic == "unit":
            tag_urns.append(SAP_UNIT_TAG_URN)
        sap_calendar_type = field_props.get("sap_calendar_type")
        if sap_calendar_type and sap_calendar_type in SAP_CALENDAR_TAG_URNS:
            tag_urns.append(SAP_CALENDAR_TAG_URNS[sap_calendar_type])
        return tag_urns

    def _maybe_warn_builtin_defaults_missing_instance(
        self, resolved: ResolvedPlatform
    ) -> None:
        """M5: warn once per platform when an S3/GCS asset routes through the
        built-in defaults table without a platform_instance.

        Without a platform_instance the emitted URN uses the generic ``s3://``
        / ``gs://`` scheme and won't merge with URNs from the operator's
        dedicated S3/GCS DataHub connector. The warning lets them know they
        likely want to override via ``platform_type_defaults`` or
        ``connection_to_platform_map``.
        """
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
        """Emit a single warning when emitted-dataset count crosses 50K with
        stateful ingestion enabled.

        At default GMS / Kafka payload limits the soft-delete checkpoint MCP
        starts to approach the ~80K-URN ceiling around here; we warn early so
        operators can switch to manual cleanup or partition the run.
        """
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
        """Validate credentials + tenant URL and probe the spaces endpoint.

        Returns a :class:`TestConnectionReport` with:
          * ``basic_connectivity`` — auth + reachability of the tenant URL.
          * ``capability_report[CONTAINERS]`` — whether we can actually list
            spaces (the lightest catalog-API call).
        """
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
