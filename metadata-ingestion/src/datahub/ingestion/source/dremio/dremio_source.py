import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_aspects import DremioAspects
from datahub.ingestion.source.dremio.dremio_config import (
    DremioSourceConfig,
    DremioSourceMapping,
)
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping,
)
from datahub.ingestion.source.dremio.dremio_entities import (
    DremioCatalog,
    DremioContainer,
    DremioDataset,
    DremioGlossaryTerm,
    DremioQuery,
    DremioSourceContainer,
)
from datahub.ingestion.source.dremio.dremio_models import DremioDatasetType
from datahub.ingestion.source.dremio.dremio_profiling import (
    DremioProfiler,
    ProfileTarget,
    build_profile_target,
)
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantQueriesRunSkipHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import (
    LINEAGE_EXTRACTION,
    METADATA_EXTRACTION,
    PROFILING,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import SchemaMetadataClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing._models import _TableName
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)

# Dremio uses 'dremio' as the default database name in all SQL contexts
DREMIO_DATABASE_NAME = "dremio"


def passes_dremio_filters(
    name: str,
    catalog_dataset_names: Set[str],
    dataset_pattern: AllowDenyPattern,
    schema_pattern: AllowDenyPattern,
) -> bool:
    """Apply the catalog walk's filters to a name discovered via query/view
    lineage so SQL-derived URNs can't produce ghost datasets the operator
    excluded via schema_pattern / dataset_pattern."""
    # Pattern matching is done against the post-`dremio.` form.
    if name.startswith(f"{DREMIO_DATABASE_NAME}."):
        name = name[len(DREMIO_DATABASE_NAME) + 1 :]

    if name in catalog_dataset_names:
        return True

    path_parts = name.split(".")
    # Dremio reflections under _accelerator_ are internal, never user-facing.
    if path_parts and path_parts[0] == "_accelerator_":
        return False

    if not dataset_pattern.allowed(name):
        return False

    # Schema gate uses `.allowed` directly: should_include_container has
    # report side-effects we don't want at lineage-emission time.
    if len(path_parts) > 1:
        container_path = ".".join(path_parts[:-1])
        if not schema_pattern.allowed(container_path):
            return False

    return True


class DremioSchemaResolver(SchemaResolver):
    """Custom schema resolver for Dremio multi-part table names.

    Dremio uses 'dremio' as the database in all URNs. For multi-part tables (>3 parts),
    uses table.parts to preserve the full hierarchy: dremio.part1.part2.part3.table

    Note: Folder names containing dots (e.g., "folder.with.dots") create ambiguous URNs
    in SQL parsing since dots are path delimiters. However, catalog-based ingestion
    correctly handles these via the Dremio API's incremental path resolution. See
    dremio_api.DremioAPIOperations.get_dataset_id() for implementation details.
    """

    def _get_table_name_parts(self, table: _TableName) -> List[Optional[str]]:
        if table.parts:
            return [DREMIO_DATABASE_NAME, *table.parts]
        elif table.database and table.database.lower() != DREMIO_DATABASE_NAME:
            return [
                DREMIO_DATABASE_NAME,
                table.database,
                table.db_schema,
                table.table,
            ]
        else:
            return [
                table.database or DREMIO_DATABASE_NAME,
                table.db_schema,
                table.table,
            ]

    def _construct_table_name(self, table: _TableName) -> str:
        parts = self._get_table_name_parts(table)
        return ".".join(filter(None, parts))

    def get_urn_for_table(
        self, table: _TableName, lower: bool = False, mixed: bool = False
    ) -> str:
        table_name = self._construct_table_name(table)
        platform_instance = self.platform_instance

        if lower:
            table_name = table_name.lower()
            if not mixed:
                platform_instance = (
                    platform_instance.lower() if platform_instance else None
                )

        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=table_name,
            env=self.env,
            platform_instance=platform_instance,
        )
        return urn


@dataclass
class DremioSourceMapEntry:
    platform: str
    source_name: str
    dremio_source_category: str
    root_path: str = ""
    database_name: str = ""
    platform_instance: Optional[str] = None
    env: Optional[str] = None


@platform_name("Dremio")
@config_class(DremioSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.DREMIO_SPACE,
        SourceCapabilityModifier.DREMIO_SOURCE,
    ],
)
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Extract column-level lineage",
    subtype_modifier=[
        SourceCapabilityModifier.TABLE,
    ],
)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.USAGE_STATS, "Enabled by default to get usage stats")
@capability(
    SourceCapability.OPERATION_CAPTURE,
    "Optionally enabled via `include_query_lineage`; generated from Dremio job history",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
    supported=True,
)
class DremioSource(StatefulIngestionSourceBase):
    """
    Source that extracts metadata from Dremio via REST API and SQL queries.

    Implementation notes:
    - Uses Dremio's REST API v3 for catalog metadata
    - Supports both Dremio Cloud (project-based) and on-premise instances
    - Uses SQLAlchemy for optional profiling queries
    - Parses SQL job history for query-based lineage when enabled
    """

    config: DremioSourceConfig
    report: DremioSourceReport

    def __init__(self, config: DremioSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.default_db = DREMIO_DATABASE_NAME
        self.config = config
        self.report = DremioSourceReport()

        self.report.window_start_time, self.report.window_end_time = (
            self.config.start_time,
            self.config.end_time,
        )

        self.source_map: Dict[str, DremioSourceMapEntry] = dict()

        api = DremioAPIOperations(self.config, self.report)
        self.source_type_mapper = api.source_type_mapper
        self.dremio_catalog = DremioCatalog(api)

        # Full URNs don't need graph resolution — only build a registry for bare names.
        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain and not self.config.domain.startswith("urn:li:domain:"):
            self.domain_registry = DomainRegistry(
                cached_domains=[self.config.domain], graph=self.ctx.graph
            )

        self.dremio_aspects = DremioAspects(
            platform=self.get_platform(),
            domain=self.config.domain,
            domain_registry=self.domain_registry,
            ingest_owner=self.config.ingest_owner,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            ui_url=api.ui_url,
        )

        # Custom resolver handles the "dremio." infix injected after platform_instance.
        self.dremio_schema_resolver = DremioSchemaResolver(
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )

        # Populated during the catalog walk; read by the aggregator's
        # is_allowed_table callback, so must exist before construction.
        self.catalog_dataset_names: Set[str] = set()

        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.get_platform()),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.dremio_schema_resolver,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
            # Gate SQL-discovered URNs through the catalog-walk filters.
            is_allowed_table=self._is_allowed_table,
        )
        self.report.sql_aggregator = self.sql_parsing_aggregator.report

        profiling_handler: Optional[ProfilingHandler] = None
        if config.stateful_ingestion and config.stateful_ingestion.enabled:
            profiling_handler = ProfilingHandler(
                source=self,
                config=config,
                pipeline_name=ctx.pipeline_name,
                run_id=ctx.run_id,
            )
        self.profiler = DremioProfiler(config, self.report, api, profiling_handler)

    def _is_allowed_table(self, name: str) -> bool:
        allowed = passes_dremio_filters(
            name=name,
            catalog_dataset_names=self.catalog_dataset_names,
            dataset_pattern=self.config.dataset_pattern,
            schema_pattern=self.config.schema_pattern,
        )
        if not allowed:
            self.report.lineage_dropped_filtered += 1
        return allowed

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "DremioSource":
        config = DremioSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return DREMIO_DATABASE_NAME

    def _build_source_map(self) -> Dict[str, DremioSourceMapEntry]:
        dremio_sources = list(self.dremio_catalog.get_sources())
        source_mappings_config = self.config.source_mappings or []

        source_map = build_dremio_source_map(
            dremio_sources,
            source_mappings_config,
            source_type_mapper=self.source_type_mapper,
        )
        logger.info(f"Full source map: {source_map}")

        self._validate_source_mappings(source_map)

        return source_map

    def _validate_source_mappings(
        self, source_map: Dict[str, DremioSourceMapEntry]
    ) -> None:
        for source_name, mapping in source_map.items():
            if (
                mapping.platform
                and mapping.platform.lower() != "dremio"
                and not mapping.platform_instance
            ):
                self.report.warning(
                    "Cross-platform lineage warning",
                    f"Source '{source_name}' maps to platform '{mapping.platform}' but has no "
                    f"platform_instance configured. This may cause URN mismatches with the upstream "
                    f"connector. Consider adding platform_instance to source_mappings configuration.",
                )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.source_map = self._build_source_map()

        with self.report.new_stage(METADATA_EXTRACTION):
            containers = self.dremio_catalog.get_containers()
            for container in containers:
                try:
                    yield from self.process_container(container)
                    logger.info(
                        f"Dremio container {container.container_name} emitted successfully"
                    )
                except Exception as exc:
                    self.report.num_containers_failed += 1
                    self.report.report_failure(
                        message="Failed to process Dremio container",
                        context=f"{'.'.join(container.path)}.{container.container_name}",
                        exc=exc,
                    )

            # Single pass over the catalog: project to slim ProfileTarget
            # tuples and harvest glossary terms inline. Both follow-up passes
            # would otherwise re-run the global catalog query, and pinning
            # full DremioDataset objects for 10k+ catalogs is expensive.
            # Dedup glossary terms by string because DremioGlossaryTerm has
            # no __eq__/__hash__.
            profiling_enabled = self.config.is_profiling_enabled()
            profile_targets: Optional[List[ProfileTarget]] = (
                [] if profiling_enabled else None
            )
            glossary_terms_seen: Set[str] = set()
            glossary_terms_collected: List[DremioGlossaryTerm] = []
            for dataset_info in self.dremio_catalog.get_datasets():
                for glossary_term in dataset_info.glossary_terms:
                    if glossary_term.glossary_term not in glossary_terms_seen:
                        glossary_terms_seen.add(glossary_term.glossary_term)
                        glossary_terms_collected.append(glossary_term)
                try:
                    yield from self.process_dataset(dataset_info)
                    logger.info(
                        f"Dremio dataset {'.'.join(dataset_info.path)}.{dataset_info.resource_name} emitted successfully"
                    )
                except Exception as exc:
                    self.report.num_datasets_failed += 1
                    self.report.report_failure(
                        message="Failed to process Dremio dataset",
                        context=f"{'.'.join(dataset_info.path)}.{dataset_info.resource_name}",
                        exc=exc,
                    )
                    # Don't queue a profile for an emission that already failed.
                    continue
                if profile_targets is not None and dataset_info.columns:
                    profile_targets.append(
                        build_profile_target(
                            dataset_info, self._make_dataset_urn(dataset_info)
                        )
                    )

            for glossary_term in glossary_terms_collected:
                try:
                    yield from self.process_glossary_term(glossary_term)
                except Exception as exc:
                    self.report.report_failure(
                        message="Failed to process Glossary terms",
                        context=f"{glossary_term.glossary_term}",
                        exc=exc,
                    )

            if self.config.include_query_lineage:
                with self.report.new_stage(LINEAGE_EXTRACTION):
                    self.get_query_lineage_workunits()

            for mcp in self.sql_parsing_aggregator.gen_metadata():
                yield mcp.as_workunit()

            if profiling_enabled and profile_targets is not None:
                with (
                    self.report.new_stage(PROFILING),
                    ThreadPoolExecutor(
                        max_workers=self.config.profiling.max_workers
                    ) as executor,
                ):
                    future_to_target = {
                        executor.submit(self.generate_profiles, target): target
                        for target in profile_targets
                    }

                    for future in as_completed(future_to_target):
                        target = future_to_target[future]
                        try:
                            yield from future.result()
                        except Exception as exc:
                            self.report.profiling_skipped_other[
                                target.resource_name
                            ] += 1
                            self.report.report_failure(
                                message="Failed to profile dataset",
                                context=target.full_table_name,
                                exc=exc,
                            )

    def process_container(
        self, container_info: DremioContainer
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a Dremio container and generate metadata workunits.
        """
        container_urn = self.dremio_aspects.get_container_urn(
            path=container_info.path, name=container_info.container_name
        )

        yield from self.dremio_aspects.populate_container_mcp(
            container_urn, container_info
        )

    def process_dataset(
        self, dataset_info: DremioDataset
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a Dremio dataset and generate metadata workunits.
        """

        schema_str = ".".join(dataset_info.path)

        dataset_name = f"{schema_str}.{dataset_info.resource_name}".lower()

        # Drop Dremio Reflections — _accelerator_ holds internal acceleration
        # structures that should never surface in the DataHub catalog.
        if dataset_info.path and dataset_info.path[0] == "_accelerator_":
            self.report.report_dropped(f"Skipping Dremio reflection: {dataset_name}")
            return

        self.report.report_entity_scanned(dataset_name, dataset_info.dataset_type.value)
        if not self.config.dataset_pattern.allowed(dataset_name):
            self.report.report_dropped(dataset_name)
            return

        self.catalog_dataset_names.add(dataset_name)

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=f"{DREMIO_DATABASE_NAME}.{dataset_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        for dremio_mcp in self.dremio_aspects.populate_dataset_mcp(
            dataset_urn, dataset_info
        ):
            yield dremio_mcp
            if isinstance(
                dremio_mcp.metadata, MetadataChangeProposalWrapper
            ) and isinstance(dremio_mcp.metadata.aspect, SchemaMetadataClass):
                self.sql_parsing_aggregator.register_schema(
                    urn=dataset_urn,
                    schema=dremio_mcp.metadata.aspect,
                )

        if dataset_info.dataset_type == DremioDatasetType.VIEW:
            if (
                self.dremio_catalog.edition == DremioEdition.ENTERPRISE
                and dataset_info.parents
            ):
                yield from self.generate_view_lineage(
                    parents=dataset_info.parents,
                    dataset_urn=dataset_urn,
                )

            if dataset_info.sql_definition:
                self.sql_parsing_aggregator.add_view_definition(
                    view_urn=dataset_urn,
                    view_definition=dataset_info.sql_definition,
                    default_db=self.default_db,
                    default_schema=dataset_info.default_schema,
                )

        elif dataset_info.dataset_type == DremioDatasetType.TABLE:
            dremio_source = dataset_info.path[0] if dataset_info.path else None

            if dremio_source:
                upstream_urn = self._map_dremio_dataset_to_urn(
                    dremio_source=dremio_source,
                    dremio_path=dataset_info.path,
                    dremio_dataset=dataset_info.resource_name,
                )
                logger.debug(f"Upstream dataset for {dataset_urn}: {upstream_urn}")

                if upstream_urn:
                    upstream_lineage = UpstreamLineage(
                        upstreams=[
                            UpstreamClass(
                                dataset=upstream_urn,
                                type=DatasetLineageTypeClass.COPY,
                            )
                        ]
                    )
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=upstream_lineage,
                    )
                    yield mcp.as_workunit()
                    self.sql_parsing_aggregator.add_known_lineage_mapping(
                        upstream_urn=upstream_urn,
                        downstream_urn=dataset_urn,
                        lineage_type=DatasetLineageTypeClass.COPY,
                    )

    def process_glossary_term(
        self, glossary_term_info: DremioGlossaryTerm
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a Dremio container and generate metadata workunits.
        """

        yield from self.dremio_aspects.populate_glossary_term_mcp(glossary_term_info)

    def _make_dataset_urn(self, dataset_info: DremioDataset) -> str:
        schema_str = ".".join(dataset_info.path)
        dataset_name = f"{schema_str}.{dataset_info.resource_name}".lower()
        return make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=f"{DREMIO_DATABASE_NAME}.{dataset_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

    def generate_profiles(self, target: ProfileTarget) -> Iterable[MetadataWorkUnit]:
        yield from self.profiler.get_workunits(target)

    def generate_view_lineage(
        self, dataset_urn: str, parents: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """Generate lineage for views, dropping parents excluded by the
        catalog walk's filters so we don't emit ghost upstream datasets."""
        upstream_urns: List[str] = []
        for upstream_table in parents:
            upstream_name = upstream_table.lower()
            if not passes_dremio_filters(
                name=upstream_name,
                catalog_dataset_names=self.catalog_dataset_names,
                dataset_pattern=self.config.dataset_pattern,
                schema_pattern=self.config.schema_pattern,
            ):
                self.report.lineage_dropped_filtered += 1
                continue
            upstream_urns.append(
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(self.get_platform()),
                    name=f"{DREMIO_DATABASE_NAME}.{upstream_name}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
            )

        if not upstream_urns:
            return

        lineage = UpstreamLineage(
            upstreams=[
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.VIEW,
                )
                for upstream_urn in upstream_urns
            ]
        )
        mcp = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=lineage,
        )

        for upstream_urn in upstream_urns:
            self.sql_parsing_aggregator.add_known_lineage_mapping(
                upstream_urn=upstream_urn,
                downstream_urn=dataset_urn,
                lineage_type=DatasetLineageTypeClass.VIEW,
            )

        yield MetadataWorkUnit(id=f"{dataset_urn}-upstreamLineage", mcp=mcp)

    def get_query_lineage_workunits(self) -> None:
        """
        Process query lineage information.
        """
        effective_start = self.config.start_time
        effective_end = self.config.end_time
        redundant_handler: Optional[RedundantQueriesRunSkipHandler] = None

        if self.config.enable_stateful_time_window:
            redundant_handler = RedundantQueriesRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )
            if redundant_handler.should_skip_this_run(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            ):
                self.report.info(
                    "Skipping query lineage/usage extraction: the current time window "
                    f"({self.config.start_time} – {self.config.end_time}) was already "
                    "fully processed in a previous run.",
                )
                return

            # Advance start_time to the end of the previous run so only new
            # job history is fetched.
            effective_start, effective_end = redundant_handler.suggest_run_time_window(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            )
            logger.info(
                f"Effective query lineage window: {effective_start} – {effective_end} "
                f"(original: {self.config.start_time} – {self.config.end_time})"
            )

        queries = self.dremio_catalog.get_queries(
            start_time=effective_start, end_time=effective_end
        )

        for query in queries:
            try:
                self.process_query(query)
            except Exception as exc:
                self.report.report_failure(
                    message="Failed to process dremio query",
                    context=f"{query.job_id}: {exc}",
                    exc=exc,
                )

        # Record the time window after successful processing so subsequent runs
        # can skip or advance past it.
        if redundant_handler is not None:
            redundant_handler.update_state(
                start_time=effective_start,
                end_time=effective_end,
                bucket_duration=self.config.bucket_duration,
            )

    def _validate_query_lineage_format(self, query: DremioQuery) -> None:
        for queried_ds in query.queried_datasets:
            queried_ds_lower = queried_ds.lower()

            # Check if this dataset exists in our catalog tracking
            if queried_ds_lower not in self.catalog_dataset_names:
                # Check for suspicious format patterns that indicate mismatch
                suspicious_patterns = [
                    ("s3://", "S3 path"),
                    ("hdfs://", "HDFS path"),
                    ("/", "file path"),
                    ("@", "versioned reference"),
                ]

                for pattern, pattern_type in suspicious_patterns:
                    if pattern in queried_ds_lower:
                        self.report.warning(
                            "Query lineage format mismatch",
                            f"Query {query.job_id} references dataset '{queried_ds}' which appears to be a "
                            f"{pattern_type} but was not found in the catalog. This may cause lineage breaks. "
                            f"Verify that source_mappings configuration correctly maps Dremio sources to "
                            f"upstream platforms, or that query logs use the same naming as catalog metadata.",
                        )
                        break
                else:
                    # Not suspicious pattern, just not in catalog yet (might be from different space/source)
                    logger.debug(
                        f"Query {query.job_id} references dataset '{queried_ds}' not found in catalog. "
                        f"This may be expected for cross-source references or if the dataset was filtered out."
                    )

    def process_query(self, query: DremioQuery) -> None:
        if query.query and query.affected_dataset:
            # Validate query dataset format matches catalog format
            self._validate_query_lineage_format(query)

            # Pre-filter explicit upstreams/downstream; is_allowed_table
            # is the second line of defence for URNs the SQL parser
            # rediscovers inside add_observed_query.
            downstream_name = query.affected_dataset.lower()
            downstream_allowed = passes_dremio_filters(
                name=downstream_name,
                catalog_dataset_names=self.catalog_dataset_names,
                dataset_pattern=self.config.dataset_pattern,
                schema_pattern=self.config.schema_pattern,
            )

            upstream_urns: List[str] = []
            for ds in query.queried_datasets:
                ds_lower = ds.lower()
                if not passes_dremio_filters(
                    name=ds_lower,
                    catalog_dataset_names=self.catalog_dataset_names,
                    dataset_pattern=self.config.dataset_pattern,
                    schema_pattern=self.config.schema_pattern,
                ):
                    self.report.lineage_dropped_filtered += 1
                    continue
                upstream_urns.append(
                    make_dataset_urn_with_platform_instance(
                        platform=make_data_platform_urn(self.get_platform()),
                        name=f"{DREMIO_DATABASE_NAME}.{ds_lower}",
                        env=self.config.env,
                        platform_instance=self.config.platform_instance,
                    )
                )

            if downstream_allowed and upstream_urns:
                downstream_urn = make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(self.get_platform()),
                    name=f"{DREMIO_DATABASE_NAME}.{downstream_name}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                self.sql_parsing_aggregator.add_known_query_lineage(
                    KnownQueryLineageInfo(
                        query_text=query.query,
                        upstreams=upstream_urns,
                        downstream=downstream_urn,
                    ),
                    merge_lineage=True,
                )
            elif not downstream_allowed:
                # One increment per query whose downstream is filtered.
                # Per-upstream drops were already counted in the loop above;
                # this is intentionally not multiplied by the upstream count
                # — see lineage_dropped_filtered's docstring for the "lineage
                # references, not edges" semantics.
                self.report.lineage_dropped_filtered += 1

        # Always register the observed query; usage stats aren't gated here.
        # Aspects for filtered URNs that the SQL parser rediscovers are blocked
        # by SqlParsingAggregator's is_allowed_table gate at emission time.
        self.sql_parsing_aggregator.add_observed_query(
            ObservedQuery(
                query=query.query,
                timestamp=query.submitted_ts,
                user=CorpUserUrn(username=query.username),
                default_db=self.default_db,
            )
        )

    def _map_dremio_dataset_to_urn(
        self,
        dremio_source: str,
        dremio_path: List[str],
        dremio_dataset: str,
    ) -> Optional[str]:
        """
        Map a Dremio dataset to a DataHub URN.
        """
        mapping = self.source_map.get(dremio_source.lower())
        if not mapping:
            return None

        platform = mapping.platform
        if not platform:
            return None

        platform_instance = mapping.platform_instance
        env = mapping.env or self.config.env

        root_path = ""
        database_name = ""

        if mapping.dremio_source_category == "file_object_storage":
            if mapping.root_path:
                root_path = f"{mapping.root_path[1:]}/"
            dremio_dataset = f"{root_path}{'/'.join(dremio_path[1:])}/{dremio_dataset}"
        else:
            if mapping.database_name:
                database_name = f"{mapping.database_name}."
            dremio_dataset = (
                f"{database_name}{'.'.join(dremio_path[1:])}.{dremio_dataset}"
            )

        if platform_instance:
            return make_dataset_urn_with_platform_instance(
                platform=platform.lower(),
                name=dremio_dataset,
                platform_instance=platform_instance,
                env=env,
            )

        return make_dataset_urn_with_platform_instance(
            platform=platform.lower(),
            name=dremio_dataset,
            platform_instance=None,
            env=env,
        )

    def get_report(self) -> SourceReport:
        """
        Get the source report.
        """
        return self.report


def build_dremio_source_map(
    dremio_sources: Iterable[DremioSourceContainer],
    source_mappings_config: List[DremioSourceMapping],
    source_type_mapper: Optional[DremioToDataHubSourceTypeMapping] = None,
) -> Dict[str, DremioSourceMapEntry]:
    """
    Builds a source mapping dictionary to support external lineage generation across
    multiple Dremio sources, based on provided configuration mappings.

    This method operates as follows:

    Returns:
        Dict[str, Dict]: A dictionary (`source_map`) where each key is a source name
                        (lowercased) and each value is another entry containing:
                        - `platform`: The source platform.
                        - `source_name`: The source name.
                        - `dremio_source_category`: The type mapped to DataHub,
                        e.g., "database", "folder".
                        - Optional `root_path`, `database_name`, `platform_instance`,
                        and `env` if provided in the configuration.
    Example:
        This method is used internally within the class to generate mappings before
        creating cross-platform lineage.

    """
    mapper = source_type_mapper or DremioToDataHubSourceTypeMapping()

    source_map = {}
    for source in dremio_sources:
        current_source_name = source.container_name

        source_type = source.dremio_source_type.lower()
        source_category = mapper.lookup_category(source_type)
        datahub_platform = mapper.lookup_datahub_platform(source_type)
        root_path = source.root_path.lower() if source.root_path else ""
        database_name = source.database_name.lower() if source.database_name else ""
        source_present = False

        for mapping in source_mappings_config:
            if mapping.source_name.lower() == current_source_name.lower():
                source_map[current_source_name.lower()] = DremioSourceMapEntry(
                    platform=mapping.platform,
                    source_name=mapping.source_name,
                    dremio_source_category=source_category,
                    root_path=root_path,
                    database_name=database_name,
                    platform_instance=mapping.platform_instance,
                    env=mapping.env,
                )
                source_present = True
                break

        if not source_present:
            source_map[current_source_name.lower()] = DremioSourceMapEntry(
                platform=datahub_platform,
                source_name=current_source_name,
                dremio_source_category=source_category,
                root_path=root_path,
                database_name=database_name,
                platform_instance=None,
                env=None,
            )

    return source_map
