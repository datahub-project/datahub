import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

import sqlglot
from sqlalchemy.dialects.oracle.base import OracleDialect

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
    is_dremio_system_name,
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
from datahub.ingestion.source.dremio.dremio_naming import (
    DREMIO_DATABASE_NAME,
    DremioSchemaResolver,
    dremio_dataset_name,
    is_dremio_platform,
)
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
from datahub.sql_parsing.sql_parsing_aggregator import (
    ObservedQuery,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement
from datahub.utilities.registries.domain_registry import DomainRegistry

logger = logging.getLogger(__name__)


def normalize_dremio_name(name: str) -> str:
    """The form catalog_dataset_names uses: lowercased, one leading `dremio.`
    removed."""
    return name.lower().removeprefix(f"{DREMIO_DATABASE_NAME}.")


def passes_dremio_filters(
    name: str,
    catalog_dataset_names: Set[str],
    dataset_pattern: AllowDenyPattern,
    schema_pattern: AllowDenyPattern,
) -> bool:
    """Apply the catalog walk's filters to a name discovered via query/view
    lineage so SQL-derived URNs can't produce ghost datasets the operator
    excluded via schema_pattern / dataset_pattern."""
    name = normalize_dremio_name(name)

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


# The Oracle and SAP HANA connectors read names through SQLAlchemy dialects
# whose normalize_name lowercases an ALL-UPPERCASE name unless it would then
# need quoting (a reserved word, or a character such as `#`).
# ponytail: HANA uses Oracle's reserved words here; a word reserved in only one
# of them keeps the wrong case. Use the sqlalchemy-hana dialect if that matters.
_NORMALIZING_DIALECT = OracleDialect()


def _match_connector_urn_case(
    platform: str, parts: List[str], convert_urns_to_lowercase: Optional[bool]
) -> List[str]:
    """Lowercase the name parts when asked to, or by default for Snowflake.
    By default, Oracle's and SAP HANA's ALL-UPPERCASE parts are lowercased like
    SQLAlchemy's normalize_name. Every other platform keeps Dremio's case."""
    platform = platform.lower()
    if convert_urns_to_lowercase is not None:
        return [p.lower() for p in parts] if convert_urns_to_lowercase else parts
    if platform == "snowflake":  # convert_urns_to_lowercase defaults to True
        return [p.lower() for p in parts]
    if platform in ("oracle", "hana"):
        return [str(_NORMALIZING_DIALECT.normalize_name(p)) for p in parts]
    return parts


@dataclass
class DremioSourceMapEntry:
    platform: str
    source_name: str
    dremio_source_category: str
    root_path: str = ""
    database_name: str = ""
    platform_instance: Optional[str] = None
    env: Optional[str] = None
    convert_urns_to_lowercase: Optional[bool] = None
    # None: unknown, so skip the database only if the path starts with it.
    database_in_path: Optional[bool] = None


def map_dremio_dataset_to_urn(
    mapping: Optional[DremioSourceMapEntry],
    dremio_path: List[str],
    dremio_dataset: str,
    default_env: str,
) -> Optional[str]:
    """The upstream URN on the mapped platform for a Dremio table."""
    if not mapping or not mapping.platform:
        return None
    platform = mapping.platform

    if is_dremio_platform(platform):
        # A Dremio-to-Dremio source shows the remote Dremio's catalog.
        name = dremio_dataset_name([*dremio_path[1:], dremio_dataset])
    elif mapping.dremio_source_category == "file_object_storage":
        # Object keys are case-sensitive; a root path of `/` adds nothing.
        name = "/".join(
            part
            for part in [mapping.root_path.strip("/"), *dremio_path[1:], dremio_dataset]
            if part
        )
    else:
        parts = [*dremio_path[1:], dremio_dataset]
        database = mapping.database_name
        if database and (
            mapping.database_in_path is False
            or (
                mapping.database_in_path is None
                and (len(parts) < 2 or parts[0].lower() != database.lower())
            )
        ):
            # The Db2 connector uppercases the database.
            parts.insert(0, database.upper() if platform.lower() == "db2" else database)
        name = ".".join(
            _match_connector_urn_case(
                platform, parts, mapping.convert_urns_to_lowercase
            )
        )

    return make_dataset_urn_with_platform_instance(
        platform=platform.lower(),
        name=name,
        platform_instance=mapping.platform_instance,
        env=mapping.env or default_env,
    )


@platform_name("Dremio")
@config_class(DremioSourceConfig)
@support_status(SupportStatus.GA)
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
        self.dremio_api = api
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
        # Views whose current definition the catalog walk registered with the
        # aggregator; observed CREATE VIEW DDL for them is stale.
        self._views_with_definition: Set[str] = set()
        self._names_not_in_catalog: Set[str] = set()
        self._unresolved_names: Set[str] = set()
        self._fallback_names_allowed = False

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
            format_queries=False,
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
        name = name.lower()
        if self.config.platform_instance:
            # The aggregator strips the instance case-sensitively, but the
            # schema resolver lowercases it, so a mixed-case one stays on.
            name = name.removeprefix(f"{self.config.platform_instance.lower()}.")
        name = normalize_dremio_name(name)
        # The aggregator only calls this after the catalog walk, so a name
        # that passes the filters but isn't in the catalog was dropped (or was
        # temporary) since the job ran; letting it through resurrects it.
        if name in self.catalog_dataset_names:
            return True
        if name.startswith(f"{DREMIO_DATABASE_NAME}."):
            # A two-part reference qualified twice by the schema resolver: the
            # aggregator would emit it as a `dremio.dremio.` ghost URN. Listed
            # apart from dropped tables because the table may well exist.
            if name not in self._unresolved_names:
                self._unresolved_names.add(name)
                self.report.lineage_dropped_unresolved_names.append(name)
            return False
        # The catalog query never returns system tables, whatever
        # include_system_tables says, so they are never in the catalog.
        if is_dremio_system_name(name) or not passes_dremio_filters(
            name=name,
            catalog_dataset_names=self.catalog_dataset_names,
            dataset_pattern=self.config.dataset_pattern,
            schema_pattern=self.config.schema_pattern,
        ):
            self.report.lineage_dropped_filtered += 1
            return False
        # Live, but returned without column rows, so the walk didn't emit it.
        if name in self.dremio_api.datasets_without_columns:
            return True
        # The name may be a live table the walk never saw; dropping it would
        # lose its lineage for good once the query time window advances, so
        # fall back to the filters alone.
        if self._catalog_names_incomplete():
            self._fallback_names_allowed = True
            return True
        if name not in self._names_not_in_catalog:
            self._names_not_in_catalog.add(name)
            self.report.lineage_dropped_not_in_catalog.append(name)
        return False

    def _catalog_names_incomplete(self) -> bool:
        # An empty catalog with query lineage on is far more likely a fetch
        # that silently returned nothing than a Dremio with no datasets. A walk
        # that only found column-less datasets did return something.
        return self.report.dataset_fetch_incomplete or not (
            self.catalog_dataset_names or self.dremio_api.datasets_without_columns
        )

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
        listed = {m.source_name.lower() for m in self.config.source_mappings or []}
        for source_name in sorted(listed - source_map.keys()):
            # A schema_pattern that allows only paths below the source also
            # filters out the source container, so it never reaches the map.
            self.report.warning(
                message="source_mappings entry matches no ingested Dremio source, so no upstream lineage is emitted for it. Check that schema_pattern allows the source itself, e.g. ^MY_SRC($|\\..*)",
                context=f"source={source_name}",
            )
        for source_name, mapping in source_map.items():
            if mapping.dremio_source_category == "unknown":
                self.report.warning(
                    message="Dremio source type has no built-in category, so its upstream URNs may be wrong. Register it with source_type_mappings",
                    context=f"source={source_name}, platform={mapping.platform}",
                )
            if (
                source_name in listed
                and mapping.platform
                and not is_dremio_platform(mapping.platform)
                and not mapping.platform_instance
            ):
                self.report.warning(
                    message="Cross-platform source mapping has no platform_instance configured, this may cause URN mismatches with the upstream connector",
                    context=f"source={source_name}, platform={mapping.platform}",
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
                    self.report.failure(
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
                    self.report.failure(
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
                    self.report.failure(
                        message="Failed to process Glossary terms",
                        context=f"{glossary_term.glossary_term}",
                        exc=exc,
                    )

            if self.config.include_query_lineage:
                with self.report.new_stage(LINEAGE_EXTRACTION):
                    self.get_query_lineage_workunits()

            for mcp in self.sql_parsing_aggregator.gen_metadata():
                yield mcp.as_workunit()
            if self.config.include_query_lineage:
                # After gen_metadata(): the aggregator applies
                # _is_allowed_table while generating.
                self._warn_query_lineage_scope()

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
                            self.report.failure(
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

        dataset_urn = self._make_dataset_urn(dataset_info)

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
                self._views_with_definition.add(dataset_name)
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
                    self.report.source_mapping_sample_upstream_urns.setdefault(
                        dremio_source.lower(), upstream_urn
                    )
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
        return make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=dremio_dataset_name([*dataset_info.path, dataset_info.resource_name]),
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
                    name=dremio_dataset_name([upstream_name]),
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
                self.report.failure(
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
                            message="Query lineage format mismatch: dataset not found in catalog, this may cause lineage breaks",
                            context=f"job_id={query.job_id}, dataset={queried_ds}, detected_type={pattern_type}",
                        )
                        break
                else:
                    # Not suspicious pattern, just not in catalog yet (might be from different space/source)
                    logger.debug(
                        f"Query {query.job_id} references dataset '{queried_ds}' not found in catalog. "
                        f"This may be expected for cross-source references or if the dataset was filtered out."
                    )

    def _warn_query_lineage_scope(self) -> None:
        # Only when a name actually took the fallback: an empty catalog is
        # also what patterns that exclude everything produce.
        if self._fallback_names_allowed:
            self.report.warning(
                title="Query lineage not limited to the catalog",
                message="The Dremio dataset fetch was incomplete or returned no "
                "datasets, so query lineage is not limited to datasets found in "
                "the catalog this run. Tables dropped since their jobs ran may "
                "reappear until the next complete run.",
                context="query_lineage",
            )
        elif self._names_not_in_catalog:
            self.report.warning(
                title="Query lineage dropped for tables not in the catalog",
                message="Job history references tables that pass schema_pattern "
                "and dataset_pattern but that the catalog walk did not find, so "
                "their lineage, usage and operations were not emitted. They may "
                "have been dropped or been temporary, or the ingestion user may "
                "not be able to see them. See lineage_dropped_not_in_catalog in "
                "the report for the names.",
                context=f"{len(self._names_not_in_catalog)} tables",
            )

    def _is_stale_view_ddl(self, sql: str) -> bool:
        """A CREATE [OR REPLACE] VIEW of a view whose current definition the
        catalog registered. Its upstreams may be outdated; the catalog's are not.
        Views without a registered definition keep their job-history lineage."""
        # Cheap pre-check: parsing returns a copy of the AST, too costly to pay
        # on every query in job history.
        if not self._views_with_definition or "view" not in sql.lower():
            return False
        try:
            statement = parse_statement(sql, get_dialect(self.get_platform()))
        except Exception:
            # This only decides whether to skip; the aggregator re-parses the
            # text and reports any failure.
            return False
        if not (isinstance(statement, sqlglot.exp.Create) and statement.kind == "VIEW"):
            return False
        # A column list wraps the view's name in a Schema.
        target = statement.this
        if isinstance(target, sqlglot.exp.Schema):
            target = target.this
        if not isinstance(target, sqlglot.exp.Table):
            return False
        name = ".".join(part.name for part in target.parts)
        return normalize_dremio_name(name) in self._views_with_definition

    def process_query(self, query: DremioQuery) -> None:
        # View lineage comes from the catalog's current definition; an older
        # CREATE [OR REPLACE] VIEW in job history would add stale upstreams.
        if self._is_stale_view_ddl(query.query):
            self.report.num_view_ddl_queries_skipped += 1
            return

        self._validate_query_lineage_format(query)

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
        return map_dremio_dataset_to_urn(
            self.source_map.get(dremio_source.lower()),
            dremio_path,
            dremio_dataset,
            self.config.env,
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
        root_path = source.root_path or ""
        database_name = source.database_name or ""
        database_in_path = mapper.lookup_database_in_path(source_type)
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
                    convert_urns_to_lowercase=mapping.convert_urns_to_lowercase,
                    database_in_path=database_in_path,
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
                database_in_path=database_in_path,
            )

    return source_map
