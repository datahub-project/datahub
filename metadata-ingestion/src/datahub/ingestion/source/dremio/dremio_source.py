import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

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
    MetadataWorkUnitProcessor,
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
    DremioDatasetType,
    DremioGlossaryTerm,
    DremioQuery,
    DremioSourceContainer,
)
from datahub.ingestion.source.dremio.dremio_profiling import DremioProfiler
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
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

logger = logging.getLogger(__name__)

# Dremio uses 'dremio' as the default database name in all SQL contexts
DREMIO_DATABASE_NAME = "dremio"


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
            platform_instance=platform_instance,
            env=self.env,
            name=table_name,
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
class DremioSource(StatefulIngestionSourceBase):
    """
    This plugin integrates with Dremio to extract and ingest metadata into DataHub.
    The following types of metadata are extracted:

    - Metadata for Spaces, Folders, Sources, and Datasets:
        - Includes physical and virtual datasets, with detailed information about each dataset.
        - Extracts metadata about Dremio's organizational hierarchy: Spaces (top-level), Folders (sub-level), and Sources (external data connections).

    - Schema and Column Information:
        - Column types and schema metadata associated with each physical and virtual dataset.
        - Extracts column-level metadata, such as names, data types, and descriptions, if available.

    - Lineage Information:
        - Dataset-level and column-level lineage tracking:
            - Dataset-level lineage shows dependencies and relationships between physical and virtual datasets.
            - Column-level lineage tracks transformations applied to individual columns across datasets.
        - Lineage information helps trace the flow of data and transformations within Dremio.

    - Ownership and Glossary Terms:
        - Metadata related to ownership of datasets, extracted from Dremio’s ownership model.
        - Glossary terms and business metadata associated with datasets, providing additional context to the data.
        - Note: Ownership information will only be available for the Cloud and Enterprise editions, it will not be available for the Community edition.

    - Optional SQL Profiling (if enabled):
        - Table, row, and column statistics can be profiled and ingested via optional SQL queries.
        - Extracts statistics about tables and columns, such as row counts and data distribution, for better insight into the dataset structure.
    """

    config: DremioSourceConfig
    report: DremioSourceReport

    def __init__(self, config: DremioSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.default_db = DREMIO_DATABASE_NAME
        self.config = config
        self.report = DremioSourceReport()

        # Set time window for query lineage extraction
        self.report.window_start_time, self.report.window_end_time = (
            self.config.start_time,
            self.config.end_time,
        )

        self.source_map: Dict[str, DremioSourceMapEntry] = dict()

        # Initialize API operations
        dremio_api = DremioAPIOperations(self.config, self.report)

        # Initialize catalog
        self.dremio_catalog = DremioCatalog(dremio_api)

        # Initialize aspects
        self.dremio_aspects = DremioAspects(
            platform=self.get_platform(),
            domain=self.config.domain,
            ingest_owner=self.config.ingest_owner,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            ui_url=dremio_api.ui_url,
        )
        self.max_workers = config.max_workers

        # Create a custom schema resolver for Dremio that handles the "dremio." infix (post platform_instance)
        self.dremio_schema_resolver = DremioSchemaResolver(
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
        )

        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.get_platform()),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            schema_resolver=self.dremio_schema_resolver,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
        )
        self.report.sql_aggregator = self.sql_parsing_aggregator.report

        # For profiling
        self.profiler = DremioProfiler(config, self.report, dremio_api)

        # Track catalog dataset names for query lineage validation
        self.catalog_dataset_names: set[str] = set()

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "DremioSource":
        config = DremioSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return DREMIO_DATABASE_NAME

    def _build_source_map(self) -> Dict[str, DremioSourceMapEntry]:
        dremio_sources = list(self.dremio_catalog.get_sources())
        source_mappings_config = self.config.source_mappings or []

        source_map = build_dremio_source_map(dremio_sources, source_mappings_config)
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

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Internal method to generate workunits for Dremio metadata.
        """

        self.source_map = self._build_source_map()

        with self.report.new_stage(METADATA_EXTRACTION):
            # Process Containers
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

            # Process Datasets
            for dataset_info in self.dremio_catalog.get_datasets():
                try:
                    yield from self.process_dataset(dataset_info)
                    logger.info(
                        f"Dremio dataset {'.'.join(dataset_info.path)}.{dataset_info.resource_name} emitted successfully"
                    )
                except Exception as exc:
                    self.report.num_datasets_failed += 1  # Increment failed datasets
                    self.report.report_failure(
                        message="Failed to process Dremio dataset",
                        context=f"{'.'.join(dataset_info.path)}.{dataset_info.resource_name}",
                        exc=exc,
                    )

            # Process Glossary Terms using streaming
            for glossary_term in self.dremio_catalog.get_glossary_terms():
                try:
                    yield from self.process_glossary_term(glossary_term)
                except Exception as exc:
                    self.report.report_failure(
                        message="Failed to process Glossary terms",
                        context=f"{glossary_term.glossary_term}",
                        exc=exc,
                    )

            # Optionally Process Query Lineage
            if self.config.include_query_lineage:
                with self.report.new_stage(LINEAGE_EXTRACTION):
                    self.get_query_lineage_workunits()

            # Generate workunit for aggregated SQL parsing results
            for mcp in self.sql_parsing_aggregator.gen_metadata():
                yield mcp.as_workunit()

            # Profiling
            if self.config.is_profiling_enabled():
                with (
                    self.report.new_stage(PROFILING),
                    ThreadPoolExecutor(
                        max_workers=self.config.profiling.max_workers
                    ) as executor,
                ):
                    # Collect datasets for profiling
                    datasets_for_profiling = list(self.dremio_catalog.get_datasets())
                    future_to_dataset = {
                        executor.submit(self.generate_profiles, dataset): dataset
                        for dataset in datasets_for_profiling
                    }

                    for future in as_completed(future_to_dataset):
                        dataset_info = future_to_dataset[future]
                        try:
                            yield from future.result()
                        except Exception as exc:
                            self.report.profiling_skipped_other[
                                dataset_info.resource_name
                            ] += 1
                            self.report.report_failure(
                                message="Failed to profile dataset",
                                context=f"{'.'.join(dataset_info.path)}.{dataset_info.resource_name}",
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

        # Filter out Dremio Reflections (internal acceleration structures in _accelerator_ schema)
        # These are Dremio's internal metadata and should not appear in the DataHub catalog
        if dataset_info.path and dataset_info.path[0] == "_accelerator_":
            self.report.report_dropped(f"Skipping Dremio reflection: {dataset_name}")
            return

        self.report.report_entity_scanned(dataset_name, dataset_info.dataset_type.value)
        if not self.config.dataset_pattern.allowed(dataset_name):
            self.report.report_dropped(dataset_name)
            return

        # Track catalog dataset names for query lineage validation
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
            # Check if the emitted aspect is SchemaMetadataClass
            if isinstance(
                dremio_mcp.metadata, MetadataChangeProposalWrapper
            ) and isinstance(dremio_mcp.metadata.aspect, SchemaMetadataClass):
                # Register the schema with the custom Dremio schema resolver
                # The resolver will ensure all URNs are constructed with the "dremio." infix
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

    def generate_profiles(
        self, dataset_info: DremioDataset
    ) -> Iterable[MetadataWorkUnit]:
        schema_str = ".".join(dataset_info.path)
        dataset_name = f"{schema_str}.{dataset_info.resource_name}".lower()
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=f"{DREMIO_DATABASE_NAME}.{dataset_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        yield from self.profiler.get_workunits(dataset_info, dataset_urn)

    def generate_view_lineage(
        self, dataset_urn: str, parents: List[str]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage information for views.
        """
        upstream_urns = [
            make_dataset_urn_with_platform_instance(
                platform=make_data_platform_urn(self.get_platform()),
                name=f"{DREMIO_DATABASE_NAME}.{upstream_table.lower()}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
            for upstream_table in parents
        ]

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

        queries = self.dremio_catalog.get_queries()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_query = {
                executor.submit(self.process_query, query): query for query in queries
            }

            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    future.result()
                except Exception as exc:
                    self.report.report_failure(
                        message="Failed to process dremio query",
                        context=f"{query.job_id}: {exc}",
                        exc=exc,
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

            upstream_urns = [
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(self.get_platform()),
                    name=f"{DREMIO_DATABASE_NAME}.{ds.lower()}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                for ds in query.queried_datasets
            ]

            downstream_urn = make_dataset_urn_with_platform_instance(
                platform=make_data_platform_urn(self.get_platform()),
                name=f"{DREMIO_DATABASE_NAME}.{query.affected_dataset.lower()}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            # Add query to SqlParsingAggregator
            self.sql_parsing_aggregator.add_known_query_lineage(
                KnownQueryLineageInfo(
                    query_text=query.query,
                    upstreams=upstream_urns,
                    downstream=downstream_urn,
                ),
                merge_lineage=True,
            )

        # Add observed query
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
    source_map = {}
    for source in dremio_sources:
        current_source_name = source.container_name

        source_type = source.dremio_source_type.lower()
        source_category = DremioToDataHubSourceTypeMapping.get_category(source_type)
        datahub_platform = DremioToDataHubSourceTypeMapping.get_datahub_platform(
            source_type
        )
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
