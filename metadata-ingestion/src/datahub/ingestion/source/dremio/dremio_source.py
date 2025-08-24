import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Optional

from pydantic import BaseModel

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
from datahub.ingestion.api.source_helpers import auto_workunit
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
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


class DremioSourceMapEntry(BaseModel):
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
        self.default_db = "dremio"
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

        # Initialize optional file-backed caching for OOM prevention
        self.file_backed_cache = None
        self.chunked_processor = None
        if self.config.enable_file_backed_cache:
            try:
                from datahub.ingestion.source.dremio.dremio_file_backed_cache import (
                    DremioChunkedProcessor,
                    DremioFileBackedCache,
                )

                self.file_backed_cache = DremioFileBackedCache(
                    cache_size=self.config.file_backed_cache_size,
                    eviction_batch_size=self.config.cache_eviction_batch_size,
                )
                self.chunked_processor = DremioChunkedProcessor(
                    cache=self.file_backed_cache,
                    max_containers_per_batch=self.config.max_containers_per_batch,
                )
                logger.info("File-backed caching enabled for OOM prevention")
            except ImportError as e:
                logger.warning(f"Failed to initialize file-backed cache: {e}")
                self.config.enable_file_backed_cache = False

        # Initialize aspects - containers always use "dremio" platform for consistency
        self.dremio_aspects = DremioAspects(
            platform="dremio",  # Always use dremio platform for containers
            domain=self.config.domain,
            ingest_owner=self.config.ingest_owner,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            ui_url=dremio_api.ui_url,
        )
        self.max_workers = config.max_workers

        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(
                "dremio"
            ),  # Always use dremio for SQL parsing
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
            is_temp_table=self._is_temp_table,
            is_allowed_table=self._is_allowed_table,
        )
        self.report.sql_aggregator = self.sql_parsing_aggregator.report

        # For profiling
        self.profiler = DremioProfiler(config, self.report, dremio_api)

        # Track discovered datasets for SQL aggregator filtering
        self.discovered_datasets: set[str] = set()

    def _is_temp_table(self, table_name: str) -> bool:
        """
        Check if a table is a temporary table that should be excluded from lineage.

        In Dremio, the main temporary/ephemeral storage is in $scratch spaces.
        """
        table_lower = table_name.lower()

        # Dremio scratch space tables - the primary temporary storage in Dremio
        return "$scratch" in table_lower

    def _is_allowed_table(self, table_name: str) -> bool:
        """
        Check if a table should be included in lineage based on our filtering patterns.

        This ensures we only generate lineage for tables that:
        1. Are discovered during metadata extraction
        2. Pass our allow/deny patterns
        3. Are not system tables (if system tables are disabled)
        """
        # Check if table was discovered during metadata extraction
        if table_name not in self.discovered_datasets:
            return False

        # Parse table name to extract components
        parts = table_name.replace("dremio.", "").split(".")
        if len(parts) < 2:
            return False

        source_name = parts[0]
        schema_parts = parts[1:-1] if len(parts) > 2 else []
        table_part = parts[-1]

        # Use our filter helper for consistent filtering
        if (
            hasattr(self.dremio_catalog.dremio_api, "filter_helper")
            and self.dremio_catalog.dremio_api.filter_helper
        ):
            schema_name = (
                ".".join([source_name] + schema_parts) if schema_parts else source_name
            )
            return self.dremio_catalog.dremio_api.filter_helper.should_include_dataset(
                source_name=source_name,
                schema_name=schema_name,
                table_name=table_part,
                dataset_type="table",  # Default to table for lineage purposes
            )

        # Fallback to basic pattern matching if no filter helper
        return self.config.dataset_pattern.allowed(
            table_name
        ) and self.config.schema_pattern.allowed(schema_name)

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "DremioSource":
        config = DremioSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return "dremio"

    def _build_source_map(self) -> Dict[str, DremioSourceMapEntry]:
        dremio_sources = self.dremio_catalog.get_sources()
        source_mappings_config = self.config.source_mappings or []

        source_map = build_dremio_source_map(dremio_sources, source_mappings_config)
        logger.info(f"Full source map: {source_map}")

        return source_map

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
            # Always use streaming approach to prevent OOM - the catalog methods are generators
            logger.info("Using streaming processing to prevent memory pressure")

            # Process Containers (streaming)
            container_count = 0
            for container in self.dremio_catalog.get_containers():
                try:
                    yield from self.process_container(container)
                    container_count += 1
                    if container_count % 10 == 0:
                        logger.info(f"Processed {container_count} containers")
                    else:
                        logger.debug(
                            f"Dremio container {container.container_name} emitted successfully"
                        )
                except Exception as exc:
                    self.report.num_containers_failed += 1
                    self.report.report_failure(
                        message="Failed to process Dremio container",
                        context=f"{'.'.join(container.path)}.{container.container_name}",
                        exc=exc,
                    )

            logger.info(f"Completed processing {container_count} containers")

            # Process Datasets (streaming)
            dataset_count = 0
            datasets_for_profiling = []  # Collect datasets for profiling if enabled

            for dataset_info in self.dremio_catalog.get_datasets():
                try:
                    yield from self.process_dataset(dataset_info)
                    dataset_count += 1

                    # Collect dataset for profiling if enabled
                    if self.config.is_profiling_enabled():
                        datasets_for_profiling.append(dataset_info)

                    if dataset_count % 50 == 0:
                        logger.info(f"Processed {dataset_count} datasets")
                    else:
                        logger.debug(
                            f"Dremio dataset {'.'.join(dataset_info.path)}.{dataset_info.resource_name} emitted successfully"
                        )
                except Exception as exc:
                    self.report.num_datasets_failed += 1
                    self.report.report_failure(
                        message="Failed to process Dremio dataset",
                        context=f"{'.'.join(dataset_info.path)}.{dataset_info.resource_name}",
                        exc=exc,
                    )

            logger.info(f"Completed processing {dataset_count} datasets")

            # Process Glossary Terms
            glossary_terms = self.dremio_catalog.get_glossary_terms()

            for glossary_term in glossary_terms:
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
            yield from auto_workunit(self.sql_parsing_aggregator.gen_metadata())

            # Profiling
            if self.config.is_profiling_enabled():
                with (
                    self.report.new_stage(PROFILING),
                    ThreadPoolExecutor(
                        max_workers=self.config.profiling.max_workers
                    ) as executor,
                ):
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

        Containers in Dremio represent organizational structures like spaces, sources,
        and folders. This method processes each container to extract metadata and
        generate appropriate DataHub workunits while respecting filtering patterns.

        The processing follows a hierarchical filtering approach:
        1. Containers are discovered recursively to find matching datasets
        2. Only containers that match schema patterns are emitted as metadata
        3. This handles Dremio's flexible hierarchy where patterns can match at any level

        Note: Container URNs always use the "dremio" platform for consistency,
        regardless of format-based platform URN settings.

        Args:
            container_info: Dremio container information including name, path, and type.
                           Must contain container_name and path attributes.

        Yields:
            MetadataWorkUnit: Workunits containing container metadata aspects such as:
            - ContainerProperties: Basic container information
            - BrowsePathsV2: Navigation paths for DataHub UI
            - DataPlatformInstance: Platform and instance information
            - SubTypes: Container classification information

        Example:
            >>> container = DremioContainer(container_name="Analytics", path=["DataLake"])
            >>> for workunit in source.process_container(container):
            ...     print(workunit.id)  # "urn:li:container:..."
        """
        # Build full container path for filtering
        path_components = container_info.path + [container_info.container_name]
        full_container_path = ".".join(path_components)

        # Check if this container should be emitted based on schema patterns
        # We use the same filtering logic as the API layer
        if (
            hasattr(self.dremio_catalog.dremio_api, "filter_helper")
            and self.dremio_catalog.dremio_api.filter_helper
        ):
            # Use the filter helper for consistent filtering
            if not self.dremio_catalog.dremio_api.filter_helper.is_schema_allowed(
                full_container_path
            ):
                logger.debug(
                    f"Container {full_container_path} excluded by schema_pattern during emission"
                )
                return
        else:
            # Fallback to basic pattern matching
            if not self.config.schema_pattern.allowed(full_container_path):
                logger.debug(
                    f"Container {full_container_path} excluded by schema_pattern during emission"
                )
                return

        container_urn = self.dremio_aspects.get_container_urn(
            path=container_info.path, name=container_info.container_name
        )

        yield from auto_workunit(
            self.dremio_aspects.populate_container_mcp(container_urn, container_info)
        )

    def process_dataset(
        self, dataset_info: DremioDataset
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a Dremio dataset and generate comprehensive metadata workunits.

        This method is the core of dataset processing, extracting metadata from Dremio
        datasets (tables and views) and converting it into DataHub-compatible workunits.
        It handles both physical datasets (tables) and virtual datasets (views) with
        appropriate lineage extraction.

        The processing pipeline includes:
        1. Platform determination (format-based or standard)
        2. URN generation with proper platform assignment
        3. Schema registration for SQL aggregator lineage
        4. Metadata aspect generation (schema, properties, lineage, etc.)
        5. View lineage extraction for virtual datasets

        Args:
            dataset_info: Complete Dremio dataset information including:
                         - resource_name: Table/view name
                         - path: Container hierarchy path
                         - dataset_type: TABLE or VIEW
                         - format_type: Storage format (for platform determination)
                         - columns: Schema information
                         - sql_definition: View SQL (for views)
                         - parents: Upstream dependencies

        Yields:
            MetadataWorkUnit: Comprehensive metadata workunits including:
            - SchemaMetadata: Column schema and data types
            - DatasetProperties: Basic dataset information
            - ViewProperties: View-specific metadata (for views)
            - UpstreamLineage: Dataset dependencies and lineage
            - DataPlatformInstance: Platform and instance information
            - Ownership: Dataset ownership information
            - GlobalTags: Applied tags and classifications
            - GlossaryTerms: Business glossary associations

        Raises:
            Exception: Re-raises any processing exceptions after logging for monitoring

        Example:
            >>> dataset = DremioDataset(
            ...     resource_name="customer_data",
            ...     path=["DataLake", "Analytics"],
            ...     dataset_type=DremioDatasetType.TABLE,
            ...     format_type="DELTA"
            ... )
            >>> for workunit in source.process_dataset(dataset):
            ...     print(f"Generated workunit: {workunit.id}")
        """

        schema_str = ".".join(dataset_info.path)

        dataset_name = f"{schema_str}.{dataset_info.resource_name}".lower()

        self.report.report_entity_scanned(dataset_name, dataset_info.dataset_type.value)
        if not self.config.dataset_pattern.allowed(dataset_name):
            self.report.report_dropped(dataset_name)
            return

        # Determine platform based on format type if enabled
        platform = self._determine_dataset_platform(dataset_info)

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(platform),
            name=f"dremio.{dataset_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Register discovered dataset for SQL aggregator filtering
        self.discovered_datasets.add(f"dremio.{dataset_name}")

        for dremio_mcp in self.dremio_aspects.populate_dataset_mcp(
            dataset_urn, dataset_info
        ):
            yield dremio_mcp
            # Check if the emitted aspect is SchemaMetadataClass
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

    def generate_profiles(
        self, dataset_info: DremioDataset
    ) -> Iterable[MetadataWorkUnit]:
        schema_str = ".".join(dataset_info.path)
        dataset_name = f"{schema_str}.{dataset_info.resource_name}".lower()

        # Use format-based platform for dataset profiling
        platform = self._determine_dataset_platform(dataset_info)

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(platform),
            name=f"dremio.{dataset_name}",
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
                name=f"dremio.{upstream_table.lower()}",
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

    def process_query(self, query: DremioQuery) -> None:
        """
        Process a single Dremio query for lineage information.
        """

        if query.query and query.affected_dataset:
            upstream_urns = [
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(self.get_platform()),
                    name=f"dremio.{ds.lower()}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                for ds in query.queried_datasets
            ]

            downstream_urn = make_dataset_urn_with_platform_instance(
                platform=make_data_platform_urn(self.get_platform()),
                name=f"dremio.{query.affected_dataset.lower()}",
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

    def close(self) -> None:
        """
        Clean up resources, including file-backed cache if enabled.
        """
        if self.file_backed_cache:
            try:
                self.file_backed_cache.close()
                logger.info("File-backed cache cleaned up successfully")
            except Exception as e:
                logger.warning(f"Error cleaning up file-backed cache: {e}")

    def _determine_dataset_platform(self, dataset_info: DremioDataset) -> str:
        """
        Determine the appropriate DataHub platform for a dataset based on its format type.

        This method implements format-based platform URN generation, allowing datasets
        to be assigned platform-specific URNs (e.g., delta-lake, iceberg) based on their
        underlying storage format, while maintaining backward compatibility.

        The platform determination follows this hierarchy:
        1. If format-based platforms are disabled, return "dremio"
        2. If format_type matches a configured mapping, return the mapped platform
        3. Otherwise, fall back to the source-based platform mapping

        Args:
            dataset_info: The Dremio dataset containing format and path information.
                         Must include format_type and path attributes.

        Returns:
            Platform name to use for the dataset URN. Examples:
            - "delta-lake" for Delta Lake tables
            - "iceberg" for Iceberg tables
            - "s3" for generic S3 data
            - "dremio" when format-based platforms are disabled

        Example:
            >>> dataset = DremioDataset(format_type="DELTA", path=["s3_source"])
            >>> platform = source._determine_dataset_platform(dataset)
            >>> print(platform)  # "delta-lake"
        """
        if not self.config.use_format_based_platform_urns:
            return self.get_platform()

        # Get the source type from the first path element (source name)
        source_type = "DREMIO"  # Default fallback
        if dataset_info.path:
            source_name = dataset_info.path[0]
            # Try to find the source type from our source map
            mapping = self.source_map.get(source_name.lower())
            if mapping:
                # Get the original Dremio source type from the mapping
                for (
                    dremio_type,
                    datahub_platform,
                ) in DremioToDataHubSourceTypeMapping.SOURCE_TYPE_MAPPING.items():
                    if datahub_platform == mapping.platform:
                        source_type = dremio_type
                        break

        # Use the format-based platform detection
        platform = DremioToDataHubSourceTypeMapping.get_platform_from_format(
            format_type=dataset_info.format_type,
            source_type=source_type,
            format_mapping=self.config.format_platform_mapping,
            use_format_based_platforms=self.config.use_format_based_platform_urns,
        )

        logger.debug(
            f"Dataset {dataset_info.resource_name} with format {dataset_info.format_type} "
            f"mapped to platform {platform}"
        )

        return platform

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

        source_type = (source.dremio_source_type or "").lower()
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
