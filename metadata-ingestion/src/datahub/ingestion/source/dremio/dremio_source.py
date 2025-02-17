import logging
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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
from datahub.ingestion.source.dremio.dremio_api import (
    DremioAPIOperations,
    DremioEdition,
)
from datahub.ingestion.source.dremio.dremio_aspects import DremioAspects
from datahub.ingestion.source.dremio.dremio_config import DremioSourceConfig
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
)
from datahub.ingestion.source.dremio.dremio_profiling import DremioProfiler
from datahub.ingestion.source.dremio.dremio_reporting import DremioSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import PROFILING
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import ChangeTypeClass, SchemaMetadataClass
from datahub.metadata.urns import CorpUserUrn
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownQueryLineageInfo,
    ObservedQuery,
    SqlParsingAggregator,
)

logger = logging.getLogger(__name__)


@platform_name("Dremio")
@config_class(DremioSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
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
        self.source_map: Dict[str, Dict] = defaultdict()

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

        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(self.get_platform()),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=self.config.usage,
        )

        # For profiling
        self.profiler = DremioProfiler(config, self.report, dremio_api)

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "DremioSource":
        config = DremioSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return "dremio"

    def _build_source_map(self) -> Dict[str, Dict]:
        """
        Builds a source mapping dictionary to support external lineage generation across
        multiple Dremio sources, based on provided configuration mappings.

        This method operates as follows:

        1. If a source mapping is present in the config:
        - For each source in the Dremio catalog, if the mapping's `source_name` matches
            the `dremio_source_type`, `root_path` and `database_name` are added to the mapping
            information, along with the platform, platform instance, and environment if they exist.
            This allows constructing the full URN for upstream lineage.

        2. If a source mapping is absent in the configuration:
        - Default mappings are created for each source name, setting `env` and `platform_instance`
            to default values and classifying the source type. This ensures all sources have a
            mapping, even if specific configuration details are missing.

        Returns:
            Dict[str, Dict]: A dictionary (`source_map`) where each key is a source name
                            (lowercased) and each value is another dictionary containing:
                            - `platform`: The source platform.
                            - `source_name`: The source name.
                            - `dremio_source_type`: The type mapped to DataHub,
                            e.g., "database", "folder".
                            - Optional `root_path`, `database_name`, `platform_instance`,
                            and `env` if provided in the configuration.
        Example:
            This method is used internally within the class to generate mappings before
            creating cross-platform lineage.

        """

        source_map = {}
        dremio_sources = self.dremio_catalog.get_sources()

        for source in dremio_sources:
            source_name = source.container_name
            if isinstance(source.dremio_source_type, str):
                source_type = source.dremio_source_type.lower()
                root_path = source.root_path.lower() if source.root_path else ""
                database_name = (
                    source.database_name.lower() if source.database_name else ""
                )
                source_present = False
                source_platform_name = source_name

                for mapping in self.config.source_mappings or []:
                    if re.search(mapping.source_name, source_type, re.IGNORECASE):
                        source_platform_name = mapping.source_name.lower()

                    datahub_source_type = (
                        DremioToDataHubSourceTypeMapping.get_datahub_source_type(
                            source_type
                        )
                    )

                    if re.search(mapping.platform, datahub_source_type, re.IGNORECASE):
                        source_platform_name = source_platform_name.lower()
                        source_map[source_platform_name] = {
                            "platform": mapping.platform,
                            "source_name": mapping.source_name,
                            "dremio_source_type": DremioToDataHubSourceTypeMapping.get_category(
                                source_type,
                            ),
                            "root_path": root_path,
                            "database_name": database_name,
                            "platform_instance": mapping.platform_instance,
                            "env": mapping.env,
                        }
                        source_present = True
                        break

                if not source_present:
                    try:
                        dremio_source_type = (
                            DremioToDataHubSourceTypeMapping.get_category(source_type)
                        )
                    except Exception as exc:
                        logger.info(
                            f"Source {source_type} is not a standard Dremio source type. "
                            f"Adding source_type {source_type} to mapping as database. Error: {exc}"
                        )

                        DremioToDataHubSourceTypeMapping.add_mapping(
                            source_type, source_name
                        )
                        dremio_source_type = (
                            DremioToDataHubSourceTypeMapping.get_category(source_type)
                        )

                    source_map[source_platform_name.lower()] = {
                        "platform": source_type,
                        "source_name": source_name,
                        "dremio_source_type": dremio_source_type,
                    }

            else:
                logger.error(
                    f'Source "{source.container_name}" is broken. Containers will not be created for source.'
                )
                logger.error(
                    f'No new cross-platform lineage will be emitted for source "{source.container_name}".'
                )
                logger.error("Fix this source in Dremio to fix this issue.")

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

        # Process Containers
        containers = self.dremio_catalog.get_containers()
        for container in containers:
            try:
                yield from self.process_container(container)
                logger.info(
                    f"Dremio container {container.container_name} emitted successfully"
                )
            except Exception as exc:
                self.report.num_containers_failed += 1  # Increment failed containers
                self.report.report_failure(
                    message="Failed to process Dremio container",
                    context=f"{'.'.join(container.path)}.{container.container_name}",
                    exc=exc,
                )

        # Process Datasets
        datasets = self.dremio_catalog.get_datasets()

        for dataset_info in datasets:
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

        # Optionally Process Query Lineage
        if self.config.include_query_lineage:
            self.get_query_lineage_workunits()

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

        # Generate workunit for aggregated SQL parsing results
        for mcp in self.sql_parsing_aggregator.gen_metadata():
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

        # Profiling
        if self.config.is_profiling_enabled():
            with ThreadPoolExecutor(
                max_workers=self.config.profiling.max_workers
            ) as executor:
                future_to_dataset = {
                    executor.submit(self.generate_profiles, dataset): dataset
                    for dataset in datasets
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

        self.report.report_entity_scanned(dataset_name, dataset_info.dataset_type.value)
        if not self.config.dataset_pattern.allowed(dataset_name):
            self.report.report_dropped(dataset_name)
            return

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=f"dremio.{dataset_name}",
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
            name=f"dremio.{dataset_name}",
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        with self.report.new_stage(f"{dataset_info.resource_name}: {PROFILING}"):
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
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName=lineage.ASPECT_NAME,
            aspect=lineage,
            changeType=ChangeTypeClass.UPSERT,
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

        platform = mapping.get("platform")
        if not platform:
            return None

        platform_instance = mapping.get(
            "platform_instance", self.config.platform_instance
        )
        env = mapping.get("env", self.config.env)

        root_path = ""
        database_name = ""

        if mapping.get("dremio_source_type") == "file_object_storage":
            if mapping.get("root_path"):
                root_path = f"{mapping['root_path'][1:]}/"
            dremio_dataset = f"{root_path}{'/'.join(dremio_path[1:])}/{dremio_dataset}"
        else:
            if mapping.get("database_name"):
                database_name = f"{mapping['database_name']}."
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
