"""This module contains the Dremio source class for DataHub ingestion"""
from collections import defaultdict

import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict, Any

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
    capability,
)
from datahub.ingestion.api.source import SourceReport, SourceCapability
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import StaleEntityRemovalHandler
from datahub.ingestion.source.state.stateful_ingestion_base import StatefulIngestionSourceBase
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata.urns import CorpUserUrn
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import ChangeTypeClass

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_data_platform_urn,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    SqlParsingAggregator,
    KnownQueryLineageInfo,
    ObservedQuery,
)

from datahub.ingestion.source.dremio.dremio_api import DremioQuery, DremioAPIOperations, DremioCatalog, DremioDataset, \
    DremioEdition, DremioContainer, DremioDatasetType
from datahub.ingestion.source.dremio.dremio_config import (
    DremioSourceConfig,
    DremioCheckpointState,
    DremioSourceMapping
)
from datahub.ingestion.source.dremio.dremio_aspects import (
    DremioAspects
)
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping
)

logger = logging.getLogger(__name__)


@platform_name("Dremio")
@config_class(DremioSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
class DremioSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, views and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    - Lineage information for views and datasets
    """

    config: DremioSourceConfig
    report: SourceReport

    def __init__(self, config: DremioSourceConfig, ctx: Any):
        super().__init__(config, ctx)
        self.config = config
        self.report = SourceReport()
        self.source_map: Dict[str, DremioSourceMapping] = defaultdict()
        self.dremio_catalog = DremioCatalog(
            dremio_api=DremioAPIOperations(self.config)
        )
        self.dremio_aspects = DremioAspects(
            platform=self.get_platform(),
            platform_instance=self.config.platform_instance,
            env=self.config.env
        )
        self.reference_source_mapping = DremioToDataHubSourceTypeMapping()
        self.max_workers = config.max_workers

        # Handle stale entity removal
        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.config,
            state_type_class=DremioCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

        self.sql_parsing_aggregator = SqlParsingAggregator(
            platform=make_data_platform_urn(
                self.get_platform()
            ),
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            graph=self.ctx.graph,
            generate_usage_statistics=True,
            generate_operations=True,
            usage_config=BaseUsageConfig(),
        )

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: Any) -> 'DremioSource':
        config = DremioSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform(self) -> str:
        return "dremio"

    def _build_source_map(self) -> Dict[str, DremioSourceMapping]:
        source_map = {}
        dremio_sources = self.dremio_catalog.get_sources()

        for source in dremio_sources:
            source_name = source.container_name
            source_type = source.dremio_source_type.lower()
            root_path = source.root_path.lower() if source.root_path else ""
            database_name = source.database_name.lower() if source.database_name else ""
            source_present = False
            source_platform_name = source_name

            for mapping in self.config.source_mappings or []:
                if re.search(mapping.platform_name, source_type, re.IGNORECASE):
                    source_platform_name = mapping.platform_name.lower()

                if re.search(
                        mapping.platform,
                        self.reference_source_mapping.get_datahub_source_type(
                            source_type
                        ),
                        re.IGNORECASE
                ):
                    source_map[source_platform_name] = mapping
                    source_map[source_platform_name].dremio_source_type = (
                        self.reference_source_mapping.get_category(
                            source_type
                        )
                    )
                    source_map[source_platform_name].rootPath = root_path
                    source_map[source_platform_name].databaseName = database_name
                    source_present = True
                    break

            if not source_present:
                try:
                    dremio_source_type = self.reference_source_mapping.get_category(
                        source_type
                    )
                except Exception as exc:
                    logger.info(
                        f"Source {source_type} is not a standard Dremio source type. "
                        f"Adding source_type {source_type} to mapping as database. Error: {exc}"
                    )

                    self.reference_source_mapping.add_mapping(
                        source_type,
                        source_name
                    )
                    dremio_source_type = self.reference_source_mapping.get_category(
                        source_type
                    )

                source_map[source_platform_name] = DremioSourceMapping(
                    platform=source_type,
                    platform_name=source_name,
                    dremio_source_type=dremio_source_type,
                )

        return source_map

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for Dremio metadata."""
        self.source_map = self._build_source_map()

        for wu in self.get_workunits_internal():
            self.report.report_workunit(wu)
            yield wu

        # Emit the stale entity removal workunits
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Internal method to generate workunits for Dremio metadata."""
        containers = self.dremio_catalog.get_containers()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_container = {
                executor.submit(self.process_container, container): container
                for container in containers
            }

            for future in as_completed(future_to_container):
                container_info = future_to_container[future]
                try:
                    yield from future.result()
                except Exception as exc:
                    self.report.report_failure(
                        f"Failed to process container {container_info.path}{container_info.container_name}: {exc}"
                    )

        datasets = self.dremio_catalog.get_datasets()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_dataset = {
                executor.submit(self.process_dataset, dataset): dataset
                for dataset in datasets
            }

            for future in as_completed(future_to_dataset):
                dataset_info = future_to_dataset[future]
                try:
                    yield from future.result()
                except Exception as exc:
                    self.report.report_failure(
                        f"Failed to process dataset {dataset_info.path}.{dataset_info.resource_name}: {exc}"
                    )

        if self.config.include_query_lineage:
            self.get_query_lineage_workunits()

        # Generate workunit for aggregated SQL parsing results
        for mcp in self.sql_parsing_aggregator.gen_metadata():
            self.report.report_workunit(mcp.as_workunit())
            yield mcp.as_workunit()

    def process_container(self, container_info: DremioContainer) -> List[MetadataWorkUnit]:
        """Process a Dremio container and generate metadata workunits."""
        workunits = []

        if container_info.path:
            container_urn = self.dremio_aspects.get_container_urn(
                path=container_info.path,
                name=container_info.container_name
            )
        else:
            container_urn = self.dremio_aspects.get_container_urn(
                path=[],
                name=container_info.container_name
            )

        aspects = self.dremio_aspects.populate_container_aspects(
            container=container_info,
        )

        for aspect_name, aspect in aspects.items():
            mcp = MetadataChangeProposalWrapper(
                entityType="container",
                entityUrn=container_urn,
                aspectName=aspect_name,
                aspect=aspect,
                changeType=ChangeTypeClass.UPSERT,
            )
            workunits.append(
                MetadataWorkUnit(
                    id=f"{container_urn}-{aspect_name}",
                    mcp=mcp,
                )
            )

        return workunits

    def process_dataset(self, dataset_info: DremioDataset) -> List[MetadataWorkUnit]:
        """Process a Dremio dataset and generate metadata workunits."""
        workunits = []

        schema_str = '.'.join(dataset_info.path)

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(self.get_platform()),
            name=f"dremio.{schema_str}.{dataset_info.resource_name}".lower(),
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Mark the entity as scanned
        self.stale_entity_removal_handler.add_entity_to_state(
            type="dataset",
            urn=dataset_urn,
        )

        aspects = self.dremio_aspects.populate_dataset_aspects(
            dataset=dataset_info,
        )

        if aspects.get("schemaMetadata"):
            self.sql_parsing_aggregator.register_schema(
                urn=dataset_urn,
                schema=aspects.get("schemaMetadata"),
            )

        for aspect_name, aspect in aspects.items():
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
                aspectName=aspect_name,
                aspect=aspect,
                changeType=ChangeTypeClass.UPSERT,
            )
            workunits.append(
                MetadataWorkUnit(
                    id=f"{dataset_urn}-{aspect_name}",
                    mcp=mcp,
                )
            )

        if dataset_info.dataset_type == DremioDatasetType.VIEW:
            if self.dremio_catalog.edition == DremioEdition.ENTERPRISE:
                workunits.extend(
                    self.generate_view_lineage(
                        parents=dataset_info.parents,
                        dataset_urn=dataset_urn,
                    )
                )

            self.sql_parsing_aggregator.add_view_definition(
                view_urn=dataset_urn,
                view_definition=dataset_info.sql_definition,
                default_db="dremio"
            )

        elif dataset_info.dataset_type == DremioDatasetType.TABLE:
            dremio_source = dataset_info.path[0] if dataset_info.path else None

            if dremio_source:
                upstream_urn = self._map_dremio_dataset_to_urn(
                    dremio_source=dremio_source,
                    dremio_path=dataset_info.path,
                    dremio_dataset=dataset_info.resource_name)

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
                        entityType="dataset",
                        entityUrn=dataset_urn,
                        aspectName="upstreamLineage",
                        aspect=upstream_lineage,
                        changeType=ChangeTypeClass.UPSERT,
                    )

                    self.sql_parsing_aggregator.add_known_lineage_mapping(
                        upstream_urn=upstream_urn,
                        downstream_urn=dataset_urn,
                        lineage_type=DatasetLineageTypeClass.COPY,
                    )

                    workunits.append(
                        MetadataWorkUnit(
                            id=f"{dataset_urn}-upstreamLineage",
                            mcp=mcp,
                        )
                    )

        return workunits

    def generate_view_lineage(
            self, parents: List[str], dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate lineage information for views."""
        if parents:
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
                aspectName="upstreamLineage",
                aspect=lineage,
                changeType=ChangeTypeClass.UPSERT,
            )

            for upstream_urn in upstream_urns:
                self.sql_parsing_aggregator.add_known_lineage_mapping(
                    upstream_urn=upstream_urn,
                    downstream_urn=dataset_urn,
                    lineage_type=DatasetLineageTypeClass.VIEW
                )

            yield MetadataWorkUnit(id=f"{dataset_urn}-upstreamLineage", mcp=mcp)

    def get_query_lineage_workunits(self) -> None:
        """Process query lineage information."""
        queries = self.dremio_catalog.get_queries()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_query = {
                executor.submit(self.process_query, query): query
                for query in queries
            }

            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    future.result()
                except Exception as exc:
                    self.report.report_failure(
                        f"Failed to process query {query.job_id}: {exc}"
                    )

    def process_query(self, query: DremioQuery) -> None:
        """Process a single Dremio query for lineage information."""
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
                )
            )

        # Add observed query
        self.sql_parsing_aggregator.add_observed_query(
            ObservedQuery(
                query=query.query,
                timestamp=query.submitted_ts,
                user=CorpUserUrn(username=query.username),
                default_db="dremio",
            )
        )

    def _map_dremio_dataset_to_urn(
            self,
            dremio_source: str,
            dremio_path: List[str],
            dremio_dataset: str,
    ) -> Optional[str]:
        """Map a Dremio dataset to a DataHub URN."""
        mapping = self.source_map.get(dremio_source.lower())
        if not mapping:
            return None

        root_path = ""
        database_name = ""

        if mapping.dremio_source_type == "file_object_storage":
            if mapping.rootPath:
                root_path = f"{mapping.rootPath[1:]}/"
            dremio_dataset = f"{root_path}{'/'.join(dremio_path[1:])}/{dremio_dataset}"
        else:
            if mapping.databaseName:
                database_name = f"{mapping.databaseName}."
            dremio_dataset = f"{database_name}{'.'.join(dremio_path[1:])}.{dremio_dataset}"

        if mapping.platform_instance:
            return make_dataset_urn_with_platform_instance(
                platform=mapping.platform,
                name=dremio_dataset,
                platform_instance=mapping.platform_instance,
                env=self.config.env
            )

        return make_dataset_urn_with_platform_instance(
            platform=mapping.platform,
            name=dremio_dataset,
            platform_instance=None,
            env=self.config.env
        )

    def get_report(self) -> SourceReport:
        """Get the source report."""
        return self.report

    def close(self) -> None:
        """Close any resources held by the source."""
        pass