"""This module contains the Dremio source class for DataHub ingestion"""

import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, List, Optional, Dict

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
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    ViewPropertiesClass,
    SubTypesClass,
    DataPlatformInstanceClass,
)

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dataset_urn_with_platform_instance,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
)
from datahub.sql_parsing.sql_parsing_aggregator import (
    SqlParsingAggregator,
    KnownQueryLineageInfo,
    ObservedQuery,
)

from datahub.ingestion.source.dremio.dremio_api import DremioQuery
from datahub.ingestion.source.dremio.dremio_config import (
    DremioSourceConfig,
    DremioCheckpointState,
    DremioSourceMapping
)
from datahub.ingestion.source.dremio.dremio_source_controller import (
    DremioController
)
from datahub.ingestion.source.dremio.dremio_datahub_source_mapping import (
    DremioToDataHubSourceTypeMapping
)

logger = logging.getLogger(__name__)


@dataclass
class DremioSpace:
    urn: str
    space_name: str
    description: Optional[str] = None


@dataclass
class DremioFolder:
    urn: str
    folder_name: str
    directory_path: list[str]
    description: Optional[str] = None


@dataclass
class DatasetInfo:
    schema: List[str]
    table: str
    type: str
    definition: str


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

    def __init__(self, config: DremioSourceConfig, ctx):
        super().__init__(config, ctx)
        self.config = config
        self.report = SourceReport()
        self.dremio_controller = DremioController(self.config.__dict__)
        self.is_enterprise_edition = self.dremio_controller.get_dremio_edition()
        self.reference_source_mapping = DremioToDataHubSourceTypeMapping()
        self.source_map = self._build_source_map()
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
    def create(cls, config_dict, ctx):
        config = DremioSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_platform(self):
        return "dremio"

    def _build_source_map(self) -> Dict[str, DremioSourceMapping]:
        source_map = {}
        dremio_sources = self.dremio_controller.get_dremio_sources()

        for source in dremio_sources:
            source_name = source.get("name").lower()
            source_type = source.get("type").lower()
            root_path = source.get("rootPath").lower() if source.get("rootPath") else ""
            database_name = source.get("databaseName").lower() if source.get("databaseName") else ""
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
                        (f"Source {source_type} is not a standard Dremio source type.",
                         f"Adding source_type {source_type} to mapping as database",
                         exc)
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
        for wu in self.get_workunits_internal():
            self.report.report_workunit(wu)
            yield wu

        # Emit the stale entity removal workunits
        yield from self.stale_entity_removal_handler.gen_removed_entity_workunits()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        datasets = self.dremio_controller.get_datasets()

        dataset_infos = [
            DatasetInfo(
                schema=schema,
                table=table,
                type=typ,
                definition=definition,
            )
            for schema, table, typ, definition in datasets
        ]

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            future_to_dataset = {
                executor.submit(
                    self.process_dataset,
                    dataset_info,
                ):
                    dataset_info for dataset_info in dataset_infos
            }

            for future in as_completed(
                    future_to_dataset
            ):
                dataset_info = future_to_dataset[future]

                try:
                    for wu in future.result():
                        yield wu

                except Exception as exc:
                    self.report.report_failure(
                        f"Failed to process dataset {dataset_info.schema}.{dataset_info.table}: {exc}"
                    )

        if self.config.include_query_lineage:
            self.get_query_lineage_workunits()

        # Generate workunit for aggregated SQL parsing results
        for mcp in self.sql_parsing_aggregator.gen_metadata():
            self.report.report_workunit(
                mcp.as_workunit()
            )
            yield mcp.as_workunit()

    def process_dataset(self, dataset_info: DatasetInfo) -> List[MetadataWorkUnit]:
        workunits = []

        schema_str = '.'.join(dataset_info.schema) if isinstance(
            dataset_info.schema, list
        ) else dataset_info.schema

        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=make_data_platform_urn(
                self.get_platform(),
            ),
            name=f"dremio.{schema_str}.{dataset_info.table}".lower(),
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )

        # Mark the entity as scanned
        self.stale_entity_removal_handler.add_entity_to_state(
            type="dataset",
            urn=dataset_urn,
        )

        aspects = self.dremio_controller.populate_dataset_aspects(
            schema=schema_str,
            folder_path=dataset_info.schema if isinstance(
                dataset_info.schema, list
            ) else [dataset_info.schema],
            table_name=dataset_info.table,
            all_tables_and_columns=self.dremio_controller.get_all_tables_and_columns(),
        )

        aspects["subTypes"] = SubTypesClass(
            typeNames=[
                dataset_info.type.title(),
            ]
        )

        aspects["dataPlatformInstance"] = DataPlatformInstanceClass(
            platform=f"urn:li:dataPlatform:{self.get_platform()}",
            instance=(
                make_dataplatform_instance_urn(
                    self.get_platform(), self.config.platform_instance,
                )
                if self.config.platform_instance
                else None
            )
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

        if dataset_info.type == 'VIEW':
            workunits.extend(
                self.add_view_properties(
                    dataset_urn=dataset_urn,
                    view_definition=dataset_info.definition,
                )
            )

            if self.is_enterprise_edition:
                workunits.extend(
                    self.generate_view_lineage(
                        schema=dataset_info.schema,
                        table=dataset_info.table,
                        dataset_urn=dataset_urn,
                    )
                )

            self.sql_parsing_aggregator.add_view_definition(
                view_urn=dataset_urn,
                view_definition=dataset_info.definition,
                default_db="dremio"
            )

        elif dataset_info.type == 'TABLE':
            dremio_source = dataset_info.schema[0] if dataset_info.schema else None

            if dremio_source:
                upstream_urn = self._map_dremio_dataset_to_urn(
                    dremio_source=dremio_source,
                    dremio_path=dataset_info.schema,
                    dremio_dataset=dataset_info.table)

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

    def add_view_properties(
            self,
            dataset_urn: str,
            view_definition: str
    ) -> Iterable[MetadataWorkUnit]:
        view_properties = ViewPropertiesClass(
            materialized=False,
            viewLanguage="SQL",
            viewLogic=view_definition,
        )
        mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="viewProperties",
            aspect=view_properties,
            changeType=ChangeTypeClass.UPSERT,
        )
        yield MetadataWorkUnit(id=f"{dataset_urn}-viewProperties", mcp=mcp)

        subtypes_mcp = MetadataChangeProposalWrapper(
            entityType="dataset",
            entityUrn=dataset_urn,
            aspectName="subTypes",
            aspect=SubTypesClass(typeNames=["View"]),
            changeType=ChangeTypeClass.UPSERT,
        )

        yield MetadataWorkUnit(
            id=f"{dataset_urn}-subTypes",
            mcp=subtypes_mcp,
        )

    def generate_view_lineage(
            self, schema: List[str], table: str, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        # Join the schema list into a string, as expected by get_parents
        schema_str = '.'.join(schema)
        upstream_tables = self.dremio_controller.get_parents(
            schema=schema_str,
            dataset=table,
        )
        if upstream_tables:
            upstream_urns = [
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(
                        self.get_platform()
                    ),
                    name=f"dremio.{upstream_table.lower()}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                for upstream_table in upstream_tables
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

    def get_query_lineage_workunits(self):
        queries = self.dremio_controller.get_all_queries()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_query = {
                executor.submit(
                    self.process_query,
                    query,
                ): query for query in queries
            }

            for future in as_completed(future_to_query):
                query = future_to_query[future]
                try:
                    future.result()
                except Exception as exc:
                    self.report.report_failure(
                        f"Failed to process query {query.job_id}: {exc}"
                    )

    def process_query(self, query: DremioQuery):
        if query.query and query.affected_dataset:
            upstream_urns = [
                make_dataset_urn_with_platform_instance(
                    platform=make_data_platform_urn(
                        self.get_platform()
                    ),
                    name=f"dremio.{ds.lower()}",
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )
                for ds in query.queried_datasets
            ]

            downstream_urn = make_dataset_urn_with_platform_instance(
                platform=make_data_platform_urn(
                    self.get_platform()
                ),
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
                user=CorpUserUrn(
                    username=query.username,
                ),
                default_db="dremio",
            )
        )

    def _map_dremio_dataset_to_urn(
            self,
            dremio_source: str,
            dremio_path: List[str],
            dremio_dataset: str,
    ) -> Optional[str]:
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

    def make_dremio_space_urn(
            self,
            space_name: str,
            platform_instance: Optional[str],
    ) -> str:
        namespace = uuid.NAMESPACE_DNS

        return make_container_urn(
            guid=str(
                uuid.uuid5(
                    namespace,
                    space_name +
                    platform_instance
                )
            )
        )

    def make_dremio_folder_urn(
            self,
            space_name: str,
            directory_path: List[str],
            platform_instance: Optional[str],
    ) -> str:
        namespace = uuid.NAMESPACE_DNS
        platform_instance = "" if not platform_instance else platform_instance
        flattened_path = "_".join(directory_path)

        return make_container_urn(
            guid=str(
                uuid.uuid5(
                    namespace,
                    space_name +
                    platform_instance +
                    flattened_path
                )
            )
        )

    def get_report(self):
        return self.report

    def close(self):
        pass
