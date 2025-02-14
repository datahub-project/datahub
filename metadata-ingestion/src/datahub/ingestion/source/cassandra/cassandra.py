import dataclasses
import json
import logging
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    add_dataset_to_container,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.cassandra.cassandra_api import (
    CassandraAPI,
    CassandraColumn,
    CassandraEntities,
    CassandraKeyspace,
    CassandraTable,
    CassandraView,
)
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig
from datahub.ingestion.source.cassandra.cassandra_profiling import CassandraProfiler
from datahub.ingestion.source.cassandra.cassandra_utils import (
    SYSTEM_KEYSPACE_LIST,
    CassandraSourceReport,
    CassandraToSchemaFieldConverter,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OtherSchemaClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME_IN_DATAHUB = "cassandra"


class KeyspaceKey(ContainerKey):
    keyspace: str


@platform_name("Cassandra")
@config_class(CassandraSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
class CassandraSource(StatefulIngestionSourceBase):
    """
    This plugin extracts the following:

    - Metadata for tables
    - Column types associated with each table column
    - The keyspace each table belongs to
    """

    config: CassandraSourceConfig
    report: CassandraSourceReport
    platform: str

    def __init__(self, ctx: PipelineContext, config: CassandraSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.platform = PLATFORM_NAME_IN_DATAHUB
        self.config = config
        self.report = CassandraSourceReport()
        self.cassandra_api = CassandraAPI(config, self.report)
        self.cassandra_data = CassandraEntities()
        # For profiling
        self.profiler = CassandraProfiler(config, self.report, self.cassandra_api)

    @classmethod
    def create(cls, config_dict, ctx):
        config = CassandraSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_platform(self) -> str:
        return PLATFORM_NAME_IN_DATAHUB

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        if not self.cassandra_api.authenticate():
            return
        keyspaces: List[CassandraKeyspace] = self.cassandra_api.get_keyspaces()
        for keyspace in keyspaces:
            keyspace_name: str = keyspace.keyspace_name
            if keyspace_name in SYSTEM_KEYSPACE_LIST:
                continue

            if not self.config.keyspace_pattern.allowed(keyspace_name):
                self.report.report_dropped(keyspace_name)
                continue

            yield from self._generate_keyspace_container(keyspace)

            try:
                yield from self._extract_tables_from_keyspace(keyspace_name)
            except Exception as e:
                self.report.num_tables_failed += 1
                self.report.failure(
                    message="Failed to extract table metadata for keyspace",
                    context=keyspace_name,
                    exc=e,
                )
            try:
                yield from self._extract_views_from_keyspace(keyspace_name)
            except Exception as e:
                self.report.num_views_failed += 1
                self.report.failure(
                    message="Failed to extract view metadata for keyspace ",
                    context=keyspace_name,
                    exc=e,
                )

        # Profiling
        if self.config.is_profiling_enabled():
            yield from self.profiler.get_workunits(self.cassandra_data)

    def _generate_keyspace_container(
        self, keyspace: CassandraKeyspace
    ) -> Iterable[MetadataWorkUnit]:
        keyspace_container_key = self._generate_keyspace_container_key(
            keyspace.keyspace_name
        )
        yield from gen_containers(
            container_key=keyspace_container_key,
            name=keyspace.keyspace_name,
            qualified_name=keyspace.keyspace_name,
            extra_properties={
                "durable_writes": str(keyspace.durable_writes),
                "replication": json.dumps(keyspace.replication),
            },
            sub_types=[DatasetContainerSubTypes.KEYSPACE],
        )

    def _generate_keyspace_container_key(self, keyspace_name: str) -> ContainerKey:
        return KeyspaceKey(
            keyspace=keyspace_name,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )

    # get all tables for a given keyspace, iterate over them to extract column metadata
    def _extract_tables_from_keyspace(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:
        self.cassandra_data.keyspaces.append(keyspace_name)
        tables: List[CassandraTable] = self.cassandra_api.get_tables(keyspace_name)
        for table in tables:
            # define the dataset urn for this table to be used downstream
            table_name: str = table.table_name
            dataset_name: str = f"{keyspace_name}.{table_name}"

            if not self.config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            self.cassandra_data.tables.setdefault(keyspace_name, []).append(table_name)
            self.report.report_entity_scanned(dataset_name, ent_type="Table")

            dataset_urn = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            # 1. Extract columns from table, then construct and emit the schemaMetadata aspect.
            try:
                yield from self._extract_columns_from_table(
                    keyspace_name, table_name, dataset_urn
                )
            except Exception as e:
                self.report.failure(
                    message="Failed to extract columns from table",
                    context=table_name,
                    exc=e,
                )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.TABLE,
                    ]
                ),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=table_name,
                    qualifiedName=f"{keyspace_name}.{table_name}",
                    description=table.comment,
                    customProperties={
                        "bloom_filter_fp_chance": str(table.bloom_filter_fp_chance),
                        "caching": json.dumps(table.caching),
                        "compaction": json.dumps(table.compaction),
                        "compression": json.dumps(table.compression),
                        "crc_check_chance": str(table.crc_check_chance),
                        "dclocal_read_repair_chance": str(
                            table.dclocal_read_repair_chance
                        ),
                        "default_time_to_live": str(table.default_time_to_live),
                        "extensions": json.dumps(table.extensions),
                        "gc_grace_seconds": str(table.gc_grace_seconds),
                        "max_index_interval": str(table.max_index_interval),
                        "min_index_interval": str(table.min_index_interval),
                        "memtable_flush_period_in_ms": str(
                            table.memtable_flush_period_in_ms
                        ),
                        "read_repair_chance": str(table.read_repair_chance),
                        "speculative_retry": str(table.speculative_retry),
                    },
                ),
            ).as_workunit()

            yield from add_dataset_to_container(
                container_key=self._generate_keyspace_container_key(keyspace_name),
                dataset_urn=dataset_urn,
            )

            if self.config.platform_instance:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=make_data_platform_urn(self.platform),
                        instance=make_dataplatform_instance_urn(
                            self.platform, self.config.platform_instance
                        ),
                    ),
                ).as_workunit()

    # get all columns for a given table, iterate over them to extract column metadata
    def _extract_columns_from_table(
        self, keyspace_name: str, table_name: str, dataset_urn: str
    ) -> Iterable[MetadataWorkUnit]:
        column_infos: List[CassandraColumn] = self.cassandra_api.get_columns(
            keyspace_name, table_name
        )
        schema_fields: List[SchemaField] = list(
            CassandraToSchemaFieldConverter.get_schema_fields(column_infos)
        )
        if not schema_fields:
            self.report.report_warning(
                message="Table has no columns, skipping", context=table_name
            )
            return

        jsonable_column_infos: List[Dict[str, Any]] = []
        for column in column_infos:
            self.cassandra_data.columns.setdefault(table_name, []).append(column)
            jsonable_column_infos.append(dataclasses.asdict(column))

        schema_metadata: SchemaMetadata = SchemaMetadata(
            schemaName=table_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(
                rawSchema=json.dumps(jsonable_column_infos)
            ),
            fields=schema_fields,
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

    def _extract_views_from_keyspace(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:
        views: List[CassandraView] = self.cassandra_api.get_views(keyspace_name)
        for view in views:
            view_name: str = view.view_name
            dataset_name: str = f"{keyspace_name}.{view_name}"
            self.report.report_entity_scanned(dataset_name)
            dataset_urn: str = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.VIEW,
                    ]
                ),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=ViewPropertiesClass(
                    materialized=True,
                    viewLogic=view.where_clause,  # Use the WHERE clause as view logic
                    viewLanguage="CQL",  # Use "CQL" as the language
                ),
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DatasetPropertiesClass(
                    name=view_name,
                    qualifiedName=f"{keyspace_name}.{view_name}",
                    description=view.comment,
                    customProperties={
                        "bloom_filter_fp_chance": str(view.bloom_filter_fp_chance),
                        "caching": json.dumps(view.caching),
                        "compaction": json.dumps(view.compaction),
                        "compression": json.dumps(view.compression),
                        "crc_check_chance": str(view.crc_check_chance),
                        "include_all_columns": str(view.include_all_columns),
                        "dclocal_read_repair_chance": str(
                            view.dclocal_read_repair_chance
                        ),
                        "default_time_to_live": str(view.default_time_to_live),
                        "extensions": json.dumps(view.extensions),
                        "gc_grace_seconds": str(view.gc_grace_seconds),
                        "max_index_interval": str(view.max_index_interval),
                        "min_index_interval": str(view.min_index_interval),
                        "memtable_flush_period_in_ms": str(
                            view.memtable_flush_period_in_ms
                        ),
                        "read_repair_chance": str(view.read_repair_chance),
                        "speculative_retry": str(view.speculative_retry),
                    },
                ),
            ).as_workunit()

            try:
                yield from self._extract_columns_from_table(
                    keyspace_name, view_name, dataset_urn
                )
            except Exception as e:
                self.report.failure(
                    message="Failed to extract columns from views",
                    context=view_name,
                    exc=e,
                )

            # Construct and emit lineage off of 'base_table_name'
            # NOTE: we don't need to use 'base_table_id' since table is always in same keyspace, see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCreateMaterializedView.html#cqlCreateMaterializedView__keyspace-name
            upstream_urn: str = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=f"{keyspace_name}.{view.table_name}",
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
            fineGrainedLineages = self.get_upstream_fields_of_field_in_datasource(
                view_name, dataset_urn, upstream_urn
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=upstream_urn,
                            type=DatasetLineageTypeClass.VIEW,
                        )
                    ],
                    fineGrainedLineages=fineGrainedLineages,
                ),
            ).as_workunit()

            yield from add_dataset_to_container(
                container_key=self._generate_keyspace_container_key(keyspace_name),
                dataset_urn=dataset_urn,
            )

            if self.config.platform_instance:
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=DataPlatformInstanceClass(
                        platform=make_data_platform_urn(self.platform),
                        instance=make_dataplatform_instance_urn(
                            self.platform, self.config.platform_instance
                        ),
                    ),
                ).as_workunit()

    def get_upstream_fields_of_field_in_datasource(
        self, table_name: str, dataset_urn: str, upstream_urn: str
    ) -> List[FineGrainedLineageClass]:
        column_infos = self.cassandra_data.columns.get(table_name, [])
        # Collect column-level lineage
        fine_grained_lineages = []
        for column_info in column_infos:
            source_column = column_info.column_name
            if source_column:
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[make_schema_field_urn(dataset_urn, source_column)],
                        upstreams=[make_schema_field_urn(upstream_urn, source_column)],
                    )
                )
        return fine_grained_lineages

    def get_report(self):
        return self.report

    def close(self):
        self.cassandra_api.close()
        super().close()
