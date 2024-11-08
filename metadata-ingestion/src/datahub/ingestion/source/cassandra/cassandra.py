import json
import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
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
from datahub.ingestion.source.cassandra.cassandra_api import CassandraAPIInterface
from datahub.ingestion.source.cassandra.cassandra_config import CassandraSourceConfig
from datahub.ingestion.source.cassandra.cassandra_utils import (
    CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES,
    SYSTEM_KEYSPACE_LIST,
    CassandraToSchemaFieldConverter,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata._schema_classes import DatasetPropertiesClass, TimeStampClass
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    OtherSchemaClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)

PLATFORM_NAME_IN_DATAHUB = "cassandra"


class KeyspaceKey(ContainerKey):
    keyspace: str


@dataclass
class CassandraSourceReport(StaleEntityRemovalSourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, index: str) -> None:
        self.filtered.append(index)


@platform_name("Cassandra")
@config_class(CassandraSourceConfig)
@support_status(SupportStatus.TESTING)
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
    # cassandra_session: Session
    platform: str

    def __init__(self, ctx: PipelineContext, config: CassandraSourceConfig):
        super().__init__(config, ctx)
        self.ctx = ctx
        self.platform = PLATFORM_NAME_IN_DATAHUB
        self.config = config
        self.report = CassandraSourceReport()
        self.cassandra_api = CassandraAPIInterface(config, self.report)

    @classmethod
    def create(cls, config_dict, ctx):
        config = CassandraSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_platform(self) -> str:
        return "cassandra"

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
        keyspaces = self.cassandra_api.get_keyspaces()
        for keyspace in keyspaces:
            keyspace_name: str = getattr(
                keyspace, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]
            )
            if keyspace_name in SYSTEM_KEYSPACE_LIST:
                continue

            if not self.config.keyspace_pattern.allowed(keyspace_name):
                self.report.report_dropped(keyspace_name)
                continue

            yield from self._generate_keyspace_container(keyspace_name)

            try:
                yield from self._extract_tables_from_keyspace(keyspace_name)
            except Exception as e:
                self.report.report_failure(
                    message="Failed to extract table metadata for keyspace",
                    context=f"{keyspace_name}",
                    exc=e,
                )

            try:
                yield from self._extract_views_from_keyspace(keyspace_name)
            except Exception as e:
                self.report.report_failure(
                    message="Failed to extract view metadata for keyspace ",
                    context=f"{keyspace_name}",
                    exc=e,
                )

    def _generate_keyspace_container(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:
        keyspace_container_key = self._generate_keyspace_container_key(keyspace_name)
        yield from gen_containers(
            container_key=keyspace_container_key,
            name=keyspace_name,
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
        tables = self.cassandra_api.get_tables(keyspace_name)
        for table in tables:
            # define the dataset urn for this table to be used downstream
            table_name: str = getattr(
                table, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"]
            )
            dataset_name: str = f"{keyspace_name}.{table_name}"
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
                self.report.report_failure(
                    message="Failed to extract columns from table",
                    context=f"{table_name}",
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
                    created=TimeStampClass(time=0),
                    customProperties={
                        "bloom_filter_fp_chance": str(table.bloom_filter_fp_chance),
                        "caching": str(table.caching),
                        "cdc": str(table.cdc),
                        "compaction": str(table.compaction),
                        "compression": str(table.compression),
                        "max_index_interval": str(table.max_index_interval),
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
        column_infos = self.cassandra_api.get_columns(keyspace_name, table_name)
        schema_fields: list[SchemaField] = list(
            CassandraToSchemaFieldConverter.get_schema_fields(column_infos)
        )
        if not schema_fields:
            logger.warning(f"Table {table_name} has no columns, skipping")
            self.report.report_warning(
                "table", f"Table {table_name} has no columns, skipping"
            )
            return

        # remove any value that is type bytes, so it can be converted to json
        jsonable_column_infos: list[dict[str, Any]] = []
        for column in column_infos:
            column_dict = column._asdict()
            jsonable_column_dict = column_dict.copy()
            for key, value in column_dict.items():
                if isinstance(value, bytes):
                    jsonable_column_dict.pop(key)
            jsonable_column_infos.append(jsonable_column_dict)

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

    # NOTE: this will only emit dataset and lineage info as we can't get columns for views from the sys schema metadata
    def _extract_views_from_keyspace(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:

        views = self.cassandra_api.get_views(keyspace_name)
        for view in views:
            view_name: str = getattr(
                view, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["view_name"]
            )
            dataset_name: str = f"{keyspace_name}.{view_name}"
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

            # Construct and emit lineage off of 'base_table_name'
            # NOTE: we don't need to use 'base_table_id' since table is always in same keyspace, see https://docs.datastax.com/en/cql-oss/3.3/cql/cql_reference/cqlCreateMaterializedView.html#cqlCreateMaterializedView__keyspace-name
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineageClass(
                    upstreams=[
                        UpstreamClass(
                            dataset=make_dataset_urn_with_platform_instance(
                                platform=self.platform,
                                name=f"{keyspace_name}.{getattr(view, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['base_table_name'])}",
                                env=self.config.env,
                                platform_instance=self.config.platform_instance,
                            ),
                            type=DatasetLineageTypeClass.VIEW,
                        )
                    ]
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

    def get_report(self):
        return self.report

    def close(self):
        self.cassandra_api.close()
        super().close()
