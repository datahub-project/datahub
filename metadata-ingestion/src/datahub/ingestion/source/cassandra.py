import json
import logging
from dataclasses import dataclass, field
from ssl import CERT_NONE, PROTOCOL_TLSv1_2, SSLContext
from typing import Any, Dict, Generator, Iterable, List, Optional, Type

from cassandra.auth import AuthProvider, PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    ContainerKey,
    KeyspaceKey,
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
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


# -------------------------------------------------- constants -------------------------------------------------- #

PLATFORM_NAME_IN_DATAHUB = "cassandra"

# we always skip over ingesting metadata about these keyspaces
SYSTEM_KEYSPACE_LIST = set(
    ["system", "system_auth", "system_schema", "system_distributed", "system_traces"]
)

# - Referencing https://docs.datastax.com/en/cql-oss/3.x/cql/cql_using/useQuerySystem.html#Table3.ColumnsinSystem_SchemaTables-Cassandra3.0 - #
# this keyspace contains details about the cassandra cluster's keyspaces, tables, and columns
SYSTEM_SCHEMA_KESPACE_NAME = "system_schema"
# these are the names of the tables we're interested in querying metadata from
CASSANDRA_SYSTEM_SCHEMA_TABLES = {
    "keyspaces": "keyspaces",
    "tables": "tables",
    "views": "views",
    "columns": "columns",
}
# these column names are present on the system_schema tables
CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES = {
    "keyspace_name": "keyspace_name",  # present on all tables
    "table_name": "table_name",  # present on tables table
    "column_name": "column_name",  # present on columns table
    "column_type": "type",  # present on columns table
    "view_name": "view_name",  # present on views table
    "base_table_name": "base_table_name",  # present on views table
    "where_clause": "where_clause",  # present on views table
}


# -------------------------------------------------- queries -------------------------------------------------- #
# get all keyspaces
GET_KEYSPACES_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['keyspaces']}"
# get all tables for a keyspace
GET_TABLES_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['tables']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s"
# get all columns for a table
GET_COLUMNS_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['columns']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s AND {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['table_name']} = %s"
# get all views for a keyspace
GET_VIEWS_QUERY = f"SELECT * FROM {SYSTEM_SCHEMA_KESPACE_NAME}.{CASSANDRA_SYSTEM_SCHEMA_TABLES['views']} WHERE {CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES['keyspace_name']} = %s"


# -------------------------------------------------- source config and reporter -------------------------------------------------- #

# config
class CassandraSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    contact_point: str = Field(
        default="localhost",
        description="The cassandra instance contact point domain (without the port).",
    )
    port: str = Field(default="9042", description="The cassandra instance port.")
    username: str = Field(
        default="",
        description=f"The username credential. Ensure user has read access to {SYSTEM_SCHEMA_KESPACE_NAME}.",
    )
    password: str = Field(default="", description="The password credential.")
    keyspace_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for tables to filter in ingestion.",
    )


# source reporter
@dataclass
class CassandraSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, index: str) -> None:
        self.filtered.append(index)


# -------------------------------------------------- main source class -------------------------------------------------- #
@platform_name("Cassandra")
@config_class(CassandraSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
class CassandraSource(Source):

    """
    This plugin extracts the following:

    - Metadata for tables
    - Column types associated with each table column
    - The keyspace each table belongs to
    """

    config: CassandraSourceConfig
    report: CassandraSourceReport
    cassandra_session: Session
    platform: str

    def __init__(self, ctx: PipelineContext, config: CassandraSourceConfig):
        self.ctx = ctx
        self.platform = PLATFORM_NAME_IN_DATAHUB
        self.config = config
        self.report = CassandraSourceReport()

        # attempt to connect to cass
        auth_provider: AuthProvider = None
        if config.username and config.password:
            ssl_context = SSLContext(PROTOCOL_TLSv1_2)
            ssl_context.verify_mode = CERT_NONE
            auth_provider = PlainTextAuthProvider(
                username=config.username, password=config.password
            )

        cluster: Cluster = (
            Cluster(
                [config.contact_point],
                port=config.port,
                auth_provider=auth_provider,
                ssl_context=ssl_context,
            )
            if auth_provider
            else Cluster(
                [config.contact_point],
                port=config.port,
            )
        )
        session: Session = cluster.connect()
        self.cassandra_session = session

    @classmethod
    def create(cls, config_dict, ctx):
        config = CassandraSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:

        # get all keyspaces and iterate through them
        # TODO: create a type for the response
        keyspaces = self.cassandra_session.execute(GET_KEYSPACES_QUERY)
        keyspaces = sorted(
            keyspaces,
            key=lambda k: getattr(
                k, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]
            ),
        )

        for keyspace in keyspaces:
            keyspace_name: str = getattr(
                keyspace, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]
            )

            # skip system keyspaces
            if keyspace_name in SYSTEM_KEYSPACE_LIST:
                continue
            # skip keyspaces not allowed by the config
            if not self.config.keyspace_pattern.allowed(keyspace_name):
                self.report.report_dropped(keyspace_name)
                continue

            # 1. Construct and emit the container aspect.
            yield from self._generate_keyspace_container(keyspace_name)

            # 2. get all tables for this keyspace and emit their metadata (dataset, container and schema info)
            try:
                yield from self._extract_tables_from_keyspace(keyspace_name)
            except Exception as e:
                logger.warning(
                    f"Failed to extract table metadata for keyspace {keyspace_name}",
                    exc_info=True,
                )
                self.report.report_warning(
                    "keyspace",
                    f"Exception while extracting keyspace tables {keyspace_name}: {e}",
                )

            # 3. get all views for this keyspace and emit their metadata (dataset, container and lineage info)
            try:
                yield from self._extract_views_from_keyspace(keyspace_name)
            except Exception as e:
                logger.warning(
                    f"Failed to extract view metadata for keyspace {keyspace_name}",
                    exc_info=True,
                )
                self.report.report_warning(
                    "keyspace",
                    f"Exception while extracting keyspace views {keyspace_name}: {e}",
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
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    # get all tables for a given keyspace, iterate over them to extract column metadata
    def _extract_tables_from_keyspace(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:
        tables = self.cassandra_session.execute(GET_TABLES_QUERY, [keyspace_name])
        tables = sorted(
            tables,
            key=lambda t: getattr(
                t, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"]
            ),
        )  # sorted so output is consistent
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
                logger.warning(
                    f"Failed to extract columns from table {table_name}", exc_info=True
                )
                self.report.report_warning(
                    "table",
                    f"Exception while extracting table columns {table_name}: {e}",
                )

            # 2. Construct and emit the status aspect.
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            # 3. Construct and emit subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.TABLE,
                    ]
                ),
            ).as_workunit()

            # [4.] TODO: If useful, we can construct and emit the datasetProperties aspect here.
            # maybe emit details about the table like bloom_filter_fp_chance, caching, cdc, comment, compaction, compression,  ... max_index_interval ...
            # custom_properties: Dict[str, str] = {}

            # [5.] NOTE: Also, we don't emit the datasetProfile aspect because cassandra doesn't have a standard profiler we can tap into to cover most cases

            # 6. Connect the table to the parent keyspace container
            yield from add_dataset_to_container(
                container_key=self._generate_keyspace_container_key(keyspace_name),
                dataset_urn=dataset_urn,
            )

            # 7. Construct and emit the platform instance aspect.
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
        # 1. Construct and emit the schemaMetadata aspect
        # 1.1 get columns for table
        # TODO: create a type for the response
        column_infos = self.cassandra_session.execute(
            GET_COLUMNS_QUERY, [keyspace_name, table_name]
        )
        column_infos = sorted(column_infos, key=lambda c: c.column_name)
        schema_fields: list[SchemaField] = list(
            CassandraToSchemaFieldConverter.get_schema_fields(column_infos)
        )
        if not schema_fields:
            logger.warn(f"Table {table_name} has no columns, skipping")
            self.report.report_warning(
                "table", f"Table {table_name} has no columns, skipping"
            )
            return

        # 1.2 Generate the SchemaMetadata aspect
        # 1.2.1 remove any value that is type bytes, so it can be converted to json
        jsonable_column_infos: list[dict[str, Any]] = []
        for column in column_infos:
            column_dict = column._asdict()
            jsonable_column_dict = column_dict.copy()
            for key, value in column_dict.items():
                if isinstance(value, bytes):
                    jsonable_column_dict.pop(key)
            jsonable_column_infos.append(jsonable_column_dict)
        # 1.2.2 generate the schemaMetadata aspect
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

        # 1.3 Emit the mcp
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=schema_metadata,
        ).as_workunit()

    # NOTE: this will only emit dataset and lineage info as we can't get columns for views from the sys schema metadata
    def _extract_views_from_keyspace(
        self, keyspace_name: str
    ) -> Iterable[MetadataWorkUnit]:
        views = self.cassandra_session.execute(GET_VIEWS_QUERY, [keyspace_name])
        views = sorted(
            views,
            key=lambda v: getattr(v, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["view_name"]),
        )
        for view in views:
            # define the dataset urn for this view to be used downstream
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

            # 1. Construct and emit the status aspect.
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()

            # 2. Construct and emit subtype
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[
                        DatasetSubTypes.VIEW,
                    ]
                ),
            ).as_workunit()

            # 3. Construct and emit lineage off of 'base_table_name'
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

            # 4. Connect the view to the parent keyspace container
            yield from add_dataset_to_container(
                container_key=self._generate_keyspace_container_key(keyspace_name),
                dataset_urn=dataset_urn,
            )

            # 5. Construct and emit the platform instance aspect.
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
        if self.cassandra_session:
            self.cassandra_session.shutdown()
        super().close()


# -------------------------------------------------- utilities and supporting classes -------------------------------------------------- #

# This class helps convert cassandra column types to SchemaFieldDataType for use by the datahaub metadata schema
class CassandraToSchemaFieldConverter:
    # FieldPath format version.
    version_string: str = "[version=2.0]"

    # Mapping from cassandra field types to SchemaFieldDataType.
    # https://cassandra.apache.org/doc/stable/cassandra/cql/types.html (version 4.1)
    _field_type_to_schema_field_type: Dict[str, Type] = {
        # Bool
        "boolean": BooleanTypeClass,
        # Binary
        "blob": BytesTypeClass,
        # Numbers
        "bigint": NumberTypeClass,
        "counter": NumberTypeClass,
        "decimal": NumberTypeClass,
        "double": NumberTypeClass,
        "float": NumberTypeClass,
        "int": NumberTypeClass,
        "smallint": NumberTypeClass,
        "tinyint": NumberTypeClass,
        "varint": NumberTypeClass,
        # Dates
        "date": DateTypeClass,
        # Times
        "duration": TimeTypeClass,
        "time": TimeTypeClass,
        "timestamp": TimeTypeClass,
        # Strings
        "text": StringTypeClass,
        "ascii": StringTypeClass,
        "inet": StringTypeClass,
        "timeuuid": StringTypeClass,
        "uuid": StringTypeClass,
        "varchar": StringTypeClass,
        # Records
        "geo_point": RecordTypeClass,
        # Arrays
        "histogram": ArrayTypeClass,
    }

    @staticmethod
    def get_column_type(cassandra_column_type: str) -> SchemaFieldDataType:
        type_class: Optional[
            Type
        ] = CassandraToSchemaFieldConverter._field_type_to_schema_field_type.get(
            cassandra_column_type
        )
        if type_class is None:
            logger.warning(
                f"Cannot map {cassandra_column_type!r} to SchemaFieldDataType, using NullTypeClass."
            )
            type_class = NullTypeClass

        return SchemaFieldDataType(type=type_class())

    def __init__(self) -> None:
        self._prefix_name_stack: List[str] = [self.version_string]

    def _get_cur_field_path(self) -> str:
        return ".".join(self._prefix_name_stack)

    def _get_schema_fields(
        self, cassandra_column_infos: List[dict[str, Any]]
    ) -> Generator[SchemaField, None, None]:
        # append each schema field (sort so output is consistent)
        for column_info in cassandra_column_infos:
            # convert namedtuple to dictionary if it isn't already one
            column_info = (
                column_info._asdict()
                if hasattr(column_info, "_asdict")
                else column_info
            )
            column_info["column_name_bytes"] = None
            column_name: str = column_info[
                CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_name"]
            ]
            cassandra_type: str = column_info[
                CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_type"]
            ]
            if cassandra_type is not None:
                self._prefix_name_stack.append(f"[type={cassandra_type}].{column_name}")
                schema_field_data_type: SchemaFieldDataType = self.get_column_type(
                    cassandra_type
                )
                schema_field: SchemaField = SchemaField(
                    fieldPath=self._get_cur_field_path(),
                    nativeDataType=cassandra_type,
                    type=schema_field_data_type,
                    description=None,
                    nullable=True,
                    recursive=False,
                )
                yield schema_field
                self._prefix_name_stack.pop()
            else:
                # Unexpected! Log a warning.
                logger.warning(
                    f"Cassandra schema does not have 'type'!"
                    f" Schema={json.dumps(column_info)}"
                )
                continue

    @classmethod
    def get_schema_fields(
        cls, cassandra_column_infos: List[dict[str, Any]]
    ) -> Generator[SchemaField, None, None]:
        converter = cls()
        yield from converter._get_schema_fields(cassandra_column_infos)
