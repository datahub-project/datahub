import datetime
import json
import logging
import os.path
import pathlib
from dataclasses import dataclass, field
from enum import auto
from functools import partial
from io import BufferedReader
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple, Type
from urllib import parse

import ijson
import requests
from pydantic import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigEnum, ConfigModel, ConfigurationError
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
    SourceCapability,
    capability,
)
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceReport,
    Source,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit_reporter
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.metadata.schema_classes import UsageAggregationClass, DataPlatformInstanceClass
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLSv1_2, CERT_NONE
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DataPlatformInstanceClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DateTypeClass,
    TimeTypeClass,
    NullTypeClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    SubTypesClass,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
)


logger = logging.getLogger(__name__)

DENY_KEYSPACE_LIST = set(["system", "system_auth", "system_schema", "system_distributed", "system_traces"])
CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES = {
    "keyspace_name": "keyspace_name",
    "table_name": "table_name",
    "column_name": "column_name",
    "column_type": "type",
}

class CassandraSourceConfig(PlatformInstanceConfigMixin, EnvConfigMixin):
    contact_point: str = Field(
        default="localhost", description="The cassandra instance contact point domain (without the port)."
    )
    port: str = Field(
        default="10350", description="The cassandra instance port."
    )
    username: str = Field(
        default=None, description="The username credential."
    )
    password: str = Field(
        default=None, description="The password credential."
    )
    excludeKeyspaces: List[str] = Field(
        default=list, description="The keyspaces to exclude."
    )


@dataclass
class CassandraSourceReport(SourceReport):
    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, index: str) -> None:
        self.filtered.append(index)



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


    def __init__(self, ctx: PipelineContext, config: CassandraSourceConfig):
        self.ctx = ctx
        self.config = config
        self.report = CassandraSourceReport()

        # attempt to connect to cass
        ssl_context = SSLContext(PROTOCOL_TLSv1_2)
        ssl_context.verify_mode = CERT_NONE
        auth_provider = PlainTextAuthProvider(username=config.username, password=config.password)
        cluster = Cluster([config.contact_point], port = config.port, auth_provider=auth_provider,ssl_context=ssl_context)
        session = cluster.connect()
        self.cassandra_session = session
        

    @classmethod
    def create(cls, config_dict, ctx):
        config = CassandraSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits_internal(
            self,
    ) -> Iterable[MetadataWorkUnit]:
        platform_name = "cassandra"

        # iterate through all keyspaces
        keyspaces = self.cassandra_session.execute("SELECT * FROM system_schema.keyspaces")
        keyspaces = sorted(keyspaces, key=lambda k: getattr(k, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]))
        for keyspace in keyspaces:
            # skip system keyspaces
            keyspace_name = getattr(keyspace, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"])
            if keyspace_name in DENY_KEYSPACE_LIST:
                continue
            # skip excluded keyspaces
            if keyspace_name in self.config.excludeKeyspaces:
                self.report.report_dropped(keyspace_name)
                continue
            
            tables = self.cassandra_session.execute("SELECT * FROM system_schema.tables WHERE "+CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"]+" = %s", [keyspace_name])
            # traverse tables in sorted order so output is consistent
            tables = sorted(tables, key=lambda t: getattr(t, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"]))
            for table in tables:
                # define the dataset urn for this table to be used downstream
                table_name = getattr(table, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"])
                dataset_name = f"{keyspace_name}.{table_name}"
                dataset_urn = make_dataset_urn_with_platform_instance(
                    platform=platform_name,
                    name=dataset_name,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                # 1. Construct and emit the schemaMetadata aspect
                # 1.1 get columns for table
                column_infos = self.cassandra_session.execute("SELECT * FROM system_schema.columns WHERE "+ CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["keyspace_name"] +" = %s AND "+CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["table_name"]+" = %s", [keyspace_name, table_name])
                column_infos = sorted(column_infos, key=lambda c: c.column_name)
                schema_fields = list(
                    CassandraToSchemaFieldConverter.get_schema_fields(column_infos)
                )
                if not schema_fields:
                    return
                
                # 1.2 Generate the SchemaMetadata aspect

                # remove any value that is type bytes, so it can be converted to json
                jsonable_columns = []
                for column in column_infos:
                    column_dict = column._asdict()
                    jsonable_column_dict = column_dict.copy()
                    # remove any value that is type bytes, so it can be converted to json
                    for key, value in column_dict.items():
                        if isinstance(value, bytes):
                            jsonable_column_dict.pop(key)
                    jsonable_columns.append(jsonable_column_dict)

                logger.info(f"jsonable_columns: {json.dumps(jsonable_columns)}")
                schema_metadata = SchemaMetadata(
                    schemaName=table_name,
                    platform=make_data_platform_urn(platform_name),
                    version=0,
                    hash="",
                    platformSchema=OtherSchemaClass(rawSchema=json.dumps(jsonable_columns)),
                    fields=schema_fields,
                )

                # 1.3 Emit the mcp
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=schema_metadata,
                ).as_workunit()


                # 2. Construct and emit the status aspect.
                yield MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=StatusClass(removed=False),
                ).as_workunit()
                

                # 3. TODO: Construct and emit the datasetProperties aspect.
                # maybe emit details about the table like bloom_filter_fp_chance, caching, cdc, comment, compaction, compression,  ... max_index_interval ...
                # custom_properties: Dict[str, str] = {}
                
                # 4. Construct and emit the platform instance aspect.
                if self.config.platform_instance:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=DataPlatformInstanceClass(
                            platform=make_data_platform_urn(platform_name),
                            instance=make_dataplatform_instance_urn(
                                platform_name, self.config.platform_instance
                            ),
                        ),
                    ).as_workunit()
                # 5. TODO: maybe emit the datasetProfile aspect.

    def get_report(self):
        return self.report

    def close(self):
        if self.cassandra_session:
            self.cassandra_session.shutdown()
        super().close()





class CassandraToSchemaFieldConverter:
    # FieldPath format version.
    version_string: str = "[version=2.0]"

    # Mapping from cassandra field types to SchemaFieldDataType.
    # https://cassandra.apache.org/doc/stable/cassandra/cql/types.html
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
        self, cassandra_column_schemas: [dict[str, any]]
    ) -> Generator[SchemaField, None, None]:
        # append each schema field (sort so output is consistent)
        for columnSchema in cassandra_column_schemas:
            columnName: str =  getattr(columnSchema, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_name"])
            cassandra_type: str =  getattr(columnSchema, CASSANDRA_SYSTEM_SCHEMA_COLUMN_NAMES["column_type"])
            if cassandra_type is not None:
                self._prefix_name_stack.append(f"[type={cassandra_type}].{columnName}")
                schema_field_data_type = self.get_column_type(cassandra_type)
                schema_field = SchemaField(
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
                    f" Schema={json.dumps(columnSchema)}"
                )
                continue

    @classmethod
    def get_schema_fields(
        cls, cassandra_column_schemas: [dict[str, any]]
    ) -> Generator[SchemaField, None, None]:
        converter = cls()
        yield from converter._get_schema_fields(cassandra_column_schemas)
