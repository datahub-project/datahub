import logging
import time
from typing import (
    Iterable,
    Optional,
    List,
)

from clickhouse_driver import Client
from clickhouse_driver.columns import service
from clickhouse_driver.columns.arraycolumn import ArrayColumn
from clickhouse_driver.columns.datecolumn import (
    DateColumn,
    Date32Column,
)
from clickhouse_driver.columns.datetimecolumn import (
    DateTimeColumn,
    DateTime64Column,
)
from clickhouse_driver.columns.decimalcolumn import (
    DecimalColumn,
    Decimal32Column,
    Decimal64Column,
    Decimal128Column,
    Decimal256Column,
)
from clickhouse_driver.columns.enumcolumn import (
    EnumColumn,
    Enum8Column,
    Enum16Column,
)
from clickhouse_driver.columns.floatcolumn import (
    FloatColumn,
    Float32Column,
    Float64Column,
)
from clickhouse_driver.columns.intcolumn import (
    IntColumn,
    UIntColumn,
    Int8Column,
    Int16Column,
    Int32Column,
    Int64Column,
    UInt8Column,
    UInt16Column,
    UInt32Column,
    UInt64Column,
    LargeIntColumn,
    Int128Column,
    UInt128Column,
    Int256Column,
    UInt256Column,
)
from clickhouse_driver.columns.ipcolumn import (
    IPv4Column,
    IPv6Column,
)
from clickhouse_driver.columns.lowcardinalitycolumn import LowCardinalityColumn
from clickhouse_driver.columns.mapcolumn import MapColumn
from clickhouse_driver.columns.stringcolumn import (
    String,
    ByteString,
    FixedString,
    ByteFixedString,
)
from clickhouse_driver.columns.tuplecolumn import TupleColumn
from clickhouse_driver.columns.uuidcolumn import UUIDColumn
from clickhouse_driver.errors import UnknownTypeError

from datahub.configuration.common import (
    ConfigModel,
    AllowDenyPattern,
)
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import (
    Source,
    SourceReport,
)
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
    SchemaMetadata,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    DateTypeClass,
    TimeTypeClass,
    NumberTypeClass,
    EnumTypeClass,
    RecordTypeClass,
    MapTypeClass,
    StringTypeClass,
    OtherSchemaClass,
    NullTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)


class ClickhouseConfig(ConfigModel):
    host: str = "localhost"
    username: Optional[str] = None
    password: Optional[str] = None
    env: str = DEFAULT_ENV
    client_kwargs: dict = {}
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


class ClickhouseSource(Source):
    config: ClickhouseConfig
    report: SourceReport

    def __hash__(self):
        return id(self)

    def __init__(self,
                 ctx: PipelineContext,
                 config: ClickhouseConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self._client = client = Client(host=self.config.host,
                                       user=self.config.username,
                                       password=self.config.password,
                                       **self.config.client_kwargs)

    @classmethod
    def create(cls,
               config_dict: dict,
               ctx: PipelineContext) -> Source:
        config = ClickhouseConfig.parse_obj(config_dict)
        return cls(ctx,
                   config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = "clickhouse"
        databases = self._client.execute('show databases')
        for database, in databases:
            if not self.config.schema_pattern.allowed(database):
                logger.info(f'Skipping database: {database}')
                continue

            tables = self._client.execute(f'show tables from {database}')
            for table, in tables:
                print(table)
                if not self.config.table_pattern.allowed(table):
                    logger.info(f'Skipping: {database}.{table}')
                    continue

                dataset_name = f'{database}.{table}'

                # 1. Prepare DatasetSnapshot
                dataset_snapshot = DatasetSnapshot(
                    urn=make_dataset_urn(platform,
                                         dataset_name,
                                         self.config.env),
                    aspects=[],
                )

                # 3. Prepare SchemaFields for SchemaMetadata
                fields: List[SchemaField] = []
                columns_query_result = self._client.execute(
                    f'select column_name, is_nullable, data_type, column_comment from information_schema.columns '
                    f'where table_schema=\'{database}\' and table_name=\'{table}\''
                )

                for column_name, is_nullable, column_raw, column_comment in columns_query_result:

                    try:
                        client_object_type = service.get_column_by_spec(
                            column_raw,
                            {
                                'context': self._client.connection.context
                            }
                        )
                    except UnknownTypeError as e:
                        if 'bool' == column_raw.lower():
                            client_object_type = Int8Column()
                        elif column_raw.startswith('AggregateFunction'):
                            client_object_type = IntColumn()
                        else:
                            client_object_type = String()

                    try:
                        datahub_type = _field_type_mapping_clickhouse[client_object_type.__class__.__name__]
                    except KeyError:
                        logger.info(f'Could not map column type: {database}.{table}.{column_raw}')
                        datahub_type = NullTypeClass

                    field = SchemaField(
                        fieldPath=column_name,
                        nativeDataType=column_raw,
                        type=SchemaFieldDataType(type=datahub_type()),
                        nullable=bool(is_nullable),
                        description=column_comment
                    )
                    fields.append(field)

                # 4. Prepare SchemaMetadata
                create_raw_query = f'show create table {database}.{table}'
                print(create_raw_query)
                table_raw = self._client.execute(create_raw_query)
                actor = 'urn:li:corpuser:etl'
                sys_time = int(time.time() * 1000)
                schema_metadata = SchemaMetadata(
                    schemaName=dataset_name,
                    version=0,
                    hash='',
                    platform=make_data_platform_urn(platform),
                    platformSchema=OtherSchemaClass(rawSchema=table_raw[0][0]),
                    fields=fields,
                    created=AuditStamp(time=sys_time,
                                       actor=actor),
                    lastModified=AuditStamp(time=sys_time,
                                            actor=actor)
                )
                dataset_snapshot.aspects.append(schema_metadata)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = MetadataWorkUnit(id=dataset_name,
                                      mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self):
        return self.report

    def close(self):
        self._client.disconnect()


# https://clickhouse.com/docs/en/sql-reference/data-types/
# https://github.com/mymarilyn/clickhouse-driver/tree/master/clickhouse_driver/columns
_field_type_mapping_clickhouse = {
    ArrayColumn.__name__: ArrayTypeClass,

    DateColumn.__name__: DateTypeClass,
    Date32Column.__name__: DateTypeClass,

    DateTimeColumn.__name__: TimeTypeClass,
    DateTime64Column.__name__: TimeTypeClass,

    DecimalColumn.__name__: NumberTypeClass,
    Decimal32Column.__name__: NumberTypeClass,
    Decimal64Column.__name__: NumberTypeClass,
    Decimal128Column.__name__: NumberTypeClass,
    Decimal256Column.__name__: NumberTypeClass,

    EnumColumn.__name__: EnumTypeClass,
    Enum8Column.__name__: EnumTypeClass,
    Enum16Column.__name__: EnumTypeClass,

    FloatColumn.__name__: NumberTypeClass,
    Float32Column.__name__: NumberTypeClass,
    Float64Column.__name__: NumberTypeClass,

    IntColumn.__name__: NumberTypeClass,
    UIntColumn.__name__: NumberTypeClass,
    Int8Column.__name__: NumberTypeClass,
    Int16Column.__name__: NumberTypeClass,
    Int32Column.__name__: NumberTypeClass,
    Int64Column.__name__: NumberTypeClass,
    UInt8Column.__name__: NumberTypeClass,
    UInt16Column.__name__: NumberTypeClass,
    UInt32Column.__name__: NumberTypeClass,
    UInt64Column.__name__: NumberTypeClass,
    LargeIntColumn.__name__: NumberTypeClass,
    Int128Column.__name__: NumberTypeClass,
    UInt128Column.__name__: NumberTypeClass,
    Int256Column.__name__: NumberTypeClass,
    UInt256Column.__name__: NumberTypeClass,

    IPv4Column.__name__: NumberTypeClass,
    IPv6Column.__name__: NumberTypeClass,

    LowCardinalityColumn.__name__: RecordTypeClass,

    MapColumn.__name__: MapTypeClass,

    String.__name__: StringTypeClass,
    ByteString.__name__: StringTypeClass,
    FixedString.__name__: StringTypeClass,
    ByteFixedString.__name__: StringTypeClass,

    TupleColumn.__name__: ArrayTypeClass,

    UUIDColumn.__name__: StringTypeClass,

    'Bool': NumberTypeClass
}
