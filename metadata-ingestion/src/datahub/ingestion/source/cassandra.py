import logging
import re
import time
from typing import (
    Iterable,
    Optional,
    List,
    Dict,
    Tuple,
    Union,
)

import cassandra.metadata
from avrogen.dict_wrapper import DictWrapper
from cassandra import cqltypes
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from dataclasses import dataclass

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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata,
    SchemaField,
    SchemaFieldDataType,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OtherSchemaClass,
    StringTypeClass,
    MapTypeClass,
    TimeTypeClass,
    NumberTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    RecordTypeClass,
    ArrayTypeClass,
    NullTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)

class CassandraConfig(ConfigModel):
    ips: List[str] = ['localhost']
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_kwargs: dict = {}
    env: str = DEFAULT_ENV
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class CassandraSource(Source):
    config: CassandraConfig
    report: SourceReport

    def __hash__(self):
        return id(self)

    def __init__(self,
                 ctx: PipelineContext,
                 config: CassandraConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        auth_provider = PlainTextAuthProvider(
            username=self.config.username,
            password=self.config.password
        )

        ips = self.config.ips
        kwargs = self.config.cluster_kwargs
        self._client = Cluster(ips,
                               auth_provider=auth_provider,
                               **kwargs)
        self._client.connect()

    @classmethod
    def create(cls,
               config_dict: dict,
               ctx: PipelineContext) -> Source:
        config = CassandraConfig.parse_obj(config_dict)
        return cls(ctx,
                   config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        platform = 'cassandra'
        keyspaces = self._get_db_keyspaces()
        for keyspace in keyspaces:
            if not self.config.schema_pattern.allowed(keyspace):
                logging.info(f'Skipping keyspace: {keyspace}')
                continue

            for table_name, table_schema in self._get_db_tables(keyspace).items():
                if not self.config.table_pattern.allowed(table_name):
                    logging.info(f'Skipping: {keyspace}.{table_name}')
                    continue

                for wu in self.prepare_metadata_for(platform,
                                          keyspace,
                                          'tables',
                                          table_name,
                                          table_schema):
                    yield wu

            for view_name, view_schema in self._get_db_views(keyspace).items():
                if not self.config.view_pattern.allowed(view_name):
                    logging.info(f'Skipping: {keyspace}.{view_name}')
                    continue
                for wu in self.prepare_metadata_for(platform,
                                          keyspace,
                                          'views',
                                          view_name,
                                          view_schema):
                    yield wu

    def prepare_metadata_for(self,
                             platform: str,
                             keyspace: str,
                             table_or_view_type: str,
                             table: str,
                             table_schema: cassandra.metadata.TableMetadata) -> Iterable[MetadataWorkUnit]:
        dataset_name = f'{keyspace}.{table}'

        # 1. Prepare DatasetSnapshot
        dataset_snapshot = DatasetSnapshot(
            urn=make_dataset_urn(platform, dataset_name, self.config.env),
            aspects=[],
        )

        # 2. Prepare DatasetPropertiesClass
        properties = {key: str(value) for key, value in table_schema.options.items()}
        dataset_properties = DatasetPropertiesClass(
            description=properties['comment'],
            customProperties=properties,
        )

        dataset_snapshot.aspects.append(dataset_properties)

        # 3. Prepare SchemaFields for SchemaMetadata
        fields: List[SchemaField] = []
        for column_name, column_schema in self._get_db_columns(table_or_view_type,
                                                               keyspace,
                                                               table).items():
            field = SchemaField(
                fieldPath=column_name,
                nativeDataType=column_schema.cql_type,
                type=self._get_db_column_type(self.report,
                                              dataset_name,
                                              column_schema.cql_type)
            )
            fields.append(field)

        # 4. Prepare SchemaMetadata
        actor = 'urn:li:corpuser:etl'
        sys_time = int(time.time() * 1000)
        primary_keys = [column_in_primary_key.name for column_in_primary_key in
                        table_schema.partition_key + table_schema.clustering_key]
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            version=0,
            hash='',
            platform=make_data_platform_urn(platform),
            platformSchema=OtherSchemaClass(rawSchema=table_schema.export_as_string()),
            fields=fields,
            created=AuditStamp(time=sys_time,
                               actor=actor),
            lastModified=AuditStamp(time=sys_time,
                                    actor=actor),
            primaryKeys=primary_keys
        )
        dataset_snapshot.aspects.append(schema_metadata)

        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        wu = MetadataWorkUnit(id=dataset_name,
                              mce=mce)
        self.report.report_workunit(wu)
        yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        self._client.shutdown()

    def _get_db_keyspaces(self) -> Dict[str, cassandra.metadata.KeyspaceMetadata]:
        return self._client.metadata.keyspaces

    def _get_db_tables(self,
                       keyspace: str) -> Dict[str, cassandra.metadata.TableMetadata]:
        return self._client.metadata.keyspaces[keyspace].tables

    def _get_db_views(self,
                      keyspace: str) -> Dict[str, cassandra.metadata.MaterializedViewMetadata]:
        return self._client.metadata.keyspaces[keyspace].views

    def _get_db_columns(self,
                        table_or_view_type: str,
                        keyspace: str,
                        table: str) -> Dict[str, cassandra.metadata.ColumnMetadata]:
        return getattr(self._client.metadata.keyspaces[keyspace], table_or_view_type)[table].columns

    def _get_db_column_type(
            self,
            report: SourceReport,
            dataset_name: str,
            column_type: str
    ) -> SchemaFieldDataType:
        type_class_instance: Optional[DictWrapper] = None

        if column_type in _field_type_mapping:
            type_class_instance = _field_type_mapping[column_type]()
        elif column_type.startswith('map'):
            key_type, value_type = self._extract_map_key_value_types(column_type)
            type_class_instance = MapTypeClass(keyType=key_type,
                                               valueType=value_type)
        elif column_type.startswith('list') or column_type.startswith('set'):
            nested_type = self._extract_map_set_nested_type(column_type)
            type_class_instance = ArrayTypeClass(nestedType=[nested_type])
        elif column_type.startswith('frozen'):
            type_class_instance = RecordTypeClass()
        else:
            report.report_warning(
                dataset_name,
                f'Unable to map type {column_type!r} to metadata schema. Mapping to NullTypeClass'
            )
            type_class_instance = NullTypeClass()

        return SchemaFieldDataType(type=type_class_instance)

    def _extract_map_set_nested_type(self,
                                     column_type: str) -> Union[str, None]:
        nested_type = None
        guessed_nested_type_search_result = re.search('<(.*)>',
                                                      column_type)
        if guessed_nested_type_search_result and len(guessed_nested_type_search_result.groups(1)):
            nested_type = guessed_nested_type_search_result.groups(1)[0]
        return nested_type

    def _extract_map_key_value_types(self,
                                     column_type: str) -> Tuple[str, str]:
        key_type = value_type = None
        guessed_search_result = re.search('<(.*),\s+(.*)>',
                                          column_type)
        if guessed_search_result and len(guessed_search_result.groups()) == 2:
            key_type, value_type = guessed_search_result.groups()
        return key_type, value_type


_field_type_mapping: Dict[str, DictWrapper] = {
    cqltypes.AsciiType.typename: NumberTypeClass,
    cqltypes.LongType.typename: NumberTypeClass,
    cqltypes.BytesType.typename: BytesTypeClass,
    cqltypes.BooleanType.typename: BooleanTypeClass,
    cqltypes.CounterColumnType.typename: NumberTypeClass,
    cqltypes.SimpleDateType.typename: DateTypeClass,
    cqltypes.DateType.typename: DateTypeClass,
    cqltypes.DecimalType.typename: NumberTypeClass,
    cqltypes.DoubleType.typename: NumberTypeClass,
    cqltypes.FloatType.typename: NumberTypeClass,
    cqltypes.FrozenType.typename: RecordTypeClass,
    cqltypes.InetAddressType.typename: StringTypeClass,
    cqltypes.Int32Type.typename: NumberTypeClass,
    cqltypes.ListType.typename: ArrayTypeClass,
    cqltypes.MapType.typename: MapTypeClass,
    cqltypes.SetType.typename: ArrayTypeClass,
    cqltypes.ShortType.typename: NumberTypeClass,
    cqltypes.UTF8Type.typename: StringTypeClass,
    cqltypes.TimeType.typename: TimeTypeClass,
    cqltypes.TimestampType.typename: TimeTypeClass,
    cqltypes.TimeUUIDType.typename: StringTypeClass,
    cqltypes.ByteType.typename: NumberTypeClass,
    cqltypes.TupleType.typename: RecordTypeClass,
    cqltypes.UUIDType.typename: StringTypeClass,
    cqltypes.VarcharType.typename: StringTypeClass,
    cqltypes.IntegerType.typename: NumberTypeClass,
}
