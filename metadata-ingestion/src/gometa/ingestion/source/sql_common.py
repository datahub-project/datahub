from sqlalchemy import create_engine
from sqlalchemy import types
from sqlalchemy.engine import reflection
from gometa.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from gometa.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from gometa.metadata.com.linkedin.pegasus2avro.schema import SchemaMetadata, MySqlDDL
from gometa.metadata.com.linkedin.pegasus2avro.common import AuditStamp

from gometa.ingestion.api.source import WorkUnit, Source, SourceReport
from gometa.configuration.common import AllowDenyPattern
from pydantic import BaseModel
import logging
import time
from typing import Optional, List, Any, Dict
from dataclasses import dataclass, field

from gometa.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaMetadata, KafkaSchema, SchemaField, SchemaFieldDataType,
    BooleanTypeClass, FixedTypeClass, StringTypeClass, BytesTypeClass, NumberTypeClass, EnumTypeClass, NullTypeClass, MapTypeClass, ArrayTypeClass, UnionTypeClass, RecordTypeClass,
)

logger = logging.getLogger(__name__)


@dataclass
class SQLSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = field(default_factory=list)
    warnings: Dict[str, List[str]] = field(default_factory=dict)

    def report_warning(self, table_name: str, reason: str) -> None:
        if table_name not in self.warnings:
            self.warnings[table_name] = []
        self.warnings[table_name].append(reason)

    def report_table_scanned(self, table_name: str) -> None:
        self.tables_scanned += 1
    
    def report_dropped(self, table_name: str) -> None:
        self.filtered.append(table_name)


class SQLAlchemyConfig(BaseModel):
    username: str
    password: str
    host_port: str
    database: str = ""
    scheme: str
    options: Optional[dict] = {}
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    def get_sql_alchemy_url(self):
        url=f'{self.scheme}://{self.username}:{self.password}@{self.host_port}/{self.database}'
        logger.debug('sql_alchemy_url={url}')
        return url


@dataclass
class SqlWorkUnit(WorkUnit):
    mce: MetadataChangeEvent 
    
    def get_metadata(self):
        return {'mce': self.mce}
    
_field_type_mapping = {
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
}

def get_column_type(sql_report: SQLSourceReport, dataset_name: str, column_type) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """

    TypeClass: Any = None
    for sql_type in _field_type_mapping.keys():
        if isinstance(column_type, sql_type):
            TypeClass = _field_type_mapping[sql_type]
            break
    
    if TypeClass is None:
        sql_report.report_warning(dataset_name, f'unable to map type {column_type} to metadata schema')
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(sql_report: SQLSourceReport, dataset_name: str, platform: str, columns) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column['name'],
            nativeDataType=repr(column['type']),
            type=get_column_type(sql_report, dataset_name, column['type']),
            description=column.get("comment", None),
        )
        canonical_schema.append(field)


    actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f'urn:li:dataPlatform:{platform}',
        version=0,
        hash="",
        platformSchema=MySqlDDL(
            #TODO: this is bug-compatible with existing scripts. Will fix later
            tableSchema = ""
        ),
        created = AuditStamp(time=sys_time, actor=actor),
        lastModified = AuditStamp(time=sys_time, actor=actor),
        fields = canonical_schema,
    )
    return schema_metadata




       
class SQLAlchemySource(Source):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config, ctx, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SQLSourceReport()


    def get_workunits(self):
        env:str = "PROD"
        sql_config = self.config
        platform = self.platform
        url = sql_config.get_sql_alchemy_url()
        engine = create_engine(url, **sql_config.options)
        inspector = reflection.Inspector.from_engine(engine)
        database = sql_config.database
        for schema in inspector.get_schema_names():
            for table in inspector.get_table_names(schema):
                if database != "":
                    dataset_name = f'{database}.{schema}.{table}'
                else:
                    dataset_name = f'{schema}.{table}'
                self.report.report_table_scanned(dataset_name)

                if sql_config.table_pattern.allowed(dataset_name):
                    columns = inspector.get_columns(table, schema)
                    mce = MetadataChangeEvent()

                    dataset_snapshot = DatasetSnapshot()
                    dataset_snapshot.urn=(
                        f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
                    )
                    schema_metadata = get_schema_metadata(self.report, dataset_name, platform, columns)
                    dataset_snapshot.aspects.append(schema_metadata)
                    mce.proposedSnapshot = dataset_snapshot
                    
                    wu = SqlWorkUnit(id=dataset_name, mce = mce)
                    self.report.report_workunit(wu)
                    yield wu 
                else:
                    self.report.report_dropped(dataset_name)
    
    def get_report(self):
        return self.report
     
    def close(self):
        pass
