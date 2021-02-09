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
from typing import Optional, List
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class SQLSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = field(default_factory=list)

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
    

def get_column_type(column_type):
        """
        Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
        """
        if isinstance(column_type, (types.Integer, types.Numeric)):
            return ("com.linkedin.pegasus2avro.schema.NumberType", {})

        if isinstance(column_type, (types.Boolean)):
            return ("com.linkedin.pegasus2avro.schema.BooleanType", {})

        if isinstance(column_type, (types.Enum)):
            return ("com.linkedin.pegasus2avro.schema.EnumType", {})

        if isinstance(column_type, (types._Binary, types.PickleType)):
            return ("com.linkedin.pegasus2avro.schema.BytesType", {})

        if isinstance(column_type, (types.ARRAY)):
            return ("com.linkedin.pegasus2avro.schema.ArrayType", {})

        if isinstance(column_type, (types.String)):
            return ("com.linkedin.pegasus2avro.schema.StringType", {})

        return ("com.linkedin.pegasus2avro.schema.NullType", {})


def get_schema_metadata(dataset_name, platform, columns) -> SchemaMetadata:


    canonical_schema = [ {
            "fieldPath": column["name"],
            "nativeDataType": repr(column["type"]),
            "type": { "type": get_column_type(column["type"]) },
            "description": column.get("comment", None)
        } for column in columns ]


    actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f'urn:li:dataPlatform:{platform}',
        version=0,
        hash="",
        #TODO: this is bug-compatible with existing scripts. Will fix later
        platformSchema=MySqlDDL(tableSchema = ""),
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
                    schema_metadata = get_schema_metadata(dataset_name, platform, columns)
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
