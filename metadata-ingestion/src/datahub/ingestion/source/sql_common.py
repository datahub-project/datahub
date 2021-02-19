import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.source import Source, SourceReport, WorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    EnumTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
)

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
    options: Optional[dict] = {}
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass


class BasicSQLAlchemyConfig(SQLAlchemyConfig):
    username: str
    password: str
    host_port: str
    database: str = ""
    scheme: str

    def get_sql_alchemy_url(self):
        url = f"{self.scheme}://{self.username}:{self.password}@{self.host_port}/{self.database}"
        return url


@dataclass
class SqlWorkUnit(WorkUnit):
    mce: MetadataChangeEvent

    def get_metadata(self):
        return {"mce": self.mce}


_field_type_mapping = {
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    # When SQLAlchemy is unable to map a type into its internally hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}


def get_column_type(
    sql_report: SQLSourceReport, dataset_name: str, column_type
) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """

    TypeClass: Any = None
    for sql_type in _field_type_mapping.keys():
        if isinstance(column_type, sql_type):
            TypeClass = _field_type_mapping[sql_type]
            break

    if TypeClass is None:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type} to metadata schema"
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: SQLSourceReport, dataset_name: str, platform: str, columns
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column["name"],
            nativeDataType=repr(column["type"]),
            type=get_column_type(sql_report, dataset_name, column["type"]),
            description=column.get("comment", None),
        )
        canonical_schema.append(field)

    actor, sys_time = "urn:li:corpuser:etl", int(time.time()) * 1000
    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        created=AuditStamp(time=sys_time, actor=actor),
        lastModified=AuditStamp(time=sys_time, actor=actor),
        fields=canonical_schema,
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
        env: str = "PROD"
        sql_config = self.config
        platform = self.platform
        url = sql_config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **sql_config.options)
        inspector = reflection.Inspector.from_engine(engine)
        database = sql_config.database
        for schema in inspector.get_schema_names():
            for table in inspector.get_table_names(schema):
                if database != "":
                    dataset_name = f"{database}.{schema}.{table}"
                else:
                    dataset_name = f"{schema}.{table}"
                self.report.report_table_scanned(dataset_name)

                if sql_config.table_pattern.allowed(dataset_name):
                    columns = inspector.get_columns(table, schema)
                    mce = MetadataChangeEvent()

                    dataset_snapshot = DatasetSnapshot()
                    dataset_snapshot.urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
                    schema_metadata = get_schema_metadata(
                        self.report, dataset_name, platform, columns
                    )
                    dataset_snapshot.aspects.append(schema_metadata)
                    mce.proposedSnapshot = dataset_snapshot

                    wu = SqlWorkUnit(id=dataset_name, mce=mce)
                    self.report.report_workunit(wu)
                    yield wu
                else:
                    self.report.report_dropped(dataset_name)

    def get_report(self):
        return self.report

    def close(self):
        pass
