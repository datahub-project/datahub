import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
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
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger = logging.getLogger(__name__)


@dataclass
class SQLSourceReport(SourceReport):
    tables_scanned = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, table_name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, table_name: str) -> None:
        self.filtered.append(table_name)


class SQLAlchemyConfig(ConfigModel):
    options: dict = {}
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass

    def get_identifier(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def standardize_schema_table_names(
        self, schema: str, table: str
    ) -> Tuple[str, str]:
        # Some SQLAlchemy dialects need a standardization step to clean the schema
        # and table names. See BigQuery for an example of when this is useful.
        return schema, table


class BasicSQLAlchemyConfig(SQLAlchemyConfig):
    username: Optional[str] = None
    password: Optional[str] = None
    host_port: str
    database: Optional[str] = None
    scheme: str

    def get_sql_alchemy_url(self):
        url = f"{self.scheme}://"
        if self.username:
            url += f"{self.username}"
            if self.password:
                url += f":{self.password}"
            url += "@"
        url += f"{self.host_port}"
        if self.database:
            url += f"/{self.database}"
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
    types.LargeBinary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    # When SQLAlchemy is unable to map a type into its internally hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}
_known_unknown_field_types = {
    types.Date,
    types.Time,
    types.DateTime,
    types.Interval,
    types.DATE,
    types.DATETIME,
    types.TIMESTAMP,
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
        for sql_type in _known_unknown_field_types:
            if isinstance(column_type, sql_type):
                TypeClass = NullTypeClass
                break

    if TypeClass is None:
        sql_report.report_warning(
            dataset_name, f"unable to map type {column_type!r} to metadata schema"
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

    def __init__(self, config: SQLAlchemyConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SQLSourceReport()

    def get_workunits(self) -> Iterable[SqlWorkUnit]:
        env: str = "PROD"
        sql_config = self.config
        platform = self.platform
        url = sql_config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **sql_config.options)
        inspector = reflection.Inspector.from_engine(engine)
        for schema in inspector.get_schema_names():
            for table in inspector.get_table_names(schema):
                schema, table = sql_config.standardize_schema_table_names(schema, table)
                dataset_name = sql_config.get_identifier(schema, table)
                self.report.report_table_scanned(dataset_name)

                if sql_config.table_pattern.allowed(dataset_name):
                    columns = inspector.get_columns(table, schema)
                    try:
                        description: Optional[str] = inspector.get_table_comment(
                            table, schema
                        )["text"]
                    except NotImplementedError:
                        description = None

                    # TODO: capture inspector.get_pk_constraint
                    # TODO: capture inspector.get_sorted_table_and_fkc_names

                    mce = MetadataChangeEvent()

                    dataset_snapshot = DatasetSnapshot()
                    dataset_snapshot.urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
                    if description is not None:
                        dataset_properties = DatasetPropertiesClass(
                            description=description,
                            # uri=dataset_name,
                        )
                        dataset_snapshot.aspects.append(dataset_properties)
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
