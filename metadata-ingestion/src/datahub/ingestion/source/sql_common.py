import logging
import time
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from sqlalchemy import create_engine
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    EnumTypeClass,
    MySqlDDL,
    NullTypeClass,
    NumberTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class SQLSourceReport(SourceReport):
    tables_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_table_scanned(self, table_name: str) -> None:
        self.tables_scanned += 1

    def report_dropped(self, table_name: str) -> None:
        self.filtered.append(table_name)


class SQLAlchemyConfig(ConfigModel):
    env: str = "PROD"
    options: dict = {}
    # Although the 'table_pattern' enables you to skip everything from certain schemas,
    # having another option to allow/deny on schema level is an optimization for the case when there is a large number
    # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
    # them out afterwards via the table_pattern.
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
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
class SqlWorkUnit(MetadataWorkUnit):
    pass


_field_type_mapping: Dict[Type[types.TypeEngine], Type] = {
    types.Integer: NumberTypeClass,
    types.Numeric: NumberTypeClass,
    types.Boolean: BooleanTypeClass,
    types.Enum: EnumTypeClass,
    types._Binary: BytesTypeClass,
    types.LargeBinary: BytesTypeClass,
    types.PickleType: BytesTypeClass,
    types.ARRAY: ArrayTypeClass,
    types.String: StringTypeClass,
    types.Date: DateTypeClass,
    types.DATE: DateTypeClass,
    types.Time: TimeTypeClass,
    types.DateTime: TimeTypeClass,
    types.DATETIME: TimeTypeClass,
    types.TIMESTAMP: TimeTypeClass,
    # When SQLAlchemy is unable to map a type into its internally hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}
_known_unknown_field_types: Set[Type[types.TypeEngine]] = {
    types.Interval,
    types.CLOB,
}


def register_custom_type(
    tp: Type[types.TypeEngine], output: Optional[Type] = None
) -> None:
    if output:
        _field_type_mapping[tp] = output
    else:
        _known_unknown_field_types.add(tp)


def get_column_type(
    sql_report: SQLSourceReport, dataset_name: str, column_type: Any
) -> SchemaFieldDataType:
    """
    Maps SQLAlchemy types (https://docs.sqlalchemy.org/en/13/core/type_basics.html) to corresponding schema types
    """

    TypeClass: Optional[Type] = None
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
    sql_report: SQLSourceReport, dataset_name: str, platform: str, columns: List[dict]
) -> SchemaMetadata:
    canonical_schema: List[SchemaField] = []
    for column in columns:
        field = SchemaField(
            fieldPath=column["name"],
            nativeDataType=repr(column["type"]),
            type=get_column_type(sql_report, dataset_name, column["type"]),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
        )
        canonical_schema.append(field)

    actor = "urn:li:corpuser:etl"
    sys_time = int(time.time() * 1000)
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
        sql_config = self.config
        platform = self.platform
        url = sql_config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **sql_config.options)
        inspector = reflection.Inspector.from_engine(engine)
        for schema in inspector.get_schema_names():
            if not sql_config.schema_pattern.allowed(schema):
                self.report.report_dropped(schema)
                continue

            for table in inspector.get_table_names(schema):
                schema, table = sql_config.standardize_schema_table_names(schema, table)
                dataset_name = sql_config.get_identifier(schema, table)
                self.report.report_table_scanned(dataset_name)

                if not sql_config.table_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                columns = inspector.get_columns(table, schema)
                try:
                    table_info: dict = inspector.get_table_comment(table, schema)
                except NotImplementedError:
                    description: Optional[str] = None
                    properties: Dict[str, str] = {}
                else:
                    description = table_info["text"]

                    # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                    properties = table_info.get("properties", {})

                # TODO: capture inspector.get_pk_constraint
                # TODO: capture inspector.get_sorted_table_and_fkc_names

                dataset_snapshot = DatasetSnapshot(
                    urn=f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{self.config.env})",
                    aspects=[],
                )
                if description is not None or properties:
                    dataset_properties = DatasetPropertiesClass(
                        description=description,
                        customProperties=properties,
                        # uri=dataset_name,
                    )
                    dataset_snapshot.aspects.append(dataset_properties)
                schema_metadata = get_schema_metadata(
                    self.report, dataset_name, platform, columns
                )
                dataset_snapshot.aspects.append(schema_metadata)

                mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
                wu = SqlWorkUnit(id=dataset_name, mce=mce)
                self.report.report_workunit(wu)
                yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
