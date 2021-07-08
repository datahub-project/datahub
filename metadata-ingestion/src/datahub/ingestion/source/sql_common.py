import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type
from urllib.parse import quote_plus

import pydantic
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
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
    RecordTypeClass,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

logger: logging.Logger = logging.getLogger(__name__)


def make_sqlalchemy_uri(
    scheme: str,
    username: Optional[str],
    password: Optional[str],
    at: Optional[str],
    db: Optional[str],
    uri_opts: Optional[Dict[str, Any]] = None,
) -> str:
    url = f"{scheme}://"
    if username is not None:
        url += f"{quote_plus(username)}"
        if password is not None:
            url += f":{quote_plus(password)}"
        url += "@"
    if at is not None:
        url += f"{at}"
    if db is not None:
        url += f"/{db}"
    if uri_opts is not None:
        if db is None:
            url += "/"
        params = "&".join(
            f"{key}={quote_plus(value)}" for (key, value) in uri_opts.items() if value
        )
        url = f"{url}?{params}"
    return url


@dataclass
class SQLSourceReport(SourceReport):
    tables_scanned: int = 0
    views_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_entity_scanned(self, name: str, ent_type: str = "table") -> None:
        """
        Entity could be a view or a table
        """
        if ent_type == "table":
            self.tables_scanned += 1
        elif ent_type == "view":
            self.views_scanned += 1
        else:
            raise KeyError(f"Unknown entity {ent_type}.")

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)


class SQLAlchemyConfig(ConfigModel):
    env: str = DEFAULT_ENV
    options: dict = {}
    # Although the 'table_pattern' enables you to skip everything from certain schemas,
    # having another option to allow/deny on schema level is an optimization for the case when there is a large number
    # of schemas that one wants to skip and you want to avoid the time to needlessly fetch those tables only to filter
    # them out afterwards via the table_pattern.
    schema_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    table_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    view_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    include_views: Optional[bool] = True
    include_tables: Optional[bool] = True

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass

    def get_identifier(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # Some SQLAlchemy dialects need a standardization step to clean the schema
        # and table names. See BigQuery for an example of when this is useful.
        return schema, entity


class BasicSQLAlchemyConfig(SQLAlchemyConfig):
    username: Optional[str] = None
    password: Optional[pydantic.SecretStr] = None
    host_port: str
    database: Optional[str] = None
    scheme: str

    def get_sql_alchemy_url(self, uri_opts=None):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password.get_secret_value() if self.password else None,
            self.host_port,
            self.database,
            uri_opts=uri_opts,
        )


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
    types.JSON: RecordTypeClass,
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


class _CustomSQLAlchemyDummyType(types.TypeDecorator):
    impl = types.LargeBinary


def make_sqlalchemy_type(name: str) -> Type[types.TypeEngine]:
    # This usage of type() dynamically constructs a class.
    # See https://stackoverflow.com/a/15247202/5004662 and
    # https://docs.python.org/3/library/functions.html#type.
    sqlalchemy_type: Type[types.TypeEngine] = type(
        name,
        (_CustomSQLAlchemyDummyType,),
        {
            "__repr__": lambda self: f"{name}()",
        },
    )
    return sqlalchemy_type


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
            type=get_column_type(sql_report, dataset_name, column["type"]),
            nativeDataType=column.get("full_type", repr(column["type"])),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
        )
        canonical_schema.append(field)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
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

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        inspector = inspect(engine)
        yield inspector

    def get_workunits(self) -> Iterable[SqlWorkUnit]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options["echo"] = True

        for inspector in self.get_inspectors():
            for schema in inspector.get_schema_names():
                if not sql_config.schema_pattern.allowed(schema):
                    self.report.report_dropped(f"{schema}.*")
                    continue

                if sql_config.include_tables:
                    yield from self.loop_tables(inspector, schema, sql_config)

                if sql_config.include_views:
                    yield from self.loop_views(inspector, schema, sql_config)

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[SqlWorkUnit]:
        for table in inspector.get_table_names(schema):
            schema, table = sql_config.standardize_schema_table_names(schema, table)
            dataset_name = sql_config.get_identifier(schema, table)
            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = inspector.get_columns(table, schema)
            try:
                # SQLALchemy stubs are incomplete and missing this method.
                # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
                table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
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
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
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
                self.report, dataset_name, self.platform, columns
            )
            dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = SqlWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[SqlWorkUnit]:
        for view in inspector.get_view_names(schema):
            schema, view = sql_config.standardize_schema_table_names(schema, view)
            dataset_name = sql_config.get_identifier(schema, view)
            self.report.report_entity_scanned(dataset_name, ent_type="view")

            if not sql_config.view_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            try:
                columns = inspector.get_columns(view, schema)
            except KeyError:
                # For certain types of views, we are unable to fetch the list of columns.
                self.report.report_warning(
                    dataset_name, "unable to get schema for this view"
                )
                schema_metadata = None
            else:
                schema_metadata = get_schema_metadata(
                    self.report, dataset_name, self.platform, columns
                )

            try:
                # SQLALchemy stubs are incomplete and missing this method.
                # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
                view_info: dict = inspector.get_table_comment(view, schema)  # type: ignore
            except NotImplementedError:
                description: Optional[str] = None
                properties: Dict[str, str] = {}
            else:
                description = view_info["text"]

                # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                properties = view_info.get("properties", {})

            try:
                view_definition = inspector.get_view_definition(view, schema)
                if view_definition is None:
                    view_definition = ""
                else:
                    # Some dialects return a TextClause instead of a raw string,
                    # so we need to convert them to a string.
                    view_definition = str(view_definition)
            except NotImplementedError:
                view_definition = ""
            properties["view_definition"] = view_definition
            properties["is_view"] = "True"

            dataset_snapshot = DatasetSnapshot(
                urn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                aspects=[],
            )
            if description is not None or properties:
                dataset_properties = DatasetPropertiesClass(
                    description=description,
                    customProperties=properties,
                    # uri=dataset_name,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            if schema_metadata:
                dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = SqlWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_report(self):
        return self.report

    def close(self):
        pass
