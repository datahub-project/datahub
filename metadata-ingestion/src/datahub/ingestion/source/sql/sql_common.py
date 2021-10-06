import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
)
from urllib.parse import quote_plus

import pydantic
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import sqltypes as types

from datahub import __package_name__
from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    ConfigurationError,
)
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.emitter.mcp import MetadataChangeProposalWrapper
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
    ForeignKeyConstraint,
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
from datahub.metadata.schema_classes import ChangeTypeClass, DatasetPropertiesClass

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

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


class GEProfilingConfig(ConfigModel):
    enabled: bool = False
    limit: Optional[int] = None
    offset: Optional[int] = None


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
    profile_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()

    include_views: Optional[bool] = True
    include_tables: Optional[bool] = True

    profiling: GEProfilingConfig = GEProfilingConfig()

    @abstractmethod
    def get_sql_alchemy_url(self):
        pass


class BasicSQLAlchemyConfig(SQLAlchemyConfig):
    username: Optional[str] = None
    password: Optional[pydantic.SecretStr] = None
    host_port: str
    database: Optional[str] = None
    database_alias: Optional[str] = None
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
    # When SQLAlchemy is unable to map a type into its internal hierarchy, it
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
    sql_report: SQLSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[dict],
    pk_constraints: dict = None,
    foreign_keys: List[ForeignKeyConstraint] = None,
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
        if (
            pk_constraints is not None
            and isinstance(pk_constraints, dict)  # some dialects (hive) return list
            and column["name"] in pk_constraints.get("constrained_columns", [])
        ):
            field.isPartOfKey = True
        canonical_schema.append(field)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=f"urn:li:dataPlatform:{platform}",
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        fields=canonical_schema,
    )
    if foreign_keys is not None and foreign_keys != []:
        schema_metadata.foreignKeys = foreign_keys

    return schema_metadata


class SQLAlchemySource(Source):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config: SQLAlchemyConfig, ctx: PipelineContext, platform: str):
        super().__init__(ctx)
        self.config = config
        self.platform = platform
        self.report = SQLSourceReport()

        if self.config.profiling.enabled and not self._can_run_profiler():
            raise ConfigurationError(
                "Table profiles requested but profiler plugin is not enabled. "
                f"Try running: pip install '{__package_name__}[sql-profiles]'"
            )

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def get_schema_names(self, inspector):
        return inspector.get_schema_names()

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", True)

        for inspector in self.get_inspectors():
            if sql_config.profiling.enabled:
                profiler = self._get_profiler_instance(inspector)

            for schema in self.get_schema_names(inspector):
                if not sql_config.schema_pattern.allowed(schema):
                    self.report.report_dropped(f"{schema}.*")
                    continue

                if sql_config.include_tables:
                    yield from self.loop_tables(inspector, schema, sql_config)

                if sql_config.include_views:
                    yield from self.loop_views(inspector, schema, sql_config)

                if sql_config.profiling.enabled:
                    yield from self.loop_profiler(
                        inspector, profiler, schema, sql_config
                    )

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # Some SQLAlchemy dialects need a standardization step to clean the schema
        # and table names. See BigQuery for an example of when this is useful.
        return schema, entity

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        # Many SQLAlchemy dialects have three-level hierarchies. This method, which
        # subclasses can override, enables them to modify the identifers as needed.
        if hasattr(self.config, "get_identifier"):
            # This path is deprecated and will eventually be removed.
            return self.config.get_identifier(schema=schema, table=entity)  # type: ignore
        else:
            return f"{schema}.{entity}"

    def get_foreign_key_metadata(self, datasetUrn, fk_dict, inspector):
        referred_dataset_name = self.get_identifier(
            schema=fk_dict["referred_schema"],
            entity=fk_dict["referred_table"],
            inspector=inspector,
        )

        source_fields = [
            f"urn:li:schemaField:({datasetUrn}, {f})"
            for f in fk_dict["constrained_columns"]
        ]
        foreign_dataset = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{referred_dataset_name},{self.config.env})"
        foreign_fields = [
            f"urn:li:schemaField:({foreign_dataset}, {f})"
            for f in fk_dict["referred_columns"]
        ]

        return ForeignKeyConstraint(
            fk_dict["name"], foreign_fields, source_fields, foreign_dataset
        )

    def loop_tables(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[SqlWorkUnit]:
        for table in inspector.get_table_names(schema):
            schema, table = self.standardize_schema_table_names(
                schema=schema, entity=table
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )
            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = inspector.get_columns(table, schema)
            if len(columns) == 0:
                self.report.report_warning(dataset_name, "missing column information")

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

            datasetUrn = f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})"
            dataset_snapshot = DatasetSnapshot(
                urn=datasetUrn,
                aspects=[],
            )

            if description is not None or properties:
                dataset_properties = DatasetPropertiesClass(
                    description=description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            pk_constraints: dict = inspector.get_pk_constraint(table, schema)
            try:
                foreign_keys = [
                    self.get_foreign_key_metadata(datasetUrn, fk_rec, inspector)
                    for fk_rec in inspector.get_foreign_keys(table, schema)
                ]
            except KeyError:
                # certain databases like MySQL cause issues due to lower-case/upper-case irregularities
                logger.debug(
                    f"{datasetUrn}: failure in foreign key extraction... skipping"
                )
                foreign_keys = []

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                pk_constraints,
                foreign_keys,
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
            schema, view = self.standardize_schema_table_names(
                schema=schema, entity=view
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=view, inspector=inspector
            )
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

    def _can_run_profiler(self) -> bool:
        try:
            from datahub.ingestion.source.ge_data_profiler import (  # noqa: F401
                DatahubGEProfiler,
            )

            return True
        except Exception:
            return False

    def _get_profiler_instance(self, inspector: Inspector) -> "DatahubGEProfiler":
        from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

        return DatahubGEProfiler(conn=inspector.bind, report=self.report)

    def loop_profiler(
        self,
        inspector: Inspector,
        profiler: "DatahubGEProfiler",
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable[MetadataWorkUnit]:
        for table in inspector.get_table_names(schema):
            schema, table = self.standardize_schema_table_names(
                schema=schema, entity=table
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )
            self.report.report_entity_scanned(f"profile of {dataset_name}")

            if not sql_config.profile_pattern.allowed(dataset_name):
                self.report.report_dropped(f"profile of {dataset_name}")
                continue

            logger.info(f"Profiling {dataset_name} (this may take a while)")
            profile = profiler.generate_profile(
                pretty_name=dataset_name,
                **self.prepare_profiler_args(schema=schema, table=table),
            )
            logger.debug(f"Finished profiling {dataset_name}")

            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=f"urn:li:dataset:(urn:li:dataPlatform:{self.platform},{dataset_name},{self.config.env})",
                changeType=ChangeTypeClass.UPSERT,
                aspectName="datasetProfile",
                aspect=profile,
            )
            wu = MetadataWorkUnit(id=f"profile-{dataset_name}", mcp=mcp)
            self.report.report_workunit(wu)
            yield wu

    def prepare_profiler_args(self, schema: str, table: str) -> dict:
        return dict(
            schema=schema,
            table=table,
            limit=self.config.profiling.limit,
            offset=self.config.profiling.offset,
        )

    def get_report(self):
        return self.report

    def close(self):
        pass
