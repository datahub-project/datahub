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
    cast,
)
from urllib.parse import quote_plus

import pydantic
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import sqltypes as types

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    DEFAULT_ENV,
    make_data_platform_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.sql_common_state import (
    BaseSQLAlchemyCheckpointState,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
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
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombinerReport

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import (
        DatahubGEProfiler,
        GEProfilerRequest,
    )

logger: logging.Logger = logging.getLogger(__name__)


def get_platform_from_sqlalchemy_uri(sqlalchemy_uri: str) -> str:
    if sqlalchemy_uri.startswith("bigquery"):
        return "bigquery"
    if sqlalchemy_uri.startswith("druid"):
        return "druid"
    if sqlalchemy_uri.startswith("mssql"):
        return "mssql"
    if (
        sqlalchemy_uri.startswith("jdbc:postgres:")
        and sqlalchemy_uri.index("redshift.amazonaws") > 0
    ):
        return "redshift"
    if sqlalchemy_uri.startswith("snowflake"):
        return "snowflake"
    if sqlalchemy_uri.startswith("presto"):
        return "presto"
    if sqlalchemy_uri.startswith("postgresql"):
        return "postgres"
    if sqlalchemy_uri.startswith("pinot"):
        return "pinot"
    if sqlalchemy_uri.startswith("oracle"):
        return "oracle"
    if sqlalchemy_uri.startswith("mysql"):
        return "mysql"
    if sqlalchemy_uri.startswith("mongodb"):
        return "mongodb"
    if sqlalchemy_uri.startswith("hive"):
        return "hive"
    if sqlalchemy_uri.startswith("awsathena"):
        return "athena"
    return "external"


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
    entities_profiled: int = 0
    filtered: List[str] = field(default_factory=list)
    soft_deleted_stale_entities: List[str] = field(default_factory=list)

    query_combiner: Optional[SQLAlchemyQueryCombinerReport] = None

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

    def report_entity_profiled(self, name: str) -> None:
        self.entities_profiled += 1

    def report_dropped(self, ent_name: str) -> None:
        self.filtered.append(ent_name)

    def report_from_query_combiner(
        self, query_combiner_report: SQLAlchemyQueryCombinerReport
    ) -> None:
        self.query_combiner = query_combiner_report

    def report_stale_entity_soft_deleted(self, urn: str) -> None:
        self.soft_deleted_stale_entities.append(urn)


class SQLAlchemyStatefulIngestionConfig(StatefulIngestionConfig):
    """
    Specialization of basic StatefulIngestionConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the SQLAlchemyConfig.
    """

    remove_stale_metadata: bool = True


class SQLAlchemyConfig(StatefulIngestionConfigBase):
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

    from datahub.ingestion.source.ge_data_profiler import GEProfilingConfig

    profiling: GEProfilingConfig = GEProfilingConfig()
    # Custom Stateful Ingestion settings
    stateful_ingestion: Optional[SQLAlchemyStatefulIngestionConfig] = None

    @pydantic.root_validator()
    def ensure_profiling_pattern_is_passed_to_profiling(
        cls, values: Dict[str, Any]
    ) -> Dict[str, Any]:
        profiling = values.get("profiling")
        if profiling is not None and profiling.enabled:
            profiling.allow_deny_patterns = values["profile_pattern"]
        return values

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
    canonical_schema: List[SchemaField] = [],
) -> SchemaMetadata:

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=make_data_platform_urn(platform),
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        fields=canonical_schema,
    )
    if foreign_keys is not None and foreign_keys != []:
        schema_metadata.foreignKeys = foreign_keys

    return schema_metadata


class SQLAlchemySource(StatefulIngestionSourceBase):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config: SQLAlchemyConfig, ctx: PipelineContext, platform: str):
        super(SQLAlchemySource, self).__init__(config, ctx)
        self.config = config
        self.platform = platform
        self.report = SQLSourceReport()

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id == self.get_default_ingestion_job_id()
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def get_default_ingestion_job_id(self) -> JobId:
        """
        Default ingestion job name that sql_common provides.
        Subclasses can override as needed.
        """
        return JobId("common_ingest_from_sql_source")

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        """
        Create the custom checkpoint with empty state for the job.
        """
        assert self.ctx.pipeline_name is not None
        if job_id == self.get_default_ingestion_job_id():
            return Checkpoint(
                job_name=job_id,
                pipeline_name=self.ctx.pipeline_name,
                platform_instance_id=self.get_platform_instance_id(),
                run_id=self.ctx.run_id,
                config=self.config,
                state=BaseSQLAlchemyCheckpointState(),
            )
        return None

    def get_schema_names(self, inspector):
        return inspector.get_schema_names()

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        config_dict = self.config.dict()
        host_port = config_dict.get("host_port", "no_host_port")
        database = config_dict.get("database", "no_database")
        return f"{self.platform}_{host_port}_{database}"

    def gen_removed_entity_workunits(self) -> Iterable[MetadataWorkUnit]:
        last_checkpoint = self.get_last_checkpoint(
            self.get_default_ingestion_job_id(), BaseSQLAlchemyCheckpointState
        )
        cur_checkpoint = self.get_current_checkpoint(
            self.get_default_ingestion_job_id()
        )
        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
            and last_checkpoint is not None
            and last_checkpoint.state is not None
            and cur_checkpoint is not None
            and cur_checkpoint.state is not None
        ):
            logger.debug("Checking for stale entity removal.")

            def soft_delete_dataset(urn: str, type: str) -> Iterable[MetadataWorkUnit]:
                logger.info(f"Soft-deleting stale entity of type {type} - {urn}.")
                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=urn,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=StatusClass(removed=True),
                )
                wu = MetadataWorkUnit(id=f"soft-delete-{type}-{urn}", mcp=mcp)
                self.report.report_workunit(wu)
                self.report.report_stale_entity_soft_deleted(urn)
                yield wu

            last_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, last_checkpoint.state
            )
            cur_checkpoint_state = cast(
                BaseSQLAlchemyCheckpointState, cur_checkpoint.state
            )

            for table_urn in last_checkpoint_state.get_table_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_dataset(table_urn, "table")

            for view_urn in last_checkpoint_state.get_view_urns_not_in(
                cur_checkpoint_state
            ):
                yield from soft_delete_dataset(view_urn, "view")

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", True)

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if sql_config.profiling.enabled:
            sql_config.options.setdefault(
                "max_overflow", sql_config.profiling.max_workers
            )

        for inspector in self.get_inspectors():
            profiler = None
            profile_requests: List["GEProfilerRequest"] = []
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

                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )

            if profiler and profile_requests:
                yield from self.loop_profiler(profile_requests, profiler)

        if self.is_stateful_ingestion_configured():
            # Clean up stale entities.
            yield from self.gen_removed_entity_workunits()

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

    def get_foreign_key_metadata(
        self,
        dataset_urn: str,
        schema: str,
        fk_dict: Dict[str, str],
        inspector: Inspector,
    ) -> ForeignKeyConstraint:
        referred_schema: Optional[str] = fk_dict.get("referred_schema")

        if not referred_schema:
            referred_schema = schema

        referred_dataset_name = self.get_identifier(
            schema=referred_schema,
            entity=fk_dict["referred_table"],
            inspector=inspector,
        )

        source_fields = [
            f"urn:li:schemaField:({dataset_urn},{f})"
            for f in fk_dict["constrained_columns"]
        ]
        foreign_dataset = make_dataset_urn(
            self.platform, referred_dataset_name, self.config.env
        )
        foreign_fields = [
            f"urn:li:schemaField:({foreign_dataset},{f})"
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
        tables_seen: Set[str] = set()
        for table in inspector.get_table_names(schema):
            schema, table = self.standardize_schema_table_names(
                schema=schema, entity=table
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )
            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
                continue

            self.report.report_entity_scanned(dataset_name, ent_type="table")

            if not sql_config.table_pattern.allowed(dataset_name):
                self.report.report_dropped(dataset_name)
                continue

            columns = []
            try:
                columns = inspector.get_columns(table, schema)
                if len(columns) == 0:
                    self.report.report_warning(
                        dataset_name, "missing column information"
                    )
            except Exception as e:
                self.report.report_warning(
                    dataset_name,
                    f"unable to get column information due to an error -> {e}",
                )

            try:
                # SQLALchemy stubs are incomplete and missing this method.
                # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
                table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
            except NotImplementedError:
                description: Optional[str] = None
                properties: Dict[str, str] = {}
            except ProgrammingError as pe:
                # Snowflake needs schema names quoted when fetching table comments.
                logger.debug(
                    f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {table}",
                    pe,
                )
                description = None
                properties = {}
                table_info: dict = inspector.get_table_comment(table, f'"{schema}"')  # type: ignore
            else:
                description = table_info["text"]

                # The "properties" field is a non-standard addition to SQLAlchemy's interface.
                properties = table_info.get("properties", {})

            dataset_urn = make_dataset_urn(self.platform, dataset_name, self.config.env)
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )
            if self.is_stateful_ingestion_configured():
                cur_checkpoint = self.get_current_checkpoint(
                    self.get_default_ingestion_job_id()
                )
                if cur_checkpoint is not None:
                    checkpoint_state = cast(
                        BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                    )
                    checkpoint_state.add_table_urn(dataset_urn)

            if description is not None or properties:
                dataset_properties = DatasetPropertiesClass(
                    description=description,
                    customProperties=properties,
                )
                dataset_snapshot.aspects.append(dataset_properties)

            pk_constraints: dict = inspector.get_pk_constraint(table, schema)
            try:
                foreign_keys = [
                    self.get_foreign_key_metadata(
                        dataset_urn, schema, fk_rec, inspector
                    )
                    for fk_rec in inspector.get_foreign_keys(table, schema)
                ]
            except KeyError:
                # certain databases like MySQL cause issues due to lower-case/upper-case irregularities
                logger.debug(
                    f"{dataset_urn}: failure in foreign key extraction... skipping"
                )
                foreign_keys = []

            schema_fields = self.get_schema_fields(
                dataset_name, columns, pk_constraints
            )

            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                pk_constraints,
                foreign_keys,
                schema_fields,
            )
            dataset_snapshot.aspects.append(schema_metadata)

            mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
            wu = SqlWorkUnit(id=dataset_name, mce=mce)
            self.report.report_workunit(wu)
            yield wu

    def get_schema_fields(
        self, dataset_name: str, columns: List[dict], pk_constraints: dict = None
    ) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            fields = self.get_schema_fields_for_column(
                dataset_name, column, pk_constraints
            )
            canonical_schema.extend(fields)
        return canonical_schema

    def get_schema_fields_for_column(
        self, dataset_name: str, column: dict, pk_constraints: dict = None
    ) -> List[SchemaField]:
        field = SchemaField(
            fieldPath=column["name"],
            type=get_column_type(self.report, dataset_name, column["type"]),
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
        return [field]

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
                schema_fields = self.get_schema_fields(dataset_name, columns)
                schema_metadata = get_schema_metadata(
                    self.report,
                    dataset_name,
                    self.platform,
                    columns,
                    canonical_schema=schema_fields,
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

            dataset_urn = make_dataset_urn(self.platform, dataset_name, self.config.env)
            dataset_snapshot = DatasetSnapshot(
                urn=dataset_urn,
                aspects=[StatusClass(removed=False)],
            )

            if self.is_stateful_ingestion_configured():
                cur_checkpoint = self.get_current_checkpoint(
                    self.get_default_ingestion_job_id()
                )
                if cur_checkpoint is not None:
                    checkpoint_state = cast(
                        BaseSQLAlchemyCheckpointState, cur_checkpoint.state
                    )
                    checkpoint_state.add_view_urn(dataset_urn)

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

    def _get_profiler_instance(self, inspector: Inspector) -> "DatahubGEProfiler":
        from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

        return DatahubGEProfiler(
            conn=inspector.bind, report=self.report, config=self.config.profiling
        )

    def loop_profiler_requests(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLAlchemyConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        for table in inspector.get_table_names(schema):
            schema, table = self.standardize_schema_table_names(
                schema=schema, entity=table
            )
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )

            if not sql_config.profile_pattern.allowed(dataset_name):
                self.report.report_dropped(f"profile of {dataset_name}")
                continue

            self.report.report_entity_profiled(dataset_name)
            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(schema=schema, table=table),
            )

    def loop_profiler(
        self, profile_requests: List["GEProfilerRequest"], profiler: "DatahubGEProfiler"
    ) -> Iterable[MetadataWorkUnit]:
        for request, profile in profiler.generate_profiles(
            profile_requests, self.config.profiling.max_workers
        ):
            if profile is None:
                continue
            dataset_name = request.pretty_name
            dataset_urn = make_dataset_urn(self.platform, dataset_name, self.config.env)
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                entityUrn=dataset_urn,
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
        )

    def get_report(self):
        return self.report

    def close(self):
        if self.is_stateful_ingestion_configured():
            # Commit the checkpoints for this run
            self.commit_checkpoints()
