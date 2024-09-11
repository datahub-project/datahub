import contextlib
import datetime
import functools
import logging
import traceback
from dataclasses import dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import sqlalchemy.dialects.postgresql.base
from sqlalchemy import create_engine, inspect, log as sqlalchemy_log
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.row import LegacyRow
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import sqltypes as types
from sqlalchemy.types import TypeDecorator, TypeEngine

from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.sql_parsing_builder import SqlParsingBuilder
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import capability
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import (
    SAMPLE_SIZE_MULTIPLIER,
    ClassificationHandler,
    ClassificationReportMixin,
)
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.sql.sql_config import SQLCommonConfig
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    downgrade_schema_from_v2,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_domain_wu,
    schema_requires_v2,
)
from datahub.ingestion.source.sql.sqlalchemy_data_reader import (
    SqlAlchemyTableDataReader,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import StatusClass
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
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
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    GlobalTagsClass,
    SubTypesClass,
    TagAssociationClass,
    UpstreamClass,
    ViewPropertiesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    sqlglot_lineage,
    view_definition_lineage_helper,
)
from datahub.telemetry import telemetry
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.lossy_collections import LossyList
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.sqlalchemy_query_combiner import SQLAlchemyQueryCombinerReport
from datahub.utilities.sqlalchemy_type_converter import (
    get_native_data_type_for_sqlalchemy_type,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.ge_data_profiler import (
        DatahubGEProfiler,
        GEProfilerRequest,
    )

logger: logging.Logger = logging.getLogger(__name__)


@dataclass
class SQLSourceReport(StaleEntityRemovalSourceReport, ClassificationReportMixin):
    tables_scanned: int = 0
    views_scanned: int = 0
    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    query_combiner: Optional[SQLAlchemyQueryCombinerReport] = None

    num_view_definitions_parsed: int = 0
    num_view_definitions_failed_parsing: int = 0
    num_view_definitions_failed_column_parsing: int = 0
    view_definitions_parsing_failures: LossyList[str] = field(default_factory=LossyList)

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


class SqlWorkUnit(MetadataWorkUnit):
    pass


_field_type_mapping: Dict[Type[TypeEngine], Type] = {
    # Note: to add dialect-specific types to this mapping, use the `register_custom_type` function.
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
    # Because the postgresql dialect is used internally by many other dialects,
    # we add some postgres types here. This is ok to do because the postgresql
    # dialect is built-in to sqlalchemy.
    sqlalchemy.dialects.postgresql.base.BYTEA: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.DOUBLE_PRECISION: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.INET: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MACADDR: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.MONEY: NumberTypeClass,
    sqlalchemy.dialects.postgresql.base.OID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.REGCLASS: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.TIMESTAMP: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.TIME: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.INTERVAL: TimeTypeClass,
    sqlalchemy.dialects.postgresql.base.BIT: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.UUID: StringTypeClass,
    sqlalchemy.dialects.postgresql.base.TSVECTOR: BytesTypeClass,
    sqlalchemy.dialects.postgresql.base.ENUM: EnumTypeClass,
    # When SQLAlchemy is unable to map a type into its internal hierarchy, it
    # assigns the NullType by default. We want to carry this warning through.
    types.NullType: NullTypeClass,
}
_known_unknown_field_types: Set[Type[TypeEngine]] = {
    types.Interval,
    types.CLOB,
}


def register_custom_type(tp: Type[TypeEngine], output: Optional[Type] = None) -> None:
    if output:
        _field_type_mapping[tp] = output
    else:
        _known_unknown_field_types.add(tp)


class _CustomSQLAlchemyDummyType(TypeDecorator):
    impl = types.LargeBinary


def make_sqlalchemy_type(name: str) -> Type[TypeEngine]:
    # This usage of type() dynamically constructs a class.
    # See https://stackoverflow.com/a/15247202/5004662 and
    # https://docs.python.org/3/library/functions.html#type.
    sqlalchemy_type: Type[TypeEngine] = type(
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
        sql_report.info(
            title="Unable to map column types to DataHub types",
            message="Got an unexpected column type. The column's parsed field type will not be populated.",
            context=f"{dataset_name} - {column_type!r}",
            log=False,
        )
        TypeClass = NullTypeClass

    return SchemaFieldDataType(type=TypeClass())


def get_schema_metadata(
    sql_report: SQLSourceReport,
    dataset_name: str,
    platform: str,
    columns: List[dict],
    pk_constraints: Optional[dict] = None,
    foreign_keys: Optional[List[ForeignKeyConstraint]] = None,
    canonical_schema: Optional[List[SchemaField]] = None,
    simplify_nested_field_paths: bool = False,
) -> SchemaMetadata:
    if (
        simplify_nested_field_paths
        and canonical_schema is not None
        and not schema_requires_v2(canonical_schema)
    ):
        canonical_schema = downgrade_schema_from_v2(canonical_schema)

    schema_metadata = SchemaMetadata(
        schemaName=dataset_name,
        platform=make_data_platform_urn(platform),
        version=0,
        hash="",
        platformSchema=MySqlDDL(tableSchema=""),
        fields=canonical_schema or [],
    )
    if foreign_keys is not None and foreign_keys != []:
        schema_metadata.foreignKeys = foreign_keys

    return schema_metadata


# config flags to emit telemetry for
config_options_to_report = [
    "include_views",
    "include_tables",
]


@dataclass
class ProfileMetadata:
    """
    A class to hold information about the table for profile enrichment
    """

    dataset_name_to_storage_bytes: Dict[str, int] = field(default_factory=dict)


@capability(
    SourceCapability.CLASSIFICATION,
    "Optionally enabled via `classification.enabled`",
    supported=True,
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Enabled by default",
    supported=True,
)
@capability(
    SourceCapability.DOMAINS,
    "Enabled by default",
    supported=True,
)
class SQLAlchemySource(StatefulIngestionSourceBase, TestableSource):
    """A Base class for all SQL Sources that use SQLAlchemy to extend"""

    def __init__(self, config: SQLCommonConfig, ctx: PipelineContext, platform: str):
        super().__init__(config, ctx)
        self.config = config
        self.platform = platform
        self.report: SQLSourceReport = SQLSourceReport()
        self.profile_metadata_info: ProfileMetadata = ProfileMetadata()

        self.classification_handler = ClassificationHandler(self.config, self.report)
        config_report = {
            config_option: config.dict().get(config_option)
            for config_option in config_options_to_report
        }

        config_report = {
            **config_report,
            "profiling_enabled": config.is_profiling_enabled(),
            "platform": platform,
        }

        telemetry.telemetry_instance.ping(
            "sql_config",
            config_report,
        )

        if config.is_profiling_enabled():
            telemetry.telemetry_instance.ping(
                "sql_profiling_config",
                config.profiling.config_for_telemetry(),
            )

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        self.views_failed_parsing: Set[str] = set()
        self.schema_resolver: SchemaResolver = SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        self._view_definition_cache: MutableMapping[str, str]
        if self.config.use_file_backed_cache:
            self._view_definition_cache = FileBackedDict[str]()
        else:
            self._view_definition_cache = {}

    @classmethod
    def test_connection(cls, config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            source = cast(
                SQLAlchemySource,
                cls.create(config_dict, PipelineContext(run_id="test_connection")),
            )
            list(source.get_inspectors())
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def error(self, log: logging.Logger, key: str, reason: str) -> None:
        self.report.report_failure(key, reason[:100])
        log.error(f"{key} => {reason}\n{traceback.format_exc()}")

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.

        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            inspector = inspect(conn)
            yield inspector

    def get_db_name(self, inspector: Inspector) -> str:
        engine = inspector.engine

        if engine and hasattr(engine, "url") and hasattr(engine.url, "database"):
            if engine.url.database is None:
                return ""
            return str(engine.url.database).strip('"')
        else:
            raise Exception("Unable to get database name from Sqlalchemy inspector")

    def get_schema_names(self, inspector):
        return inspector.get_schema_names()

    def get_allowed_schemas(self, inspector: Inspector, db_name: str) -> Iterable[str]:
        # this function returns the schema names which are filtered by schema_pattern.
        for schema in self.get_schema_names(inspector):
            if not self.config.schema_pattern.allowed(schema):
                self.report.report_dropped(f"{schema}.*")
                continue
            else:
                self.add_information_for_schema(inspector, schema)
                yield schema

    def gen_database_containers(
        self,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def gen_schema_containers(
        self,
        schema: str,
        database: str,
        extra_properties: Optional[Dict[str, Any]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        schema_container_key = gen_schema_key(
            db_name=database,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_schema_container(
            database=database,
            schema=schema,
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.SCHEMA],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            extra_properties=extra_properties,
        )

    def add_table_to_schema_container(
        self,
        dataset_urn: str,
        db_name: str,
        schema: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_container_key = gen_schema_key(
            db_name=db_name,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=schema_container_key,
        )

    def get_database_level_workunits(
        self,
        inspector: Inspector,
        database: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.gen_database_containers(database=database)

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from self.gen_schema_containers(schema=schema, database=database)

        if self.config.include_tables:
            yield from self.loop_tables(inspector, schema, self.config)

        if self.config.include_views:
            yield from self.loop_views(inspector, schema, self.config)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            functools.partial(
                auto_incremental_lineage, self.config.incremental_lineage
            ),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        sql_config = self.config
        if logger.isEnabledFor(logging.DEBUG):
            # If debug logging is enabled, we also want to echo each SQL query issued.
            sql_config.options.setdefault("echo", True)
            # Patch to avoid duplicate logging
            # Known issue with sqlalchemy https://stackoverflow.com/questions/60804288/pycharm-duplicated-log-for-sqlalchemy-echo-true
            sqlalchemy_log._add_default_handler = lambda x: None  # type: ignore

        # Extra default SQLAlchemy option for better connection pooling and threading.
        # https://docs.sqlalchemy.org/en/14/core/pooling.html#sqlalchemy.pool.QueuePool.params.max_overflow
        if sql_config.is_profiling_enabled():
            sql_config.options.setdefault(
                "max_overflow", sql_config.profiling.max_workers
            )

        for inspector in self.get_inspectors():
            profiler = None
            profile_requests: List["GEProfilerRequest"] = []
            if sql_config.is_profiling_enabled():
                profiler = self.get_profiler_instance(inspector)
                try:
                    self.add_profile_metadata(inspector)
                except Exception as e:
                    self.warn(
                        logger,
                        "profile_metadata",
                        f"Failed to get enrichment data for profile {e}",
                    )

            db_name = self.get_db_name(inspector)
            yield from self.get_database_level_workunits(
                inspector=inspector,
                database=db_name,
            )

            for schema in self.get_allowed_schemas(inspector, db_name):
                self.add_information_for_schema(inspector, schema)

                yield from self.get_schema_level_workunits(
                    inspector=inspector,
                    schema=schema,
                    database=db_name,
                )

                if profiler:
                    profile_requests += list(
                        self.loop_profiler_requests(inspector, schema, sql_config)
                    )

            if profiler and profile_requests:
                yield from self.loop_profiler(
                    profile_requests, profiler, platform=self.platform
                )

        if self.config.include_view_lineage:
            yield from self.get_view_lineage()

    def get_view_lineage(self) -> Iterable[MetadataWorkUnit]:
        builder = SqlParsingBuilder(
            generate_lineage=True,
            generate_usage_statistics=False,
            generate_operations=False,
        )
        for dataset_name in self._view_definition_cache.keys():
            view_definition = self._view_definition_cache[dataset_name]
            result = self._run_sql_parser(
                dataset_name,
                view_definition,
                self.schema_resolver,
            )
            if result and result.out_tables:
                # This does not yield any workunits but we use
                # yield here to execute this method
                yield from builder.process_sql_parsing_result(
                    result=result,
                    query=view_definition,
                    is_view_ddl=True,
                    include_column_lineage=self.config.include_view_column_lineage,
                )
            else:
                self.views_failed_parsing.add(dataset_name)
        yield from builder.gen_workunits()

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        # Many SQLAlchemy dialects have three-level hierarchies. This method, which
        # subclasses can override, enables them to modify the identifiers as needed.
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
        foreign_dataset = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=referred_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )
        foreign_fields = [
            f"urn:li:schemaField:({foreign_dataset},{f})"
            for f in fk_dict["referred_columns"]
        ]

        return ForeignKeyConstraint(
            fk_dict["name"], foreign_fields, source_fields, foreign_dataset
        )

    def make_data_reader(self, inspector: Inspector) -> Optional[DataReader]:
        """
        Subclasses can override this with source-specific data reader
        if source provides clause to pick random sample instead of current
        limit-based sample
        """
        if (
            self.classification_handler
            and self.classification_handler.is_classification_enabled()
        ):
            return SqlAlchemyTableDataReader.create(inspector)

        return None

    def loop_tables(  # noqa: C901
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        tables_seen: Set[str] = set()
        data_reader = self.make_data_reader(inspector)
        with data_reader or contextlib.nullcontext():
            try:
                for table in inspector.get_table_names(schema):
                    dataset_name = self.get_identifier(
                        schema=schema, entity=table, inspector=inspector
                    )

                    if dataset_name not in tables_seen:
                        tables_seen.add(dataset_name)
                    else:
                        logger.debug(
                            f"{dataset_name} has already been seen, skipping..."
                        )
                        continue

                    self.report.report_entity_scanned(dataset_name, ent_type="table")
                    if not sql_config.table_pattern.allowed(dataset_name):
                        self.report.report_dropped(dataset_name)
                        continue

                    try:
                        yield from self._process_table(
                            dataset_name,
                            inspector,
                            schema,
                            table,
                            sql_config,
                            data_reader,
                        )
                    except Exception as e:
                        self.report.warning(
                            "Error processing table",
                            context=f"{schema}.{table}",
                            exc=e,
                        )
            except Exception as e:
                self.report.failure(
                    "Error processing tables",
                    context=schema,
                    exc=e,
                )

    def add_information_for_schema(self, inspector: Inspector, schema: str) -> None:
        pass

    def get_extra_tags(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[Dict[str, List[str]]]:
        return None

    def get_partitions(
        self, inspector: Inspector, schema: str, table: str
    ) -> Optional[List[str]]:
        return None

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        columns = self._get_columns(dataset_name, inspector, schema, table)
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )

        description, properties, location_urn = self.get_table_properties(
            inspector, schema, table
        )

        dataset_properties = DatasetPropertiesClass(
            name=table,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)

        if self.config.include_table_location_lineage and location_urn:
            external_upstream_table = UpstreamClass(
                dataset=location_urn,
                type=DatasetLineageTypeClass.COPY,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_snapshot.urn,
                aspect=UpstreamLineage(upstreams=[external_upstream_table]),
            ).as_workunit()

        extra_tags = self.get_extra_tags(inspector, schema, table)
        pk_constraints: dict = inspector.get_pk_constraint(table, schema)
        partitions: Optional[List[str]] = self.get_partitions(inspector, schema, table)
        foreign_keys = self._get_foreign_keys(dataset_urn, inspector, schema, table)
        schema_fields = self.get_schema_fields(
            dataset_name,
            columns,
            inspector,
            pk_constraints,
            tags=extra_tags,
            partition_keys=partitions,
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
        self._classify(dataset_name, schema, table, data_reader, schema_metadata)

        dataset_snapshot.aspects.append(schema_metadata)
        if self.config.include_view_lineage:
            self.schema_resolver.add_schema_metadata(dataset_urn, schema_metadata)
        db_name = self.get_db_name(inspector)

        yield from self.add_table_to_schema_container(
            dataset_urn=dataset_urn, db_name=db_name, schema=schema
        )
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataWorkUnit(
            id=f"{dataset_name}-subtypes",
            mcp=MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=SubTypesClass(typeNames=[DatasetSubTypes.TABLE]),
            ),
        )

        if self.config.domain:
            assert self.domain_registry
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=sql_config.domain,
                domain_registry=self.domain_registry,
            )

    def _classify(
        self,
        dataset_name: str,
        schema: str,
        table: str,
        data_reader: Optional[DataReader],
        schema_metadata: SchemaMetadata,
    ) -> None:
        try:
            if (
                self.classification_handler.is_classification_enabled_for_table(
                    dataset_name
                )
                and data_reader
                and schema_metadata.fields
            ):
                self.classification_handler.classify_schema_fields(
                    dataset_name,
                    schema_metadata,
                    partial(
                        data_reader.get_sample_data_for_table,
                        [schema, table],
                        int(
                            self.config.classification.sample_size
                            * SAMPLE_SIZE_MULTIPLIER
                        ),
                    ),
                )
        except Exception as e:
            logger.debug(
                f"Failed to classify table columns for {dataset_name} due to error -> {e}",
                exc_info=e,
            )
            self.report.report_warning(
                "Failed to classify table columns",
                dataset_name,
            )

    def get_database_properties(
        self, inspector: Inspector, database: str
    ) -> Optional[Dict[str, str]]:
        return None

    def get_schema_properties(
        self, inspector: Inspector, database: str, schema: str
    ) -> Optional[Dict[str, str]]:
        return None

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description: Optional[str] = None
        properties: Dict[str, str] = {}

        # The location cannot be fetched generically, but subclasses may override
        # this method and provide a location.
        location: Optional[str] = None

        try:
            # SQLAlchemy stubs are incomplete and missing this method.
            # PR: https://github.com/dropbox/sqlalchemy-stubs/pull/223.
            table_info: dict = inspector.get_table_comment(table, schema)  # type: ignore
        except NotImplementedError:
            return description, properties, location
        except ProgrammingError as pe:
            # Snowflake needs schema names quoted when fetching table comments.
            logger.debug(
                f"Encountered ProgrammingError. Retrying with quoted schema name for schema {schema} and table {table}",
                pe,
            )
            table_info: dict = inspector.get_table_comment(table, f'"{schema}"')  # type: ignore

        description = table_info.get("text")
        if isinstance(description, LegacyRow):
            # Handling for value type tuple which is coming for dialect 'db2+ibm_db'
            description = table_info["text"][0]

        # The "properties" field is a non-standard addition to SQLAlchemy's interface.
        properties = table_info.get("properties", {})
        return description, properties, location

    def get_dataplatform_instance_aspect(
        self, dataset_urn: str
    ) -> Optional[MetadataWorkUnit]:
        # If we are a platform instance based source, emit the instance aspect
        if self.config.platform_instance:
            return MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        self.platform, self.config.platform_instance
                    ),
                ),
            ).as_workunit()
        else:
            return None

    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[dict]:
        columns = []
        try:
            columns = inspector.get_columns(table, schema)
            if len(columns) == 0:
                self.warn(logger, "missing column information", dataset_name)
        except Exception as e:
            logger.error(traceback.format_exc())
            self.warn(
                logger,
                dataset_name,
                f"unable to get column information due to an error -> {e}",
            )
        return columns

    def _get_foreign_keys(
        self, dataset_urn: str, inspector: Inspector, schema: str, table: str
    ) -> List[ForeignKeyConstraint]:
        try:
            foreign_keys = [
                self.get_foreign_key_metadata(dataset_urn, schema, fk_rec, inspector)
                for fk_rec in inspector.get_foreign_keys(table, schema)
            ]
        except KeyError:
            # certain databases like MySQL cause issues due to lower-case/upper-case irregularities
            logger.debug(
                f"{dataset_urn}: failure in foreign key extraction... skipping"
            )
            foreign_keys = []
        return foreign_keys

    def get_schema_fields(
        self,
        dataset_name: str,
        columns: List[dict],
        inspector: Inspector,
        pk_constraints: Optional[dict] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[Dict[str, List[str]]] = None,
    ) -> List[SchemaField]:
        canonical_schema = []
        for column in columns:
            column_tags: Optional[List[str]] = None
            if tags:
                column_tags = tags.get(column["name"], [])
            fields = self.get_schema_fields_for_column(
                dataset_name,
                column,
                inspector,
                pk_constraints,
                tags=column_tags,
                partition_keys=partition_keys,
            )
            canonical_schema.extend(fields)
        return canonical_schema

    def get_schema_fields_for_column(
        self,
        dataset_name: str,
        column: dict,
        inspector: Inspector,
        pk_constraints: Optional[dict] = None,
        partition_keys: Optional[List[str]] = None,
        tags: Optional[List[str]] = None,
    ) -> List[SchemaField]:
        gtc: Optional[GlobalTagsClass] = None
        if tags:
            tags_str = [make_tag_urn(t) for t in tags]
            tags_tac = [TagAssociationClass(t) for t in tags_str]
            gtc = GlobalTagsClass(tags_tac)
        full_type = column.get("full_type")
        field = SchemaField(
            fieldPath=column["name"],
            type=get_column_type(self.report, dataset_name, column["type"]),
            nativeDataType=(
                full_type
                if full_type is not None
                else get_native_data_type_for_sqlalchemy_type(
                    column["type"],
                    inspector=inspector,
                )
            ),
            description=column.get("comment", None),
            nullable=column["nullable"],
            recursive=False,
            globalTags=gtc,
        )
        if (
            pk_constraints is not None
            and isinstance(pk_constraints, dict)  # some dialects (hive) return list
            and column["name"] in pk_constraints.get("constrained_columns", [])
        ):
            field.isPartOfKey = True

        if partition_keys is not None and column["name"] in partition_keys:
            field.isPartitioningKey = True

        return [field]

    def loop_views(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        try:
            for view in inspector.get_view_names(schema):
                dataset_name = self.get_identifier(
                    schema=schema, entity=view, inspector=inspector
                )
                self.report.report_entity_scanned(dataset_name, ent_type="view")

                if not sql_config.view_pattern.allowed(dataset_name):
                    self.report.report_dropped(dataset_name)
                    continue

                try:
                    yield from self._process_view(
                        dataset_name=dataset_name,
                        inspector=inspector,
                        schema=schema,
                        view=view,
                        sql_config=sql_config,
                    )
                except Exception as e:
                    self.report.warning(
                        "Error processing view",
                        context=f"{schema}.{view}",
                        exc=e,
                    )
        except Exception as e:
            self.report.failure(
                "Error processing views",
                context=schema,
                exc=e,
            )

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        try:
            columns = inspector.get_columns(view, schema)
        except KeyError:
            # For certain types of views, we are unable to fetch the list of columns.
            self.warn(logger, dataset_name, "unable to get schema for this view")
            schema_metadata = None
        else:
            schema_fields = self.get_schema_fields(dataset_name, columns, inspector)
            schema_metadata = get_schema_metadata(
                self.report,
                dataset_name,
                self.platform,
                columns,
                canonical_schema=schema_fields,
            )
            if self.config.include_view_lineage:
                self.schema_resolver.add_schema_metadata(dataset_urn, schema_metadata)
        description, properties, _ = self.get_table_properties(inspector, schema, view)
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
        if view_definition and self.config.include_view_lineage:
            self._view_definition_cache[dataset_name] = view_definition

        dataset_snapshot = DatasetSnapshot(
            urn=dataset_urn,
            aspects=[StatusClass(removed=False)],
        )
        db_name = self.get_db_name(inspector)
        yield from self.add_table_to_schema_container(
            dataset_urn=dataset_urn,
            db_name=db_name,
            schema=schema,
        )

        dataset_properties = DatasetPropertiesClass(
            name=view,
            description=description,
            customProperties=properties,
        )
        dataset_snapshot.aspects.append(dataset_properties)
        if schema_metadata:
            dataset_snapshot.aspects.append(schema_metadata)
        mce = MetadataChangeEvent(proposedSnapshot=dataset_snapshot)
        yield SqlWorkUnit(id=dataset_name, mce=mce)
        dpi_aspect = self.get_dataplatform_instance_aspect(dataset_urn=dataset_urn)
        if dpi_aspect:
            yield dpi_aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypesClass(typeNames=[DatasetSubTypes.VIEW]),
        ).as_workunit()
        if "view_definition" in properties:
            view_definition_string = properties["view_definition"]
            view_properties_aspect = ViewPropertiesClass(
                materialized=False, viewLanguage="SQL", viewLogic=view_definition_string
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=view_properties_aspect,
            ).as_workunit()

        if self.config.domain and self.domain_registry:
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=sql_config.domain,
                domain_registry=self.domain_registry,
            )

    def _run_sql_parser(
        self, view_identifier: str, query: str, schema_resolver: SchemaResolver
    ) -> Optional[SqlParsingResult]:
        try:
            database, schema = self.get_db_schema(view_identifier)
        except ValueError:
            logger.warning(f"Invalid view identifier: {view_identifier}")
            return None
        raw_lineage = sqlglot_lineage(
            query,
            schema_resolver=schema_resolver,
            default_db=database,
            default_schema=schema,
        )
        view_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            view_identifier,
            self.config.platform_instance,
            self.config.env,
        )

        if raw_lineage.debug_info.table_error:
            logger.debug(
                f"Failed to parse lineage for view {view_identifier}: "
                f"{raw_lineage.debug_info.table_error}"
            )
            self.report.num_view_definitions_failed_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Table-level sql parsing error for view {view_identifier}: {raw_lineage.debug_info.table_error}"
            )
            return None

        elif raw_lineage.debug_info.column_error:
            self.report.num_view_definitions_failed_column_parsing += 1
            self.report.view_definitions_parsing_failures.append(
                f"Column-level sql parsing error for view {view_identifier}: {raw_lineage.debug_info.column_error}"
            )
        else:
            self.report.num_view_definitions_parsed += 1
        return view_definition_lineage_helper(raw_lineage, view_urn)

    def get_db_schema(self, dataset_identifier: str) -> Tuple[Optional[str], str]:
        database, schema, _view = dataset_identifier.split(".", 2)
        return database, schema

    def get_profiler_instance(self, inspector: Inspector) -> "DatahubGEProfiler":
        from datahub.ingestion.source.ge_data_profiler import DatahubGEProfiler

        return DatahubGEProfiler(
            conn=inspector.bind,
            report=self.report,
            config=self.config.profiling,
            platform=self.platform,
            env=self.config.env,
        )

    def get_profile_args(self) -> Dict:
        """Passed down to GE profiler"""
        return {}

    # Override if needed
    def generate_partition_profiler_query(
        self, schema: str, table: str, partition_datetime: Optional[datetime.datetime]
    ) -> Tuple[Optional[str], Optional[str]]:
        return None, None

    def is_table_partitioned(
        self, database: Optional[str], schema: str, table: str
    ) -> Optional[bool]:
        return None

    # Override if needed
    def generate_profile_candidates(
        self,
        inspector: Inspector,
        threshold_time: Optional[datetime.datetime],
        schema: str,
    ) -> Optional[List[str]]:
        raise NotImplementedError()

    # Override if you want to do additional checks
    def is_dataset_eligible_for_profiling(
        self,
        dataset_name: str,
        sql_config: SQLCommonConfig,
        inspector: Inspector,
        profile_candidates: Optional[List[str]],
    ) -> bool:
        return (
            sql_config.table_pattern.allowed(dataset_name)
            and sql_config.profile_pattern.allowed(dataset_name)
        ) and (
            profile_candidates is None
            or (profile_candidates is not None and dataset_name in profile_candidates)
        )

    def loop_profiler_requests(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable["GEProfilerRequest"]:
        from datahub.ingestion.source.ge_data_profiler import GEProfilerRequest

        tables_seen: Set[str] = set()
        profile_candidates = None  # Default value if profile candidates not available.
        if (
            sql_config.profiling.profile_if_updated_since_days is not None
            or sql_config.profiling.profile_table_size_limit is not None
            or sql_config.profiling.profile_table_row_limit is None
        ):
            try:
                threshold_time: Optional[datetime.datetime] = None
                if sql_config.profiling.profile_if_updated_since_days is not None:
                    threshold_time = datetime.datetime.now(
                        datetime.timezone.utc
                    ) - datetime.timedelta(
                        sql_config.profiling.profile_if_updated_since_days
                    )
                profile_candidates = self.generate_profile_candidates(
                    inspector, threshold_time, schema
                )
            except NotImplementedError:
                logger.debug("Source does not support generating profile candidates.")

        for table in inspector.get_table_names(schema):
            dataset_name = self.get_identifier(
                schema=schema, entity=table, inspector=inspector
            )
            if not self.is_dataset_eligible_for_profiling(
                dataset_name, sql_config, inspector, profile_candidates
            ):
                if self.config.profiling.report_dropped_profiles:
                    self.report.report_dropped(f"profile of {dataset_name}")
                continue

            if dataset_name not in tables_seen:
                tables_seen.add(dataset_name)
            else:
                logger.debug(f"{dataset_name} has already been seen, skipping...")
                continue

            (partition, custom_sql) = self.generate_partition_profiler_query(
                schema, table, self.config.profiling.partition_datetime
            )

            if partition is None and self.is_table_partitioned(
                database=None, schema=schema, table=table
            ):
                self.warn(
                    logger,
                    "profile skipped as partitioned table is empty or partition id was invalid",
                    dataset_name,
                )
                continue

            if (
                partition is not None
                and not self.config.profiling.partition_profiling_enabled
            ):
                logger.debug(
                    f"{dataset_name} and partition {partition} is skipped because profiling.partition_profiling_enabled property is disabled"
                )
                continue

            self.report.report_entity_profiled(dataset_name)
            logger.debug(
                f"Preparing profiling request for {schema}, {table}, {partition}"
            )
            yield GEProfilerRequest(
                pretty_name=dataset_name,
                batch_kwargs=self.prepare_profiler_args(
                    inspector=inspector,
                    schema=schema,
                    table=table,
                    partition=partition,
                    custom_sql=custom_sql,
                ),
            )

    def add_profile_metadata(self, inspector: Inspector) -> None:
        """
        Method to add profile metadata in a sub-class that can be used to enrich profile metadata.
        This is meant to change self.profile_metadata_info in the sub-class.
        """
        pass

    def loop_profiler(
        self,
        profile_requests: List["GEProfilerRequest"],
        profiler: "DatahubGEProfiler",
        platform: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        for request, profile in profiler.generate_profiles(
            profile_requests,
            self.config.profiling.max_workers,
            platform=platform,
            profiler_args=self.get_profile_args(),
        ):
            if profile is None:
                continue
            dataset_name = request.pretty_name
            if (
                dataset_name in self.profile_metadata_info.dataset_name_to_storage_bytes
                and profile.sizeInBytes is None
            ):
                profile.sizeInBytes = (
                    self.profile_metadata_info.dataset_name_to_storage_bytes[
                        dataset_name
                    ]
                )
            dataset_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dataset_name,
                self.config.platform_instance,
                self.config.env,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=profile,
            ).as_workunit()

    def prepare_profiler_args(
        self,
        inspector: Inspector,
        schema: str,
        table: str,
        partition: Optional[str],
        custom_sql: Optional[str] = None,
    ) -> dict:
        return dict(
            schema=schema, table=table, partition=partition, custom_sql=custom_sql
        )

    def get_report(self):
        return self.report
