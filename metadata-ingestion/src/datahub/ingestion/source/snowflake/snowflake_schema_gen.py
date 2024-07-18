import itertools
import logging
from typing import Dict, Iterable, List, Optional, Union

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    classification_workunit_processor,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SNOWFLAKE_DATABASE,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakePermissionError,
)
from datahub.ingestion.source.snowflake.snowflake_data_reader import SnowflakeDataReader
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SCHEMA_PARALLELISM,
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeSchema,
    SnowflakeTable,
    SnowflakeTag,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
    SnowflakeStructuredReportMixin,
    SnowsightUrlBuilder,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
    gen_schema_container,
    gen_schema_key,
    get_dataplatform_instance_aspect,
    get_domain_wu,
)
from datahub.ingestion.source_report.ingestion_stage import (
    METADATA_EXTRACTION,
    PROFILING,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    GlobalTags,
    Status,
    SubTypes,
    TagAssociation,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
    DateType,
    ForeignKeyConstraint,
    MySqlDDL,
    NullType,
    NumberType,
    RecordType,
    SchemaField,
    SchemaFieldDataType,
    SchemaMetadata,
    StringType,
    TimeType,
)
from datahub.metadata.com.linkedin.pegasus2avro.tag import TagProperties
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

# https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
SNOWFLAKE_FIELD_TYPE_MAPPINGS = {
    "DATE": DateType,
    "BIGINT": NumberType,
    "BINARY": BytesType,
    # 'BIT': BIT,
    "BOOLEAN": BooleanType,
    "CHAR": NullType,
    "CHARACTER": NullType,
    "DATETIME": TimeType,
    "DEC": NumberType,
    "DECIMAL": NumberType,
    "DOUBLE": NumberType,
    "FIXED": NumberType,
    "FLOAT": NumberType,
    "INT": NumberType,
    "INTEGER": NumberType,
    "NUMBER": NumberType,
    # 'OBJECT': ?
    "REAL": NumberType,
    "BYTEINT": NumberType,
    "SMALLINT": NumberType,
    "STRING": StringType,
    "TEXT": StringType,
    "TIME": TimeType,
    "TIMESTAMP": TimeType,
    "TIMESTAMP_TZ": TimeType,
    "TIMESTAMP_LTZ": TimeType,
    "TIMESTAMP_NTZ": TimeType,
    "TINYINT": NumberType,
    "VARBINARY": BytesType,
    "VARCHAR": StringType,
    "VARIANT": RecordType,
    "OBJECT": NullType,
    "ARRAY": ArrayType,
    "GEOGRAPHY": NullType,
}


class SnowflakeSchemaGenerator(SnowflakeStructuredReportMixin):
    platform = "snowflake"

    def __init__(
        self,
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        connection: SnowflakeConnection,
        filters: SnowflakeFilter,
        identifiers: SnowflakeIdentifierBuilder,
        domain_registry: Optional[DomainRegistry],
        profiler: Optional[SnowflakeProfiler],
        aggregator: Optional[SqlParsingAggregator],
        snowsight_url_builder: Optional[SnowsightUrlBuilder],
    ) -> None:
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = report
        self.connection: SnowflakeConnection = connection
        self.filters: SnowflakeFilter = filters
        self.identifiers: SnowflakeIdentifierBuilder = identifiers

        self.data_dictionary: SnowflakeDataDictionary = SnowflakeDataDictionary(
            connection=self.connection
        )
        self.report.data_dictionary_cache = self.data_dictionary

        self.domain_registry: Optional[DomainRegistry] = domain_registry
        self.classification_handler = ClassificationHandler(self.config, self.report)
        self.tag_extractor = SnowflakeTagExtractor(
            config, self.data_dictionary, self.report
        )
        self.profiler: Optional[SnowflakeProfiler] = profiler
        self.snowsight_url_builder: Optional[
            SnowsightUrlBuilder
        ] = snowsight_url_builder

        # These are populated as side-effects of get_workunits_internal.
        self.databases: List[SnowflakeDatabase] = []
        self.aggregator: Optional[SqlParsingAggregator] = aggregator

    def get_connection(self) -> SnowflakeConnection:
        return self.connection

    @property
    def structured_reporter(self) -> SourceReport:
        return self.report

    def gen_dataset_urn(self, dataset_identifier: str) -> str:
        return self.identifiers.gen_dataset_urn(dataset_identifier)

    def snowflake_identifier(self, identifier: str) -> str:
        return self.identifiers.snowflake_identifier(identifier)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self.databases = []
        for database in self.get_databases() or []:
            self.report.report_entity_scanned(database.name, "database")
            if not self.filters.filter_config.database_pattern.allowed(database.name):
                self.report.report_dropped(f"{database.name}.*")
            else:
                self.databases.append(database)

        if len(self.databases) == 0:
            return

        try:
            for snowflake_db in self.databases:
                self.report.set_ingestion_stage(snowflake_db.name, METADATA_EXTRACTION)
                yield from self._process_database(snowflake_db)

        except SnowflakePermissionError as e:
            self.structured_reporter.failure(
                GENERIC_PERMISSION_ERROR_KEY,
                exc=e,
            )
            return

    def get_databases(self) -> Optional[List[SnowflakeDatabase]]:
        try:
            # `show databases` is required only to get one  of the databases
            # whose information_schema can be queried to start with.
            databases = self.data_dictionary.show_databases()
        except Exception as e:
            self.structured_reporter.failure(
                "Failed to list databases",
                exc=e,
            )
            return None
        else:
            ischema_databases: List[
                SnowflakeDatabase
            ] = self.get_databases_from_ischema(databases)

            if len(ischema_databases) == 0:
                self.structured_reporter.failure(
                    GENERIC_PERMISSION_ERROR_KEY,
                    "No databases found. Please check permissions.",
                )
            return ischema_databases

    def get_databases_from_ischema(
        self, databases: List[SnowflakeDatabase]
    ) -> List[SnowflakeDatabase]:
        ischema_databases: List[SnowflakeDatabase] = []
        for database in databases:
            try:
                ischema_databases = self.data_dictionary.get_databases(database.name)
                break
            except Exception:
                # query fails if "USAGE" access is not granted for database
                # This is okay, because `show databases` query lists all databases irrespective of permission,
                # if role has `MANAGE GRANTS` privilege. (not advisable)
                logger.debug(
                    f"Failed to list databases {database.name} information_schema"
                )
                # SNOWFLAKE database always shows up even if permissions are missing
                if database == SNOWFLAKE_DATABASE:
                    continue
                logger.info(
                    f"The role {self.report.role} has `MANAGE GRANTS` privilege. This is not advisable and also not required."
                )

        return ischema_databases

    def _process_database(
        self, snowflake_db: SnowflakeDatabase
    ) -> Iterable[MetadataWorkUnit]:
        db_name = snowflake_db.name

        try:
            pass
            # self.query(SnowflakeQuery.use_database(db_name))
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                # This may happen if REFERENCE_USAGE permissions are set
                # We can not run show queries on database in such case.
                # This need not be a failure case.
                self.structured_reporter.warning(
                    "Insufficient privileges to operate on database, skipping. Please grant USAGE permissions on database to extract its metadata.",
                    db_name,
                )
            else:
                logger.debug(
                    f"Failed to use database {db_name} due to error {e}",
                    exc_info=e,
                )
                self.structured_reporter.warning(
                    "Failed to get schemas for database", db_name, exc=e
                )
            return

        if self.config.extract_tags != TagOption.skip:
            snowflake_db.tags = self.tag_extractor.get_tags_on_object(
                domain="database", db_name=db_name
            )

        if self.config.include_technical_schema:
            yield from self.gen_database_containers(snowflake_db)

        self.fetch_schemas_for_database(snowflake_db, db_name)

        if self.config.include_technical_schema and snowflake_db.tags:
            for tag in snowflake_db.tags:
                yield from self._process_tag(tag)

        # Caches tables for a single database. Consider moving to disk or S3 when possible.
        db_tables: Dict[str, List[SnowflakeTable]] = {}
        yield from self._process_db_schemas(snowflake_db, db_tables)

        if self.profiler and db_tables:
            self.report.set_ingestion_stage(snowflake_db.name, PROFILING)
            yield from self.profiler.get_workunits(snowflake_db, db_tables)

    def _process_db_schemas(
        self,
        snowflake_db: SnowflakeDatabase,
        db_tables: Dict[str, List[SnowflakeTable]],
    ) -> Iterable[MetadataWorkUnit]:
        def _process_schema_worker(
            snowflake_schema: SnowflakeSchema,
        ) -> Iterable[MetadataWorkUnit]:
            for wu in self._process_schema(
                snowflake_schema, snowflake_db.name, db_tables
            ):
                yield wu

        for wu in ThreadedIteratorExecutor.process(
            worker_func=_process_schema_worker,
            args_list=[
                (snowflake_schema,) for snowflake_schema in snowflake_db.schemas
            ],
            max_workers=SCHEMA_PARALLELISM,
        ):
            yield wu

    def fetch_schemas_for_database(
        self, snowflake_db: SnowflakeDatabase, db_name: str
    ) -> None:
        schemas: List[SnowflakeSchema] = []
        try:
            for schema in self.data_dictionary.get_schemas_for_database(db_name):
                self.report.report_entity_scanned(schema.name, "schema")
                if not is_schema_allowed(
                    self.filters.filter_config.schema_pattern,
                    schema.name,
                    db_name,
                    self.filters.filter_config.match_fully_qualified_names,
                ):
                    self.report.report_dropped(f"{db_name}.{schema.name}.*")
                else:
                    schemas.append(schema)
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                error_msg = f"Failed to get schemas for database {db_name}. Please check permissions."
                # Ideal implementation would use PEP 678 – Enriching Exceptions with Notes
                raise SnowflakePermissionError(error_msg) from e.__cause__
            else:
                self.structured_reporter.warning(
                    "Failed to get schemas for database",
                    db_name,
                    exc=e,
                )

        if not schemas:
            self.structured_reporter.warning(
                "No schemas found in database. If schemas exist, please grant USAGE permissions on them.",
                db_name,
            )
        else:
            snowflake_db.schemas = schemas

    def _process_schema(
        self,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
        db_tables: Dict[str, List[SnowflakeTable]],
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name

        if self.config.extract_tags != TagOption.skip:
            snowflake_schema.tags = self.tag_extractor.get_tags_on_object(
                schema_name=schema_name, db_name=db_name, domain="schema"
            )

        if self.config.include_technical_schema:
            yield from self.gen_schema_containers(snowflake_schema, db_name)

        # We need to do this first so that we can use it when fetching columns.
        if self.config.include_tables:
            tables = self.fetch_tables_for_schema(
                snowflake_schema, db_name, schema_name
            )
        if self.config.include_views:
            views = self.fetch_views_for_schema(snowflake_schema, db_name, schema_name)

        if self.config.include_tables:
            db_tables[schema_name] = tables

            if self.config.include_technical_schema:
                data_reader = self.make_data_reader()
                for table in tables:
                    table_wu_generator = self._process_table(
                        table, snowflake_schema, db_name
                    )

                    yield from classification_workunit_processor(
                        table_wu_generator,
                        self.classification_handler,
                        data_reader,
                        [db_name, schema_name, table.name],
                    )

        if self.config.include_views:
            if (
                self.aggregator
                and self.config.include_view_lineage
                and self.config.parse_view_ddl
            ):
                for view in views:
                    view_identifier = self.identifiers.get_dataset_identifier(
                        view.name, schema_name, db_name
                    )
                    if view.view_definition:
                        self.aggregator.add_view_definition(
                            view_urn=self.identifiers.gen_dataset_urn(view_identifier),
                            view_definition=view.view_definition,
                            default_db=db_name,
                            default_schema=schema_name,
                        )

            if self.config.include_technical_schema:
                for view in views:
                    yield from self._process_view(view, snowflake_schema, db_name)

        if self.config.include_technical_schema and snowflake_schema.tags:
            for tag in snowflake_schema.tags:
                yield from self._process_tag(tag)

        if not snowflake_schema.views and not snowflake_schema.tables:
            self.structured_reporter.warning(
                title="No tables/views found in schema",
                message="If tables exist, please grant REFERENCES or SELECT permissions on them.",
                context=f"{db_name}.{schema_name}",
            )

    def fetch_views_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeView]:
        try:
            views: List[SnowflakeView] = []
            for view in self.get_views_for_schema(schema_name, db_name):
                view_name = self.identifiers.get_dataset_identifier(
                    view.name, schema_name, db_name
                )

                self.report.report_entity_scanned(view_name, "view")

                if not self.filters.filter_config.view_pattern.allowed(view_name):
                    self.report.report_dropped(view_name)
                else:
                    views.append(view)
            snowflake_schema.views = [view.name for view in views]
            return views
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                # Ideal implementation would use PEP 678 – Enriching Exceptions with Notes
                error_msg = f"Failed to get views for schema {db_name}.{schema_name}. Please check permissions."

                raise SnowflakePermissionError(error_msg) from e.__cause__
            else:
                self.structured_reporter.warning(
                    "Failed to get views for schema",
                    f"{db_name}.{schema_name}",
                    exc=e,
                )
                return []

    def fetch_tables_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeTable]:
        try:
            tables: List[SnowflakeTable] = []
            for table in self.get_tables_for_schema(schema_name, db_name):
                table_identifier = self.identifiers.get_dataset_identifier(
                    table.name, schema_name, db_name
                )
                self.report.report_entity_scanned(table_identifier)
                if not self.filters.filter_config.table_pattern.allowed(
                    table_identifier
                ):
                    self.report.report_dropped(table_identifier)
                else:
                    tables.append(table)
            snowflake_schema.tables = [table.name for table in tables]
            return tables
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                # Ideal implementation would use PEP 678 – Enriching Exceptions with Notes
                error_msg = f"Failed to get tables for schema {db_name}.{schema_name}. Please check permissions."
                raise SnowflakePermissionError(error_msg) from e.__cause__
            else:
                self.structured_reporter.warning(
                    "Failed to get tables for schema",
                    f"{db_name}.{schema_name}",
                    exc=e,
                )
                return []

    def make_data_reader(self) -> Optional[SnowflakeDataReader]:
        if self.classification_handler.is_classification_enabled() and self.connection:
            return SnowflakeDataReader.create(
                self.connection, self.snowflake_identifier
            )

        return None

    def _process_table(
        self,
        table: SnowflakeTable,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name
        table_identifier = self.identifiers.get_dataset_identifier(
            table.name, schema_name, db_name
        )

        try:
            table.columns = self.get_columns_for_table(
                table.name, snowflake_schema, db_name
            )
            table.column_count = len(table.columns)
            if self.config.extract_tags != TagOption.skip:
                table.column_tags = self.tag_extractor.get_column_tags_for_table(
                    table.name, schema_name, db_name
                )
        except Exception as e:
            self.structured_reporter.warning(
                "Failed to get columns for table", table_identifier, exc=e
            )

        if self.config.extract_tags != TagOption.skip:
            table.tags = self.tag_extractor.get_tags_on_object(
                table_name=table.name,
                schema_name=schema_name,
                db_name=db_name,
                domain="table",
            )

        if self.config.include_technical_schema:
            if self.config.include_primary_keys:
                self.fetch_pk_for_table(table, schema_name, db_name, table_identifier)

            if self.config.include_foreign_keys:
                self.fetch_foreign_keys_for_table(
                    table, schema_name, db_name, table_identifier
                )

            yield from self.gen_dataset_workunits(table, schema_name, db_name)

    def fetch_foreign_keys_for_table(
        self,
        table: SnowflakeTable,
        schema_name: str,
        db_name: str,
        table_identifier: str,
    ) -> None:
        try:
            table.foreign_keys = self.get_fk_constraints_for_table(
                table.name, schema_name, db_name
            )
        except Exception as e:
            self.structured_reporter.warning(
                "Failed to get foreign keys for table", table_identifier, exc=e
            )

    def fetch_pk_for_table(
        self,
        table: SnowflakeTable,
        schema_name: str,
        db_name: str,
        table_identifier: str,
    ) -> None:
        try:
            table.pk = self.get_pk_constraints_for_table(
                table.name, schema_name, db_name
            )
        except Exception as e:
            self.structured_reporter.warning(
                "Failed to get primary key for table", table_identifier, exc=e
            )

    def _process_view(
        self,
        view: SnowflakeView,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name
        view_name = self.identifiers.get_dataset_identifier(
            view.name, schema_name, db_name
        )

        try:
            view.columns = self.get_columns_for_table(
                view.name, snowflake_schema, db_name
            )
            if self.config.extract_tags != TagOption.skip:
                view.column_tags = self.tag_extractor.get_column_tags_for_table(
                    view.name, schema_name, db_name
                )
        except Exception as e:
            self.structured_reporter.warning(
                "Failed to get columns for view", view_name, exc=e
            )

        if self.config.extract_tags != TagOption.skip:
            view.tags = self.tag_extractor.get_tags_on_object(
                table_name=view.name,
                schema_name=schema_name,
                db_name=db_name,
                domain="table",
            )

        if self.config.include_technical_schema:
            yield from self.gen_dataset_workunits(view, schema_name, db_name)

    def _process_tag(self, tag: SnowflakeTag) -> Iterable[MetadataWorkUnit]:
        tag_identifier = tag.identifier()

        if self.report.is_tag_processed(tag_identifier):
            return

        self.report.report_tag_processed(tag_identifier)

        yield from self.gen_tag_workunits(tag)

    def gen_dataset_workunits(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        if table.tags:
            for tag in table.tags:
                yield from self._process_tag(tag)
        for column_name in table.column_tags:
            for tag in table.column_tags[column_name]:
                yield from self._process_tag(tag)

        dataset_name = self.identifiers.get_dataset_identifier(
            table.name, schema_name, db_name
        )
        dataset_urn = self.identifiers.gen_dataset_urn(dataset_name)

        status = Status(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=status
        ).as_workunit()

        schema_metadata = self.gen_schema_metadata(table, schema_name, db_name)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

        dataset_properties = self.get_dataset_properties(table, schema_name, db_name)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

        schema_container_key = gen_schema_key(
            db_name=self.snowflake_identifier(db_name),
            schema=self.snowflake_identifier(schema_name),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from add_table_to_schema_container(
            dataset_urn=dataset_urn,
            parent_container_key=schema_container_key,
        )
        dpi_aspect = get_dataplatform_instance_aspect(
            dataset_urn=dataset_urn,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )
        if dpi_aspect:
            yield dpi_aspect

        subTypes = SubTypes(
            typeNames=(
                [DatasetSubTypes.VIEW]
                if isinstance(table, SnowflakeView)
                else [DatasetSubTypes.TABLE]
            )
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subTypes
        ).as_workunit()

        if self.domain_registry:
            yield from get_domain_wu(
                dataset_name=dataset_name,
                entity_urn=dataset_urn,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
            )

        if table.tags:
            tag_associations = [
                TagAssociation(
                    tag=make_tag_urn(self.snowflake_identifier(tag.identifier()))
                )
                for tag in table.tags
            ]
            global_tags = GlobalTags(tag_associations)
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=global_tags
            ).as_workunit()

        if isinstance(table, SnowflakeView) and table.view_definition is not None:
            view_properties_aspect = ViewProperties(
                materialized=table.materialized,
                viewLanguage="SQL",
                viewLogic=(
                    table.view_definition
                    if self.config.include_view_definitions
                    else ""
                ),
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=view_properties_aspect
            ).as_workunit()

    def get_dataset_properties(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        schema_name: str,
        db_name: str,
    ) -> DatasetProperties:
        return DatasetProperties(
            name=table.name,
            created=(
                TimeStamp(time=int(table.created.timestamp() * 1000))
                if table.created is not None
                else None
            ),
            lastModified=(
                TimeStamp(time=int(table.last_altered.timestamp() * 1000))
                if table.last_altered is not None
                else None
            ),
            description=table.comment,
            qualifiedName=f"{db_name}.{schema_name}.{table.name}",
            customProperties={},
            externalUrl=(
                self.snowsight_url_builder.get_external_url_for_table(
                    table.name,
                    schema_name,
                    db_name,
                    (
                        SnowflakeObjectDomain.TABLE
                        if isinstance(table, SnowflakeTable)
                        else SnowflakeObjectDomain.VIEW
                    ),
                )
                if self.snowsight_url_builder
                else None
            ),
        )

    def gen_tag_workunits(self, tag: SnowflakeTag) -> Iterable[MetadataWorkUnit]:
        tag_urn = make_tag_urn(self.snowflake_identifier(tag.identifier()))

        tag_properties_aspect = TagProperties(
            name=tag.display_name(),
            description=f"Represents the Snowflake tag `{tag._id_prefix_as_str()}` with value `{tag.value}`.",
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=tag_urn, aspect=tag_properties_aspect
        ).as_workunit()

    def gen_schema_metadata(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        schema_name: str,
        db_name: str,
    ) -> SchemaMetadata:
        dataset_name = self.identifiers.get_dataset_identifier(
            table.name, schema_name, db_name
        )
        dataset_urn = self.identifiers.gen_dataset_urn(dataset_name)

        foreign_keys: Optional[List[ForeignKeyConstraint]] = None
        if isinstance(table, SnowflakeTable) and len(table.foreign_keys) > 0:
            foreign_keys = self.build_foreign_keys(table, dataset_urn)

        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=[
                SchemaField(
                    fieldPath=self.snowflake_identifier(col.name),
                    type=SchemaFieldDataType(
                        SNOWFLAKE_FIELD_TYPE_MAPPINGS.get(col.data_type, NullType)()
                    ),
                    # NOTE: nativeDataType will not be in sync with older connector
                    nativeDataType=col.get_precise_native_type(),
                    description=col.comment,
                    nullable=col.is_nullable,
                    isPartOfKey=(
                        col.name in table.pk.column_names
                        if isinstance(table, SnowflakeTable) and table.pk is not None
                        else None
                    ),
                    globalTags=(
                        GlobalTags(
                            [
                                TagAssociation(
                                    make_tag_urn(
                                        self.snowflake_identifier(tag.identifier())
                                    )
                                )
                                for tag in table.column_tags[col.name]
                            ]
                        )
                        if col.name in table.column_tags
                        else None
                    ),
                )
                for col in table.columns
            ],
            foreignKeys=foreign_keys,
        )

        if self.aggregator:
            self.aggregator.register_schema(urn=dataset_urn, schema=schema_metadata)

        return schema_metadata

    def build_foreign_keys(
        self, table: SnowflakeTable, dataset_urn: str
    ) -> List[ForeignKeyConstraint]:
        foreign_keys = []
        for fk in table.foreign_keys:
            foreign_dataset = make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=self.identifiers.get_dataset_identifier(
                    fk.referred_table, fk.referred_schema, fk.referred_database
                ),
                env=self.config.env,
                platform_instance=self.config.platform_instance,
            )
            foreign_keys.append(
                ForeignKeyConstraint(
                    name=fk.name,
                    foreignDataset=foreign_dataset,
                    foreignFields=[
                        make_schema_field_urn(
                            foreign_dataset,
                            self.snowflake_identifier(col),
                        )
                        for col in fk.referred_column_names
                    ],
                    sourceFields=[
                        make_schema_field_urn(
                            dataset_urn,
                            self.snowflake_identifier(col),
                        )
                        for col in fk.column_names
                    ],
                )
            )
        return foreign_keys

    def gen_database_containers(
        self, database: SnowflakeDatabase
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            self.snowflake_identifier(database.name),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            name=database.name,
            database=self.snowflake_identifier(database.name),
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
            domain_registry=self.domain_registry,
            domain_config=self.config.domain,
            external_url=(
                self.snowsight_url_builder.get_external_url_for_database(database.name)
                if self.snowsight_url_builder
                else None
            ),
            description=database.comment,
            created=(
                int(database.created.timestamp() * 1000)
                if database.created is not None
                else None
            ),
            last_modified=(
                int(database.last_altered.timestamp() * 1000)
                if database.last_altered is not None
                else (
                    int(database.created.timestamp() * 1000)
                    if database.created is not None
                    else None
                )
            ),
            tags=(
                [self.snowflake_identifier(tag.identifier()) for tag in database.tags]
                if database.tags
                else None
            ),
        )

    def gen_schema_containers(
        self, schema: SnowflakeSchema, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = self.snowflake_identifier(schema.name)
        database_container_key = gen_database_key(
            database=self.snowflake_identifier(db_name),
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        schema_container_key = gen_schema_key(
            db_name=self.snowflake_identifier(db_name),
            schema=schema_name,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_schema_container(
            name=schema.name,
            schema=self.snowflake_identifier(schema.name),
            database=self.snowflake_identifier(db_name),
            database_container_key=database_container_key,
            domain_config=self.config.domain,
            schema_container_key=schema_container_key,
            sub_types=[DatasetContainerSubTypes.SCHEMA],
            domain_registry=self.domain_registry,
            description=schema.comment,
            external_url=(
                self.snowsight_url_builder.get_external_url_for_schema(
                    schema.name, db_name
                )
                if self.snowsight_url_builder
                else None
            ),
            created=(
                int(schema.created.timestamp() * 1000)
                if schema.created is not None
                else None
            ),
            last_modified=(
                int(schema.last_altered.timestamp() * 1000)
                if schema.last_altered is not None
                else None
            ),
            tags=(
                [self.snowflake_identifier(tag.identifier()) for tag in schema.tags]
                if schema.tags
                else None
            ),
        )

    def get_tables_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeTable]:
        tables = self.data_dictionary.get_tables_for_database(db_name)

        # get all tables for database failed,
        # falling back to get tables for schema
        if tables is None:
            self.report.num_get_tables_for_schema_queries += 1
            return self.data_dictionary.get_tables_for_schema(schema_name, db_name)

        # Some schema may not have any table
        return tables.get(schema_name, [])

    def get_views_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        views = self.data_dictionary.get_views_for_database(db_name)

        # Some schema may not have any table
        return views.get(schema_name, [])

    def get_columns_for_table(
        self, table_name: str, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeColumn]:
        schema_name = snowflake_schema.name
        columns = self.data_dictionary.get_columns_for_schema(
            schema_name,
            db_name,
            cache_exclude_all_objects=itertools.chain(
                snowflake_schema.tables, snowflake_schema.views
            ),
        )

        # Access to table but none of its columns - is this possible ?
        return columns.get(table_name, [])

    def get_pk_constraints_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> Optional[SnowflakePK]:
        constraints = self.data_dictionary.get_pk_constraints_for_schema(
            schema_name, db_name
        )

        # Access to table but none of its constraints - is this possible ?
        return constraints.get(table_name)

    def get_fk_constraints_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeFK]:
        constraints = self.data_dictionary.get_fk_constraints_for_schema(
            schema_name, db_name
        )

        # Access to table but none of its constraints - is this possible ?
        return constraints.get(table_name, [])
