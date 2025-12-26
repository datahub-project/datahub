import itertools
import json
import logging
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import sqlglot
import sqlglot.expressions

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_group_urn,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_structured_properties_to_entity_wu
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    classification_workunit_processor,
)
from datahub.ingestion.source.aws.s3_util import make_s3_urn_for_lineage
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    DatasetContainerSubTypes,
)
from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SNOWFLAKE_DATABASE,
    STREAMLIT_PLATFORM,
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
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SCHEMA_PARALLELISM,
    BaseProcedure,
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeDynamicTable,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeSchema,
    SnowflakeSemanticView,
    SnowflakeStream,
    SnowflakeStreamlitApp,
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
    split_qualified_name,
)
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_schema_container,
    get_dataplatform_instance_aspect,
    get_domain_wu,
)
from datahub.ingestion.source.sql.stored_procedures.base import (
    generate_procedure_container_workunits,
    generate_procedure_workunits,
)
from datahub.ingestion.source_report.ingestion_stage import (
    EXTERNAL_TABLE_DDL_LINEAGE,
    LINEAGE_EXTRACTION,
    METADATA_EXTRACTION,
    IngestionHighStage,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    GlobalTags,
    Status,
    SubTypes,
    TagAssociation,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageTypeClass,
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
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
    UpstreamClass,
    UpstreamLineageClass,
)
from datahub.metadata.urns import (
    SchemaFieldUrn,
    StructuredPropertyUrn,
)
from datahub.sdk.dashboard import Dashboard
from datahub.sql_parsing.sql_parsing_aggregator import (
    KnownLineageMapping,
    KnownQueryLineageInfo,
    SqlParsingAggregator,
)
from datahub.sql_parsing.sql_parsing_common import QueryType
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
)
from datahub.sql_parsing.sqlglot_utils import get_dialect, parse_statement
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)

# https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
# TODO: Move to the standardized types in sql_types.py
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
        config: SnowflakeV2Config,  # FIXME: SnowflakeSummary is passing here SnowflakeSummaryConfig
        report: SnowflakeV2Report,  # FIXME: SnowflakeSummary is passing here SnowflakeSummaryReport
        connection: SnowflakeConnection,
        filters: SnowflakeFilter,
        identifiers: SnowflakeIdentifierBuilder,
        domain_registry: Optional[DomainRegistry],
        profiler: Optional[SnowflakeProfiler],
        aggregator: Optional[SqlParsingAggregator],
        snowsight_url_builder: Optional[SnowsightUrlBuilder],
        fetch_views_from_information_schema: bool = False,
    ) -> None:
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = report
        self.connection: SnowflakeConnection = connection
        self.filters: SnowflakeFilter = filters
        self.identifiers: SnowflakeIdentifierBuilder = identifiers

        self.data_dictionary: SnowflakeDataDictionary = SnowflakeDataDictionary(
            connection=self.connection,
            report=self.report,
            fetch_views_from_information_schema=fetch_views_from_information_schema,
        )
        self.report.data_dictionary_cache = self.data_dictionary

        self.domain_registry: Optional[DomainRegistry] = domain_registry
        self.classification_handler = ClassificationHandler(self.config, self.report)
        self.tag_extractor = SnowflakeTagExtractor(
            config, self.data_dictionary, self.report, identifiers
        )
        self.profiler: Optional[SnowflakeProfiler] = profiler
        self.snowsight_url_builder: Optional[SnowsightUrlBuilder] = (
            snowsight_url_builder
        )

        # These are populated as side-effects of get_workunits_internal.
        self.databases: List[SnowflakeDatabase] = []

        self.aggregator = aggregator

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
        if self.config.extract_tags_as_structured_properties:
            logger.info("Creating structured property templates for tags")
            yield from self.tag_extractor.create_structured_property_templates()
            # We have to wait until cache invalidates to make sure the structured property template is available
            logger.info(
                f"Waiting for {self.config.structured_properties_template_cache_invalidation_interval} seconds for structured properties cache to invalidate"
            )
            time.sleep(
                self.config.structured_properties_template_cache_invalidation_interval
            )
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
                with self.report.new_stage(
                    f"{snowflake_db.name}: {METADATA_EXTRACTION}"
                ):
                    yield from self._process_database(snowflake_db)

            with self.report.new_stage(f"*: {EXTERNAL_TABLE_DDL_LINEAGE}"):
                discovered_tables: List[str] = [
                    self.identifiers.get_dataset_identifier(
                        table_name, schema.name, db.name
                    )
                    for db in self.databases
                    for schema in db.schemas
                    for table_name in schema.tables
                ]
                if self.aggregator:
                    for entry in self._external_tables_ddl_lineage(discovered_tables):
                        self.aggregator.add(entry)

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
            ischema_databases: List[SnowflakeDatabase] = (
                self.get_databases_from_ischema(databases)
            )

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
            with self.report.new_high_stage(IngestionHighStage.PROFILING):
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
            self._process_tags(snowflake_schema, schema_name, db_name, domain="schema")

        if self.config.include_technical_schema:
            yield from self.gen_schema_containers(snowflake_schema, db_name)

        tables, views, streams = [], [], []

        if self.config.include_tables:
            tables = self.fetch_tables_for_schema(
                snowflake_schema, db_name, schema_name
            )
        if self.config.include_views:
            views = self.fetch_views_for_schema(snowflake_schema, db_name, schema_name)

        if self.config.include_tables:
            db_tables[schema_name] = tables
            yield from self._process_tables(
                tables, snowflake_schema, db_name, schema_name
            )

        if self.config.include_views:
            yield from self._process_views(
                views, snowflake_schema, db_name, schema_name
            )

        if self.config.semantic_views.enabled:
            semantic_views = self.fetch_semantic_views_for_schema(
                snowflake_schema, db_name
            )
            yield from self._process_semantic_views(
                semantic_views, snowflake_schema, db_name, schema_name
            )

        if self.config.include_streams:
            self.report.num_get_streams_for_schema_queries += 1
            streams = self.fetch_streams_for_schema(
                snowflake_schema,
                db_name,
            )
            yield from self._process_streams(streams, snowflake_schema, db_name)

        if self.config.include_procedures:
            procedures = self.fetch_procedures_for_schema(snowflake_schema, db_name)
            yield from self._process_procedures(procedures, snowflake_schema, db_name)

        if self.config.include_streamlits:
            # TODO: Consider streaming apps one-by-one instead of loading all in memory
            streamlit_apps = self.fetch_streamlit_apps(snowflake_schema, db_name)
            yield from self._process_streamlit_apps(streamlit_apps)

        if self.config.include_technical_schema and snowflake_schema.tags:
            yield from self._process_tags_in_schema(snowflake_schema)

        if (
            not snowflake_schema.views
            and not snowflake_schema.tables
            and not snowflake_schema.semantic_views
            and not snowflake_schema.streams
        ):
            self.structured_reporter.info(
                title="No tables/views/semantic views/streams found in schema",
                message="If objects exist, please grant REFERENCES or SELECT permissions on them.",
                context=f"{db_name}.{schema_name}",
            )

    def _process_tags(
        self,
        snowflake_schema: SnowflakeSchema,
        schema_name: str,
        db_name: str,
        domain: str,
    ) -> None:
        snowflake_schema.tags = self.tag_extractor.get_tags_on_object(
            schema_name=schema_name, db_name=db_name, domain=domain
        )

    def _process_tables(
        self,
        tables: List[SnowflakeTable],
        snowflake_schema: SnowflakeSchema,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.include_technical_schema:
            data_reader = self.make_data_reader()
            for table in tables:
                # Handle dynamic table definitions for lineage
                if (
                    isinstance(table, SnowflakeDynamicTable)
                    and table.definition
                    and self.aggregator
                ):
                    table_identifier = self.identifiers.get_dataset_identifier(
                        table.name, schema_name, db_name
                    )
                    self.aggregator.add_view_definition(
                        view_urn=self.identifiers.gen_dataset_urn(table_identifier),
                        view_definition=table.definition,
                        default_db=db_name,
                        default_schema=schema_name,
                    )

                table_wu_generator = self._process_table(
                    table, snowflake_schema, db_name
                )
                yield from classification_workunit_processor(
                    table_wu_generator,
                    self.classification_handler,
                    data_reader,
                    [db_name, schema_name, table.name],
                )

    def _process_views(
        self,
        views: List[SnowflakeView],
        snowflake_schema: SnowflakeSchema,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        if self.aggregator:
            for view in views:
                view_identifier = self.identifiers.get_dataset_identifier(
                    view.name, schema_name, db_name
                )
                if view.is_secure and not view.view_definition:
                    view.view_definition = self.fetch_secure_view_definition(
                        view.name, schema_name, db_name
                    )
                if view.view_definition:
                    self.aggregator.add_view_definition(
                        view_urn=self.identifiers.gen_dataset_urn(view_identifier),
                        view_definition=view.view_definition,
                        default_db=db_name,
                        default_schema=schema_name,
                    )
                elif view.is_secure:
                    self.report.num_secure_views_missing_definition += 1

        if self.config.include_technical_schema:
            for view in views:
                yield from self._process_view(view, snowflake_schema, db_name)

    def _process_semantic_views(
        self,
        semantic_views: List[SnowflakeSemanticView],
        snowflake_schema: SnowflakeSchema,
        db_name: str,
        schema_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        logger.debug(
            f"Processing {len(semantic_views)} semantic views: {[sv.name for sv in semantic_views]}"
        )

        # Pre-compute and store upstream URNs for each semantic view
        # This must happen before _process_semantic_view so column lineage can access them
        for semantic_view in semantic_views:
            upstream_urns = []
            if semantic_view.base_tables:
                for base_table_id in semantic_view.base_tables:
                    base_table_identifier = self.identifiers.get_dataset_identifier(
                        base_table_id.table,
                        base_table_id.schema,
                        base_table_id.database,
                    )
                    is_allowed = self.filters.is_dataset_pattern_allowed(
                        base_table_identifier, SnowflakeObjectDomain.TABLE
                    )
                    if is_allowed:
                        upstream_urn = self.identifiers.gen_dataset_urn(
                            base_table_identifier
                        )
                        upstream_urns.append(upstream_urn)

            # Store for use in column lineage generation
            semantic_view.resolved_upstream_urns = upstream_urns
            logger.debug(
                f"Pre-computed {len(upstream_urns)} upstream URNs for semantic view {semantic_view.name}"
            )

        # STEP 1: Yield dataset work units first (this registers schemas with the aggregator)
        # This also registers explicit column lineage LAST to override auto-generation
        if self.config.include_technical_schema:
            for semantic_view in semantic_views:
                yield from self._process_semantic_view(
                    semantic_view, snowflake_schema, db_name
                )

        # STEP 2: Add view definitions to aggregator (for reference/documentation only)
        # Lineage is now emitted directly as MCPs in STEP 1, bypassing aggregator auto-generation
        if self.aggregator:
            logger.info(
                f"Adding view definitions to aggregator for {len(semantic_views)} semantic views"
            )
            for semantic_view in semantic_views:
                semantic_view_identifier = self.identifiers.get_dataset_identifier(
                    semantic_view.name, schema_name, db_name
                )
                semantic_view_urn = self.identifiers.gen_dataset_urn(
                    semantic_view_identifier
                )

                # Add the YAML definition to aggregator (for reference only, not for lineage)
                if semantic_view.view_definition:
                    self.aggregator.add_view_definition(
                        view_urn=semantic_view_urn,
                        view_definition=semantic_view.view_definition,
                        default_db=db_name,
                        default_schema=schema_name,
                    )

                # Update report counter
                upstream_urns = semantic_view.resolved_upstream_urns
                self.report.num_table_to_view_edges_scanned += len(upstream_urns)

                logger.debug(
                    f"View definition added for {semantic_view.name} "
                    f"({len(upstream_urns)} upstream tables)"
                )

        logger.info(
            f"Completed lineage processing for all {len(semantic_views)} semantic views"
        )

    def _process_streams(
        self,
        streams: List[SnowflakeStream],
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        for stream in streams:
            yield from self._process_stream(stream, snowflake_schema, db_name)

    def _process_procedures(
        self,
        procedures: List[BaseProcedure],
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        if self.config.include_technical_schema:
            if procedures:
                yield from generate_procedure_container_workunits(
                    self.identifiers.gen_database_key(
                        db_name,
                    ),
                    self.identifiers.gen_schema_key(
                        db_name=db_name,
                        schema_name=snowflake_schema.name,
                    ),
                )
            for procedure in procedures:
                yield from self._process_procedure(procedure, snowflake_schema, db_name)

    def _process_streamlit_apps(
        self,
        streamlit_apps: List[SnowflakeStreamlitApp],
    ) -> Iterable[MetadataWorkUnit]:
        for app in streamlit_apps:
            yield from self._process_streamlit_app(app)

    def _process_streamlit_app(
        self,
        app: SnowflakeStreamlitApp,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a Snowflake Streamlit app and generate DataHub metadata work units.
        Streamlit apps are modeled as dashboard entities in DataHub with the "Streamlit"
        subtype. This method uses the Dashboard SDK class to generate all required aspects
        including dashboard info, ownership, browse paths, and tags.
        Args:
            app: The Snowflake Streamlit app to process
        Yields:
            MetadataWorkUnit: Work units for each aspect of the dashboard entity
        """
        dashboard_id = self.identifiers.snowflake_identifier(
            f"{app.database_name}.{app.schema_name}.{app.url_id}"
        )

        custom_properties = {
            "name": app.name,
            "owner": app.owner,
            "owner_role_type": app.owner_role_type,
            "url_id": app.url_id,
        }
        if app.comment:
            custom_properties["comment"] = app.comment

        database_container_key = self.identifiers.gen_database_key(app.database_name)
        schema_container_key = self.identifiers.gen_schema_key(
            app.database_name, app.schema_name
        )

        dashboard = Dashboard(
            platform=STREAMLIT_PLATFORM,
            name=dashboard_id,
            display_name=app.title,
            platform_instance=self.config.platform_instance,
            custom_properties=custom_properties,
            created_at=app.created,
            created_by=make_group_urn(app.owner),
            last_modified=app.created,
            last_modified_by=make_group_urn(app.owner),
            subtype=BIAssetSubTypes.STREAMLIT,
            parent_container=[
                database_container_key.as_urn(),
                schema_container_key.as_urn(),
            ],
            external_url=(
                self.snowsight_url_builder.get_external_url_for_streamlit(
                    app.name, app.schema_name, app.database_name
                )
                if self.snowsight_url_builder
                else None
            ),
        )

        yield from dashboard.as_workunits()

    def _process_tags_in_schema(
        self, snowflake_schema: SnowflakeSchema
    ) -> Iterable[MetadataWorkUnit]:
        if snowflake_schema.tags:
            for tag in snowflake_schema.tags:
                yield from self._process_tag(tag)

    def fetch_secure_view_definition(
        self, table_name: str, schema_name: str, db_name: str
    ) -> Optional[str]:
        try:
            view_definitions = self.data_dictionary.get_secure_view_definitions()
            return view_definitions[db_name][schema_name][table_name]
        except KeyError:
            # Received secure view definitions but the view is not present in results
            self.structured_reporter.info(
                title="Secure view definition not found",
                message="Lineage will be missing for the view.",
                context=f"{db_name}.{schema_name}.{table_name}",
            )
            return None
        except Exception as e:
            action_msg = (
                "Please check permissions."
                if isinstance(e, SnowflakePermissionError)
                else ""
            )

            self.structured_reporter.warning(
                title="Failed to get secure views definitions",
                message=f"Lineage will be missing for the view. {action_msg}",
                context=f"{db_name}.{schema_name}.{table_name}",
                exc=e,
            )
            return None

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

    def fetch_semantic_views_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeSemanticView]:
        schema_name = snowflake_schema.name
        try:
            semantic_views_from_db = self.get_semantic_views_for_schema(
                schema_name, db_name
            )
            logger.info(
                f"Retrieved {len(semantic_views_from_db)} semantic views for {db_name}.{schema_name}"
            )

            semantic_views: List[SnowflakeSemanticView] = []
            for semantic_view in semantic_views_from_db:
                try:
                    semantic_view_name = self.identifiers.get_dataset_identifier(
                        semantic_view.name, schema_name, db_name
                    )

                    self.report.report_entity_scanned(
                        semantic_view_name, "semantic view"
                    )

                    if not self.filters.is_semantic_view_allowed(semantic_view_name):
                        self.report.report_dropped(semantic_view_name)
                    else:
                        semantic_views.append(semantic_view)
                except Exception as e:
                    logger.warning(
                        f"Failed to process semantic view {semantic_view}: {e}",
                        exc_info=True,
                    )
                    continue

            snowflake_schema.semantic_views = [sv.name for sv in semantic_views]
            logger.info(
                f"Successfully processed {len(semantic_views)} semantic views for {db_name}.{schema_name}"
            )
            return semantic_views
        except SnowflakePermissionError as e:
            error_msg = f"Failed to get semantic views for schema {db_name}.{schema_name}. Please check permissions."
            raise SnowflakePermissionError(error_msg) from e.__cause__
        except Exception as e:
            # Log the error but continue - semantic views might not be supported or accessible
            logger.warning(
                f"Could not fetch semantic views for schema {db_name}.{schema_name}: {e}",
                exc_info=True,
            )
            self.structured_reporter.info(
                title="Could not fetch semantic views for schema",
                message="Semantic views may not be supported in your Snowflake edition or may require additional privileges.",
                context=f"{db_name}.{schema_name}",
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

    def _process_semantic_view(
        self,
        semantic_view: SnowflakeSemanticView,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name
        semantic_view_name = self.identifiers.get_dataset_identifier(
            semantic_view.name, schema_name, db_name
        )
        logger.debug(
            f"Processing semantic view {semantic_view.name}, "
            f"include_technical_schema={self.config.include_technical_schema}"
        )

        # Columns should already be populated by _populate_semantic_view_columns
        # Only try to fetch from information_schema.columns if not already populated
        if not semantic_view.columns:
            try:
                semantic_view.columns = self.get_columns_for_table(
                    semantic_view.name, snowflake_schema, db_name
                )
            except Exception as e:
                # Semantic views may not have column info available in information_schema
                logger.debug(
                    f"Could not fetch columns for semantic view {semantic_view_name}: {e}"
                )
                semantic_view.columns = []

        # Try to get column tags if configured
        if self.config.extract_tags != TagOption.skip:
            try:
                semantic_view.column_tags = (
                    self.tag_extractor.get_column_tags_for_table(
                        semantic_view.name, schema_name, db_name
                    )
                )
            except Exception as e:
                logger.debug(
                    f"Could not fetch column tags for semantic view {semantic_view_name}: {e}"
                )
                semantic_view.column_tags = {}

        # Try to get tags on the semantic view itself
        if self.config.extract_tags != TagOption.skip:
            try:
                semantic_view.tags = self.tag_extractor.get_tags_on_object(
                    table_name=semantic_view.name,
                    schema_name=schema_name,
                    db_name=db_name,
                    domain="table",
                )
            except Exception as e:
                logger.debug(
                    f"Could not fetch tags for semantic view {semantic_view_name}: {e}"
                )
                semantic_view.tags = None

        # Emit schema FIRST, then lineage (DataHub needs columns before lineage references)
        if self.config.include_technical_schema:
            yield from self.gen_dataset_workunits(semantic_view, schema_name, db_name)
        else:
            logger.debug(
                f"Skipping schema for {semantic_view.name} (include_technical_schema=False)"
            )

        # Generate and emit column lineage AFTER schema
        if self.config.semantic_views.column_lineage:
            try:
                semantic_view_urn = self.identifiers.gen_dataset_urn(semantic_view_name)
                column_lineages = self._generate_column_lineage_for_semantic_view(
                    semantic_view, semantic_view_urn, db_name
                )

                # Emit lineage directly as an MCP
                upstream_urns = semantic_view.resolved_upstream_urns
                if upstream_urns:
                    yield from self._emit_semantic_view_lineage(
                        semantic_view,
                        semantic_view_urn,
                        column_lineages,
                        upstream_urns,
                    )
                else:
                    logger.debug(
                        f"No upstream URNs found for {semantic_view.name}, skipping lineage"
                    )
            except Exception as e:
                self.structured_reporter.warning(
                    title="Failed to generate column lineage for semantic view",
                    message=str(e),
                    context=semantic_view.name,
                    exc=e,
                )

    def _process_tag(self, tag: SnowflakeTag) -> Iterable[MetadataWorkUnit]:
        use_sp = self.config.extract_tags_as_structured_properties

        identifier = (
            self.snowflake_identifier(tag.structured_property_identifier())
            if use_sp
            else tag.tag_identifier()
        )

        if self.report.is_tag_processed(identifier):
            return

        self.report.report_tag_processed(identifier)

        if use_sp:
            return

        yield from self.gen_tag_workunits(tag)

    def _format_tags_as_structured_properties(
        self, tags: List[SnowflakeTag]
    ) -> Dict[StructuredPropertyUrn, str]:
        return {
            StructuredPropertyUrn(
                self.snowflake_identifier(tag.structured_property_identifier())
            ): tag.value
            for tag in tags
        }

    def gen_dataset_workunits(
        self,
        table: Union[
            SnowflakeTable, SnowflakeView, SnowflakeSemanticView, SnowflakeStream
        ],
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

        schema_container_key = self.identifiers.gen_schema_key(db_name, schema_name)

        if self.config.extract_tags_as_structured_properties:
            yield from self.gen_column_tags_as_structured_properties(dataset_urn, table)

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

        subTypes = SubTypes(typeNames=[table.get_subtype()])

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
            if self.config.extract_tags_as_structured_properties:
                yield from add_structured_properties_to_entity_wu(
                    dataset_urn,
                    self._format_tags_as_structured_properties(table.tags),
                )
            else:
                tag_associations = [
                    TagAssociation(
                        tag=make_tag_urn(
                            self.snowflake_identifier(tag.tag_identifier())
                        )
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

        if (
            isinstance(table, SnowflakeSemanticView)
            and table.view_definition is not None
        ):
            # view_definition contains the DDL from GET_DDL (CREATE SEMANTIC VIEW ...)
            view_properties_aspect = ViewProperties(
                materialized=False,
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
        table: Union[
            SnowflakeTable, SnowflakeView, SnowflakeSemanticView, SnowflakeStream
        ],
        schema_name: str,
        db_name: str,
    ) -> DatasetProperties:
        custom_properties = {}

        if isinstance(table, SnowflakeTable):
            custom_properties.update(
                {
                    k: v
                    for k, v in {
                        "CLUSTERING_KEY": table.clustering_key,
                        "IS_HYBRID": "true" if table.is_hybrid else None,
                        "IS_DYNAMIC": "true" if table.is_dynamic else None,
                        "IS_ICEBERG": "true" if table.is_iceberg else None,
                    }.items()
                    if v
                }
            )

            if isinstance(table, SnowflakeDynamicTable):
                if table.target_lag:
                    custom_properties["TARGET_LAG"] = table.target_lag

        if isinstance(table, SnowflakeView) and table.is_secure:
            custom_properties["IS_SECURE"] = "true"

        # Add table-level synonyms for semantic views
        if isinstance(table, SnowflakeSemanticView) and table.table_synonyms:
            # Format: LOGICAL_TABLE_NAME -> "synonym1, synonym2"
            for logical_table, synonyms in table.table_synonyms.items():
                if synonyms:
                    custom_properties[f"TABLE_SYNONYM_{logical_table}"] = ", ".join(
                        synonyms
                    )

        elif isinstance(table, SnowflakeStream):
            custom_properties.update(
                {
                    k: v
                    for k, v in {
                        "SOURCE_TYPE": table.source_type,
                        "TYPE": table.type,
                        "STALE": table.stale,
                        "MODE": table.mode,
                        "INVALID_REASON": table.invalid_reason,
                        "OWNER_ROLE_TYPE": table.owner_role_type,
                        "TABLE_NAME": table.table_name,
                        "BASE_TABLES": table.base_tables,
                        "STALE_AFTER": (
                            table.stale_after.isoformat() if table.stale_after else None
                        ),
                    }.items()
                    if v
                }
            )

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
            customProperties=custom_properties,
            externalUrl=(
                self.snowsight_url_builder.get_external_url_for_table(
                    table.name,
                    schema_name,
                    db_name,
                    (
                        SnowflakeObjectDomain.DYNAMIC_TABLE
                        if isinstance(table, SnowflakeTable) and table.is_dynamic
                        else SnowflakeObjectDomain.TABLE
                        if isinstance(table, SnowflakeTable)
                        else SnowflakeObjectDomain.SEMANTIC_VIEW
                        if isinstance(table, SnowflakeSemanticView)
                        else SnowflakeObjectDomain.VIEW
                    ),
                )
                if self.snowsight_url_builder
                else None
            ),
        )

    def gen_tag_workunits(self, tag: SnowflakeTag) -> Iterable[MetadataWorkUnit]:
        tag_urn = make_tag_urn(self.snowflake_identifier(tag.tag_identifier()))

        tag_properties_aspect = TagProperties(
            name=tag.tag_display_name(),
            description=f"Represents the Snowflake tag `{tag._id_prefix_as_str()}` with value `{tag.value}`.",
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=tag_urn, aspect=tag_properties_aspect
        ).as_workunit()

    def gen_column_tags_as_structured_properties(
        self,
        dataset_urn: str,
        table: Union[
            SnowflakeTable, SnowflakeView, SnowflakeSemanticView, SnowflakeStream
        ],
    ) -> Iterable[MetadataWorkUnit]:
        for column_name in table.column_tags:
            schema_field_urn = SchemaFieldUrn(dataset_urn, column_name).urn()
            yield from add_structured_properties_to_entity_wu(
                schema_field_urn,
                self._format_tags_as_structured_properties(
                    table.column_tags[column_name]
                ),
            )

    def _build_json_props(
        self,
        col_name: str,
        column_subtypes: Dict[str, str],
        column_synonyms: Dict[str, List[str]],
    ) -> Optional[str]:
        """
        Build jsonProps for semantic view columns.

        Includes columnSubType (DIMENSION, FACT, METRIC) and synonyms (alternative names).
        """
        json_props: Dict[str, Any] = {}

        # Add column subtype if available
        if col_name in column_subtypes:
            json_props["columnSubType"] = column_subtypes[col_name]

        # Add synonyms if available
        col_name_upper = col_name.upper()
        if col_name_upper in column_synonyms:
            json_props["synonyms"] = column_synonyms[col_name_upper]

        # Only return jsonProps if there's something to include
        if json_props:
            return json.dumps(json_props)
        return None

    def _build_semantic_view_tags(
        self,
        col_name: str,
        column_subtypes: Dict[str, str],
        existing_tags: Optional[GlobalTags],
    ) -> Optional[GlobalTags]:
        """
        Build GlobalTags for semantic view columns.

        Adds DIMENSION, FACT, and METRIC tags based on column subtypes.
        Merges with existing Snowflake tags if present.
        """
        # Start with existing tags if any
        tag_associations = []
        if existing_tags and existing_tags.tags:
            tag_associations.extend(existing_tags.tags)

        # Add subtype tags (DIMENSION, FACT, METRIC)
        if col_name in column_subtypes:
            subtype_str = column_subtypes[col_name]
            # Handle comma-separated subtypes (e.g., "DIMENSION,FACT")
            subtypes = [s.strip() for s in subtype_str.split(",")]

            for subtype in subtypes:
                tag_urn = make_tag_urn(subtype)
                tag_associations.append(TagAssociationClass(tag=tag_urn))

        # Return GlobalTags if we have any tags
        if tag_associations:
            return GlobalTagsClass(tags=tag_associations)
        return None

    def gen_schema_metadata(
        self,
        table: Union[
            SnowflakeTable, SnowflakeView, SnowflakeSemanticView, SnowflakeStream
        ],
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

        logger.debug(
            f"gen_schema_metadata for {table.name}: type={type(table).__name__}, "
            f"columns={len(table.columns)}"
        )

        # Get column subtypes, synonyms, and primary keys for semantic views
        column_subtypes = {}
        column_synonyms = {}
        primary_key_columns = set()
        if isinstance(table, SnowflakeSemanticView):
            column_subtypes = table.column_subtypes
            column_synonyms = table.column_synonyms
            primary_key_columns = table.primary_key_columns
            # table_synonyms available via table.table_synonyms if needed

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
                        # For regular tables, use pk.column_names
                        col.name in table.pk.column_names
                        if isinstance(table, SnowflakeTable) and table.pk is not None
                        # For semantic views, use primary_key_columns from SEMANTIC_TABLES
                        else (
                            col.name.upper() in primary_key_columns
                            if isinstance(table, SnowflakeSemanticView) and col.name
                            else None
                        )
                    ),
                    globalTags=(
                        # For semantic views, add DIMENSION/FACT/METRIC tags
                        self._build_semantic_view_tags(
                            col.name,
                            column_subtypes,
                            GlobalTags(
                                [
                                    TagAssociation(
                                        make_tag_urn(
                                            self.snowflake_identifier(
                                                tag.tag_identifier()
                                            )
                                        )
                                    )
                                    for tag in table.column_tags[col.name]
                                ]
                            )
                            if col.name in table.column_tags
                            and not self.config.extract_tags_as_structured_properties
                            else None,
                        )
                        if isinstance(table, SnowflakeSemanticView)
                        # For regular tables, use existing Snowflake tags
                        else (
                            GlobalTags(
                                [
                                    TagAssociation(
                                        make_tag_urn(
                                            self.snowflake_identifier(
                                                tag.tag_identifier()
                                            )
                                        )
                                    )
                                    for tag in table.column_tags[col.name]
                                ]
                            )
                            if col.name in table.column_tags
                            and not self.config.extract_tags_as_structured_properties
                            else None
                        )
                    ),
                    # Add column subtype and synonyms for semantic views
                    jsonProps=(
                        self._build_json_props(
                            col.name, column_subtypes, column_synonyms
                        )
                        if isinstance(table, SnowflakeSemanticView)
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

    def _emit_semantic_view_lineage(
        self,
        semantic_view: SnowflakeSemanticView,
        semantic_view_urn: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
        upstream_table_urns: List[str],
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit lineage directly as an MCP with explicit FineGrainedLineage.

        This bypasses the SQL aggregator's auto-generation entirely, ensuring
        only our validated column lineages are emitted.

        Args:
            semantic_view: The semantic view entity
            semantic_view_urn: URN of the semantic view
            fine_grained_lineages: List of validated column-level lineage mappings
            upstream_table_urns: List of all upstream table URNs
        """
        # Create UpstreamClass for each upstream table
        upstreams = []
        for upstream_urn in upstream_table_urns:
            upstreams.append(
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.VIEW,
                )
            )

        # Create UpstreamLineage with explicit FineGrainedLineage
        upstream_lineage = UpstreamLineageClass(
            upstreams=upstreams,
            fineGrainedLineages=fine_grained_lineages
            if fine_grained_lineages
            else None,
        )

        # Emit as MCP
        lineage_mcp = MetadataChangeProposalWrapper(
            entityUrn=semantic_view_urn,
            aspect=upstream_lineage,
        )

        logger.debug(
            f"Emitting lineage MCP for {semantic_view.name}: "
            f"{len(upstreams)} table edges, {len(fine_grained_lineages)} column mappings"
        )

        yield lineage_mcp.as_workunit()

    def _register_semantic_view_column_lineage(
        self,
        semantic_view: SnowflakeSemanticView,
        semantic_view_urn: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
        upstream_table_urns: List[str],
    ) -> None:
        """
        Register explicit table and column lineage with the SQL aggregator.

        This prevents the aggregator from auto-generating incorrect column lineage
        based on column name matching. We register both table and column lineage
        together in a single call.

        Args:
            semantic_view: The semantic view entity
            semantic_view_urn: URN of the semantic view
            fine_grained_lineages: List of column-level lineage mappings (validated, only actual columns)
            upstream_table_urns: List of all upstream table URNs (for table-level lineage)
        """
        # Convert FineGrainedLineageClass to ColumnLineageInfo
        column_lineage_infos = []

        for fg_lineage in fine_grained_lineages:
            # Extract column name from URN
            # Format: urn:li:schemaField:(urn:li:dataset:(...),column_name)
            if not fg_lineage.downstreams:
                continue
            downstream_urn = fg_lineage.downstreams[0]

            # Parse schema field URN: extract dataset URN and column name
            if downstream_urn.startswith(
                "urn:li:schemaField:("
            ) and downstream_urn.endswith(")"):
                inner = downstream_urn[len("urn:li:schemaField:(") : -1]
                last_comma = inner.rfind(",")
                if last_comma > 0:
                    downstream_col = inner[last_comma + 1 :]
                else:
                    logger.debug(
                        f"Could not parse downstream URN (no comma found): {downstream_urn}"
                    )
                    continue
            else:
                logger.debug(f"Unexpected downstream URN format: {downstream_urn}")
                continue

            upstream_cols = []
            if not fg_lineage.upstreams:
                continue
            for upstream_field_urn in fg_lineage.upstreams:
                # Parse schema field URN for upstream
                if upstream_field_urn.startswith(
                    "urn:li:schemaField:("
                ) and upstream_field_urn.endswith(")"):
                    inner = upstream_field_urn[len("urn:li:schemaField:(") : -1]
                    last_comma = inner.rfind(",")
                    if last_comma > 0:
                        table_urn = inner[:last_comma]
                        col_name = inner[last_comma + 1 :]
                        upstream_cols.append(
                            ColumnRef(table=table_urn, column=col_name)
                        )
                    else:
                        logger.debug(
                            f"Could not parse upstream URN (no comma): {upstream_field_urn}"
                        )
                else:
                    logger.debug(
                        f"Unexpected upstream URN format: {upstream_field_urn}"
                    )

            if upstream_cols:
                column_lineage_infos.append(
                    ColumnLineageInfo(
                        downstream=DownstreamColumnRef(
                            table=semantic_view_urn, column=downstream_col
                        ),
                        upstreams=upstream_cols,
                    )
                )

        if column_lineage_infos:
            if self.aggregator:
                self.aggregator.add_known_query_lineage(
                    KnownQueryLineageInfo(
                        query_text=f"-- Semantic view {semantic_view.name} definition",
                        downstream=semantic_view_urn,
                        upstreams=upstream_table_urns,
                        column_lineage=column_lineage_infos,
                        query_type=QueryType.CREATE_VIEW,
                    )
                )
                logger.debug(
                    f"Registered lineage for {semantic_view.name}: "
                    f"{len(upstream_table_urns)} upstream tables, "
                    f"{len(column_lineage_infos)} column mappings"
                )

    def _is_table_allowed(
        self, db_name: str, schema_name: str, table_name: str
    ) -> bool:
        """
        Check if table passes filter patterns.

        Args:
            db_name: Database name
            schema_name: Schema name
            table_name: Table name

        Returns:
            True if table is allowed by patterns, False otherwise
        """
        table_identifier = self.identifiers.get_dataset_identifier(
            table_name, schema_name, db_name
        )
        return self.filters.is_dataset_pattern_allowed(
            table_identifier, SnowflakeObjectDomain.TABLE
        )

    def _verify_column_exists_in_table(
        self, db_name: str, schema_name: str, table_name: str, column_name: str
    ) -> bool:
        """
        Verify if a column exists in a specific table.

        Uses the aggregator's schema resolver to check if the column exists
        in the already-fetched table schema.

        Returns:
            True if column exists, False otherwise
        """
        if not self.aggregator:
            # If no aggregator, we can't verify - assume it exists
            logger.debug(
                f"No aggregator available, assuming column {column_name} exists in {table_name}"
            )
            return True

        # Build table URN
        table_identifier = self.identifiers.get_dataset_identifier(
            table_name, schema_name, db_name
        )
        table_urn = self.identifiers.gen_dataset_urn(table_identifier)

        # Access aggregator's internal schema resolver to verify column existence.
        # NOTE: This uses private members (_schema_resolver, _resolve_schema_info) which
        # creates coupling to SqlParsingAggregator internals. Consider exposing a public
        # method if this pattern is needed more broadly.
        try:
            schema_info = self.aggregator._schema_resolver._resolve_schema_info(
                table_urn
            )
        except AttributeError:
            # Schema resolver API changed - fail open (assume column exists)
            return True

        if not schema_info:
            # Schema not found - assume column exists (may not be ingested yet)
            return True

        # Check if column exists in the schema (case-insensitive)
        column_name_lower = column_name.lower()
        return column_name_lower in schema_info

    def _extract_columns_from_expression(
        self, expression: str, dialect: str = "snowflake"
    ) -> List[Tuple[Optional[str], str]]:
        """
        Parse SQL expression and extract column references using sqlglot.

        Returns list of tuples: [(table_name, column_name), ...]
        If no table qualifier, table_name will be None.
        """
        if not expression:
            return []

        try:
            # Parse the expression
            dialect_obj = get_dialect(dialect)
            parsed = parse_statement(expression, dialect=dialect_obj)

            # Extract all Column nodes from the parsed expression tree
            # sqlglot.expressions.Column represents a column reference in SQL
            columns = []
            for col_node in parsed.find_all(sqlglot.expressions.Column):
                # Get the column name
                col_name = col_node.name
                if col_name:
                    # Get table qualifier if present (e.g., "ORDERS" in "ORDERS.ORDER_TOTAL_METRIC")
                    table_name = col_node.table if hasattr(col_node, "table") else None
                    # Normalize empty string to None for consistency
                    table_name = table_name.upper() if table_name else None

                    # Normalize column name to uppercase (Snowflake standard)
                    col_upper = col_name.upper()

                    # Store as tuple (table, column)
                    col_ref = (table_name, col_upper)
                    if col_ref not in columns:
                        columns.append(col_ref)

            logger.debug(
                f"Extracted {len(columns)} columns from expression '{expression}': {columns}"
            )
            return columns

        except Exception as e:
            logger.warning(
                f"Failed to parse expression '{expression}' for column extraction: {e}. "
                f"Skipping derived column lineage."
            )
            return []

    def _handle_chained_derivation(
        self,
        source_col: str,
        source_table_full_name: str,
        semantic_view: "SnowflakeSemanticView",
        downstream_field_urn: str,
        col_name_upper: str,
        fine_grained_lineages: List["FineGrainedLineageClass"],
        context_table: Optional[str] = None,
    ) -> None:
        """Handle chained derivation when source column is itself derived."""
        # Check if source_col is itself a derived column
        derived_col_expression = None
        for sv_col in semantic_view.columns:
            if sv_col.name and sv_col.name.upper() == source_col and sv_col.expression:
                derived_col_expression = sv_col.expression
                break

        if derived_col_expression:
            logger.debug(
                f"Source column {source_col} is derived, resolving: {derived_col_expression}"
            )
            resolved_sources = self._resolve_derived_column_sources(
                derived_col_expression, semantic_view, context_table=context_table
            )
            for rec_db, rec_schema, rec_table, rec_col in resolved_sources:
                if not self._is_table_allowed(rec_db, rec_schema, rec_table):
                    continue
                rec_table_identifier = self.identifiers.get_dataset_identifier(
                    rec_table, rec_schema, rec_db
                )
                rec_table_urn = self.identifiers.gen_dataset_urn(rec_table_identifier)
                rec_field_urn = make_schema_field_urn(
                    rec_table_urn, self.snowflake_identifier(rec_col)
                )
                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[rec_field_urn],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field_urn],
                    )
                )

    # Maximum recursion depth for resolving chained derived columns.
    # 5 levels handles most real-world metric chains (e.g., metric -> derived -> aggregate -> column).
    # Higher values risk infinite loops in malformed definitions; lower values may miss deep chains.
    _MAX_DERIVATION_DEPTH = 5

    def _resolve_derived_column_sources(
        self,
        expression: str,
        semantic_view: "SnowflakeSemanticView",
        depth: int = 0,
        visited: Optional[Set[str]] = None,
        context_table: Optional[str] = None,
    ) -> List[Tuple[str, str, str, str]]:
        """
        Recursively resolve a derived column's expression to physical source columns.

        Args:
            expression: SQL expression to parse
            semantic_view: The semantic view containing the column
            depth: Current recursion depth
            visited: Set of already-visited column names to detect cycles
            context_table: The logical table name to use for unqualified columns

        Returns:
            List of tuples: (source_db, source_schema, source_table, source_col)
        """
        if depth > self._MAX_DERIVATION_DEPTH:
            logger.warning(
                f"Max derivation depth ({self._MAX_DERIVATION_DEPTH}) reached for expression: {expression}"
            )
            return []

        if visited is None:
            visited = set()

        source_columns = self._extract_columns_from_expression(
            expression, dialect="snowflake"
        )

        resolved_sources: List[Tuple[str, str, str, str]] = []

        for table_qualifier, source_col in source_columns:
            # Use context_table for unqualified columns
            effective_table = table_qualifier if table_qualifier else context_table
            col_key = f"{effective_table or ''}.{source_col}"

            # Cycle detection
            if col_key in visited:
                logger.debug(f"Cycle detected at {col_key}, skipping")
                continue
            visited.add(col_key)

            if not effective_table:
                logger.debug(f"No table context for {source_col}, skipping")
                continue

            physical_table_tuple = semantic_view.logical_to_physical_table.get(
                effective_table
            )
            if not physical_table_tuple:
                logger.debug(f"No physical table for {effective_table}, skipping")
                continue

            source_db, source_schema, source_table = physical_table_tuple

            # Check if source column exists in physical table
            exists = self._verify_column_exists_in_table(
                source_db, source_schema, source_table, source_col
            )

            if exists:
                resolved_sources.append(
                    (source_db, source_schema, source_table, source_col)
                )
            else:
                # Check if it's another derived column (chained derivation)
                found_derived = False
                for sv_col in semantic_view.columns:
                    if (
                        sv_col.name
                        and sv_col.name.upper() == source_col
                        and sv_col.expression
                    ):
                        # Recursively resolve, passing effective_table as context
                        nested_sources = self._resolve_derived_column_sources(
                            sv_col.expression,
                            semantic_view,
                            depth + 1,
                            visited.copy(),
                            context_table=effective_table,
                        )
                        resolved_sources.extend(nested_sources)
                        found_derived = True
                        break
                if not found_derived:
                    logger.debug(
                        f"Column {source_col} not in physical table and not derived"
                    )

        return resolved_sources

    def _process_unassociated_columns(
        self,
        semantic_view: SnowflakeSemanticView,
        semantic_view_urn: str,
        fine_grained_lineages: List["FineGrainedLineageClass"],
    ) -> None:
        """
        Handle columns without table associations (e.g., cross-table derived metrics).

        These are metrics/facts that reference other metrics/facts across tables.
        Example: TEST_DERIVED_METRIC with expression
        "ORDERS.ORDER_TOTAL_METRIC+TRANSACTIONS.TRANSACTION_AMOUNT_METRIC"
        """
        processed_columns = set(semantic_view.column_table_mappings.keys())

        unprocessed_columns = [
            col
            for col in semantic_view.columns
            if col.name and col.name.upper() not in processed_columns and col.expression
        ]

        for col in unprocessed_columns:
            col_name_upper = col.name.upper()
            column_expression = col.expression

            if not column_expression:
                continue

            downstream_field_urn = make_schema_field_urn(
                semantic_view_urn,
                self.snowflake_identifier(col_name_upper),
            )

            # Use depth-limited recursive resolution
            resolved_sources = self._resolve_derived_column_sources(
                column_expression, semantic_view
            )

            if not resolved_sources:
                logger.debug(
                    f"No physical sources resolved for column {col_name_upper}"
                )
                continue

            for source_db, source_schema, source_table, source_col in resolved_sources:
                if not self._is_table_allowed(source_db, source_schema, source_table):
                    continue

                source_table_identifier = self.identifiers.get_dataset_identifier(
                    source_table, source_schema, source_db
                )
                source_table_urn = self.identifiers.gen_dataset_urn(
                    source_table_identifier
                )
                source_field_urn = make_schema_field_urn(
                    source_table_urn,
                    self.snowflake_identifier(source_col),
                )

                fine_grained_lineages.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[source_field_urn],
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field_urn],
                    )
                )

    def _generate_column_lineage_for_semantic_view(
        self,
        semantic_view: SnowflakeSemanticView,
        semantic_view_urn: str,
        db_name: str,
    ) -> List[FineGrainedLineageClass]:
        """
        Generate column-level lineage for a semantic view.

        Maps semantic view columns (dimensions, facts, metrics) to their source columns
        in base tables.

        Returns:
            List of FineGrainedLineage objects
        """
        # Use column_table_mappings from INFORMATION_SCHEMA instead of parsed DDL
        # This is more reliable as the parser may miss some columns (e.g., those with source_column=None)
        if not semantic_view.column_table_mappings:
            logger.warning(
                f"No column_table_mappings found for {semantic_view.name}. "
                f"Column lineage will not be generated."
            )
            return []

        if not semantic_view.logical_to_physical_table:
            logger.warning(
                f"No logical_to_physical_table mapping found for {semantic_view.name}. "
                f"Cannot generate column lineage."
            )
            return []

        fine_grained_lineages: List[FineGrainedLineageClass] = []
        warned_mappings: Set[str] = (
            set()
        )  # Track warned mappings to avoid duplicate logs

        logger.debug(
            f"Generating column lineage for semantic view {semantic_view.name}. "
            f"Columns: {len(semantic_view.column_table_mappings)}, "
            f"Tables: {len(semantic_view.logical_to_physical_table)}"
        )

        # Iterate through all columns that have table mappings
        for (
            col_name_upper,
            logical_table_names,
        ) in semantic_view.column_table_mappings.items():
            # For each logical table this column appears in
            for logical_table_name in logical_table_names:
                # Find the physical base table for this logical table using the direct mapping
                # from INFORMATION_SCHEMA.SEMANTIC_TABLES (more reliable than parsed DDL)
                logical_table_upper = logical_table_name.upper()
                base_table_tuple = semantic_view.logical_to_physical_table.get(
                    logical_table_upper
                )

                if not base_table_tuple:
                    if logical_table_upper not in warned_mappings:
                        warned_mappings.add(logical_table_upper)
                        logger.warning(
                            f"Could not find physical table mapping for logical table '{logical_table_name}'. "
                            f"Available mappings: {list(semantic_view.logical_to_physical_table.keys())}"
                        )
                        self.report.report_warning(
                            semantic_view.name,
                            f"Missing logical table mapping: {logical_table_name}",
                        )
                    continue

                base_db, base_schema, base_table = base_table_tuple
                base_table_full_name = f"{base_db}.{base_schema}.{base_table}"
                base_table_identifier = self.identifiers.get_dataset_identifier(
                    base_table, base_schema, base_db
                )
                base_table_urn = self.identifiers.gen_dataset_urn(base_table_identifier)

                # Check if the base table is filtered out by table patterns
                if not self._is_table_allowed(base_db, base_schema, base_table):
                    logger.debug(
                        f"Skipping lineage from {col_name_upper} to {base_table_full_name}: "
                        f"table is filtered by table_pattern"
                    )
                    continue

                # Create downstream field URN (needed for both direct and derived lineage)
                downstream_field_urn = make_schema_field_urn(
                    semantic_view_urn,
                    self.snowflake_identifier(col_name_upper),
                )

                # Verify the column actually exists in the upstream table
                upstream_table_has_column = self._verify_column_exists_in_table(
                    base_db, base_schema, base_table, col_name_upper
                )

                if not upstream_table_has_column:
                    # Column not found directly - check if it's a derived column with an expression
                    logger.debug(
                        f"Column {col_name_upper} not found in {base_table_full_name}. "
                        f"Checking if it's a derived column with an expression..."
                    )

                    # Look up the column's expression from semantic view metadata
                    column_expression = None
                    for col in semantic_view.columns:
                        if col.name and col.name.upper() == col_name_upper:
                            # Expression was stored during _populate_semantic_view_columns
                            column_expression = col.expression
                            break

                    if column_expression:
                        # Extract source columns from the expression using sqlglot
                        # Returns list of tuples: [(table_qualifier, column_name), ...]
                        source_columns = self._extract_columns_from_expression(
                            column_expression, dialect="snowflake"
                        )

                        if source_columns:
                            logger.debug(
                                f"Extracted {len(source_columns)} source columns from expression: {source_columns}"
                            )

                            # Create lineage for each source column
                            for table_qualifier, source_col in source_columns:
                                # Resolve the physical table for this source column
                                if table_qualifier:
                                    # Table-qualified reference (e.g., ORDERS.ORDER_TOTAL_METRIC)
                                    # Look up the physical table for this logical table
                                    physical_table_tuple = (
                                        semantic_view.logical_to_physical_table.get(
                                            table_qualifier
                                        )
                                    )
                                    if not physical_table_tuple:
                                        logger.warning(
                                            f"Logical table '{table_qualifier}' not found in mapping. "
                                            f"Skipping lineage for {table_qualifier}.{source_col}"
                                        )
                                        continue

                                    source_db, source_schema, source_table = (
                                        physical_table_tuple
                                    )
                                else:
                                    # Unqualified reference - use current base table
                                    source_db, source_schema, source_table = (
                                        base_db,
                                        base_schema,
                                        base_table,
                                    )

                                source_table_full_name = (
                                    f"{source_db}.{source_schema}.{source_table}"
                                )

                                # Verify source column exists in the resolved table
                                if not self._verify_column_exists_in_table(
                                    source_db, source_schema, source_table, source_col
                                ):
                                    # Try chained derivation resolution
                                    # Pass the logical table context for nested unqualified columns
                                    effective_logical_table = (
                                        table_qualifier
                                        if table_qualifier
                                        else logical_table_upper
                                    )
                                    self._handle_chained_derivation(
                                        source_col,
                                        source_table_full_name,
                                        semantic_view,
                                        downstream_field_urn,
                                        col_name_upper,
                                        fine_grained_lineages,
                                        context_table=effective_logical_table,
                                    )
                                    continue

                                # Check if source table is allowed by filters
                                if not self._is_table_allowed(
                                    source_db, source_schema, source_table
                                ):
                                    logger.debug(
                                        f"Skipping lineage from {source_col} to filtered table {source_table_full_name}"
                                    )
                                    continue

                                # Build URNs for source column to derived column lineage
                                source_table_identifier = (
                                    self.identifiers.get_dataset_identifier(
                                        source_table, source_schema, source_db
                                    )
                                )
                                source_table_urn = self.identifiers.gen_dataset_urn(
                                    source_table_identifier
                                )
                                source_field_urn = make_schema_field_urn(
                                    source_table_urn,
                                    self.snowflake_identifier(source_col),
                                )

                                # Create FineGrainedLineage for the derived column
                                fine_grained_lineages.append(
                                    FineGrainedLineageClass(
                                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                        upstreams=[source_field_urn],
                                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                        downstreams=[downstream_field_urn],
                                    )
                                )
                        else:
                            logger.warning(
                                f"Could not extract source columns from expression: {column_expression}. "
                                f"Skipping derived column lineage."
                            )
                    else:
                        logger.warning(
                            f"Column {col_name_upper} not found in {base_table_full_name} "
                            f"and has no expression. Skipping lineage."
                        )

                    # Move to next column (don't add to lineage list)
                    continue
                else:
                    # Create upstream field URN for direct column lineage
                    upstream_field_urn = make_schema_field_urn(
                        base_table_urn,
                        self.snowflake_identifier(col_name_upper),
                    )

                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[upstream_field_urn],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[downstream_field_urn],
                        )
                    )

        # Second pass: Handle columns without table associations
        self._process_unassociated_columns(
            semantic_view, semantic_view_urn, fine_grained_lineages
        )

        logger.debug(
            f"Generated {len(fine_grained_lineages)} column lineage mappings for {semantic_view.name}"
        )

        return fine_grained_lineages

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
        database_container_key = self.identifiers.gen_database_key(
            database.name,
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
                [
                    self.snowflake_identifier(tag.tag_identifier())
                    for tag in database.tags
                ]
                if database.tags
                and not self.config.extract_tags_as_structured_properties
                else None
            ),
            structured_properties=(
                self._format_tags_as_structured_properties(database.tags)
                if database.tags and self.config.extract_tags_as_structured_properties
                else None
            ),
        )

    def gen_schema_containers(
        self, schema: SnowflakeSchema, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        database_container_key = self.identifiers.gen_database_key(db_name)

        schema_container_key = self.identifiers.gen_schema_key(db_name, schema.name)

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
                [self.snowflake_identifier(tag.tag_identifier()) for tag in schema.tags]
                if schema.tags and not self.config.extract_tags_as_structured_properties
                else None
            ),
            structured_properties=(
                self._format_tags_as_structured_properties(schema.tags)
                if schema.tags and self.config.extract_tags_as_structured_properties
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
            return self.data_dictionary.get_tables_for_schema(
                db_name=db_name,
                schema_name=schema_name,
            )

        # Some schema may not have any table
        return tables.get(schema_name, [])

    def get_views_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeView]:
        views = self.data_dictionary.get_views_for_database(db_name)

        if views is not None:
            # Some schemas may not have any views
            return views.get(schema_name, [])

        # Usually this fails when there are too many views in the schema.
        # Fall back to per-schema queries.
        self.report.num_get_views_for_schema_queries += 1
        return self.data_dictionary.get_views_for_schema_using_information_schema(
            db_name=db_name,
            schema_name=schema_name,
        )

    def get_semantic_views_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeSemanticView]:
        semantic_views = self.data_dictionary.get_semantic_views_for_database(db_name)

        if semantic_views is not None:
            return semantic_views.get(schema_name, [])

        return (
            self.data_dictionary.get_semantic_views_for_schema_using_information_schema(
                db_name=db_name,
                schema_name=schema_name,
            )
        )

    def get_columns_for_table(
        self, table_name: str, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeColumn]:
        schema_name = snowflake_schema.name
        columns = self.data_dictionary.get_columns_for_schema(
            schema_name,
            db_name,
            cache_exclude_all_objects=itertools.chain(
                snowflake_schema.tables,
                snowflake_schema.views,
                snowflake_schema.semantic_views,
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

    # Handles the case for explicitly created external tables.
    # NOTE: Snowflake does not log this information to the access_history table.
    def _external_tables_ddl_lineage(
        self, discovered_tables: List[str]
    ) -> Iterable[KnownLineageMapping]:
        external_tables_query: str = SnowflakeQuery.show_external_tables()
        try:
            for db_row in self.connection.query(external_tables_query):
                key = self.identifiers.get_dataset_identifier(
                    db_row["name"], db_row["schema_name"], db_row["database_name"]
                )

                if key not in discovered_tables:
                    continue
                if db_row["location"].startswith("s3://"):
                    yield KnownLineageMapping(
                        upstream_urn=make_s3_urn_for_lineage(
                            db_row["location"], self.config.env
                        ),
                        downstream_urn=self.identifiers.gen_dataset_urn(key),
                    )
                    self.report.num_external_table_edges_scanned += 1

                self.report.num_external_table_edges_scanned += 1
        except Exception as e:
            self.structured_reporter.warning(
                "External table ddl lineage extraction failed",
                exc=e,
            )

    def fetch_streams_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeStream]:
        try:
            streams: List[SnowflakeStream] = []
            for stream in self.get_streams_for_schema(snowflake_schema.name, db_name):
                stream_identifier = self.identifiers.get_dataset_identifier(
                    stream.name, snowflake_schema.name, db_name
                )

                self.report.report_entity_scanned(stream_identifier, "stream")

                if not self.filters.is_dataset_pattern_allowed(
                    stream_identifier, SnowflakeObjectDomain.STREAM
                ):
                    self.report.report_dropped(stream_identifier)
                else:
                    streams.append(stream)
            snowflake_schema.streams = [stream.name for stream in streams]
            return streams
        except Exception as e:
            self.structured_reporter.warning(
                title="Failed to get streams for schema",
                message="Please check permissions"
                if isinstance(e, SnowflakePermissionError)
                else "",
                context=f"{db_name}.{snowflake_schema.name}",
                exc=e,
            )
            return []

    def get_streams_for_schema(
        self, schema_name: str, db_name: str
    ) -> List[SnowflakeStream]:
        streams = self.data_dictionary.get_streams_for_database(db_name)

        return streams.get(schema_name, [])

    def fetch_procedures_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[BaseProcedure]:
        try:
            procedures: List[BaseProcedure] = []
            for procedure in self.get_procedures_for_schema(snowflake_schema, db_name):
                procedure_qualified_name = self.identifiers.get_dataset_identifier(
                    procedure.name, snowflake_schema.name, db_name
                )
                self.report.report_entity_scanned(procedure_qualified_name, "procedure")

                if self.filters.is_procedure_allowed(procedure_qualified_name):
                    procedures.append(procedure)
                else:
                    self.report.report_dropped(procedure_qualified_name)
            return procedures
        except Exception as e:
            self.structured_reporter.warning(
                title="Failed to get procedures for schema",
                message="Please check permissions"
                if isinstance(e, SnowflakePermissionError)
                else "",
                context=f"{db_name}.{snowflake_schema.name}",
                exc=e,
            )
            return []

    def get_procedures_for_schema(
        self,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> List[BaseProcedure]:
        procedures = self.data_dictionary.get_procedures_for_database(db_name)

        return procedures.get(snowflake_schema.name, [])

    def fetch_streamlit_apps(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeStreamlitApp]:
        try:
            streamlit_apps: List[SnowflakeStreamlitApp] = []
            for app in self.get_streamlit_apps(snowflake_schema, db_name):
                app_qualified_name = f"{db_name}.{snowflake_schema.name}.{app.name}"
                self.report.report_entity_scanned(
                    app_qualified_name, SnowflakeObjectDomain.STREAMLIT
                )

                if self.filters.is_streamlit_allowed(app_qualified_name):
                    streamlit_apps.append(app)
                else:
                    self.report.report_dropped(app_qualified_name)

            return streamlit_apps
        except SnowflakePermissionError as e:
            self.structured_reporter.warning(
                title="Permission denied for Streamlit apps",
                message="Your Snowflake role lacks permissions to list Streamlit apps.",
                context=f"{db_name}.{snowflake_schema.name}",
                exc=e,
            )
            return []
        except Exception as e:
            self.structured_reporter.warning(
                title="Failed to get Streamlit apps",
                message="Unexpected error occurred while querying Streamlit apps",
                context=f"{db_name}.{snowflake_schema.name}",
                exc=e,
            )
            return []

    def get_streamlit_apps(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> List[SnowflakeStreamlitApp]:
        streamlit_apps = self.data_dictionary.get_streamlit_apps_for_database(db_name)
        return streamlit_apps.get(snowflake_schema.name, [])

    def _process_stream(
        self,
        stream: SnowflakeStream,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name

        try:
            # Retrieve and register the schema without metadata to prevent columns from mapping upstream
            stream.columns = self.get_columns_for_stream(stream.table_name)
            yield from self.gen_dataset_workunits(stream, schema_name, db_name)

            if self.config.include_column_lineage:
                with self.report.new_stage(f"*: {LINEAGE_EXTRACTION}"):
                    self.populate_stream_upstreams(stream, db_name, schema_name)

        except Exception as e:
            self.structured_reporter.warning(
                "Failed to get columns for stream:", stream.name, exc=e
            )

    def _process_procedure(
        self,
        procedure: BaseProcedure,
        snowflake_schema: SnowflakeSchema,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        try:
            # TODO: For CLL, we should process procedures after all tables are processed
            yield from generate_procedure_workunits(
                procedure,
                database_key=self.identifiers.gen_database_key(
                    db_name,
                ),
                schema_key=self.identifiers.gen_schema_key(
                    db_name, snowflake_schema.name
                ),
                schema_resolver=(
                    self.aggregator._schema_resolver if self.aggregator else None
                ),
            )
        except Exception as e:
            self.structured_reporter.warning(
                title="Failed to ingest stored procedure",
                message="",
                context=procedure.name,
                exc=e,
            )

    def get_columns_for_stream(
        self,
        source_object: str,  # Qualified name of source table/view
    ) -> List[SnowflakeColumn]:
        """
        Get column information for a stream by getting source object columns and adding metadata columns.
        Stream includes all columns from source object plus metadata columns like:
        - METADATA$ACTION
        - METADATA$ISUPDATE
        - METADATA$ROW_ID
        """
        columns: List[SnowflakeColumn] = []

        source_parts = split_qualified_name(source_object)

        source_db, source_schema, source_name = source_parts

        # Get columns from source object
        source_columns = self.data_dictionary.get_columns_for_schema(
            source_schema, source_db, itertools.chain([source_name])
        ).get(source_name, [])

        # Add all source columns
        columns.extend(source_columns)

        # Add standard stream metadata columns
        metadata_columns = [
            SnowflakeColumn(
                name="METADATA$ACTION",
                ordinal_position=len(columns) + 1,
                is_nullable=False,
                data_type="VARCHAR",
                comment="Type of DML operation (INSERT/DELETE)",
                character_maximum_length=10,
                numeric_precision=None,
                numeric_scale=None,
            ),
            SnowflakeColumn(
                name="METADATA$ISUPDATE",
                ordinal_position=len(columns) + 2,
                is_nullable=False,
                data_type="BOOLEAN",
                comment="Whether row is from UPDATE operation",
                character_maximum_length=None,
                numeric_precision=None,
                numeric_scale=None,
            ),
            SnowflakeColumn(
                name="METADATA$ROW_ID",
                ordinal_position=len(columns) + 3,
                is_nullable=False,
                data_type="NUMBER",
                comment="Unique row identifier",
                character_maximum_length=None,
                numeric_precision=38,
                numeric_scale=0,
            ),
        ]

        columns.extend(metadata_columns)

        return columns

    def populate_stream_upstreams(
        self, stream: SnowflakeStream, db_name: str, schema_name: str
    ) -> None:
        """
        Populate Streams upstream tables
        """
        self.report.num_streams_with_known_upstreams += 1
        if self.aggregator:
            source_parts = split_qualified_name(stream.table_name)
            source_db, source_schema, source_name = source_parts

            dataset_identifier = self.identifiers.get_dataset_identifier(
                stream.name, schema_name, db_name
            )
            dataset_urn = self.identifiers.gen_dataset_urn(dataset_identifier)

            upstream_identifier = self.identifiers.get_dataset_identifier(
                source_name, source_schema, source_db
            )
            upstream_urn = self.identifiers.gen_dataset_urn(upstream_identifier)

            logger.debug(
                f"""upstream_urn: {upstream_urn}, downstream_urn: {dataset_urn}"""
            )

            self.aggregator.add_known_lineage_mapping(
                upstream_urn=upstream_urn,
                downstream_urn=dataset_urn,
                lineage_type=DatasetLineageTypeClass.COPY,
            )
