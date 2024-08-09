import functools
import itertools
import logging
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Type, Union

import humanfriendly

# These imports verify that the dependencies are available.
import pydantic
import redshift_connector

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn_with_platform_instance,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.incremental_lineage_helper import auto_incremental_lineage
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import create_dataset_props_patch_builder
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import (
    ClassificationHandler,
    classification_workunit_processor,
)
from datahub.ingestion.source.common.data_reader import DataReader
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.redshift.config import RedshiftConfig
from datahub.ingestion.source.redshift.exception import handle_redshift_exceptions_yield
from datahub.ingestion.source.redshift.lineage import RedshiftLineageExtractor
from datahub.ingestion.source.redshift.lineage_v2 import RedshiftSqlLineageV2
from datahub.ingestion.source.redshift.profile import RedshiftProfiler
from datahub.ingestion.source.redshift.redshift_data_reader import RedshiftDataReader
from datahub.ingestion.source.redshift.redshift_schema import (
    RedshiftColumn,
    RedshiftDataDictionary,
    RedshiftSchema,
    RedshiftTable,
    RedshiftView,
)
from datahub.ingestion.source.redshift.report import RedshiftReport
from datahub.ingestion.source.redshift.usage import RedshiftUsageExtractor
from datahub.ingestion.source.sql.sql_common import SqlWorkUnit
from datahub.ingestion.source.sql.sql_types import resolve_postgres_modified_type
from datahub.ingestion.source.sql.sql_utils import (
    add_table_to_schema_container,
    gen_database_container,
    gen_database_key,
    gen_lineage,
    gen_schema_container,
    gen_schema_key,
    get_dataplatform_instance_aspect,
    get_domain_wu,
)
from datahub.ingestion.source.state.profiling_state_handler import ProfilingHandler
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
    RedundantUsageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import (
    LINEAGE_EXTRACTION,
    METADATA_EXTRACTION,
    PROFILING,
    USAGE_EXTRACTION_INGESTION,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import SubTypes, TimeStamp
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetProperties,
    ViewProperties,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayType,
    BooleanType,
    BytesType,
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
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from datahub.utilities import memory_footprint
from datahub.utilities.dedup_list import deduplicate_list
from datahub.utilities.mapping import Constants
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)


@platform_name("Redshift")
@config_class(RedshiftConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Optionally enabled via configuration")
@capability(
    SourceCapability.LINEAGE_FINE,
    "Optionally enabled via configuration (`mixed` or `sql_based` lineage needs to be enabled)",
)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, can be disabled via configuration `include_usage_statistics`",
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
@capability(
    SourceCapability.CLASSIFICATION,
    "Optionally enabled via `classification.enabled`",
    supported=True,
)
class RedshiftSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, views and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling
    - Table lineage
    - Usage statistics

    ### Prerequisites

    This source needs to access system tables that require extra permissions.
    To grant these permissions, please alter your datahub Redshift user the following way:
    ```sql
    ALTER USER datahub_user WITH SYSLOG ACCESS UNRESTRICTED;
    GRANT SELECT ON pg_catalog.svv_table_info to datahub_user;
    GRANT SELECT ON pg_catalog.svl_user_info to datahub_user;
    ```

    :::note

    Giving a user unrestricted access to system tables gives the user visibility to data generated by other users. For example, STL_QUERY and STL_QUERYTEXT contain the full text of INSERT, UPDATE, and DELETE statements.

    :::

    ### Lineage

    There are multiple lineage collector implementations as Redshift does not support table lineage out of the box.

    #### stl_scan_based
    The stl_scan based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) and [stl_scan](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_SCAN.html) system tables to
    discover lineage between tables.
    Pros:
    - Fast
    - Reliable

    Cons:
    - Does not work with Spectrum/external tables because those scans do not show up in stl_scan table.
    - If a table is depending on a view then the view won't be listed as dependency. Instead the table will be connected with the view's dependencies.

    #### sql_based
    The sql_based based collector uses Redshift's [stl_insert](https://docs.aws.amazon.com/redshift/latest/dg/r_STL_INSERT.html) to discover all the insert queries
    and uses sql parsing to discover the dependencies.

    Pros:
    - Works with Spectrum tables
    - Views are connected properly if a table depends on it

    Cons:
    - Slow.
    - Less reliable as the query parser can fail on certain queries

    #### mixed
    Using both collector above and first applying the sql based and then the stl_scan based one.

    Pros:
    - Works with Spectrum tables
    - Views are connected properly if a table depends on it
    - A bit more reliable than the sql_based one only

    Cons:
    - Slow
    - May be incorrect at times as the query parser can fail on certain queries

    :::note

    The redshift stl redshift tables which are used for getting data lineage retain at most seven days of log history, and sometimes closer to 2-5 days. This means you cannot extract lineage from queries issued outside that window.

    :::

    ### Profiling
    Profiling runs sql queries on the redshift cluster to get statistics about the tables. To be able to do that, the user needs to have read access to the tables that should be profiled.

    If you don't want to grant read access to the tables you can enable table level profiling which will get table statistics without reading the data.
    ```yaml
    profiling:
      profile_table_level_only: true
    ```
    """

    REDSHIFT_FIELD_TYPE_MAPPINGS: Dict[
        str,
        Type[
            Union[
                ArrayType,
                BytesType,
                BooleanType,
                NumberType,
                RecordType,
                StringType,
                TimeType,
                NullType,
            ]
        ],
    ] = {
        "BYTES": BytesType,
        "BOOL": BooleanType,
        "BOOLEAN": BooleanType,
        "DOUBLE": NumberType,
        "DOUBLE PRECISION": NumberType,
        "DECIMAL": NumberType,
        "NUMERIC": NumberType,
        "BIGNUMERIC": NumberType,
        "BIGDECIMAL": NumberType,
        "FLOAT64": NumberType,
        "INT": NumberType,
        "INT64": NumberType,
        "SMALLINT": NumberType,
        "INTEGER": NumberType,
        "BIGINT": NumberType,
        "TINYINT": NumberType,
        "BYTEINT": NumberType,
        "STRING": StringType,
        "TIME": TimeType,
        "TIMESTAMP": TimeType,
        "DATE": TimeType,
        "DATETIME": TimeType,
        "GEOGRAPHY": NullType,
        "JSON": NullType,
        "INTERVAL": NullType,
        "ARRAY": ArrayType,
        "STRUCT": RecordType,
        "CHARACTER VARYING": StringType,
        "CHARACTER": StringType,
        "CHAR": StringType,
        "TIMESTAMP WITHOUT TIME ZONE": TimeType,
        "REAL": NumberType,
        "VARCHAR": StringType,
        "TIMESTAMPTZ": TimeType,
        "GEOMETRY": NullType,
        "HLLSKETCH": NullType,
        "TIMETZ": TimeType,
        "VARBYTE": StringType,
    }

    def get_platform_instance_id(self) -> str:
        """
        The source identifier such as the specific source host address required for stateful ingestion.
        Individual subclasses need to override this method appropriately.
        """
        return str(self.platform)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            RedshiftConfig.Config.extra = (
                pydantic.Extra.allow
            )  # we are okay with extra fields during this stage
            config = RedshiftConfig.parse_obj(config_dict)
            # source = RedshiftSource(config, report)
            connection: redshift_connector.Connection = (
                RedshiftSource.get_redshift_connection(config)
            )
            cur = connection.cursor()
            cur.execute("select 1")
            test_report.basic_connectivity = CapabilityReport(capable=True)

            test_report.capability_report = {}
            try:
                RedshiftDataDictionary.get_schemas(connection, database=config.database)
                test_report.capability_report[
                    SourceCapability.SCHEMA_METADATA
                ] = CapabilityReport(capable=True)
            except Exception as e:
                test_report.capability_report[
                    SourceCapability.SCHEMA_METADATA
                ] = CapabilityReport(capable=False, failure_reason=str(e))

        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    def get_report(self) -> RedshiftReport:
        return self.report

    eskind_to_platform = {1: "glue", 2: "hive", 3: "postgres", 4: "redshift"}

    def __init__(self, config: RedshiftConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.catalog_metadata: Dict = {}
        self.config: RedshiftConfig = config
        self.report: RedshiftReport = RedshiftReport()
        self.classification_handler = ClassificationHandler(self.config, self.report)
        self.platform = "redshift"
        self.domain_registry = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=list(self.config.domain.keys()), graph=self.ctx.graph
            )

        self.redundant_lineage_run_skip_handler: Optional[
            RedundantLineageRunSkipHandler
        ] = None
        if self.config.enable_stateful_lineage_ingestion:
            self.redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        self.profiling_state_handler: Optional[ProfilingHandler] = None
        if self.config.enable_stateful_profiling:
            self.profiling_state_handler = ProfilingHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        self.data_dictionary = RedshiftDataDictionary(
            is_serverless=self.config.is_serverless
        )

        self.db_tables: Dict[str, Dict[str, List[RedshiftTable]]] = {}
        self.db_views: Dict[str, Dict[str, List[RedshiftView]]] = {}
        self.db_schemas: Dict[str, Dict[str, RedshiftSchema]] = {}

        self.add_config_to_report()

    @classmethod
    def create(cls, config_dict, ctx):
        config = RedshiftConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def get_redshift_connection(
        config: RedshiftConfig,
    ) -> redshift_connector.Connection:
        host, port = config.host_port.split(":")
        conn = redshift_connector.connect(
            host=host,
            port=int(port),
            user=config.username,
            database=config.database,
            password=config.password.get_secret_value() if config.password else None,
            **config.extra_client_options,
        )

        conn.autocommit = True

        return conn

    def gen_database_container(self, database: str) -> Iterable[MetadataWorkUnit]:
        database_container_key = gen_database_key(
            database=database,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from gen_database_container(
            database=database,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.DATABASE],
        )

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
        connection = self._try_get_redshift_connection(self.config)

        if connection is None:
            # If we failed to establish a connection, short circuit the connector.
            return

        database = self.config.database
        logger.info(f"Processing db {database}")
        self.report.report_ingestion_stage_start(METADATA_EXTRACTION)
        self.db_tables[database] = defaultdict()
        self.db_views[database] = defaultdict()
        self.db_schemas.setdefault(database, {})

        # TODO: Ideally, we'd push down exception handling to the place where the connection is used, as opposed to keeping
        # this fallback. For now, this gets us broad coverage quickly.
        yield from handle_redshift_exceptions_yield(
            self.report, self._extract_metadata, connection, database
        )

    def _extract_metadata(
        self, connection: redshift_connector.Connection, database: str
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:

        yield from self.gen_database_container(
            database=database,
        )

        self.cache_tables_and_views(connection, database)

        self.report.tables_in_mem_size[database] = humanfriendly.format_size(
            memory_footprint.total_size(self.db_tables)
        )
        self.report.views_in_mem_size[database] = humanfriendly.format_size(
            memory_footprint.total_size(self.db_views)
        )

        if self.config.use_lineage_v2:
            lineage_extractor = RedshiftSqlLineageV2(
                config=self.config,
                report=self.report,
                context=self.ctx,
                database=database,
                redundant_run_skip_handler=self.redundant_lineage_run_skip_handler,
            )

            yield from lineage_extractor.aggregator.register_schemas_from_stream(
                self.process_schemas(connection, database)
            )

            self.report.report_ingestion_stage_start(LINEAGE_EXTRACTION)
            yield from self.extract_lineage_v2(
                connection=connection,
                database=database,
                lineage_extractor=lineage_extractor,
            )

            all_tables = self.get_all_tables()
        else:
            yield from self.process_schemas(connection, database)

            all_tables = self.get_all_tables()

            if (
                self.config.include_table_lineage
                or self.config.include_view_lineage
                or self.config.include_copy_lineage
            ):
                self.report.report_ingestion_stage_start(LINEAGE_EXTRACTION)
                yield from self.extract_lineage(
                    connection=connection, all_tables=all_tables, database=database
                )

        self.report.report_ingestion_stage_start(USAGE_EXTRACTION_INGESTION)
        if self.config.include_usage_statistics:
            yield from self.extract_usage(
                connection=connection, all_tables=all_tables, database=database
            )

        if self.config.is_profiling_enabled():
            self.report.report_ingestion_stage_start(PROFILING)
            profiler = RedshiftProfiler(
                config=self.config,
                report=self.report,
                state_handler=self.profiling_state_handler,
            )
            yield from profiler.get_workunits(self.db_tables)

    def process_schemas(self, connection, database):
        for schema in self.data_dictionary.get_schemas(
            conn=connection, database=database
        ):
            if not is_schema_allowed(
                self.config.schema_pattern,
                schema.name,
                database,
                self.config.match_fully_qualified_names,
            ):
                self.report.report_dropped(f"{database}.{schema.name}")
                continue

            logger.info(f"Processing schema: {database}.{schema.name}")

            self.db_schemas[database][schema.name] = schema
            yield from self.process_schema(connection, database, schema)

    def make_data_reader(
        self,
        connection: redshift_connector.Connection,
    ) -> Optional[DataReader]:
        if self.classification_handler.is_classification_enabled():
            return RedshiftDataReader.create(connection)

        return None

    def process_schema(
        self,
        connection: redshift_connector.Connection,
        database: str,
        schema: RedshiftSchema,
    ) -> Iterable[MetadataWorkUnit]:
        report_key = f"{database}.{schema.name}"
        with PerfTimer() as timer:
            schema_container_key = gen_schema_key(
                db_name=database,
                schema=schema.name,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            database_container_key = gen_database_key(
                database=database,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )

            yield from gen_schema_container(
                schema=schema.name,
                database=database,
                schema_container_key=schema_container_key,
                database_container_key=database_container_key,
                domain_config=self.config.domain,
                domain_registry=self.domain_registry,
                sub_types=[DatasetSubTypes.SCHEMA],
            )

            schema_columns: Dict[str, Dict[str, List[RedshiftColumn]]] = {}
            schema_columns[schema.name] = self.data_dictionary.get_columns_for_schema(
                conn=connection, schema=schema
            )

            if self.config.include_tables:
                data_reader = self.make_data_reader(connection)
                logger.info(f"Process tables in schema {database}.{schema.name}")
                if (
                    self.db_tables[schema.database]
                    and schema.name in self.db_tables[schema.database]
                ):
                    for table in self.db_tables[schema.database][schema.name]:
                        table.columns = schema_columns[schema.name].get(table.name, [])
                        table.column_count = len(table.columns)
                        table_wu_generator = self._process_table(
                            table, database=database
                        )
                        yield from classification_workunit_processor(
                            table_wu_generator,
                            self.classification_handler,
                            data_reader,
                            [schema.database, schema.name, table.name],
                        )
                        self.report.table_processed[report_key] = (
                            self.report.table_processed.get(
                                f"{database}.{schema.name}", 0
                            )
                            + 1
                        )
                        logger.debug(
                            f"Table processed: {schema.database}.{schema.name}.{table.name}"
                        )
                else:
                    self.report.info(
                        title="No tables found in some schemas",
                        message="No tables found in some schemas. This may be due to insufficient privileges for the provided user.",
                        context=f"Schema: {schema.database}.{schema.name}",
                    )
            else:
                logger.info("Table processing disabled, skipping")

            if self.config.include_views:
                logger.info(f"Process views in schema {schema.database}.{schema.name}")
                if (
                    self.db_views[schema.database]
                    and schema.name in self.db_views[schema.database]
                ):
                    for view in self.db_views[schema.database][schema.name]:
                        view.columns = schema_columns[schema.name].get(view.name, [])
                        view.column_count = len(view.columns)
                        yield from self._process_view(
                            table=view, database=database, schema=schema
                        )

                        self.report.view_processed[report_key] = (
                            self.report.view_processed.get(
                                f"{database}.{schema.name}", 0
                            )
                            + 1
                        )
                        logger.debug(
                            f"Table processed: {schema.database}.{schema.name}.{view.name}"
                        )
                else:
                    self.report.info(
                        title="No views found in some schemas",
                        message="No views found in some schemas. This may be due to insufficient privileges for the provided user.",
                        context=f"Schema: {schema.database}.{schema.name}",
                    )
            else:
                logger.info("View processing disabled, skipping")

            self.report.metadata_extraction_sec[report_key] = round(
                timer.elapsed_seconds(), 2
            )

    def _process_table(
        self,
        table: RedshiftTable,
        database: str,
    ) -> Iterable[MetadataWorkUnit]:
        datahub_dataset_name = f"{database}.{table.schema}.{table.name}"

        self.report.report_entity_scanned(datahub_dataset_name)

        if not self.config.table_pattern.allowed(datahub_dataset_name):
            self.report.report_dropped(datahub_dataset_name)
            return

        yield from self.gen_table_dataset_workunits(
            table, database=database, dataset_name=datahub_dataset_name
        )

    def _process_view(
        self, table: RedshiftView, database: str, schema: RedshiftSchema
    ) -> Iterable[MetadataWorkUnit]:
        datahub_dataset_name = f"{database}.{schema.name}.{table.name}"

        self.report.report_entity_scanned(datahub_dataset_name)

        if not self.config.table_pattern.allowed(datahub_dataset_name):
            self.report.report_dropped(datahub_dataset_name)
            return

        yield from self.gen_view_dataset_workunits(
            view=table,
            database=database,
            schema=schema.name,
        )

    def gen_table_dataset_workunits(
        self,
        table: RedshiftTable,
        database: str,
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties = {}

        if table.type:
            custom_properties["table_type"] = table.type

        if table.location:
            custom_properties["location"] = table.location

        if table.input_parameters:
            custom_properties["input_parameters"] = table.input_parameters

        if table.output_parameters:
            custom_properties["output_parameters"] = table.output_parameters

        if table.dist_style:
            custom_properties["dist_style"] = table.dist_style

        if table.parameters:
            custom_properties["parameters"] = table.parameters

        if table.serde_parameters:
            custom_properties["serde_parameters"] = table.serde_parameters

        assert table.schema
        assert table.type
        yield from self.gen_dataset_workunits(
            table=table,
            database=database,
            schema=table.schema,
            sub_type=DatasetSubTypes.TABLE,
            custom_properties=custom_properties,
        )

    # TODO: Remove to common?
    def gen_view_dataset_workunits(
        self,
        view: RedshiftView,
        database: str,
        schema: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield from self.gen_dataset_workunits(
            table=view,
            database=self.config.database,
            schema=schema,
            sub_type=DatasetSubTypes.VIEW,
            custom_properties={},
        )

        datahub_dataset_name = f"{database}.{schema}.{view.name}"
        dataset_urn = self.gen_dataset_urn(datahub_dataset_name)
        if view.ddl:
            view_properties_aspect = ViewProperties(
                materialized=view.materialized,
                viewLanguage="SQL",
                viewLogic=view.ddl,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=view_properties_aspect
            ).as_workunit()

    # TODO: Remove to common?
    def gen_schema_fields(self, columns: List[RedshiftColumn]) -> List[SchemaField]:
        schema_fields: List[SchemaField] = []

        for col in columns:
            tags: List[TagAssociationClass] = []
            if col.dist_key:
                tags.append(TagAssociationClass(make_tag_urn(Constants.TAG_DIST_KEY)))

            if col.sort_key:
                tags.append(TagAssociationClass(make_tag_urn(Constants.TAG_SORT_KEY)))

            data_type = self.REDSHIFT_FIELD_TYPE_MAPPINGS.get(col.data_type)
            # We have to remove the precision part to properly parse it
            if data_type is None:
                # attempt Postgres modified type
                data_type = resolve_postgres_modified_type(col.data_type.lower())

            if any(type in col.data_type.lower() for type in ["struct", "array"]):
                fields = self.data_dictionary.get_schema_fields_for_column(col)
                schema_fields.extend(fields)
            else:
                field = SchemaField(
                    fieldPath=col.name,
                    type=SchemaFieldDataType(data_type() if data_type else NullType()),
                    # NOTE: nativeDataType will not be in sync with older connector
                    nativeDataType=col.data_type,
                    description=col.comment,
                    nullable=col.is_nullable,
                    globalTags=GlobalTagsClass(tags=tags),
                )
                schema_fields.append(field)
        return schema_fields

    # TODO: Move to common?
    def gen_schema_metadata(
        self,
        dataset_urn: str,
        table: Union[RedshiftTable, RedshiftView],
        dataset_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        schema_metadata = SchemaMetadata(
            schemaName=dataset_name,
            platform=make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=MySqlDDL(tableSchema=""),
            fields=self.gen_schema_fields(table.columns),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def gen_dataset_urn(self, datahub_dataset_name: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=datahub_dataset_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    # TODO: Move to common
    def gen_dataset_workunits(
        self,
        table: Union[RedshiftTable, RedshiftView],
        database: str,
        schema: str,
        sub_type: str,
        custom_properties: Optional[Dict[str, str]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        datahub_dataset_name = f"{database}.{schema}.{table.name}"
        dataset_urn = self.gen_dataset_urn(datahub_dataset_name)

        yield from self.gen_schema_metadata(
            dataset_urn, table, str(datahub_dataset_name)
        )

        dataset_properties = DatasetProperties(
            name=table.name,
            created=(
                TimeStamp(time=int(table.created.timestamp() * 1000))
                if table.created
                else None
            ),
            lastModified=(
                TimeStamp(time=int(table.last_altered.timestamp() * 1000))
                if table.last_altered
                else None
            ),
            description=table.comment,
            qualifiedName=str(datahub_dataset_name),
            customProperties=custom_properties,
        )
        if self.config.patch_custom_properties:
            patch_builder = create_dataset_props_patch_builder(
                dataset_urn, dataset_properties
            )
            for patch_mcp in patch_builder.build():
                yield MetadataWorkUnit(
                    id=f"{dataset_urn}-{patch_mcp.aspectName}", mcp_raw=patch_mcp
                )
        else:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn, aspect=dataset_properties
            ).as_workunit()

        # TODO: Check if needed
        # if tags_to_add:
        #    yield gen_tags_aspect_workunit(dataset_urn, tags_to_add)

        schema_container_key = gen_schema_key(
            db_name=database,
            schema=schema,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

        yield from add_table_to_schema_container(
            dataset_urn,
            parent_container_key=schema_container_key,
        )
        dpi_aspect = get_dataplatform_instance_aspect(
            dataset_urn=dataset_urn,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )
        if dpi_aspect:
            yield dpi_aspect

        subTypes = SubTypes(typeNames=[sub_type])
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=subTypes
        ).as_workunit()

        if self.domain_registry:
            yield from get_domain_wu(
                dataset_name=str(datahub_dataset_name),
                entity_urn=dataset_urn,
                domain_registry=self.domain_registry,
                domain_config=self.config.domain,
            )

    def cache_tables_and_views(self, connection, database):
        tables, views = self.data_dictionary.get_tables_and_views(
            conn=connection,
            skip_external_tables=self.config.skip_external_tables,
        )
        for schema in tables:
            if not is_schema_allowed(
                self.config.schema_pattern,
                schema,
                database,
                self.config.match_fully_qualified_names,
            ):
                logger.debug(
                    f"Not caching table for schema {database}.{schema} which is not allowed by schema_pattern"
                )
                continue

            self.db_tables[database][schema] = []
            for table in tables[schema]:
                if self.config.table_pattern.allowed(
                    f"{database}.{schema}.{table.name}"
                ):
                    self.db_tables[database][schema].append(table)
                    self.report.table_cached[f"{database}.{schema}"] = (
                        self.report.table_cached.get(f"{database}.{schema}", 0) + 1
                    )
                else:
                    logger.debug(
                        f"Table {database}.{schema}.{table.name} is filtered by table_pattern"
                    )
                    self.report.table_filtered[f"{database}.{schema}"] = (
                        self.report.table_filtered.get(f"{database}.{schema}", 0) + 1
                    )

        for schema in views:
            if not is_schema_allowed(
                self.config.schema_pattern,
                schema,
                database,
                self.config.match_fully_qualified_names,
            ):
                logger.debug(
                    f"Not caching views for schema {database}.{schema} which is not allowed by schema_pattern"
                )
                continue

            self.db_views[database][schema] = []
            for view in views[schema]:
                if self.config.view_pattern.allowed(f"{database}.{schema}.{view.name}"):
                    self.db_views[database][schema].append(view)
                    self.report.view_cached[f"{database}.{schema}"] = (
                        self.report.view_cached.get(f"{database}.{schema}", 0) + 1
                    )
                else:
                    logger.debug(
                        f"View {database}.{schema}.{view.name} is filtered by view_pattern"
                    )
                    self.report.view_filtered[f"{database}.{schema}"] = (
                        self.report.view_filtered.get(f"{database}.{schema}", 0) + 1
                    )

    def get_all_tables(
        self,
    ) -> Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]]:
        all_tables: Dict[
            str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]
        ] = defaultdict(dict)
        for db in set().union(self.db_tables, self.db_views):
            tables = self.db_tables.get(db, {})
            views = self.db_views.get(db, {})
            for schema in set().union(tables, views):
                all_tables[db][schema] = [
                    *tables.get(schema, []),
                    *views.get(schema, []),
                ]
        return all_tables

    def extract_usage(
        self,
        connection: redshift_connector.Connection,
        database: str,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataWorkUnit]:
        with PerfTimer() as timer:
            redundant_usage_run_skip_handler: Optional[
                RedundantUsageRunSkipHandler
            ] = None
            if self.config.enable_stateful_usage_ingestion:
                redundant_usage_run_skip_handler = RedundantUsageRunSkipHandler(
                    source=self,
                    config=self.config,
                    pipeline_name=self.ctx.pipeline_name,
                    run_id=self.ctx.run_id,
                )
            usage_extractor = RedshiftUsageExtractor(
                config=self.config,
                connection=connection,
                report=self.report,
                dataset_urn_builder=self.gen_dataset_urn,
                redundant_run_skip_handler=redundant_usage_run_skip_handler,
            )

            yield from usage_extractor.get_usage_workunits(all_tables=all_tables)

            self.report.usage_extraction_sec[database] = round(
                timer.elapsed_seconds(), 2
            )

    def extract_lineage(
        self,
        connection: redshift_connector.Connection,
        database: str,
        all_tables: Dict[str, Dict[str, List[Union[RedshiftView, RedshiftTable]]]],
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_lineage():
            return

        lineage_extractor = RedshiftLineageExtractor(
            config=self.config,
            report=self.report,
            context=self.ctx,
            redundant_run_skip_handler=self.redundant_lineage_run_skip_handler,
        )

        with PerfTimer() as timer:
            lineage_extractor.populate_lineage(
                database=database, connection=connection, all_tables=all_tables
            )

            self.report.lineage_extraction_sec[f"{database}"] = round(
                timer.elapsed_seconds(), 2
            )
            yield from self.generate_lineage(
                database, lineage_extractor=lineage_extractor
            )

            if self.redundant_lineage_run_skip_handler:
                # Update the checkpoint state for this run.
                self.redundant_lineage_run_skip_handler.update_state(
                    self.config.start_time, self.config.end_time
                )

    def extract_lineage_v2(
        self,
        connection: redshift_connector.Connection,
        database: str,
        lineage_extractor: RedshiftSqlLineageV2,
    ) -> Iterable[MetadataWorkUnit]:
        if not self._should_ingest_lineage():
            return

        with PerfTimer() as timer:
            all_tables = self.get_all_tables()

            lineage_extractor.build(
                connection=connection, all_tables=all_tables, db_schemas=self.db_schemas
            )

            yield from lineage_extractor.generate()

            self.report.lineage_extraction_sec[f"{database}"] = round(
                timer.elapsed_seconds(), 2
            )

        if self.redundant_lineage_run_skip_handler:
            # Update the checkpoint state for this run.
            self.redundant_lineage_run_skip_handler.update_state(
                lineage_extractor.start_time, lineage_extractor.end_time
            )

    def _should_ingest_lineage(self) -> bool:
        if (
            self.redundant_lineage_run_skip_handler
            and self.redundant_lineage_run_skip_handler.should_skip_this_run(
                cur_start_time=self.config.start_time,
                cur_end_time=self.config.end_time,
            )
        ):
            # Skip this run
            self.report.report_warning(
                "lineage-extraction",
                "Skip this run as there was already a run for current ingestion window.",
            )
            return False

        return True

    def generate_lineage(
        self, database: str, lineage_extractor: RedshiftLineageExtractor
    ) -> Iterable[MetadataWorkUnit]:
        logger.info(f"Generate lineage for {database}")
        for schema in deduplicate_list(
            itertools.chain(self.db_tables[database], self.db_views[database])
        ):
            if (
                database not in self.db_schemas
                or schema not in self.db_schemas[database]
            ):
                logger.warning(
                    f"Either database {database} or {schema} exists in the lineage but was not discovered earlier. Something went wrong."
                )
                continue

            table_or_view: Union[RedshiftTable, RedshiftView]
            for table_or_view in (
                []
                + self.db_tables[database].get(schema, [])
                + self.db_views[database].get(schema, [])
            ):
                datahub_dataset_name = f"{database}.{schema}.{table_or_view.name}"
                dataset_urn = self.gen_dataset_urn(datahub_dataset_name)

                lineage_info = lineage_extractor.get_lineage(
                    table_or_view,
                    dataset_urn,
                    self.db_schemas[database][schema],
                )
                if lineage_info:
                    # incremental lineage generation is taken care by auto_incremental_lineage
                    yield from gen_lineage(dataset_urn, lineage_info)

    def add_config_to_report(self):
        self.report.stateful_lineage_ingestion_enabled = (
            self.config.enable_stateful_lineage_ingestion
        )
        self.report.stateful_usage_ingestion_enabled = (
            self.config.enable_stateful_usage_ingestion
        )
        self.report.window_start_time, self.report.window_end_time = (
            self.config.start_time,
            self.config.end_time,
        )

    def _try_get_redshift_connection(
        self,
        config: RedshiftConfig,
    ) -> Optional[redshift_connector.Connection]:
        try:
            return RedshiftSource.get_redshift_connection(config)
        except redshift_connector.Error as e:
            error_message = str(e).lower()
            if "password authentication failed" in error_message:
                self.report.report_failure(
                    title="Invalid credentials",
                    message="Failed to connect to Redshift. Please verify your username, password, and database.",
                    exc=e,
                )
            elif "timeout" in error_message:
                self.report.report_failure(
                    title="Unable to connect",
                    message="Failed to connect to Redshift. Please verify your host name and port number.",
                    exc=e,
                )
            elif "communication error" in error_message:
                self.report.report_failure(
                    title="Unable to connect",
                    message="Failed to connect to Redshift. Please verify that the host name is valid and reachable.",
                    exc=e,
                )
            elif "database" in error_message and "does not exist" in error_message:
                self.report.report_failure(
                    title="Database does not exist",
                    message="Failed to connect to Redshift. Please verify that the provided database exists and the provided user has access to it.",
                    exc=e,
                )
            else:
                self.report.report_failure(
                    title="Unable to connect",
                    message="Failed to connect to Redshift. Please verify your connection details.",
                    exc=e,
                )
            return None
