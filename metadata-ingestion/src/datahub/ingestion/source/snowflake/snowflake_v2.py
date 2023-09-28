import json
import logging
import os
import os.path
import platform
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional, Union

import pandas as pd
from snowflake.connector import SnowflakeConnection

from datahub.configuration.pattern_utils import is_schema_allowed
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    Source,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.glossary.classification_mixin import ClassificationHandler
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SNOWFLAKE_DATABASE,
    SnowflakeEdition,
    SnowflakeObjectDomain,
)
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeColumn,
    SnowflakeDatabase,
    SnowflakeDataDictionary,
    SnowflakeFK,
    SnowflakePK,
    SnowflakeQuery,
    SnowflakeSchema,
    SnowflakeTable,
    SnowflakeTag,
    SnowflakeView,
)
from datahub.ingestion.source.snowflake.snowflake_shares import SnowflakeSharesHandler
from datahub.ingestion.source.snowflake.snowflake_tag import SnowflakeTagExtractor
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeConnectionMixin,
    SnowflakePermissionError,
    SnowflakeQueryMixin,
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
from datahub.utilities.file_backed_collections import FileBackedDict
from datahub.utilities.perf_timer import PerfTimer
from datahub.utilities.registries.domain_registry import DomainRegistry
from datahub.utilities.sqlglot_lineage import SchemaResolver

logger: logging.Logger = logging.getLogger(__name__)

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


@platform_name("Snowflake", doc_order=1)
@config_class(SnowflakeV2Config)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(
    SourceCapability.DATA_PROFILING,
    "Optionally enabled via configuration `profiling.enabled`",
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default, can be disabled via configuration `include_table_lineage` and `include_view_lineage`",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default, can be disabled via configuration `include_column_lineage`",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Enabled by default, can be disabled via configuration `include_usage_stats`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Optionally enabled via `stateful_ingestion.remove_stale_metadata`",
    supported=True,
)
@capability(
    SourceCapability.TAGS,
    "Optionally enabled via `extract_tags`",
    supported=True,
)
class SnowflakeV2Source(
    SnowflakeQueryMixin,
    SnowflakeConnectionMixin,
    SnowflakeCommonMixin,
    StatefulIngestionSourceBase,
    TestableSource,
):
    def __init__(self, ctx: PipelineContext, config: SnowflakeV2Config):
        super().__init__(config, ctx)
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = SnowflakeV2Report()
        self.logger = logger
        self.snowsight_base_url: Optional[str] = None
        self.connection: Optional[SnowflakeConnection] = None

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        # For database, schema, tables, views, etc
        self.data_dictionary = SnowflakeDataDictionary()

        self.lineage_extractor: Optional[SnowflakeLineageExtractor] = None
        if self.config.include_table_lineage:
            redundant_lineage_run_skip_handler: Optional[
                RedundantLineageRunSkipHandler
            ] = None
            if self.config.enable_stateful_lineage_ingestion:
                redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                    source=self,
                    config=self.config,
                    pipeline_name=self.ctx.pipeline_name,
                    run_id=self.ctx.run_id,
                )
            self.lineage_extractor = SnowflakeLineageExtractor(
                config,
                self.report,
                dataset_urn_builder=self.gen_dataset_urn,
                redundant_run_skip_handler=redundant_lineage_run_skip_handler,
            )

        self.usage_extractor: Optional[SnowflakeUsageExtractor] = None
        if self.config.include_usage_stats or self.config.include_operational_stats:
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
            self.usage_extractor = SnowflakeUsageExtractor(
                config,
                self.report,
                dataset_urn_builder=self.gen_dataset_urn,
                redundant_run_skip_handler=redundant_usage_run_skip_handler,
            )

        self.tag_extractor = SnowflakeTagExtractor(
            config, self.data_dictionary, self.report
        )

        self.profiling_state_handler: Optional[ProfilingHandler] = None
        if self.config.enable_stateful_profiling:
            self.profiling_state_handler = ProfilingHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        if config.is_profiling_enabled():
            # For profiling
            self.profiler = SnowflakeProfiler(
                config, self.report, self.profiling_state_handler
            )

        if self.config.classification.enabled:
            self.classification_handler = ClassificationHandler(
                self.config, self.report
            )

        # Caches tables for a single database. Consider moving to disk or S3 when possible.
        self.db_tables: Dict[str, List[SnowflakeTable]] = {}

        self.view_definitions: FileBackedDict[str] = FileBackedDict()
        self.add_config_to_report()

        self.sql_parser_schema_resolver = self._init_schema_resolver()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = SnowflakeV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()

        try:
            connection_conf = SnowflakeV2Config.parse_obj_allow_extras(config_dict)

            connection: SnowflakeConnection = connection_conf.get_connection()
            assert connection

            test_report.basic_connectivity = CapabilityReport(capable=True)

            test_report.capability_report = SnowflakeV2Source.check_capabilities(
                connection, connection_conf
            )
            connection.close()

        except Exception as e:
            logger.error(f"Failed to test connection due to {e}", exc_info=e)
            if test_report.basic_connectivity is None:
                test_report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=f"{e}"
                )
            else:
                test_report.internal_failure = True
                test_report.internal_failure_reason = f"{e}"

        return test_report

    @staticmethod
    def check_capabilities(
        conn: SnowflakeConnection, connection_conf: SnowflakeV2Config
    ) -> Dict[Union[SourceCapability, str], CapabilityReport]:
        # Currently only overall capabilities are reported.
        # Resource level variations in capabilities are not considered.

        @dataclass
        class SnowflakePrivilege:
            privilege: str
            object_name: str
            object_type: str

        def query(query):
            logger.info("Query : {}".format(query))
            resp = conn.cursor().execute(query)
            return resp

        _report: Dict[Union[SourceCapability, str], CapabilityReport] = dict()
        privileges: List[SnowflakePrivilege] = []
        capabilities: List[SourceCapability] = [c.capability for c in SnowflakeV2Source.get_capabilities() if c.capability not in (SourceCapability.PLATFORM_INSTANCE, SourceCapability.DOMAINS, SourceCapability.DELETION_DETECTION)]  # type: ignore

        cur = query("select current_role()")
        current_role = [row[0] for row in cur][0]

        cur = query("select current_secondary_roles()")
        secondary_roles_str = json.loads([row[0] for row in cur][0])["roles"]
        secondary_roles = (
            [] if secondary_roles_str == "" else secondary_roles_str.split(",")
        )

        roles = [current_role] + secondary_roles

        # PUBLIC role is automatically granted to every role
        if "PUBLIC" not in roles:
            roles.append("PUBLIC")
        i = 0

        while i < len(roles):
            role = roles[i]
            i = i + 1
            # for some roles, quoting is necessary. for example test-role
            cur = query(f'show grants to role "{role}"')
            for row in cur:
                privilege = SnowflakePrivilege(
                    privilege=row[1], object_type=row[2], object_name=row[3]
                )
                privileges.append(privilege)

                if privilege.object_type in (
                    "DATABASE",
                    "SCHEMA",
                ) and privilege.privilege in ("OWNERSHIP", "USAGE"):
                    _report[SourceCapability.CONTAINERS] = CapabilityReport(
                        capable=True
                    )
                    _report[SourceCapability.TAGS] = CapabilityReport(capable=True)
                elif privilege.object_type in (
                    "TABLE",
                    "VIEW",
                    "MATERIALIZED_VIEW",
                ):
                    _report[SourceCapability.SCHEMA_METADATA] = CapabilityReport(
                        capable=True
                    )
                    _report[SourceCapability.DESCRIPTIONS] = CapabilityReport(
                        capable=True
                    )

                    # Table level profiling is supported without SELECT access
                    # if privilege.privilege in ("SELECT", "OWNERSHIP"):
                    _report[SourceCapability.DATA_PROFILING] = CapabilityReport(
                        capable=True
                    )

                    if privilege.object_name.startswith("SNOWFLAKE.ACCOUNT_USAGE."):
                        # if access to "snowflake" shared database, access to all account_usage views is automatically granted
                        # Finer access control is not yet supported for shares
                        # https://community.snowflake.com/s/article/Error-Granting-individual-privileges-on-imported-database-is-not-allowed-Use-GRANT-IMPORTED-PRIVILEGES-instead
                        _report[SourceCapability.LINEAGE_COARSE] = CapabilityReport(
                            capable=True
                        )

                        _report[SourceCapability.LINEAGE_FINE] = CapabilityReport(
                            capable=True
                        )

                        _report[SourceCapability.USAGE_STATS] = CapabilityReport(
                            capable=True
                        )
                        _report[SourceCapability.TAGS] = CapabilityReport(capable=True)

                # If all capabilities supported, no need to continue
                if set(capabilities) == set(_report.keys()):
                    break

                # Due to this, entire role hierarchy is considered
                if (
                    privilege.object_type == "ROLE"
                    and privilege.privilege == "USAGE"
                    and privilege.object_name not in roles
                ):
                    roles.append(privilege.object_name)

        cur = query("select current_warehouse()")
        current_warehouse = [row[0] for row in cur][0]

        default_failure_messages = {
            SourceCapability.SCHEMA_METADATA: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.DESCRIPTIONS: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.DATA_PROFILING: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.CONTAINERS: "Current role does not have permissions to use any database",
            SourceCapability.LINEAGE_COARSE: "Current role does not have permissions to snowflake account usage views",
            SourceCapability.LINEAGE_FINE: "Current role does not have permissions to snowflake account usage views",
            SourceCapability.USAGE_STATS: "Current role does not have permissions to snowflake account usage views",
            SourceCapability.TAGS: "Either no tags have been applied to objects, or the current role does not have permission to access the objects or to snowflake account usage views ",
        }

        for c in capabilities:  # type:ignore
            # These capabilities do not work without active warehouse
            if current_warehouse is None and c in (
                SourceCapability.SCHEMA_METADATA,
                SourceCapability.DESCRIPTIONS,
                SourceCapability.DATA_PROFILING,
                SourceCapability.LINEAGE_COARSE,
                SourceCapability.LINEAGE_FINE,
                SourceCapability.USAGE_STATS,
                SourceCapability.TAGS,
            ):
                failure_message = (
                    f"Current role {current_role} does not have permissions to use warehouse {connection_conf.warehouse}. Please check the grants associated with this role."
                    if connection_conf.warehouse is not None
                    else "No default warehouse set for user. Either set default warehouse for user or configure warehouse in recipe"
                )
                _report[c] = CapabilityReport(
                    capable=False,
                    failure_reason=failure_message,
                )

            if c in _report.keys():
                continue

            # If some capabilities are missing, then mark them as not capable
            _report[c] = CapabilityReport(
                capable=False,
                failure_reason=default_failure_messages[c],
            )

        return _report

    def _init_schema_resolver(self) -> SchemaResolver:
        if not self.config.include_technical_schema and self.config.parse_view_ddl:
            if self.ctx.graph:
                return self.ctx.graph.initialize_schema_resolver_from_datahub(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                )
            else:
                logger.warning(
                    "Failed to load schema info from DataHub as DataHubGraph is missing.",
                )
        return SchemaResolver(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._snowflake_clear_ocsp_cache()

        self.connection = self.create_connection()
        if self.connection is None:
            return

        self.inspect_session_metadata()

        if self.config.include_external_url:
            self.snowsight_base_url = self.get_snowsight_base_url()

        if self.report.default_warehouse is None:
            self.report_warehouse_failure()
            return

        self.data_dictionary.set_connection(self.connection)
        databases: List[SnowflakeDatabase] = []

        for database in self.get_databases() or []:
            self.report.report_entity_scanned(database.name, "database")
            if not self.config.database_pattern.allowed(database.name):
                self.report.report_dropped(f"{database.name}.*")
            else:
                databases.append(database)

        if len(databases) == 0:
            return

        for snowflake_db in databases:
            try:
                self.report.set_ingestion_stage(snowflake_db.name, METADATA_EXTRACTION)
                yield from self._process_database(snowflake_db)

            except SnowflakePermissionError as e:
                # FIXME - This may break stateful ingestion if new tables than previous run are emitted above
                # and stateful ingestion is enabled
                self.report_error(GENERIC_PERMISSION_ERROR_KEY, str(e))
                return

        self.connection.close()

        self.report_cache_info()

        # TODO: The checkpoint state for stale entity detection can be committed here.

        if self.config.shares:
            yield from SnowflakeSharesHandler(
                self.config, self.report, self.gen_dataset_urn
            ).get_shares_workunits(databases)

        discovered_tables: List[str] = [
            self.get_dataset_identifier(table_name, schema.name, db.name)
            for db in databases
            for schema in db.schemas
            for table_name in schema.tables
        ]
        discovered_views: List[str] = [
            self.get_dataset_identifier(table_name, schema.name, db.name)
            for db in databases
            for schema in db.schemas
            for table_name in schema.views
        ]

        if len(discovered_tables) == 0 and len(discovered_views) == 0:
            self.report_error(
                GENERIC_PERMISSION_ERROR_KEY,
                "No tables/views found. Please check permissions.",
            )
            return

        discovered_datasets = discovered_tables + discovered_views

        if self.config.include_table_lineage and self.lineage_extractor:
            self.report.set_ingestion_stage("*", LINEAGE_EXTRACTION)
            yield from self.lineage_extractor.get_workunits(
                discovered_tables=discovered_tables,
                discovered_views=discovered_views,
                schema_resolver=self.sql_parser_schema_resolver,
                view_definitions=self.view_definitions,
            )

        if (
            self.config.include_usage_stats or self.config.include_operational_stats
        ) and self.usage_extractor:
            yield from self.usage_extractor.get_usage_workunits(discovered_datasets)

    def report_cache_info(self):
        lru_cache_functions: List[Callable] = [
            self.data_dictionary.get_tables_for_database,
            self.data_dictionary.get_views_for_database,
            self.data_dictionary.get_columns_for_schema,
            self.data_dictionary.get_pk_constraints_for_schema,
            self.data_dictionary.get_fk_constraints_for_schema,
        ]
        for func in lru_cache_functions:
            self.report.lru_cache_info[func.__name__] = func.cache_info()._asdict()  # type: ignore

    def report_warehouse_failure(self):
        if self.config.warehouse is not None:
            self.report_error(
                GENERIC_PERMISSION_ERROR_KEY,
                f"Current role does not have permissions to use warehouse {self.config.warehouse}. Please update permissions.",
            )
        else:
            self.report_error(
                "no-active-warehouse",
                "No default warehouse set for user. Either set default warehouse for user or configure warehouse in recipe.",
            )

    def get_databases(self) -> Optional[List[SnowflakeDatabase]]:
        try:
            # `show databases` is required only to get one  of the databases
            # whose information_schema can be queried to start with.
            databases = self.data_dictionary.show_databases()
        except Exception as e:
            logger.debug(f"Failed to list databases due to error {e}", exc_info=e)
            self.report_error(
                "list-databases",
                f"Failed to list databases due to error {e}",
            )
            return None
        else:
            ischema_databases: List[
                SnowflakeDatabase
            ] = self.get_databases_from_ischema(databases)

            if len(ischema_databases) == 0:
                self.report_error(
                    GENERIC_PERMISSION_ERROR_KEY,
                    "No databases found. Please check permissions.",
                )
            return ischema_databases

    def get_databases_from_ischema(self, databases):
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
            self.query(SnowflakeQuery.use_database(db_name))
        except Exception as e:
            if isinstance(e, SnowflakePermissionError):
                # This may happen if REFERENCE_USAGE permissions are set
                # We can not run show queries on database in such case.
                # This need not be a failure case.
                self.report_warning(
                    "Insufficient privileges to operate on database, skipping. Please grant USAGE permissions on database to extract its metadata.",
                    db_name,
                )
            else:
                logger.debug(
                    f"Failed to use database {db_name} due to error {e}",
                    exc_info=e,
                )
                self.report_warning(
                    "Failed to get schemas for database",
                    db_name,
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

        self.db_tables = {}
        for snowflake_schema in snowflake_db.schemas:
            yield from self._process_schema(snowflake_schema, db_name)

        if self.config.is_profiling_enabled() and self.db_tables:
            self.report.set_ingestion_stage(snowflake_db.name, PROFILING)
            yield from self.profiler.get_workunits(snowflake_db, self.db_tables)

    def fetch_schemas_for_database(
        self, snowflake_db: SnowflakeDatabase, db_name: str
    ) -> None:
        schemas: List[SnowflakeSchema] = []
        try:
            for schema in self.data_dictionary.get_schemas_for_database(db_name):
                self.report.report_entity_scanned(schema.name, "schema")
                if not is_schema_allowed(
                    self.config.schema_pattern,
                    schema.name,
                    db_name,
                    self.config.match_fully_qualified_names,
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
                logger.debug(
                    f"Failed to get schemas for database {db_name} due to error {e}",
                    exc_info=e,
                )
                self.report_warning(
                    "Failed to get schemas for database",
                    db_name,
                )

        if not schemas:
            self.report_warning(
                "No schemas found in database. If schemas exist, please grant USAGE permissions on them.",
                db_name,
            )
        else:
            snowflake_db.schemas = schemas

    def _process_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str
    ) -> Iterable[MetadataWorkUnit]:
        schema_name = snowflake_schema.name

        if self.config.extract_tags != TagOption.skip:
            snowflake_schema.tags = self.tag_extractor.get_tags_on_object(
                schema_name=schema_name, db_name=db_name, domain="schema"
            )

        if self.config.include_technical_schema:
            yield from self.gen_schema_containers(snowflake_schema, db_name)

        if self.config.include_tables:
            tables = self.fetch_tables_for_schema(
                snowflake_schema, db_name, schema_name
            )
            self.db_tables[schema_name] = tables

            if self.config.include_technical_schema:
                for table in tables:
                    yield from self._process_table(table, schema_name, db_name)

        if self.config.include_views:
            views = self.fetch_views_for_schema(snowflake_schema, db_name, schema_name)
            if self.config.parse_view_ddl:
                for view in views:
                    key = self.get_dataset_identifier(view.name, schema_name, db_name)
                    if view.view_definition:
                        self.view_definitions[key] = view.view_definition

            if self.config.include_technical_schema:
                for view in views:
                    yield from self._process_view(view, schema_name, db_name)

        if self.config.include_technical_schema and snowflake_schema.tags:
            for tag in snowflake_schema.tags:
                yield from self._process_tag(tag)

        if not snowflake_schema.views and not snowflake_schema.tables:
            self.report_warning(
                "No tables/views found in schema. If tables exist, please grant REFERENCES or SELECT permissions on them.",
                f"{db_name}.{schema_name}",
            )

    def fetch_views_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeView]:
        try:
            views: List[SnowflakeView] = []
            for view in self.get_views_for_schema(schema_name, db_name):
                view_name = self.get_dataset_identifier(view.name, schema_name, db_name)

                self.report.report_entity_scanned(view_name, "view")

                if not self.config.view_pattern.allowed(view_name):
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
                logger.debug(
                    f"Failed to get views for schema {db_name}.{schema_name} due to error {e}",
                    exc_info=e,
                )
                self.report_warning(
                    "Failed to get views for schema",
                    f"{db_name}.{schema_name}",
                )
                return []

    def fetch_tables_for_schema(
        self, snowflake_schema: SnowflakeSchema, db_name: str, schema_name: str
    ) -> List[SnowflakeTable]:
        try:
            tables: List[SnowflakeTable] = []
            for table in self.get_tables_for_schema(schema_name, db_name):
                table_identifier = self.get_dataset_identifier(
                    table.name, schema_name, db_name
                )
                self.report.report_entity_scanned(table_identifier)
                if not self.config.table_pattern.allowed(table_identifier):
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
                logger.debug(
                    f"Failed to get tables for schema {db_name}.{schema_name} due to error {e}",
                    exc_info=e,
                )
                self.report_warning(
                    "Failed to get tables for schema",
                    f"{db_name}.{schema_name}",
                )
                return []

    def _process_table(
        self,
        table: SnowflakeTable,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        table_identifier = self.get_dataset_identifier(table.name, schema_name, db_name)

        self.fetch_columns_for_table(table, schema_name, db_name, table_identifier)

        self.fetch_pk_for_table(table, schema_name, db_name, table_identifier)

        self.fetch_foreign_keys_for_table(table, schema_name, db_name, table_identifier)

        dataset_name = self.get_dataset_identifier(table.name, schema_name, db_name)

        self.fetch_sample_data_for_classification(
            table, schema_name, db_name, dataset_name
        )

        if self.config.extract_tags != TagOption.skip:
            table.tags = self.tag_extractor.get_tags_on_object(
                table_name=table.name,
                schema_name=schema_name,
                db_name=db_name,
                domain="table",
            )

        if self.config.include_technical_schema:
            if table.tags:
                for tag in table.tags:
                    yield from self._process_tag(tag)
            for column_name in table.column_tags:
                for tag in table.column_tags[column_name]:
                    yield from self._process_tag(tag)

            yield from self.gen_dataset_workunits(table, schema_name, db_name)

    def fetch_sample_data_for_classification(
        self, table: SnowflakeTable, schema_name: str, db_name: str, dataset_name: str
    ) -> None:
        if (
            table.columns
            and self.config.classification.enabled
            and self.classification_handler.is_classification_enabled_for_table(
                dataset_name
            )
        ):
            try:
                table.sample_data = self.get_sample_values_for_table(
                    table.name, schema_name, db_name
                )
            except Exception as e:
                logger.debug(
                    f"Failed to get sample values for dataset {dataset_name} due to error {e}",
                    exc_info=e,
                )
                if isinstance(e, SnowflakePermissionError):
                    self.report_warning(
                        "Failed to get sample values for dataset. Please grant SELECT permissions on dataset.",
                        dataset_name,
                    )
                else:
                    self.report_warning(
                        "Failed to get sample values for dataset",
                        dataset_name,
                    )

    def fetch_foreign_keys_for_table(
        self, table, schema_name, db_name, table_identifier
    ):
        try:
            table.foreign_keys = self.get_fk_constraints_for_table(
                table.name, schema_name, db_name
            )
        except Exception as e:
            logger.debug(
                f"Failed to get foreign key for table {table_identifier} due to error {e}",
                exc_info=e,
            )
            self.report_warning("Failed to get foreign key for table", table_identifier)

    def fetch_pk_for_table(self, table, schema_name, db_name, table_identifier):
        try:
            table.pk = self.get_pk_constraints_for_table(
                table.name, schema_name, db_name
            )
        except Exception as e:
            logger.debug(
                f"Failed to get primary key for table {table_identifier} due to error {e}",
                exc_info=e,
            )
            self.report_warning("Failed to get primary key for table", table_identifier)

    def fetch_columns_for_table(self, table, schema_name, db_name, table_identifier):
        try:
            table.columns = self.get_columns_for_table(table.name, schema_name, db_name)
            table.column_count = len(table.columns)
            if self.config.extract_tags != TagOption.skip:
                table.column_tags = self.tag_extractor.get_column_tags_for_table(
                    table.name, schema_name, db_name
                )
        except Exception as e:
            logger.debug(
                f"Failed to get columns for table {table_identifier} due to error {e}",
                exc_info=e,
            )
            self.report_warning("Failed to get columns for table", table_identifier)

    def _process_view(
        self,
        view: SnowflakeView,
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        view_name = self.get_dataset_identifier(view.name, schema_name, db_name)

        try:
            view.columns = self.get_columns_for_table(view.name, schema_name, db_name)
            if self.config.extract_tags != TagOption.skip:
                view.column_tags = self.tag_extractor.get_column_tags_for_table(
                    view.name, schema_name, db_name
                )
        except Exception as e:
            logger.debug(
                f"Failed to get columns for view {view_name} due to error {e}",
                exc_info=e,
            )
            self.report_warning("Failed to get columns for view", view_name)

        if self.config.extract_tags != TagOption.skip:
            view.tags = self.tag_extractor.get_tags_on_object(
                table_name=view.name,
                schema_name=schema_name,
                db_name=db_name,
                domain="table",
            )

        if self.config.include_technical_schema:
            if view.tags:
                for tag in view.tags:
                    yield from self._process_tag(tag)
            for column_name in view.column_tags:
                for tag in view.column_tags[column_name]:
                    yield from self._process_tag(tag)

            yield from self.gen_dataset_workunits(view, schema_name, db_name)

    def _process_tag(self, tag: SnowflakeTag) -> Iterable[MetadataWorkUnit]:
        tag_identifier = tag.identifier()

        if self.report.is_tag_processed(tag_identifier):
            return

        self.report.report_tag_processed(tag_identifier)

        yield from self.gen_tag_workunits(tag)

    def gen_dataset_urn(self, dataset_identifier: str) -> str:
        return make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_identifier,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
        )

    def gen_dataset_workunits(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        schema_name: str,
        db_name: str,
    ) -> Iterable[MetadataWorkUnit]:
        dataset_name = self.get_dataset_identifier(table.name, schema_name, db_name)
        dataset_urn = self.gen_dataset_urn(dataset_name)

        status = Status(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=status
        ).as_workunit()

        schema_metadata = self.gen_schema_metadata(table, schema_name, db_name)
        # TODO: classification is only run for snowflake tables.
        # Should we run classification for snowflake views as well?
        self.classify_snowflake_table(table, dataset_name, schema_metadata)
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
            typeNames=[DatasetSubTypes.VIEW]
            if isinstance(table, SnowflakeView)
            else [DatasetSubTypes.TABLE]
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
                viewLogic=table.view_definition,
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
            created=TimeStamp(time=int(table.created.timestamp() * 1000))
            if table.created is not None
            else None,
            lastModified=TimeStamp(time=int(table.last_altered.timestamp() * 1000))
            if table.last_altered is not None
            else TimeStamp(time=int(table.created.timestamp() * 1000))
            if table.created is not None
            else None,
            description=table.comment,
            qualifiedName=f"{db_name}.{schema_name}.{table.name}",
            customProperties={},
            externalUrl=self.get_external_url_for_table(
                table.name,
                schema_name,
                db_name,
                SnowflakeObjectDomain.TABLE
                if isinstance(table, SnowflakeTable)
                else SnowflakeObjectDomain.VIEW,
            )
            if self.config.include_external_url
            else None,
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
        dataset_name = self.get_dataset_identifier(table.name, schema_name, db_name)
        dataset_urn = self.gen_dataset_urn(dataset_name)

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
                    isPartOfKey=col.name in table.pk.column_names
                    if isinstance(table, SnowflakeTable) and table.pk is not None
                    else None,
                    globalTags=GlobalTags(
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
                    else None,
                )
                for col in table.columns
            ],
            foreignKeys=foreign_keys,
        )

        if self.config.parse_view_ddl:
            self.sql_parser_schema_resolver.add_schema_metadata(
                urn=dataset_urn, schema_metadata=schema_metadata
            )

        return schema_metadata

    def build_foreign_keys(
        self, table: SnowflakeTable, dataset_urn: str
    ) -> List[ForeignKeyConstraint]:
        foreign_keys = []
        for fk in table.foreign_keys:
            foreign_dataset = make_dataset_urn(
                self.platform,
                self.get_dataset_identifier(
                    fk.referred_table, fk.referred_schema, fk.referred_database
                ),
                self.config.env,
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

    def classify_snowflake_table(
        self,
        table: Union[SnowflakeTable, SnowflakeView],
        dataset_name: str,
        schema_metadata: SchemaMetadata,
    ) -> None:
        if (
            isinstance(table, SnowflakeTable)
            and self.config.classification.enabled
            and self.classification_handler.is_classification_enabled_for_table(
                dataset_name
            )
        ):
            if table.sample_data is not None:
                table.sample_data.columns = [
                    self.snowflake_identifier(col) for col in table.sample_data.columns
                ]

            try:
                self.classification_handler.classify_schema_fields(
                    dataset_name,
                    schema_metadata,
                    table.sample_data.to_dict(orient="list")
                    if table.sample_data is not None
                    else {},
                )
            except Exception as e:
                logger.debug(
                    f"Failed to classify table columns for {dataset_name} due to error -> {e}",
                    exc_info=e,
                )
                self.report_warning(
                    "Failed to classify table columns",
                    dataset_name,
                )
            finally:
                # Cleaning up sample_data fetched for classification
                table.sample_data = None

    def get_report(self) -> SourceReport:
        return self.report

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
            external_url=self.get_external_url_for_database(database.name)
            if self.config.include_external_url
            else None,
            description=database.comment,
            created=int(database.created.timestamp() * 1000)
            if database.created is not None
            else None,
            last_modified=int(database.last_altered.timestamp() * 1000)
            if database.last_altered is not None
            else int(database.created.timestamp() * 1000)
            if database.created is not None
            else None,
            tags=[self.snowflake_identifier(tag.identifier()) for tag in database.tags]
            if database.tags
            else None,
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
            external_url=self.get_external_url_for_schema(schema.name, db_name)
            if self.config.include_external_url
            else None,
            created=int(schema.created.timestamp() * 1000)
            if schema.created is not None
            else None,
            last_modified=int(schema.last_altered.timestamp() * 1000)
            if schema.last_altered is not None
            else int(schema.created.timestamp() * 1000)
            if schema.created is not None
            else None,
            tags=[self.snowflake_identifier(tag.identifier()) for tag in schema.tags]
            if schema.tags
            else None,
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

        # get all views for database failed,
        # falling back to get views for schema
        if views is None:
            self.report.num_get_views_for_schema_queries += 1
            return self.data_dictionary.get_views_for_schema(schema_name, db_name)

        # Some schema may not have any table
        return views.get(schema_name, [])

    def get_columns_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> List[SnowflakeColumn]:
        columns = self.data_dictionary.get_columns_for_schema(schema_name, db_name)

        # get all columns for schema failed,
        # falling back to get columns for table
        if columns is None:
            self.report.num_get_columns_for_table_queries += 1
            return self.data_dictionary.get_columns_for_table(
                table_name, schema_name, db_name
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

    def add_config_to_report(self) -> None:
        self.report.cleaned_account_id = self.config.get_account()
        self.report.ignore_start_time_lineage = self.config.ignore_start_time_lineage
        self.report.upstream_lineage_in_report = self.config.upstream_lineage_in_report
        self.report.include_technical_schema = self.config.include_technical_schema
        self.report.include_usage_stats = self.config.include_usage_stats
        self.report.include_operational_stats = self.config.include_operational_stats
        self.report.include_column_lineage = self.config.include_column_lineage
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

    def inspect_session_metadata(self) -> None:
        try:
            logger.info("Checking current version")
            for db_row in self.query(SnowflakeQuery.current_version()):
                self.report.saas_version = db_row["CURRENT_VERSION()"]
        except Exception as e:
            self.report_error("version", f"Error: {e}")
        try:
            logger.info("Checking current role")
            for db_row in self.query(SnowflakeQuery.current_role()):
                self.report.role = db_row["CURRENT_ROLE()"]
        except Exception as e:
            self.report_error("version", f"Error: {e}")
        try:
            logger.info("Checking current warehouse")
            for db_row in self.query(SnowflakeQuery.current_warehouse()):
                self.report.default_warehouse = db_row["CURRENT_WAREHOUSE()"]
        except Exception as e:
            self.report_error("current_warehouse", f"Error: {e}")

        try:
            logger.info("Checking current edition")
            if self.is_standard_edition():
                self.report.edition = SnowflakeEdition.STANDARD
            else:
                self.report.edition = SnowflakeEdition.ENTERPRISE
        except Exception:
            self.report.edition = None

    # Ideally we do not want null values in sample data for a column.
    # However that would require separate query per column and
    # that would be expensive, hence not done. To compensale for possibility
    # of some null values in collected sample, we fetch extra (20% more)
    # rows than configured sample_size.
    def get_sample_values_for_table(
        self, table_name: str, schema_name: str, db_name: str
    ) -> pd.DataFrame:
        # Create a cursor object.
        logger.debug(
            f"Collecting sample values for table {db_name}.{schema_name}.{table_name}"
        )

        actual_sample_size = self.config.classification.sample_size * 1.2
        with PerfTimer() as timer:
            cur = self.get_connection().cursor()
            # Execute a statement that will generate a result set.
            sql = f'select * from "{db_name}"."{schema_name}"."{table_name}" sample ({actual_sample_size} rows);'

            cur.execute(sql)
            # Fetch the result set from the cursor and deliver it as the Pandas DataFrame.

            dat = cur.fetchall()
            df = pd.DataFrame(dat, columns=[col.name for col in cur.description])
            time_taken = timer.elapsed_seconds()
            logger.debug(
                f"Finished collecting sample values for table {db_name}.{schema_name}.{table_name};{df.shape[0]} rows; took {time_taken:.3f} seconds"
            )

        return df

    # domain is either "view" or "table"
    def get_external_url_for_table(
        self, table_name: str, schema_name: str, db_name: str, domain: str
    ) -> Optional[str]:
        if self.snowsight_base_url is not None:
            return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/{domain}/{table_name}/"
        return None

    def get_external_url_for_schema(
        self, schema_name: str, db_name: str
    ) -> Optional[str]:
        if self.snowsight_base_url is not None:
            return f"{self.snowsight_base_url}#/data/databases/{db_name}/schemas/{schema_name}/"
        return None

    def get_external_url_for_database(self, db_name: str) -> Optional[str]:
        if self.snowsight_base_url is not None:
            return f"{self.snowsight_base_url}#/data/databases/{db_name}/"
        return None

    def get_snowsight_base_url(self) -> Optional[str]:
        try:
            # See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#finding-the-region-and-locator-for-an-account
            for db_row in self.query(SnowflakeQuery.current_account()):
                account_locator = db_row["CURRENT_ACCOUNT()"]

            for db_row in self.query(SnowflakeQuery.current_region()):
                region = db_row["CURRENT_REGION()"]

            self.report.account_locator = account_locator
            self.report.region = region

            # Returned region may be in the form <region_group>.<region>, see https://docs.snowflake.com/en/sql-reference/functions/current_region.html
            region = region.split(".")[-1].lower()
            account_locator = account_locator.lower()

            cloud, cloud_region_id = self.get_cloud_region_from_snowflake_region_id(
                region
            )

            # For privatelink, account identifier ends with .privatelink
            # See https://docs.snowflake.com/en/user-guide/organizations-connect.html#private-connectivity-urls
            return self.create_snowsight_base_url(
                account_locator,
                cloud_region_id,
                cloud,
                self.config.account_id.endswith(".privatelink"),  # type:ignore
            )

        except Exception as e:
            self.warn(
                self.logger,
                "snowsight url",
                f"unable to get snowsight base url due to an error -> {e}",
            )
            return None

    def is_standard_edition(self) -> bool:
        try:
            self.query(SnowflakeQuery.show_tags())
            return False
        except Exception as e:
            if "Unsupported feature 'TAG'" in str(e):
                return True
            raise

    def _snowflake_clear_ocsp_cache(self) -> None:
        # Because of some issues with the Snowflake Python connector, we wipe the OCSP cache.
        #
        # Why is this necessary:
        # 1. Snowflake caches OCSP (certificate revocation) responses in a file on disk.
        #       https://github.com/snowflakedb/snowflake-connector-python/blob/502e49f65368d4eed2d6f543b43139cc96e03c00/src/snowflake/connector/ocsp_snowflake.py#L78-L108
        # 2. It uses pickle to serialize the cache to disk.
        #       https://github.com/snowflakedb/snowflake-connector-python/blob/502e49f65368d4eed2d6f543b43139cc96e03c00/src/snowflake/connector/cache.py#L483-L495
        # 3. In some cases, pyspark objects seem to make their way into the cache. This introduces a hard
        #    dependency on the specific version of pyspark that the cache was written with because of the pickle
        #    serialization process.
        #
        # As an example, if you run snowflake ingestion normally with pyspark v3.2.1, then downgrade pyspark to v3.0.3,
        # and then run ingestion again, you will get an error like this:
        #
        #     error 250001: Could not connect to Snowflake backend after 0 attempt(s).
        #     ModuleNotFoundError: No module named 'pyspark'
        #
        # While ideally the snowflake-connector-python library would be fixed to not serialize pyspark objects,
        # or to handle serde errors gracefully, or to use a robust serialization format instead of pickle,
        # we're stuck with this workaround for now.

        # This file selection logic is mirrored from the snowflake-connector-python library.
        # See https://github.com/snowflakedb/snowflake-connector-python/blob/502e49f65368d4eed2d6f543b43139cc96e03c00/src/snowflake/connector/cache.py#L349-L369
        plat = platform.system().lower()
        if plat == "darwin":
            file_path = os.path.join(
                "~", "Library", "Caches", "Snowflake", "ocsp_response_validation_cache"
            )
        elif plat == "windows":
            file_path = os.path.join(
                "~",
                "AppData",
                "Local",
                "Snowflake",
                "Caches",
                "ocsp_response_validation_cache",
            )
        else:
            # linux is the default fallback for snowflake
            file_path = os.path.join(
                "~", ".cache", "snowflake", "ocsp_response_validation_cache"
            )

        file_path = os.path.expanduser(file_path)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            logger.debug(f'Failed to remove OCSP cache file at "{file_path}"')

    def close(self) -> None:
        super().close()
        StatefulIngestionSourceBase.close(self)
        self.view_definitions.close()
        self.sql_parser_schema_resolver.close()
        if self.lineage_extractor:
            self.lineage_extractor.close()
        if self.usage_extractor:
            self.usage_extractor.close()
