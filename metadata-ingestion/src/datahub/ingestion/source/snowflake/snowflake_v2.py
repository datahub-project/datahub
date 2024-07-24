import functools
import json
import logging
import os
import os.path
import platform
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Union

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
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
    Source,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import (
    GENERIC_PERMISSION_ERROR_KEY,
    SnowflakeEdition,
)
from datahub.ingestion.source.snowflake.snowflake_assertion import (
    SnowflakeAssertionsHandler,
)
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_lineage_v2 import (
    SnowflakeLineageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_profiler import SnowflakeProfiler
from datahub.ingestion.source.snowflake.snowflake_queries import (
    SnowflakeQueriesExtractor,
    SnowflakeQueriesExtractorConfig,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeQuery,
)
from datahub.ingestion.source.snowflake.snowflake_schema_gen import (
    SnowflakeSchemaGenerator,
)
from datahub.ingestion.source.snowflake.snowflake_shares import SnowflakeSharesHandler
from datahub.ingestion.source.snowflake.snowflake_usage_v2 import (
    SnowflakeUsageExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeFilter,
    SnowflakeIdentifierBuilder,
    SnowsightUrlBuilder,
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
    QUERIES_EXTRACTION,
)
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.registries.domain_registry import DomainRegistry

logger: logging.Logger = logging.getLogger(__name__)


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
@capability(
    SourceCapability.CLASSIFICATION,
    "Optionally enabled via `classification.enabled`",
    supported=True,
)
class SnowflakeV2Source(
    SnowflakeCommonMixin,
    StatefulIngestionSourceBase,
    TestableSource,
):
    def __init__(self, ctx: PipelineContext, config: SnowflakeV2Config):
        super().__init__(config, ctx)
        self.config: SnowflakeV2Config = config
        self.report: SnowflakeV2Report = SnowflakeV2Report()

        self.filters = SnowflakeFilter(
            filter_config=self.config, structured_reporter=self.report
        )
        self.identifiers = SnowflakeIdentifierBuilder(
            identifier_config=self.config, structured_reporter=self.report
        )

        self.domain_registry: Optional[DomainRegistry] = None
        if self.config.domain:
            self.domain_registry = DomainRegistry(
                cached_domains=[k for k in self.config.domain], graph=self.ctx.graph
            )

        self.connection = self.config.get_connection()

        # For database, schema, tables, views, etc
        self.data_dictionary = SnowflakeDataDictionary(connection=self.connection)
        self.lineage_extractor: Optional[SnowflakeLineageExtractor] = None
        self.aggregator: Optional[SqlParsingAggregator] = None

        if self.config.use_queries_v2 or self.config.include_table_lineage:
            self.aggregator = SqlParsingAggregator(
                platform=self.identifiers.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
                eager_graph_load=(
                    # If we're ingestion schema metadata for tables/views, then we will populate
                    # schemas into the resolver as we go. We only need to do a bulk fetch
                    # if we're not ingesting schema metadata as part of ingestion.
                    (
                        self.config.include_technical_schema
                        and self.config.include_tables
                        and self.config.include_views
                    )
                    and not self.config.lazy_schema_resolver
                ),
                generate_usage_statistics=False,
                generate_operations=False,
                format_queries=self.config.format_sql_queries,
            )
            self.report.sql_aggregator = self.aggregator.report

        if self.config.include_table_lineage:
            assert self.aggregator is not None
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
                connection=self.connection,
                filters=self.filters,
                identifiers=self.identifiers,
                redundant_run_skip_handler=redundant_lineage_run_skip_handler,
                sql_aggregator=self.aggregator,
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
                connection=self.connection,
                filter=self.filters,
                identifiers=self.identifiers,
                redundant_run_skip_handler=redundant_usage_run_skip_handler,
            )

        self.profiling_state_handler: Optional[ProfilingHandler] = None
        if self.config.enable_stateful_profiling:
            self.profiling_state_handler = ProfilingHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        # For profiling
        self.profiler: Optional[SnowflakeProfiler] = None
        if config.is_profiling_enabled():
            self.profiler = SnowflakeProfiler(
                config, self.report, self.profiling_state_handler
            )

        self.add_config_to_report()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Source":
        config = SnowflakeV2Config.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()

        try:
            connection_conf = SnowflakeConnectionConfig.parse_obj_allow_extras(
                config_dict
            )

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
        conn: SnowflakeConnection, connection_conf: SnowflakeConnectionConfig
    ) -> Dict[Union[SourceCapability, str], CapabilityReport]:
        # Currently only overall capabilities are reported.
        # Resource level variations in capabilities are not considered.

        @dataclass
        class SnowflakePrivilege:
            privilege: str
            object_name: str
            object_type: str

        _report: Dict[Union[SourceCapability, str], CapabilityReport] = dict()
        privileges: List[SnowflakePrivilege] = []
        capabilities: List[SourceCapability] = [c.capability for c in SnowflakeV2Source.get_capabilities() if c.capability not in (SourceCapability.PLATFORM_INSTANCE, SourceCapability.DOMAINS, SourceCapability.DELETION_DETECTION)]  # type: ignore

        cur = conn.query("select current_role()")
        current_role = [row["CURRENT_ROLE()"] for row in cur][0]

        cur = conn.query("select current_secondary_roles()")
        secondary_roles_str = json.loads(
            [row["CURRENT_SECONDARY_ROLES()"] for row in cur][0]
        )["roles"]
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
            cur = conn.query(f'show grants to role "{role}"')
            for row in cur:
                privilege = SnowflakePrivilege(
                    privilege=row["privilege"],
                    object_type=row["granted_on"],
                    object_name=row["name"],
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
                    _report[SourceCapability.CLASSIFICATION] = CapabilityReport(
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

        cur = conn.query("select current_warehouse()")
        current_warehouse = [row["CURRENT_WAREHOUSE()"] for row in cur][0]

        default_failure_messages = {
            SourceCapability.SCHEMA_METADATA: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.DESCRIPTIONS: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.DATA_PROFILING: "Either no tables exist or current role does not have permissions to access them",
            SourceCapability.CLASSIFICATION: "Either no tables exist or current role does not have permissions to access them",
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
                SourceCapability.CLASSIFICATION,
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        self._snowflake_clear_ocsp_cache()

        self.connection = self.config.get_connection()
        if self.connection is None:
            return

        self.inspect_session_metadata(self.connection)

        snowsight_url_builder = None
        if self.config.include_external_url:
            snowsight_url_builder = self.get_snowsight_url_builder()

        if self.report.default_warehouse is None:
            self.report_warehouse_failure()
            return

        schema_extractor = SnowflakeSchemaGenerator(
            config=self.config,
            report=self.report,
            connection=self.connection,
            domain_registry=self.domain_registry,
            profiler=self.profiler,
            aggregator=self.aggregator,
            snowsight_url_builder=snowsight_url_builder,
            filters=self.filters,
            identifiers=self.identifiers,
        )

        self.report.set_ingestion_stage("*", METADATA_EXTRACTION)
        yield from schema_extractor.get_workunits_internal()

        databases = schema_extractor.databases

        # TODO: The checkpoint state for stale entity detection can be committed here.

        if self.config.shares:
            yield from SnowflakeSharesHandler(
                self.config, self.report
            ).get_shares_workunits(databases)

        discovered_tables: List[str] = [
            self.identifiers.get_dataset_identifier(table_name, schema.name, db.name)
            for db in databases
            for schema in db.schemas
            for table_name in schema.tables
        ]
        discovered_views: List[str] = [
            self.identifiers.get_dataset_identifier(table_name, schema.name, db.name)
            for db in databases
            for schema in db.schemas
            for table_name in schema.views
        ]

        if len(discovered_tables) == 0 and len(discovered_views) == 0:
            self.structured_reporter.failure(
                GENERIC_PERMISSION_ERROR_KEY,
                "No tables/views found. Please check permissions.",
            )
            return

        discovered_datasets = discovered_tables + discovered_views

        if self.config.use_queries_v2:
            self.report.set_ingestion_stage("*", "View Parsing")
            assert self.aggregator is not None
            yield from auto_workunit(self.aggregator.gen_metadata())

            self.report.set_ingestion_stage("*", QUERIES_EXTRACTION)

            schema_resolver = self.aggregator._schema_resolver

            queries_extractor = SnowflakeQueriesExtractor(
                connection=self.connection,
                config=SnowflakeQueriesExtractorConfig(
                    window=self.config,
                    temporary_tables_pattern=self.config.temporary_tables_pattern,
                    include_lineage=self.config.include_table_lineage,
                    include_usage_statistics=self.config.include_usage_stats,
                    include_operations=self.config.include_operational_stats,
                ),
                structured_report=self.report,
                filters=self.filters,
                identifiers=self.identifiers,
                schema_resolver=schema_resolver,
            )

            # TODO: This is slightly suboptimal because we create two SqlParsingAggregator instances with different configs
            # but a shared schema resolver. That's fine for now though - once we remove the old lineage/usage extractors,
            # it should be pretty straightforward to refactor this and only initialize the aggregator once.
            self.report.queries_extractor = queries_extractor.report
            yield from queries_extractor.get_workunits_internal()

        else:
            if self.config.include_table_lineage and self.lineage_extractor:
                self.report.set_ingestion_stage("*", LINEAGE_EXTRACTION)
                yield from self.lineage_extractor.get_workunits(
                    discovered_tables=discovered_tables,
                    discovered_views=discovered_views,
                )

            if (
                self.config.include_usage_stats or self.config.include_operational_stats
            ) and self.usage_extractor:
                yield from self.usage_extractor.get_usage_workunits(discovered_datasets)

        if self.config.include_assertion_results:
            yield from SnowflakeAssertionsHandler(
                self.config, self.report, self.connection, self.identifiers
            ).get_assertion_workunits(discovered_datasets)

        self.connection.close()

    def report_warehouse_failure(self) -> None:
        if self.config.warehouse is not None:
            self.structured_reporter.failure(
                GENERIC_PERMISSION_ERROR_KEY,
                f"Current role does not have permissions to use warehouse {self.config.warehouse}. Please update permissions.",
            )
        else:
            self.structured_reporter.failure(
                "Could not use a Snowflake warehouse",
                "No default warehouse set for user. Either set a default warehouse for the user or configure a warehouse in the recipe.",
            )

    def get_report(self) -> SourceReport:
        return self.report

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

    def inspect_session_metadata(self, connection: SnowflakeConnection) -> None:
        try:
            logger.info("Checking current version")
            for db_row in connection.query(SnowflakeQuery.current_version()):
                self.report.saas_version = db_row["CURRENT_VERSION()"]
        except Exception as e:
            self.structured_reporter.failure(
                "Could not determine the current Snowflake version",
                exc=e,
            )
        try:
            logger.info("Checking current role")
            for db_row in connection.query(SnowflakeQuery.current_role()):
                self.report.role = db_row["CURRENT_ROLE()"]
        except Exception as e:
            self.structured_reporter.failure(
                "Could not determine the current Snowflake role",
                exc=e,
            )
        try:
            logger.info("Checking current warehouse")
            for db_row in connection.query(SnowflakeQuery.current_warehouse()):
                self.report.default_warehouse = db_row["CURRENT_WAREHOUSE()"]
        except Exception as e:
            self.structured_reporter.failure(
                "Could not determine the current Snowflake warehouse",
                exc=e,
            )

        try:
            logger.info("Checking current edition")
            if self.is_standard_edition():
                self.report.edition = SnowflakeEdition.STANDARD
            else:
                self.report.edition = SnowflakeEdition.ENTERPRISE
        except Exception:
            self.report.edition = None

    def get_snowsight_url_builder(self) -> Optional[SnowsightUrlBuilder]:
        try:
            # See https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#finding-the-region-and-locator-for-an-account
            for db_row in self.connection.query(SnowflakeQuery.current_account()):
                account_locator = db_row["CURRENT_ACCOUNT()"]

            for db_row in self.connection.query(SnowflakeQuery.current_region()):
                region = db_row["CURRENT_REGION()"]

            self.report.account_locator = account_locator
            self.report.region = region

            # Returned region may be in the form <region_group>.<region>, see https://docs.snowflake.com/en/sql-reference/functions/current_region.html
            region = region.split(".")[-1].lower()
            account_locator = account_locator.lower()

            return SnowsightUrlBuilder(
                account_locator,
                region,
                # For privatelink, account identifier ends with .privatelink
                # See https://docs.snowflake.com/en/user-guide/organizations-connect.html#private-connectivity-urls
                privatelink=self.config.account_id.endswith(".privatelink"),
            )

        except Exception as e:
            self.report.warning(
                title="External URL Generation Failed",
                message="We were unable to infer the Snowsight base URL for your Snowflake account. External URLs will not be generated.",
                exc=e,
            )
            return None

    def is_standard_edition(self) -> bool:
        try:
            self.connection.query(SnowflakeQuery.show_tags())
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
        if self.lineage_extractor:
            self.lineage_extractor.close()
        if self.usage_extractor:
            self.usage_extractor.close()
