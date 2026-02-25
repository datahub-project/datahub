import logging
import re
import urllib.parse
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple

import sqlalchemy.dialects.mssql
from pydantic import ValidationInfo, field_validator, model_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
    ResourceClosedError,
)
from sqlalchemy.sql import quoted_name

import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
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
from datahub.ingestion.api.source import StructuredLogLevel
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.sql.mssql.alias_filter import MSSQLAliasFilter
from datahub.ingestion.source.sql.mssql.job_models import (
    JobStep,
    MSSQLDataFlow,
    MSSQLDataJob,
    MSSQLJob,
    MSSQLProceduresContainer,
    ProcedureDependency,
    ProcedureLineageStream,
    ProcedureParameter,
    StoredProcedure,
)
from datahub.ingestion.source.sql.mssql.query_lineage_extractor import (
    MSSQLLineageExtractor,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    register_custom_type,
)
from datahub.ingestion.source.sql.sql_config import (
    BasicSQLAlchemyConfig,
)
from datahub.ingestion.source.sql.sql_report import SQLSourceReport
from datahub.ingestion.source.sql.sqlalchemy_uri import make_sqlalchemy_uri
from datahub.ingestion.source.sql.stored_procedures.base import (
    generate_procedure_lineage,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.utilities.file_backed_collections import FileBackedList
from datahub.utilities.perf_timer import PerfTimer

logger: logging.Logger = logging.getLogger(__name__)

# MSSQL uses 3-part naming: database.schema.table
MSSQL_QUALIFIED_NAME_PARTS = 3


def _validate_int_range(
    value: int, field_name: str, min_val: int, max_val: Optional[int] = None
) -> int:
    """
    Helper to validate integer is within acceptable range.

    Raises ValueError with descriptive message if validation fails.
    """
    if value < min_val:
        min_desc = (
            "positive"
            if min_val == 1
            else "non-negative"
            if min_val == 0
            else f">= {min_val}"
        )
        raise ValueError(
            f"{field_name} must be {min_desc}. Please set it to a value >= {min_val}."
        )

    if max_val is not None and value > max_val:
        raise ValueError(
            f"{field_name} must be <= {max_val} to avoid memory issues. "
            f"Please reduce the value to {max_val} or less."
        )

    return value


def _validate_string_list_limits(
    value: Optional[List[str]],
    field_name: str,
    max_count: int,
    max_item_length: int,
) -> Optional[List[str]]:
    """
    Helper to validate list of strings has reasonable limits.

    Raises ValueError with descriptive message if validation fails.
    """
    if value is None:
        return value

    if len(value) > max_count:
        raise ValueError(
            f"{field_name} cannot exceed {max_count} patterns (got {len(value)})"
        )

    for i, item in enumerate(value):
        if not item or not item.strip():
            raise ValueError(
                f"{field_name}: Pattern at index {i} is empty or whitespace-only"
            )

        if len(item) > max_item_length:
            raise ValueError(
                f"{field_name}: Pattern at index {i} exceeds {max_item_length} characters (got {len(item)})"
            )

    return value


register_custom_type(sqlalchemy.dialects.mssql.BIT, models.BooleanTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.MONEY, models.NumberTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SMALLMONEY, models.NumberTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SQL_VARIANT, models.UnionTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.UNIQUEIDENTIFIER, models.StringTypeClass)

# Patterns copied from Snowflake source
DEFAULT_TEMP_TABLES_PATTERNS = [
    r".*\.FIVETRAN_.*_STAGING\..*",  # fivetran
    r".*__DBT_TMP$",  # dbt (for databases without native temp tables; MSSQL uses #temp instead)
    rf".*\.SEGMENT_{UUID_REGEX}",  # segment
    rf".*\.STAGING_.*_{UUID_REGEX}",  # stitch
    r".*\.(GE_TMP_|GE_TEMP_|GX_TEMP_)[0-9A-F]{8}",  # great expectations
]


class SQLServerConfig(BasicSQLAlchemyConfig, BaseUsageConfig):
    host_port: str = Field(default="localhost:1433", description="MSSQL host URL.")
    scheme: HiddenFromDocs[str] = Field(default="mssql+pytds")

    # TODO: rename to include_procedures ?
    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures. Requires access to the 'sys' schema.",
    )
    include_stored_procedures_code: bool = Field(
        default=True, description="Include information about object code."
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for stored procedures to filter in ingestion."
        "Specify regex to match the entire procedure name in database.schema.procedure_name format. e.g. to match all procedures starting with customer in Customer database and public schema, use the regex 'Customer.public.customer.*'",
    )
    include_jobs: bool = Field(
        default=True,
        description="Include ingest of MSSQL Jobs. Requires access to the 'msdb' and 'sys' schema.",
    )
    include_descriptions: bool = Field(
        default=True, description="Include table descriptions information."
    )
    _use_odbc_removed = pydantic_removed_field("use_odbc")
    uri_args: Dict[str, str] = Field(
        default={},
        description="Arguments to URL-encode when connecting. See https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver15.",
    )
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    database: Optional[str] = Field(
        default=None,
        description="database (catalog). If set to Null, all databases will be considered for ingestion.",
    )
    convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Enable to convert the SQL Server assets urns to lowercase",
    )
    include_lineage: bool = Field(
        default=True,
        description="Enable lineage extraction for stored procedures",
    )
    include_containers_for_pipelines: bool = Field(
        default=False,
        description="Enable the container aspects ingestion for both pipelines and tasks. Note that this feature requires the corresponding model support in the backend, which was introduced in version 0.15.0.1.",
    )
    temporary_tables_pattern: List[str] = Field(
        default=DEFAULT_TEMP_TABLES_PATTERNS,
        description="[Advanced] Regex patterns for temporary tables to filter in lineage ingestion. Specify regex to "
        "match the entire table name in database.schema.table format. Defaults are to set in such a way "
        "to ignore the temporary staging tables created by known ETL tools.",
    )
    quote_schemas: bool = Field(
        default=False,
        description="Represent a schema identifiers combined with quoting preferences. See [sqlalchemy quoted_name docs](https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.quoted_name).",
    )
    is_aws_rds: Optional[bool] = Field(
        default=None,
        description="Indicates if the SQL Server instance is running on AWS RDS. When None (default), automatic detection will be attempted using server name analysis.",
    )

    include_query_lineage: bool = Field(
        default=False,
        description=(
            "Enable query-based lineage extraction from Query Store or DMVs. "
            "Query Store is preferred (SQL Server 2016+) and must be enabled on the database. "
            "Falls back to DMV-based extraction (sys.dm_exec_cached_plans) for older versions. "
            "Requires VIEW SERVER STATE permission. See documentation for setup instructions."
        ),
    )

    max_queries_to_extract: int = Field(
        default=1000,
        description=(
            "Maximum number of queries to extract for lineage analysis. "
            "Queries are prioritized by execution time and frequency."
        ),
    )

    min_query_calls: int = Field(
        default=1,
        description=(
            "Minimum number of executions required for a query to be included. "
            "Set higher to focus on frequently-used queries."
        ),
    )

    query_exclude_patterns: Optional[List[str]] = Field(
        default=None,
        description=(
            "SQL LIKE patterns to exclude from query extraction. "
            "Example: ['%sys.%', '%temp_%'] to exclude system and temp tables."
        ),
    )

    include_usage_statistics: bool = Field(
        default=False,
        description=(
            "Generate usage statistics from query history. Requires include_query_lineage to be enabled. "
            "Collects metrics like unique user counts, query frequencies, and column access patterns. "
            "Statistics appear in DataHub UI under the Dataset Profile > Usage tab."
        ),
    )

    @field_validator("uri_args", mode="after")
    @classmethod
    def validate_uri_args(
        cls, v: Dict[str, Any], info: ValidationInfo, **kwargs: Any
    ) -> Dict[str, Any]:
        is_odbc = info.context.get("is_odbc", False) if info.context else False
        if is_odbc and not info.data["sqlalchemy_uri"] and "driver" not in v:
            raise ValueError(
                "uri_args must contain a 'driver' option when using mssql-odbc source type"
            )
        elif not is_odbc and v:
            raise ValueError(
                "uri_args is not supported for source type 'mssql'. Use source type 'mssql-odbc' instead."
            )
        return v

    @field_validator("max_queries_to_extract")
    @classmethod
    def validate_max_queries_to_extract(cls, value: int) -> int:
        """Validate max_queries_to_extract is within reasonable range."""
        return _validate_int_range(
            value, "max_queries_to_extract", min_val=1, max_val=10000
        )

    @field_validator("min_query_calls")
    @classmethod
    def validate_min_query_calls(cls, value: int) -> int:
        """Validate min_query_calls is non-negative."""
        return _validate_int_range(value, "min_query_calls", min_val=0)

    @field_validator("query_exclude_patterns")
    @classmethod
    def validate_query_exclude_patterns(
        cls, value: Optional[List[str]]
    ) -> Optional[List[str]]:
        """Validate query_exclude_patterns has reasonable limits."""
        return _validate_string_list_limits(
            value, "query_exclude_patterns", max_count=100, max_item_length=500
        )

    @model_validator(mode="after")
    def validate_usage_statistics_dependency(self) -> "SQLServerConfig":
        """Validate that include_usage_statistics requires include_query_lineage."""
        if self.include_usage_statistics and not self.include_query_lineage:
            raise ValueError(
                "include_usage_statistics requires include_query_lineage to be enabled. "
                "Please add 'include_query_lineage: true' to your configuration."
            )
        return self

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
        is_odbc: bool = False,
    ) -> str:
        current_db = current_db or self.database

        scheme = self.scheme
        if is_odbc:
            scheme = "mssql+pyodbc"

            # ODBC requires a database name, otherwise it will interpret host_port
            # as a pre-defined ODBC connection name.
            current_db = current_db or "master"

        uri: str = self.sqlalchemy_uri or make_sqlalchemy_uri(
            scheme,  # type: ignore
            self.username,
            self.password.get_secret_value() if self.password else None,
            self.host_port,  # type: ignore
            current_db,
            uri_opts=uri_opts,
        )
        if is_odbc:
            final_uri_args = self.uri_args.copy()
            if final_uri_args and current_db:
                final_uri_args.update({"database": current_db})

            uri = (
                f"{uri}?{urllib.parse.urlencode(final_uri_args)}"
                if final_uri_args
                else uri
            )
        return uri

    @property
    def db(self):
        return self.database


@platform_name("Microsoft SQL Server", id="mssql")
@config_class(SQLServerConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default to get lineage for stored procedures via `include_lineage` and for views via `include_view_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.STORED_PROCEDURE,
        SourceCapabilityModifier.VIEW,
    ],
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Enabled by default to get lineage for stored procedures via `include_lineage` and for views via `include_view_column_lineage`",
    subtype_modifier=[
        SourceCapabilityModifier.STORED_PROCEDURE,
        SourceCapabilityModifier.VIEW,
    ],
)
class SQLServerSource(SQLAlchemySource):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, views and tables
    - Column types associated with each table/view
    - Table, row, and column statistics via optional SQL profiling

    Two source types are available:
    - `mssql`: Uses [python-tds](https://github.com/denisenkom/pytds) library (pure Python, easier to install)
    - `mssql-odbc`: Uses [pyodbc](https://github.com/mkleehammer/pyodbc) library (required for encryption, Azure managed services)

    If you need encryption (e.g., for Azure SQL), use source type `mssql-odbc` and configure `uri_args` with your ODBC driver settings.
    """

    report: SQLSourceReport

    def __init__(
        self, config: SQLServerConfig, ctx: PipelineContext, is_odbc: bool = False
    ):
        super().__init__(config, ctx, "mssql")
        self.config: SQLServerConfig = config
        self._is_odbc = is_odbc
        self.current_database = None
        self.table_descriptions: Dict[str, str] = {}
        self.column_descriptions: Dict[str, str] = {}
        self.stored_procedures: FileBackedList[StoredProcedure] = FileBackedList()
        self.tsql_alias_cleaner: Optional[MSSQLAliasFilter] = None
        self._discovered_table_cache: Dict[str, bool] = {}

        self.report = SQLSourceReport()
        if self.config.include_lineage and not self.config.convert_urns_to_lowercase:
            self.report.warning(
                title="Potential issue with lineage",
                message="Lineage may not resolve accurately because 'convert_urns_to_lowercase' is False. To ensure lineage correct, set 'convert_urns_to_lowercase' to True.",
            )

        self.sql_aggregator: Optional[SqlParsingAggregator] = None
        self.lineage_extractor: Optional[MSSQLLineageExtractor] = None
        if self.config.include_query_lineage:
            if self.config.include_usage_statistics and self.ctx.graph is None:
                raise ValueError(
                    "Usage statistics generation requires a DataHub graph connection (ctx.graph). "
                    "You have enabled 'include_usage_statistics: true' but no graph connection is available. "
                    "Please provide a graph connection in your pipeline configuration or disable usage statistics."
                )

            self.sql_aggregator = SqlParsingAggregator(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                graph=self.ctx.graph,
                generate_lineage=True,
                generate_queries=True,
                generate_usage_statistics=self.config.include_usage_statistics,
                usage_config=self.config
                if self.config.include_usage_statistics
                else None,
            )
            logger.info("SQL parsing aggregator initialized for query-based lineage")

        if self.config.include_descriptions:
            for inspector in self.get_inspectors():
                db_name: str = self.get_db_name(inspector)
                with inspector.engine.connect() as conn:
                    if self._is_odbc:
                        self._add_output_converters(conn)
                    self._populate_table_descriptions(conn, db_name)
                    self._populate_column_descriptions(conn, db_name)

    @staticmethod
    def _add_output_converters(conn: Connection) -> None:
        def handle_sql_variant_as_string(value):
            try:
                return value.decode("utf-16le")
            except UnicodeDecodeError:
                return value.decode("Windows-1251")

        # see https://stackoverflow.com/questions/45677374/pandas-pyodbc-odbc-sql-type-150-is-not-yet-supported
        # and https://stackoverflow.com/questions/11671170/adding-output-converter-to-pyodbc-connection-in-sqlalchemy
        try:
            conn.connection.add_output_converter(-150, handle_sql_variant_as_string)
        except AttributeError as e:
            logger.debug(
                "Failed to mount output converter for MSSQL data type -150 due to %s",
                e,
            )

    def _populate_table_descriptions(self, conn: Connection, db_name: str) -> None:
        # see https://stackoverflow.com/questions/5953330/how-do-i-map-the-id-in-sys-extended-properties-to-an-object-name
        # also see https://www.mssqltips.com/sqlservertip/5384/working-with-sql-server-extended-properties/
        table_metadata = conn.execute(
            """
            SELECT
              SCHEMA_NAME(T.SCHEMA_ID) AS schema_name,
              T.NAME AS table_name,
              EP.VALUE AS table_description
            FROM sys.tables AS T
            INNER JOIN sys.extended_properties AS EP
              ON EP.MAJOR_ID = T.[OBJECT_ID]
              AND EP.MINOR_ID = 0
              AND EP.NAME = 'MS_Description'
              AND EP.CLASS = 1
            """
        )
        for row in table_metadata:
            self.table_descriptions[
                f"{db_name}.{row['schema_name']}.{row['table_name']}"
            ] = row["table_description"]

    def _populate_column_descriptions(self, conn: Connection, db_name: str) -> None:
        column_metadata = conn.execute(
            """
            SELECT
              SCHEMA_NAME(T.SCHEMA_ID) AS schema_name,
              T.NAME AS table_name,
              C.NAME AS column_name ,
              EP.VALUE AS column_description
            FROM sys.tables AS T
            INNER JOIN sys.all_columns AS C
              ON C.OBJECT_ID = T.[OBJECT_ID]
            INNER JOIN sys.extended_properties AS EP
              ON EP.MAJOR_ID = T.[OBJECT_ID]
              AND EP.MINOR_ID = C.COLUMN_ID
              AND EP.NAME = 'MS_Description'
              AND EP.CLASS = 1
            """
        )
        for row in column_metadata:
            self.column_descriptions[
                f"{db_name}.{row['schema_name']}.{row['table_name']}.{row['column_name']}"
            ] = row["column_description"]

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "SQLServerSource":
        # Determine ODBC mode based on source type.
        # Use 'mssql-odbc' for ODBC connections, 'mssql' for non-ODBC (pytds) connections.
        source_type = getattr(
            getattr(ctx.pipeline_config, "source", None), "type", None
        )
        is_odbc = source_type == "mssql-odbc"

        config = SQLServerConfig.model_validate(
            config_dict, context={"is_odbc": is_odbc}
        )
        return cls(config, ctx, is_odbc=is_odbc)

    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description, properties, location_urn = super().get_table_properties(
            inspector, schema, table
        )
        db_name: str = self.get_db_name(inspector)
        description = self.table_descriptions.get(
            f"{db_name}.{schema}.{table}", description
        )
        return description, properties, location_urn

    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[Dict]:
        columns: List[Dict] = super()._get_columns(
            dataset_name, inspector, schema, table
        )
        db_name: str = self.get_db_name(inspector)
        for column in columns:
            description: Optional[str] = self.column_descriptions.get(
                f"{db_name}.{schema}.{table}.{column['name']}",
            )
            if description:
                column["comment"] = description
        return columns

    def get_database_level_workunits(
        self,
        inspector: Inspector,
        database: str,
    ) -> Iterable[MetadataWorkUnit]:
        yield from super().get_database_level_workunits(
            inspector=inspector,
            database=database,
        )
        if self.config.include_jobs:
            try:
                yield from self.loop_jobs(inspector, self.config)
            except Exception as e:
                self.report.failure(
                    message="Failed to list jobs",
                    title="SQL Server Jobs Extraction",
                    context="Error occurred during database-level job extraction",
                    exc=e,
                )

    def _detect_rds_environment(self, conn: Connection) -> bool:
        """
        Detect if running on AWS RDS vs on-premises SQL Server.

        RDS restricts msdb table access; on-prem allows faster direct queries.
        Detection errors fall back to on-prem mode with automatic retry logic.

        IMPORTANT: For production use, set is_aws_rds explicitly in config to avoid
        heuristic detection failures. This method uses best-effort pattern matching.
        """
        if self.config.is_aws_rds is not None:
            logger.info(
                "Using explicit is_aws_rds configuration: %s", self.config.is_aws_rds
            )
            return self.config.is_aws_rds

        try:
            result = conn.execute("SELECT @@servername AS server_name")
            server_name_row = result.fetchone()
            if server_name_row:
                server_name = server_name_row["server_name"].lower()

                # Primary patterns: Official RDS/AWS domain suffixes (high confidence)
                high_confidence_patterns = [
                    ".rds.amazonaws.com",  # Official RDS endpoint: mydb.abc123.us-east-1.rds.amazonaws.com
                    ".rds.amazonaws.com.cn",  # China region RDS
                ]

                # Secondary patterns: Common AWS naming (medium confidence)
                medium_confidence_patterns = [
                    "-rds-",  # Custom names with RDS marker: mycompany-rds-prod
                    ".ec2-",  # EC2 instance pattern: ip-10-0-0-1.ec2-internal
                ]

                # Tertiary patterns: Prefix-based (low confidence, only match at start)
                # Uses startswith() to avoid false positives like "amazing_server"
                prefix_patterns = [
                    "amazon-",  # Amazon-prefixed: amazon-rds-prod (NOT "amazing")
                    "amzn-",  # AWS standard prefix: amzn-sqlserver-001
                    "aws-",  # AWS-prefixed: aws-rds-db
                ]

                # Check in order of confidence
                is_rds = any(
                    pattern in server_name for pattern in high_confidence_patterns
                )

                if not is_rds:
                    is_rds = any(
                        pattern in server_name for pattern in medium_confidence_patterns
                    )

                if not is_rds:
                    is_rds = any(
                        server_name.startswith(pattern) for pattern in prefix_patterns
                    )

                if is_rds:
                    logger.info(
                        "AWS RDS detected based on server name pattern: %s", server_name
                    )
                else:
                    logger.info(
                        "Non-RDS environment detected (server name: %s). "
                        "If this is incorrect, set 'is_aws_rds: true' in config.",
                        server_name,
                    )

                return is_rds
            else:
                logger.warning(
                    "Could not retrieve server name, assuming non-RDS. "
                    "Set 'is_aws_rds' explicitly in config if needed."
                )
                return False

        except Exception as e:
            logger.warning(
                "Failed to detect RDS environment (error: %s), assuming non-RDS. "
                "Set 'is_aws_rds' explicitly in config to avoid detection issues.",
                e,
            )
            return False

    def _get_jobs(self, conn: Connection, db_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Get job information with environment detection to choose optimal method first.
        """
        is_rds = self._detect_rds_environment(conn)
        methods = (
            [self._get_jobs_via_stored_procedures, self._get_jobs_via_direct_query]
            if is_rds
            else [self._get_jobs_via_direct_query, self._get_jobs_via_stored_procedures]
        )

        return self._get_jobs_with_fallback(conn, db_name, methods, is_rds)

    def _get_jobs_with_fallback(
        self,
        conn: Connection,
        db_name: str,
        methods: List[Callable],
        is_rds: bool,
    ) -> Dict[str, Dict[str, Any]]:
        """Try each method in order, handle all exception types once."""
        context_suffix = "managed" if is_rds else "on_prem"
        env_desc = "managed environment" if is_rds else "on-premises environment"
        last_exception: Optional[Exception] = None

        for method in methods:
            method_name = getattr(
                method, "__name__", getattr(method, "_mock_name", str(method))
            )
            try:
                jobs = method(conn, db_name)
                logger.info("Retrieved jobs using %s (%s)", method_name, env_desc)
                return jobs
            except (DatabaseError, OperationalError, ProgrammingError) as e:
                logger.warning("%s failed: %s", method_name, e)
                last_exception = e
            except (KeyError, TypeError) as e:
                logger.error("Job structure error %s: %s", method_name, e)
                last_exception = e
                self.report.failure(
                    message=f"Job structure error: {e}",
                    title="SQL Server Jobs Extraction",
                    context=f"job_structure_error_{context_suffix}",
                    exc=e,
                )
            except Exception as e:
                logger.error("Unexpected error %s: %s", method_name, e, exc_info=True)
                last_exception = e
                self.report.failure(
                    message=f"Unexpected error: {e}",
                    title="SQL Server Jobs Extraction",
                    context=f"job_extraction_error_{context_suffix}",
                    exc=e,
                )

        if is_rds:
            self.report.failure(
                message="Failed to retrieve jobs in managed environment (both stored procedures and direct query). "
                "This is expected on AWS RDS and some managed SQL instances that restrict msdb access. "
                "Jobs extraction will be skipped.",
                title="SQL Server Jobs Extraction",
                context="managed_environment_msdb_restricted",
                exc=last_exception,
            )
        else:
            self.report.failure(
                message="Failed to retrieve jobs in on-premises environment (both direct query and stored procedures). "
                "Verify the DataHub user has SELECT permissions on msdb.dbo.sysjobs and msdb.dbo.sysjobsteps, "
                "or EXECUTE permissions on sp_help_job and sp_help_jobstep.",
                title="SQL Server Jobs Extraction",
                context="on_prem_msdb_permission_denied",
                exc=last_exception,
            )

        logger.error(
            "All job retrieval methods failed for %s",
            "RDS/managed" if is_rds else "on-premises",
        )
        return {}

    def _get_jobs_via_stored_procedures(
        self, conn: Connection, db_name: str
    ) -> Dict[str, Dict[str, Any]]:
        jobs: Dict[str, Dict[str, Any]] = {}

        jobs_result = conn.execute("EXEC msdb.dbo.sp_help_job")
        jobs_data = {}

        for row in jobs_result.mappings():
            job_id = str(row["job_id"])
            jobs_data[job_id] = {
                "job_id": job_id,
                "name": row["name"],
                "description": row.get("description", ""),
                "date_created": row.get("date_created"),
                "date_modified": row.get("date_modified"),
                "enabled": row.get("enabled", 1),
            }

        for job_id, job_info in jobs_data.items():
            try:
                # job_id from sp_help_job output (database metadata)
                steps_result = conn.execute(
                    text("EXEC msdb.dbo.sp_help_jobstep @job_id = :job_id"),
                    {"job_id": job_id},
                )

                job_steps = {}
                for step_row in steps_result.mappings():
                    step_database = step_row.get("database_name", "")
                    if step_database.lower() == db_name.lower() or not step_database:
                        step_data = {
                            "job_id": job_id,
                            "job_name": job_info["name"],
                            "description": job_info["description"],
                            "date_created": job_info["date_created"],
                            "date_modified": job_info["date_modified"],
                            "step_id": step_row["step_id"],
                            "step_name": step_row["step_name"],
                            "subsystem": step_row.get("subsystem", ""),
                            "command": step_row.get("command", ""),
                            "database_name": step_database,
                        }
                        job_steps[step_row["step_id"]] = step_data

                if job_steps:
                    jobs[job_info["name"]] = job_steps

            except (DatabaseError, OperationalError, ProgrammingError) as step_error:
                logger.warning(
                    "Database error retrieving steps for job %s (job may be inaccessible or deleted): %s",
                    job_info["name"],
                    step_error,
                )
                continue
            except (KeyError, TypeError, AttributeError) as struct_error:
                logger.warning(
                    "Data structure error processing steps for job %s: %s. Job will be skipped.",
                    job_info["name"],
                    struct_error,
                )
                continue
            except Exception as unexpected_error:
                logger.error(
                    "Unexpected error retrieving steps for job %s: %s (%s). Job will be skipped.",
                    job_info["name"],
                    unexpected_error,
                    type(unexpected_error).__name__,
                    exc_info=True,
                )
                continue

        return jobs

    def _get_jobs_via_direct_query(
        self, conn: Connection, db_name: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Original method using direct table access for on-premises SQL Server.
        """
        jobs_data = conn.execute(
            f"""
            SELECT
                job.job_id,
                job.name,
                job.description,
                job.date_created,
                job.date_modified,
                steps.step_id,
                steps.step_name,
                steps.subsystem,
                steps.command,
                steps.database_name
            FROM
                msdb.dbo.sysjobs job
            INNER JOIN
                msdb.dbo.sysjobsteps steps
            ON
                job.job_id = steps.job_id
            where database_name = '{db_name}'
            """
        )

        jobs: Dict[str, Dict[str, Any]] = {}
        for row in jobs_data:
            step_data = dict(
                job_id=row["job_id"],
                job_name=row["name"],
                description=row["description"],
                date_created=row["date_created"],
                date_modified=row["date_modified"],
                step_id=row["step_id"],
                step_name=row["step_name"],
                subsystem=row["subsystem"],
                command=row["command"],
                database_name=row["database_name"],
            )
            if row["name"] in jobs:
                jobs[row["name"]][row["step_id"]] = step_data
            else:
                jobs[row["name"]] = {row["step_id"]: step_data}

        return jobs

    def loop_jobs(
        self,
        inspector: Inspector,
        sql_config: SQLServerConfig,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Loop MS SQL jobs as dataFlow-s.
        Now supports both managed and on-premises SQL Server.
        """
        db_name = self.get_db_name(inspector)

        try:
            with inspector.engine.connect() as conn:
                jobs = self._get_jobs(conn, db_name)

                if not jobs:
                    logger.info("No jobs found for database: %s", db_name)
                    return

                logger.info("Found %d jobs for database: %s", len(jobs), db_name)

                for job_name, job_steps in jobs.items():
                    try:
                        job = MSSQLJob(
                            name=job_name,
                            env=sql_config.env,
                            db=db_name,
                            platform_instance=sql_config.platform_instance,
                        )
                        data_flow = MSSQLDataFlow(entity=job)
                        yield from self.construct_flow_workunits(data_flow=data_flow)
                        yield from self.loop_job_steps(job, job_steps)

                    except Exception as job_error:
                        logger.warning(
                            "Failed to process job %s: %s", job_name, job_error
                        )
                        self.report.warning(
                            message=f"Failed to process job {job_name}",
                            title="SQL Server Jobs Extraction",
                            context="Error occurred while processing individual job",
                            exc=job_error,
                        )
                        continue

        except Exception as e:
            error_message = f"Failed to retrieve jobs for database {db_name}: {e}"
            logger.error(error_message)

            # Provide specific guidance for permission issues
            if "permission" in str(e).lower() or "denied" in str(e).lower():
                permission_guidance = (
                    "For managed SQL Server services, ensure the following permissions are granted:\n"
                    "GRANT EXECUTE ON msdb.dbo.sp_help_job TO datahub_read;\n"
                    "GRANT EXECUTE ON msdb.dbo.sp_help_jobstep TO datahub_read;\n"
                    "For on-premises SQL Server, you may also need:\n"
                    "GRANT SELECT ON msdb.dbo.sysjobs TO datahub_read;\n"
                    "GRANT SELECT ON msdb.dbo.sysjobsteps TO datahub_read;"
                )
                logger.info(permission_guidance)

            raise e

    def loop_job_steps(
        self, job: MSSQLJob, job_steps: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        for _step_id, step_data in job_steps.items():
            step = JobStep(
                job_name=job.formatted_name,
                step_name=step_data["step_name"],
                flow=job,
            )
            data_job = MSSQLDataJob(entity=step)
            for data_name, data_value in step_data.items():
                data_job.add_property(name=data_name, value=str(data_value))
            yield from self.construct_job_workunits(data_job)

    def loop_stored_procedures(
        self,
        inspector: Inspector,
        schema: str,
        sql_config: SQLServerConfig,  # type: ignore
    ) -> Iterable[MetadataWorkUnit]:
        """
        Loop schema data for get stored procedures as dataJob-s.
        """
        db_name = self.get_db_name(inspector)
        procedure_flow_name = f"{db_name}.{schema}.stored_procedures"
        mssql_default_job = MSSQLProceduresContainer(
            name=procedure_flow_name,
            env=sql_config.env,
            db=db_name,
            platform_instance=sql_config.platform_instance,
        )
        data_flow = MSSQLDataFlow(entity=mssql_default_job)
        with inspector.engine.connect() as conn:
            procedures_data_list = self._get_stored_procedures(conn, db_name, schema)
            procedures: List[StoredProcedure] = []
            for procedure_data in procedures_data_list:
                procedure_full_name = f"{db_name}.{schema}.{procedure_data['name']}"
                if not self.config.procedure_pattern.allowed(procedure_full_name):
                    self.report.report_dropped(procedure_full_name)
                    continue
                procedures.append(
                    StoredProcedure(flow=mssql_default_job, **procedure_data)
                )

            if procedures:
                yield from self.construct_flow_workunits(data_flow=data_flow)
            for procedure in procedures:
                yield from self._process_stored_procedure(conn, procedure)

    def _process_stored_procedure(
        self, conn: Connection, procedure: StoredProcedure
    ) -> Iterable[MetadataWorkUnit]:
        upstream = self._get_procedure_upstream(conn, procedure)
        downstream = self._get_procedure_downstream(conn, procedure)
        data_job = MSSQLDataJob(
            entity=procedure,
        )
        # TODO: because of this upstream and downstream are more dependencies,
        #  can't be used as DataJobInputOutput.
        #  Should be reorganized into lineage.
        data_job.add_property("procedure_depends_on", str(upstream.as_property))
        data_job.add_property("depending_on_procedure", str(downstream.as_property))
        procedure_definition, procedure_code = self._get_procedure_code(conn, procedure)
        procedure.code = procedure_code
        if procedure_definition:
            data_job.add_property("definition", procedure_definition)
        if procedure_code and self.config.include_stored_procedures_code:
            data_job.add_property("code", procedure_code)
        procedure_inputs = self._get_procedure_inputs(conn, procedure)
        properties = self._get_procedure_properties(conn, procedure)
        data_job.add_property(
            "input parameters", str([param.name for param in procedure_inputs])
        )
        for param in procedure_inputs:
            data_job.add_property(f"parameter {param.name}", str(param.properties))
        for property_name, property_value in properties.items():
            data_job.add_property(property_name, str(property_value))
        if self.config.include_lineage:
            self.stored_procedures.append(procedure)
        yield from self.construct_job_workunits(
            data_job,
            include_lineage=False,
        )

    @staticmethod
    def _get_procedure_downstream(
        conn: Connection, procedure: StoredProcedure
    ) -> ProcedureLineageStream:
        downstream_data = conn.execute(
            f"""
            SELECT DISTINCT OBJECT_SCHEMA_NAME ( referencing_id ) AS [schema],
                OBJECT_NAME(referencing_id) AS [name],
                o.type_desc AS [type]
            FROM sys.sql_expression_dependencies AS sed
            INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
            left join sys.objects o1 on sed.referenced_id = o1.object_id
            WHERE referenced_id = OBJECT_ID(N'{procedure.escape_full_name}')
                AND o.type_desc in ('TABLE_TYPE', 'VIEW', 'USER_TABLE')
            """
        )
        downstream_dependencies = []
        for row in downstream_data:
            downstream_dependencies.append(
                ProcedureDependency(
                    db=procedure.db,
                    schema=row["schema"],
                    name=row["name"],
                    type=row["type"],
                    env=procedure.flow.env,
                    server=procedure.flow.platform_instance,
                )
            )
        return ProcedureLineageStream(dependencies=downstream_dependencies)

    @staticmethod
    def _get_procedure_upstream(
        conn: Connection, procedure: StoredProcedure
    ) -> ProcedureLineageStream:
        upstream_data = conn.execute(
            f"""
            SELECT DISTINCT
                coalesce(lower(referenced_database_name), db_name()) AS db,
                referenced_schema_name AS [schema],
                referenced_entity_name AS [name],
                o1.type_desc AS [type]
            FROM sys.sql_expression_dependencies AS sed
            INNER JOIN sys.objects AS o ON sed.referencing_id = o.object_id
            left join sys.objects o1 on sed.referenced_id = o1.object_id
            WHERE referencing_id = OBJECT_ID(N'{procedure.escape_full_name}')
                AND referenced_schema_name is not null
                AND o1.type_desc in ('TABLE_TYPE', 'VIEW', 'SQL_STORED_PROCEDURE', 'USER_TABLE')
            """
        )
        upstream_dependencies = []
        for row in upstream_data:
            upstream_dependencies.append(
                ProcedureDependency(
                    db=row["db"],
                    schema=row["schema"],
                    name=row["name"],
                    type=row["type"],
                    env=procedure.flow.env,
                    server=procedure.flow.platform_instance,
                )
            )
        return ProcedureLineageStream(dependencies=upstream_dependencies)

    @staticmethod
    def _get_procedure_inputs(
        conn: Connection, procedure: StoredProcedure
    ) -> List[ProcedureParameter]:
        inputs_data = conn.execute(
            f"""
            SELECT
                name,
                type_name(user_type_id) AS 'type'
            FROM sys.parameters
            WHERE object_id = object_id('{procedure.escape_full_name}')
            """
        )
        inputs_list = []
        for row in inputs_data:
            inputs_list.append(ProcedureParameter(name=row["name"], type=row["type"]))
        return inputs_list

    @staticmethod
    def _get_procedure_code(
        conn: Connection, procedure: StoredProcedure
    ) -> Tuple[Optional[str], Optional[str]]:
        # procedure.db and escape_full_name from sys.procedures (database metadata)
        query = (
            "EXEC ["
            + procedure.db
            + "].dbo.sp_helptext '"
            + procedure.escape_full_name
            + "'"
        )
        try:
            code_data = conn.execute(query)
        except ProgrammingError:
            logger.warning(
                "Denied permission for read text from procedure '%s'",
                procedure.full_name,
            )
            return None, None
        code_list = []
        code_slice_index = 0
        code_slice_text = "create procedure"
        try:
            for index, row in enumerate(code_data):
                code_list.append(row["Text"])
                if code_slice_text in re.sub(" +", " ", row["Text"].lower()).strip():
                    code_slice_index = index
            definition = "".join(code_list[:code_slice_index])
            code = "".join(code_list[code_slice_index:])

        except ResourceClosedError:
            logger.warning(
                "Connection was closed from procedure '%s'",
                procedure.full_name,
            )
            return None, None
        return definition, code

    @staticmethod
    def _get_procedure_properties(
        conn: Connection, procedure: StoredProcedure
    ) -> Dict[str, Any]:
        properties_data = conn.execute(
            f"""
            SELECT
                create_date as date_created,
                modify_date as date_modified
            FROM sys.procedures
            WHERE object_id = object_id('{procedure.escape_full_name}')
            """
        )
        properties = {}
        for row in properties_data:
            properties = dict(
                date_created=row["date_created"], date_modified=row["date_modified"]
            )
        return properties

    @staticmethod
    def _get_stored_procedures(
        conn: Connection, db_name: str, schema: str
    ) -> List[Dict[str, str]]:
        stored_procedures_data = conn.execute(
            f"""
            SELECT
                pr.name as procedure_name,
                s.name as schema_name
            FROM
                [{db_name}].[sys].[procedures] pr
            INNER JOIN
                [{db_name}].[sys].[schemas] s ON pr.schema_id = s.schema_id
            where s.name = '{schema}'
            """
        )
        procedures_list = []
        for row in stored_procedures_data:
            procedures_list.append(
                dict(db=db_name, schema=row["schema_name"], name=row["procedure_name"])
            )
        return procedures_list

    def construct_job_workunits(
        self,
        data_job: MSSQLDataJob,
        include_lineage: bool = True,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.as_datajob_info_aspect,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.as_subtypes_aspect,
        ).as_workunit()

        data_platform_instance_aspect = data_job.as_maybe_platform_instance_aspect
        if data_platform_instance_aspect:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=data_platform_instance_aspect,
            ).as_workunit()

        if self.config.include_containers_for_pipelines:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=data_job.as_container_aspect,
            ).as_workunit()

        if include_lineage:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=data_job.as_datajob_input_output_aspect,
            ).as_workunit()

        if (
            self.config.include_stored_procedures_code
            and isinstance(data_job.entity, StoredProcedure)
            and data_job.entity.code is not None
        ):
            yield MetadataChangeProposalWrapper(
                entityUrn=data_job.urn,
                aspect=models.DataTransformLogicClass(
                    transforms=[
                        models.DataTransformClass(
                            queryStatement=models.QueryStatementClass(
                                value=data_job.entity.code,
                                language=models.QueryLanguageClass.SQL,
                            ),
                        )
                    ]
                ),
            ).as_workunit()

    def construct_flow_workunits(
        self,
        data_flow: MSSQLDataFlow,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_flow.urn,
            aspect=data_flow.as_dataflow_info_aspect,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=data_flow.urn,
            aspect=data_flow.as_subtypes_aspect,
        ).as_workunit()

        data_platform_instance_aspect = data_flow.as_maybe_platform_instance_aspect
        if data_platform_instance_aspect:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_flow.urn,
                aspect=data_platform_instance_aspect,
            ).as_workunit()

        if self.config.include_containers_for_pipelines:
            yield MetadataChangeProposalWrapper(
                entityUrn=data_flow.urn,
                aspect=data_flow.as_container_aspect,
            ).as_workunit()

    def get_inspectors(self) -> Iterable[Inspector]:
        # This method can be overridden in the case that you want to dynamically
        # run on multiple databases.
        url = self.config.get_sql_alchemy_url(is_odbc=self._is_odbc)
        logger.debug("sql_alchemy_url=%s", url)
        engine = create_engine(url, **self.config.options)

        if (
            self.config.database
            and self.config.database != ""
            or (self.config.sqlalchemy_uri and self.config.sqlalchemy_uri != "")
        ):
            inspector = inspect(engine)
            yield inspector
        else:
            with engine.begin() as conn:
                databases = conn.execute(
                    "SELECT name FROM master.sys.databases WHERE name NOT IN \
                  ('master', 'model', 'msdb', 'tempdb', 'Resource', \
                       'distribution' , 'reportserver', 'reportservertempdb'); "
                ).fetchall()

            for db in databases:
                if self.config.database_pattern.allowed(db["name"]):
                    url = self.config.get_sql_alchemy_url(
                        current_db=db["name"], is_odbc=self._is_odbc
                    )
                    engine = create_engine(url, **self.config.options)
                    inspector = inspect(engine)
                    self.current_database = db["name"]
                    yield inspector

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        regular = f"{schema}.{entity}"
        qualified_table_name = regular
        if self.config.database:
            qualified_table_name = f"{self.config.database}.{regular}"
        if self.current_database:
            qualified_table_name = f"{self.current_database}.{regular}"
        return (
            qualified_table_name.lower()
            if self.config.convert_urns_to_lowercase
            else qualified_table_name
        )

    def _filter_procedure_lineage(
        self,
        mcps: Iterable[MetadataChangeProposalWrapper],
        procedure_name: Optional[str] = None,
    ) -> Iterator[MetadataChangeProposalWrapper]:
        """
        Filter out unqualified table URNs from stored procedure lineage.

        Delegates to MSSQLAliasFilter to handle TSQL-specific alias quirks.
        See alias_filter.py module docstring for detailed explanation.
        """
        if self.tsql_alias_cleaner is None:
            platform_instance = self.get_schema_resolver().platform_instance
            self.tsql_alias_cleaner = MSSQLAliasFilter(
                is_discovered_table=self.is_discovered_table,
                platform_instance=platform_instance,
            )

        yield from self.tsql_alias_cleaner.filter_procedure_lineage(
            mcps, procedure_name
        )

    def _get_query_based_lineage_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract and emit query-based lineage from Query Store or DMVs.

        This supplements view and stored procedure lineage with lineage extracted
        from executed queries (INSERT INTO SELECT, CTAS, etc.).
        """
        logger.info(
            "Starting query-based lineage extraction from SQL Server query history"
        )

        for inspector in self.get_inspectors():
            if self.sql_aggregator is None:
                logger.warning(
                    "SQL aggregator not initialized, skipping query-based lineage extraction. "
                    "Check initialization errors above."
                )
                self.report.report_warning(
                    message=(
                        "Query-based lineage was enabled but SQL aggregator failed to initialize. "
                        "No query-based lineage will be extracted. Check earlier error messages."
                    ),
                    context="query_lineage_skipped",
                )
                return

            with inspector.engine.connect() as connection:
                self.lineage_extractor = MSSQLLineageExtractor(
                    config=self.config,
                    connection=connection,
                    report=self.report,
                    sql_aggregator=self.sql_aggregator,
                    default_schema="dbo",
                )

                try:
                    self.lineage_extractor.populate_lineage_from_queries()
                except Exception as e:
                    logger.error(
                        "Unexpected error during query lineage extraction: %s. "
                        "Continuing with other lineage sources.",
                        e,
                    )
                    self.report.report_failure(
                        message=(
                            f"Query lineage extraction failed with unexpected error: {e}. "
                            "Check that Query Store is enabled or VIEW SERVER STATE permission is granted. "
                            "See documentation for setup instructions: "
                            "https://datahubproject.io/docs/generated/ingestion/sources/mssql"
                        ),
                        context="query_lineage_extraction_failed",
                    )

        with PerfTimer() as timer:
            mcp_count = 0
            if self.sql_aggregator:
                try:
                    mcp: MetadataChangeProposalWrapper
                    for mcp in self.sql_aggregator.gen_metadata():
                        yield mcp.as_workunit()
                        mcp_count += 1
                except Exception as e:
                    logger.error(
                        "Failed to generate metadata from SQL aggregator: %s",
                        e,
                    )
                    self.report.report_failure(
                        message=(
                            f"Lineage metadata generation failed: {e}. "
                            "This may indicate issues with the DataHub graph connection or schema resolution. "
                            "Check your graph configuration and ensure all required schemas are accessible."
                        ),
                        context="lineage_metadata_generation_failed",
                    )

        logger.info(
            "Generated %d lineage workunits from queries in %.2f seconds",
            mcp_count,
            timer.elapsed_seconds(),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from super().get_workunits_internal()

        # This is done at the end so that we will have access to tables
        # from all databases in schema_resolver and discovered_tables
        if self.stored_procedures:
            logger.info(
                "Processing %d stored procedure(s) for lineage extraction",
                len(self.stored_procedures),
            )

            for procedure in self.stored_procedures:
                with self.report.report_exc(
                    message="Failed to parse stored procedure lineage",
                    context=procedure.full_name,
                    level=StructuredLogLevel.WARN,
                ):
                    for workunit in auto_workunit(
                        self._filter_procedure_lineage(
                            generate_procedure_lineage(
                                schema_resolver=self.get_schema_resolver(),
                                procedure=procedure.to_base_procedure(),
                                procedure_job_urn=MSSQLDataJob(entity=procedure).urn,
                                is_temp_table=lambda name: not self.is_discovered_table(
                                    name
                                ),
                                default_db=procedure.db,
                                default_schema=procedure.schema,
                                report_failure=lambda name: self._report_procedure_failure(
                                    name
                                ),
                            ),
                            procedure_name=procedure.name,
                        )
                    ):
                        yield workunit

        if self.config.include_query_lineage and self.sql_aggregator:
            yield from self._get_query_based_lineage_workunits()

    def _report_procedure_failure(self, procedure_name: str) -> None:
        """Report a stored procedure lineage extraction failure to the aggregator."""
        if hasattr(self, "aggregator") and self.aggregator is not None:
            self.aggregator.report.num_procedures_failed += 1
            self.aggregator.report.procedure_parse_failures.append(procedure_name)

    def is_discovered_table(self, name: str) -> bool:
        """
        Check if a table name refers to a discovered/real table in the schema.

        Returns True if the table exists in discovered schemas, False if it's
        a temp table, alias, or undiscovered table that should be filtered out.

        Note: Uses instance-level caching to improve performance for repeated calls.
        """
        if name in self._discovered_table_cache:
            return self._discovered_table_cache[name]

        if any(
            re.match(pattern, name, flags=re.IGNORECASE)
            for pattern in self.config.temporary_tables_pattern
        ):
            result = False
            self._discovered_table_cache[name] = result
            return result

        try:
            parts = name.split(".")
            table_name = parts[-1]

            if table_name.startswith("#"):
                result = False
                self._discovered_table_cache[name] = result
                return result

            standardized_name = self.standardize_identifier_case(name)

            if hasattr(self, "aggregator") and self.aggregator is not None:
                schema_resolver = self.get_schema_resolver()

                urn = make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=standardized_name,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                if schema_resolver.has_urn(urn):
                    result = True
                    self._discovered_table_cache[name] = result
                    return result

            if len(parts) >= MSSQL_QUALIFIED_NAME_PARTS:
                schema_name = parts[-2]
                db_name = parts[-3]

                if (
                    self.config.database_pattern.allowed(db_name)
                    and self.config.schema_pattern.allowed(schema_name)
                    and self.config.table_pattern.allowed(name)
                ):
                    if standardized_name not in self.discovered_datasets:
                        result = False
                    else:
                        result = True
                    self._discovered_table_cache[name] = result
                    return result
                else:
                    result = False
                    self._discovered_table_cache[name] = result
                    return result

            # For names with fewer than MSSQL_QUALIFIED_NAME_PARTS,
            # treat as undiscovered since we can't verify
            result = False
            self._discovered_table_cache[name] = result
            return result

        except Exception as e:
            logger.warning("Error parsing table name %s: %s", name, e)
            result = False
            self._discovered_table_cache[name] = result
            return result

    def standardize_identifier_case(self, table_ref_str: str) -> str:
        return (
            table_ref_str.lower()
            if self.config.convert_urns_to_lowercase
            else table_ref_str
        )

    def get_allowed_schemas(self, inspector: Inspector, db_name: str) -> Iterable[str]:
        for schema in super().get_allowed_schemas(inspector, db_name):
            if self.config.quote_schemas:
                yield quoted_name(schema, True)
            else:
                yield schema

    def get_db_name(self, inspector: Inspector) -> str:
        engine = inspector.engine

        try:
            if (
                engine
                and hasattr(engine, "url")
                and hasattr(engine.url, "database")
                and engine.url.database
            ):
                return str(engine.url.database).strip('"')

            if (
                engine
                and hasattr(engine, "url")
                and hasattr(engine.url, "query")
                and "odbc_connect" in engine.url.query
            ):
                # According to the ODBC connection keywords: https://learn.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver17#supported-dsnconnection-string-keywords-and-connection-attributes
                database = re.search(
                    r"DATABASE=([^;]*);",
                    urllib.parse.unquote_plus(str(engine.url.query["odbc_connect"])),
                    flags=re.IGNORECASE,
                )

                if database and database.group(1):
                    return database.group(1)

            return ""

        except Exception as e:
            raise RuntimeError(
                "Unable to get database name from Sqlalchemy inspector"
            ) from e

    def close(self) -> None:
        if self.sql_aggregator:
            self.sql_aggregator.close()
        super().close()
