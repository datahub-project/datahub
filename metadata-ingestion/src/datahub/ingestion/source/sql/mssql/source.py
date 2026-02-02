import logging
import re
import urllib.parse
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import sqlalchemy.dialects.mssql
from pydantic import ValidationInfo, field_validator
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError, ResourceClosedError
from sqlalchemy.sql import quoted_name

import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.configuration.pattern_utils import UUID_REGEX
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mce_builder import (
    dataset_urn_to_key,
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
from datahub.metadata.schema_classes import DataJobInputOutputClass
from datahub.metadata.urns import DatasetUrn
from datahub.utilities.file_backed_collections import FileBackedList
from datahub.utilities.urns.error import InvalidUrnError

logger: logging.Logger = logging.getLogger(__name__)

# MSSQL uses 3-part naming: database.schema.table
MSSQL_QUALIFIED_NAME_PARTS = 3

register_custom_type(sqlalchemy.dialects.mssql.BIT, models.BooleanTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.MONEY, models.NumberTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SMALLMONEY, models.NumberTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SQL_VARIANT, models.UnionTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.UNIQUEIDENTIFIER, models.StringTypeClass)

# Patterns copied from Snowflake source
DEFAULT_TEMP_TABLES_PATTERNS = [
    r".*\.FIVETRAN_.*_STAGING\..*",  # fivetran
    r".*__DBT_TMP$",  # dbt
    rf".*\.SEGMENT_{UUID_REGEX}",  # segment
    rf".*\.STAGING_.*_{UUID_REGEX}",  # stitch
    r".*\.(GE_TMP_|GE_TEMP_|GX_TEMP_)[0-9A-F]{8}",  # great expectations
]


class SQLServerConfig(BasicSQLAlchemyConfig):
    # defaults
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
    # Soft deprecation: use_odbc is removed, source type determines ODBC mode
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

    @field_validator("uri_args", mode="after")
    @classmethod
    def validate_uri_args(
        cls, v: Dict[str, Any], info: ValidationInfo, **kwargs: Any
    ) -> Dict[str, Any]:
        # Use Pydantic validation context to get is_odbc flag
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

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
        is_odbc: bool = False,
    ) -> str:
        current_db = current_db or self.database

        scheme = self.scheme
        if is_odbc:
            # Ensure that the import is available.
            import pyodbc  # noqa: F401

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
        # Cache the table and column descriptions
        self.config: SQLServerConfig = config
        self._is_odbc = is_odbc
        self.current_database = None
        self.table_descriptions: Dict[str, str] = {}
        self.column_descriptions: Dict[str, str] = {}
        self.stored_procedures: FileBackedList[StoredProcedure] = FileBackedList()

        self.report = SQLSourceReport()
        if self.config.include_lineage and not self.config.convert_urns_to_lowercase:
            self.report.warning(
                title="Potential issue with lineage",
                message="Lineage may not resolve accurately because 'convert_urns_to_lowercase' is False. To ensure lineage correct, set 'convert_urns_to_lowercase' to True.",
            )

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
                f"Failed to mount output converter for MSSQL data type -150 due to {e}"
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

        # Pass is_odbc via Pydantic validation context for use in validators
        config = SQLServerConfig.model_validate(
            config_dict, context={"is_odbc": is_odbc}
        )
        return cls(config, ctx, is_odbc=is_odbc)

    # override to get table descriptions
    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Dict[str, str], Optional[str]]:
        description, properties, location_urn = super().get_table_properties(
            inspector, schema, table
        )
        # Update description if available.
        db_name: str = self.get_db_name(inspector)
        description = self.table_descriptions.get(
            f"{db_name}.{schema}.{table}", description
        )
        return description, properties, location_urn

    # override to get column descriptions
    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[Dict]:
        columns: List[Dict] = super()._get_columns(
            dataset_name, inspector, schema, table
        )
        # Update column description if available.
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
        Detect if we're running in an RDS/managed environment vs on-premises.
        Uses explicit configuration if provided, otherwise attempts automatic detection.
        Returns True if RDS/managed, False if on-premises.
        """
        if self.config.is_aws_rds is not None:
            logger.info(
                f"Using explicit is_aws_rds configuration: {self.config.is_aws_rds}"
            )
            return self.config.is_aws_rds

        try:
            result = conn.execute("SELECT @@servername AS server_name")
            server_name_row = result.fetchone()
            if server_name_row:
                server_name = server_name_row["server_name"].lower()

                aws_indicators = ["amazon", "amzn", "amaz", "ec2", "rds.amazonaws.com"]
                is_rds = any(indicator in server_name for indicator in aws_indicators)
                if is_rds:
                    logger.info(f"AWS RDS detected based on server name: {server_name}")
                else:
                    logger.info(
                        f"Non-RDS environment detected based on server name: {server_name}"
                    )

                return is_rds
            else:
                logger.warning(
                    "Could not retrieve server name, assuming non-RDS environment"
                )
                return False

        except Exception as e:
            logger.warning(
                f"Failed to detect RDS/managed vs on-prem env, assuming non-RDS environment ({e})"
            )
            return False

    def _get_jobs(self, conn: Connection, db_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Get job information with environment detection to choose optimal method first.
        """
        jobs: Dict[str, Dict[str, Any]] = {}

        # Detect environment to choose optimal method first
        is_rds = self._detect_rds_environment(conn)

        if is_rds:
            # Managed environment - try stored procedures first
            try:
                jobs = self._get_jobs_via_stored_procedures(conn, db_name)
                logger.info(
                    "Successfully retrieved jobs using stored procedures (managed environment)"
                )
                return jobs
            except Exception as sp_error:
                logger.warning(
                    f"Failed to retrieve jobs via stored procedures in managed environment: {sp_error}"
                )
                # Try direct query as fallback (might work in some managed environments)
                try:
                    jobs = self._get_jobs_via_direct_query(conn, db_name)
                    logger.info(
                        "Successfully retrieved jobs using direct query fallback in managed environment"
                    )
                    return jobs
                except Exception as direct_error:
                    self.report.failure(
                        message="Failed to retrieve jobs in managed environment",
                        title="SQL Server Jobs Extraction",
                        context="Both stored procedures and direct query methods failed",
                        exc=direct_error,
                    )
        else:
            # On-premises environment - try direct query first (usually faster)
            try:
                jobs = self._get_jobs_via_direct_query(conn, db_name)
                logger.info(
                    "Successfully retrieved jobs using direct query (on-premises environment)"
                )
                return jobs
            except Exception as direct_error:
                logger.warning(
                    f"Failed to retrieve jobs via direct query in on-premises environment: {direct_error}"
                )
                # Try stored procedures as fallback
                try:
                    jobs = self._get_jobs_via_stored_procedures(conn, db_name)
                    logger.info(
                        "Successfully retrieved jobs using stored procedures fallback in on-premises environment"
                    )
                    return jobs
                except Exception as sp_error:
                    self.report.failure(
                        message="Failed to retrieve jobs in on-premises environment",
                        title="SQL Server Jobs Extraction",
                        context="Both direct query and stored procedures methods failed",
                        exc=sp_error,
                    )

        return jobs

    def _get_jobs_via_stored_procedures(
        self, conn: Connection, db_name: str
    ) -> Dict[str, Dict[str, Any]]:
        jobs: Dict[str, Dict[str, Any]] = {}

        # First, get all jobs
        jobs_result = conn.execute("EXEC msdb.dbo.sp_help_job")
        jobs_data = {}

        # SQLAlchemy 1.3 support was dropped in Sept 2023 (PR #8810)
        # SQLAlchemy 1.4+ returns LegacyRow objects that don't support dictionary-style .get() method
        # Use .mappings() to get MappingResult with dictionary-like rows that support .get()
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

        # Now get job steps for each job, filtering by database
        for job_id, job_info in jobs_data.items():
            try:
                # Get steps for this specific job
                steps_result = conn.execute(
                    f"EXEC msdb.dbo.sp_help_jobstep @job_id = '{job_id}'"
                )

                job_steps = {}
                # Use .mappings() for dictionary-like access (SQLAlchemy 1.4+ compatibility)
                for step_row in steps_result.mappings():
                    # Only include steps that run against our target database
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

                # Only add job if it has relevant steps
                if job_steps:
                    jobs[job_info["name"]] = job_steps

            except Exception as step_error:
                logger.warning(
                    f"Failed to get steps for job {job_info['name']}: {step_error}"
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
                    logger.info(f"No jobs found for database: {db_name}")
                    return

                logger.info(f"Found {len(jobs)} jobs for database: {db_name}")

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
                        logger.warning(f"Failed to process job {job_name}: {job_error}")
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
            # These will be used to construct lineage
            self.stored_procedures.append(procedure)
        yield from self.construct_job_workunits(
            data_job,
            # For stored procedure lineage is ingested later
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
        query = f"EXEC [{procedure.db}].dbo.sp_helptext '{procedure.escape_full_name}'"
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
        logger.debug(f"sql_alchemy_url={url}")
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

    def _is_qualified_table_urn(
        self, urn: str, platform_instance: Optional[str] = None
    ) -> bool:
        """Check if a table URN represents a fully qualified table name.

        MSSQL uses 3-part naming: database.schema.table.
        This helps identify real tables vs. unqualified aliases (e.g., 'dst' in TSQL UPDATE statements).

        Args:
            urn: Dataset URN
            platform_instance: Platform instance to strip from the name if present

        Returns:
            True if the table name is fully qualified (>= 3 parts), False otherwise
        """
        try:
            dataset_urn = DatasetUrn.from_string(urn)
            name = dataset_urn.name

            # Strip platform_instance prefix if present
            if platform_instance and name.startswith(f"{platform_instance}."):
                name = name[len(platform_instance) + 1 :]

            # Check if name has at least 3 parts (database.schema.table)
            return len(name.split(".")) >= MSSQL_QUALIFIED_NAME_PARTS
        except Exception:
            return False

    def _filter_upstream_aliases(
        self, upstream_urns: List[str], platform_instance: Optional[str] = None
    ) -> List[str]:
        """Filter spurious TSQL aliases from upstream lineage using is_temp_table().

        TSQL syntax like "UPDATE dst FROM table dst" causes the parser to extract
        both 'dst' (alias) and 'table' (real table). These aliases appear as upstream
        references but aren't real tables.

        Uses the existing is_temp_table() method to identify aliases:
        - Tables in schema_resolver: Real tables (keep)
        - Tables in discovered_datasets: Real tables (keep)
        - Undiscovered tables: Likely aliases (filter)

        Args:
            upstream_urns: List of upstream dataset URNs
            platform_instance: Platform instance for prefix stripping (consistency with _filter_procedure_lineage)

        Returns:
            Filtered list with only real tables
        """
        if not upstream_urns:
            return []

        filtered = []

        for urn in upstream_urns:
            try:
                dataset_urn = DatasetUrn.from_string(urn)
                table_name = dataset_urn.name

                # Strip platform_instance prefix if present
                # (dataset_urn.name includes platform_instance, but discovered_datasets doesn't)
                if platform_instance and table_name.startswith(f"{platform_instance}."):
                    table_name = table_name[len(platform_instance) + 1 :]

                # Reuse existing is_temp_table() logic to filter aliases
                if not self.is_temp_table(table_name):
                    filtered.append(urn)
            except (InvalidUrnError, ValueError) as e:
                # Keep URNs we can't parse to preserve data integrity.
                # If truly malformed, downstream systems will reject with clear errors.
                # If our parser has a bug, we don't silently lose valid data.
                logger.warning(f"Error parsing URN {urn}: {e}")
                filtered.append(urn)

        return filtered

    def _remap_column_lineage_for_alias(
        self,
        table_urn: str,
        column_name: str,
        aspect: DataJobInputOutputClass,
        procedure_name: Optional[str],
    ) -> List[str]:
        """Remap a column lineage entry from a filtered alias to real table(s).

        Root Cause: TSQL allows table aliases in UPDATE/DELETE statements:
            UPDATE dst SET col=val FROM real_table dst
        sqlglot extracts both 'dst' (alias) and 'real_table' as separate tables.
        When we filter 'dst' from outputDatasets (it's not a real table), column
        lineages still reference it. We must remap those references to real tables.

        Why matching by name: sqlglot often parses aliases with incorrect
        database/schema qualifiers (e.g., "staging.dbo.dst" when dst actually
        refers to "timeseries.dbo.european_priips_kid_information").

        Remapping Strategy:
        1. Try to match by table name only (last component: "dst" matches "xxx.dst")
        2. Exactly one match → remap to that table (common case)
        3. Multiple matches → remap to all, log warning (ambiguous)
        4. No name match → remap to all real downstreams, log warning (fallback)

        This preserves column lineage after alias filtering.

        Args:
            table_urn: The filtered alias table URN
            column_name: The column name
            aspect: DataJobInputOutputClass with outputDatasets
            procedure_name: Procedure name for logging

        Returns:
            List of remapped field URNs pointing to real tables
        """
        remapped_urns: List[str] = []

        try:
            alias_key = dataset_urn_to_key(table_urn)
            if not alias_key:
                logger.warning(
                    f"Could not parse alias URN {table_urn} for column remapping in {procedure_name}"
                )
                return remapped_urns

            alias_table_name = alias_key.name.split(".")[-1]

            # Find real tables with matching table name
            matching_tables = []
            if aspect.outputDatasets:
                for real_table_urn in aspect.outputDatasets:
                    real_key = dataset_urn_to_key(real_table_urn)
                    if real_key:
                        real_table_name = real_key.name.split(".")[-1]
                        if real_table_name == alias_table_name:
                            matching_tables.append(real_table_urn)

            # Remap based on number of matches
            if len(matching_tables) == 1:
                remapped_urn = (
                    f"urn:li:schemaField:({matching_tables[0]},{column_name})"
                )
                remapped_urns.append(remapped_urn)
            elif len(matching_tables) > 1:
                # Multiple matches - remap to all
                for real_table_urn in matching_tables:
                    remapped_urn = (
                        f"urn:li:schemaField:({real_table_urn},{column_name})"
                    )
                    remapped_urns.append(remapped_urn)
                logger.warning(
                    f"Multiple tables match alias {alias_table_name} in {procedure_name}, "
                    f"remapped column {column_name} to all {len(matching_tables)} matches"
                )
            else:
                # No table name match - fallback to all real tables
                if aspect.outputDatasets:
                    for real_table_urn in aspect.outputDatasets:
                        remapped_urn = (
                            f"urn:li:schemaField:({real_table_urn},{column_name})"
                        )
                        remapped_urns.append(remapped_urn)
                    logger.warning(
                        f"No table name match for alias {alias_table_name} in {procedure_name}, "
                        f"remapped column {column_name} to all {len(aspect.outputDatasets)} real tables"
                    )
        except Exception as e:
            logger.warning(
                f"Error parsing alias URN {table_urn} for column remapping in {procedure_name}: {e}"
            )

        if not remapped_urns:
            logger.warning(
                f"Could not remap column lineage for filtered alias {table_urn}.{column_name} in {procedure_name} - "
                f"no real downstream tables available"
            )

        return remapped_urns

    def _filter_downstream_fields(
        self,
        cll_index: int,
        downstream_fields: List[str],
        filtered_downstream_aliases: Optional[set],
        field_urn_pattern: re.Pattern,
        platform_instance: Optional[str],
        aspect: DataJobInputOutputClass,
        procedure_name: Optional[str],
    ) -> List[str]:
        """Filter and remap downstream fields in a column lineage entry.

        Args:
            cll_index: Index of the column lineage entry (for logging)
            downstream_fields: List of downstream field URNs to filter
            filtered_downstream_aliases: Set of table URNs that were filtered as aliases
            field_urn_pattern: Regex pattern to extract table URN and column name
            platform_instance: Platform instance for URN checking
            aspect: DataJobInputOutputClass with outputDatasets
            procedure_name: Procedure name for logging

        Returns:
            List of filtered downstream field URNs
        """
        filtered_downstreams = []
        for field_urn in downstream_fields:
            match = field_urn_pattern.search(field_urn)
            if match:
                table_urn = match.group(1)
                column_name = match.group(2)

                # Check if this downstream points to a filtered alias
                if (
                    filtered_downstream_aliases
                    and table_urn in filtered_downstream_aliases
                ):
                    # Remap to real table(s)
                    remapped_urns = self._remap_column_lineage_for_alias(
                        table_urn, column_name, aspect, procedure_name
                    )
                    filtered_downstreams.extend(remapped_urns)
                elif self._is_qualified_table_urn(table_urn, platform_instance):
                    filtered_downstreams.append(field_urn)
                else:
                    logger.debug(
                        f"Filtered unqualified downstream field in column lineage for {procedure_name}: {field_urn}"
                    )
            else:
                filtered_downstreams.append(field_urn)
        return filtered_downstreams

    def _filter_column_lineage(
        self,
        aspect: DataJobInputOutputClass,
        platform_instance: Optional[str],
        procedure_name: Optional[str],
        filtered_downstream_aliases: Optional[set] = None,
    ) -> None:
        """Filter column lineage (fineGrainedLineages) to remove aliases.

        Applies same 2-step filtering as table-level lineage:
        1. Check if table has 3+ parts (_is_qualified_table_urn)
        2. Check if table is real vs alias (_filter_upstream_aliases for upstreams)
        3. Remap column lineages from filtered downstream aliases to real tables

        Args:
            aspect: DataJobInputOutputClass with lineage to filter
            platform_instance: Platform instance for URN parsing
            procedure_name: Procedure name for logging
            filtered_downstream_aliases: Set of downstream table URNs that were
                filtered as aliases. Column lineages pointing to these will be
                remapped to real downstream tables.

        Modifies aspect.fineGrainedLineages in place.
        """
        if not aspect.fineGrainedLineages:
            return

        filtered_column_lineage = []
        field_urn_pattern = re.compile(r"urn:li:schemaField:\((.*),(.*)\)")

        for cll_index, cll in enumerate(aspect.fineGrainedLineages, 1):
            # Filter upstreams: same logic as inputDatasets
            if cll.upstreams:
                # Step 1: Filter by qualification (3+ parts)
                qualified_upstream_fields = []
                for field_urn in cll.upstreams:
                    match = field_urn_pattern.search(field_urn)
                    if match:
                        table_urn = match.group(1)
                        if self._is_qualified_table_urn(table_urn, platform_instance):
                            qualified_upstream_fields.append(field_urn)
                        else:
                            logger.debug(
                                f"Filtered unqualified upstream field in column lineage for {procedure_name}: {field_urn}"
                            )
                    else:
                        qualified_upstream_fields.append(field_urn)

                # Step 2: Filter aliases (extract table URNs and check)
                upstream_table_urns = []
                field_to_table_map = {}
                for field_urn in qualified_upstream_fields:
                    match = field_urn_pattern.search(field_urn)
                    if match:
                        table_urn = match.group(1)
                        upstream_table_urns.append(table_urn)
                        field_to_table_map[field_urn] = table_urn

                # Apply alias filtering to table URNs
                real_table_urns = set(
                    self._filter_upstream_aliases(
                        upstream_table_urns, platform_instance
                    )
                )

                # Keep only field URNs whose tables passed the filter
                filtered_upstreams = [
                    field_urn
                    for field_urn in qualified_upstream_fields
                    if field_to_table_map.get(field_urn) in real_table_urns
                    or field_urn not in field_to_table_map
                ]

                # Log filtered aliases
                for field_urn in qualified_upstream_fields:
                    if field_urn not in filtered_upstreams:
                        table_urn = field_to_table_map.get(field_urn, "unknown")
                        logger.debug(
                            f"Filtered alias upstream field in column lineage for {procedure_name}: {field_urn} (table: {table_urn})"
                        )

                cll.upstreams = filtered_upstreams

            # Filter downstreams: check qualification and remap aliases
            if cll.downstreams:
                cll.downstreams = self._filter_downstream_fields(
                    cll_index,
                    cll.downstreams,
                    filtered_downstream_aliases,
                    field_urn_pattern,
                    platform_instance,
                    aspect,
                    procedure_name,
                )

            # Only keep column lineage if it has both upstreams and downstreams
            if cll.upstreams and cll.downstreams:
                filtered_column_lineage.append(cll)

        aspect.fineGrainedLineages = (
            filtered_column_lineage if filtered_column_lineage else None
        )

    def _filter_procedure_lineage(
        self,
        mcps: Iterable[MetadataChangeProposalWrapper],
        procedure_name: Optional[str] = None,
    ) -> Iterator[MetadataChangeProposalWrapper]:
        """Filter out unqualified table URNs from stored procedure lineage.

        TSQL syntax like "UPDATE dst FROM table dst" causes sqlglot to extract
        both 'dst' (alias) and 'table' (real table). Unqualified aliases create
        invalid URNs that cause DataHub sink to reject the entire aspect.

        This filter removes URNs with < 3 parts (database.schema.table).

        Args:
            mcps: MCPs from generate_procedure_lineage()
            procedure_name: Procedure name for logging

        Yields:
            Filtered MCPs with only qualified table URNs
        """
        platform_instance = self.get_schema_resolver().platform_instance

        for mcp in mcps:
            # Only filter dataJobInputOutput aspects
            if mcp.aspect and isinstance(mcp.aspect, DataJobInputOutputClass):
                aspect: DataJobInputOutputClass = mcp.aspect

                # Filter inputs: first unqualified tables, then aliases
                if aspect.inputDatasets:
                    qualified_inputs = [
                        urn
                        for urn in aspect.inputDatasets
                        if self._is_qualified_table_urn(urn, platform_instance)
                    ]
                    aspect.inputDatasets = self._filter_upstream_aliases(
                        qualified_inputs, platform_instance
                    )

                # Filter outputs: only unqualified tables
                # Track filtered downstream aliases for column lineage remapping
                filtered_downstream_aliases = set()
                if aspect.outputDatasets:
                    original_outputs = aspect.outputDatasets.copy()
                    aspect.outputDatasets = [
                        urn
                        for urn in aspect.outputDatasets
                        if self._is_qualified_table_urn(urn, platform_instance)
                    ]
                    # Identify which outputs were filtered (these are aliases)
                    filtered_downstream_aliases = set(original_outputs) - set(
                        aspect.outputDatasets
                    )

                # Filter column lineage (with remapping for filtered downstream aliases)
                self._filter_column_lineage(
                    aspect,
                    platform_instance,
                    procedure_name,
                    filtered_downstream_aliases,
                )

                # Skip aspect only if BOTH inputs and outputs are empty
                if not aspect.inputDatasets and not aspect.outputDatasets:
                    logger.warning(
                        f"Skipping lineage for {procedure_name}: all tables were filtered"
                    )
                    continue

            yield mcp

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from super().get_workunits_internal()

        # This is done at the end so that we will have access to tables
        # from all databases in schema_resolver and discovered_tables
        if self.stored_procedures:
            logger.info(
                f"Processing {len(self.stored_procedures)} stored procedure(s) for lineage extraction"
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
                                is_temp_table=self.is_temp_table,
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

    def _report_procedure_failure(self, procedure_name: str) -> None:
        """Report a stored procedure lineage extraction failure to the aggregator."""
        if hasattr(self, "aggregator") and self.aggregator is not None:
            self.aggregator.report.num_procedures_failed += 1
            self.aggregator.report.procedure_parse_failures.append(procedure_name)

    def is_temp_table(self, name: str) -> bool:
        """Check if a table name refers to a temp table or unresolved alias.

        Note: This method is called for each upstream table during lineage filtering.
        If profiling shows this as a bottleneck, consider caching parsed URNs or
        standardized names to reduce redundant processing.
        """
        if any(
            re.match(pattern, name, flags=re.IGNORECASE)
            for pattern in self.config.temporary_tables_pattern
        ):
            return True

        try:
            parts = name.split(".")
            table_name = parts[-1]

            # TSQL temp tables start with #
            if table_name.startswith("#"):
                return True

            # Standardize case early to ensure consistent lookups
            # This must match how get_identifier() stores names in discovered_datasets
            standardized_name = self.standardize_identifier_case(name)

            # Check if the table exists in schema_resolver
            # If we have schema information for it, it's a real table (not an alias)
            # Only check schema_resolver if aggregator is initialized (not in unit tests)
            if hasattr(self, "aggregator") and self.aggregator is not None:
                schema_resolver = self.get_schema_resolver()

                # Use standardized name for URN to match how tables are registered
                urn = make_dataset_urn_with_platform_instance(
                    platform=self.platform,
                    name=standardized_name,
                    env=self.config.env,
                    platform_instance=self.config.platform_instance,
                )

                if schema_resolver.has_urn(urn):
                    return False

            # If not in schema_resolver, check against discovered_datasets
            # For qualified names (>=3 parts), also validate against patterns
            if len(parts) >= MSSQL_QUALIFIED_NAME_PARTS:
                schema_name = parts[-2]
                db_name = parts[-3]

                if (
                    self.config.database_pattern.allowed(db_name)
                    and self.config.schema_pattern.allowed(schema_name)
                    and self.config.table_pattern.allowed(name)
                ):
                    # Table matches our ingestion patterns but wasn't discovered
                    # This is likely an alias or undiscovered table - treat as temp
                    if standardized_name not in self.discovered_datasets:
                        return True
                    else:
                        return False
                else:
                    # Qualified name outside our patterns and not in schema_resolver
                    # No evidence it's a real table - filter it out
                    return True

            # For names with fewer than MSSQL_QUALIFIED_NAME_PARTS (1-part or 2-part),
            # treat as alias/temp table since we can't verify they're real tables
            # without full qualification. This handles common TSQL aliases like "dst", "src".
            # Consistent with _is_qualified_table_urn which requires 3+ parts.
            return True

        except Exception as e:
            # If parsing fails, safer to exclude (return True = treat as temp/alias)
            # than to include potentially spurious aliases in lineage
            logger.warning(f"Error parsing table name {name}: {e}")
            return True

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
