import logging
import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pydantic
import sqlalchemy.dialects.mssql
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError, ResourceClosedError

import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.pattern_utils import UUID_REGEX
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
    SqlWorkUnit,
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
from datahub.utilities.file_backed_collections import FileBackedList

logger: logging.Logger = logging.getLogger(__name__)

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
    scheme: str = Field(default="mssql+pytds", description="", hidden_from_docs=True)

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
    use_odbc: bool = Field(
        default=False,
        description="See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc.",
    )
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

    @pydantic.validator("uri_args")
    def passwords_match(cls, v, values, **kwargs):
        if values["use_odbc"] and "driver" not in v:
            raise ValueError("uri_args must contain a 'driver' option")
        elif not values["use_odbc"] and v:
            raise ValueError("uri_args is not supported when ODBC is disabled")
        return v

    def get_sql_alchemy_url(
        self,
        uri_opts: Optional[Dict[str, Any]] = None,
        current_db: Optional[str] = None,
    ) -> str:
        if self.use_odbc:
            # Ensure that the import is available.
            import pyodbc  # noqa: F401

            self.scheme = "mssql+pyodbc"

        uri: str = self.sqlalchemy_uri or make_sqlalchemy_uri(
            self.scheme,  # type: ignore
            self.username,
            self.password.get_secret_value() if self.password else None,
            self.host_port,  # type: ignore
            current_db if current_db else self.database,
            uri_opts=uri_opts,
        )
        if self.use_odbc:
            uri = f"{uri}?{urllib.parse.urlencode(self.uri_args)}"
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
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class SQLServerSource(SQLAlchemySource):
    """
    This plugin extracts the following:
    - Metadata for databases, schemas, views and tables
    - Column types associated with each table/view
    - Table, row, and column statistics via optional SQL profiling
    We have two options for the underlying library used to connect to SQL Server: (1) [python-tds](https://github.com/denisenkom/pytds) and (2) [pyodbc](https://github.com/mkleehammer/pyodbc). The TDS library is pure Python and hence easier to install.
    If you do use pyodbc, make sure to change the source type from `mssql` to `mssql-odbc` so that we pull in the right set of dependencies. This will be needed in most cases where encryption is required, such as managed SQL Server services in Azure.
    """

    report: SQLSourceReport

    def __init__(self, config: SQLServerConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "mssql")
        # Cache the table and column descriptions
        self.config: SQLServerConfig = config
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
                    if self.config.use_odbc:
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
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)

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

    def get_schema_level_workunits(
        self,
        inspector: Inspector,
        schema: str,
        database: str,
    ) -> Iterable[Union[MetadataWorkUnit, SqlWorkUnit]]:
        yield from super().get_schema_level_workunits(
            inspector=inspector,
            schema=schema,
            database=database,
        )
        if self.config.include_stored_procedures:
            try:
                yield from self.loop_stored_procedures(inspector, schema, self.config)
            except Exception as e:
                self.report.failure(
                    message="Failed to list stored procedures",
                    title="SQL Server Stored Procedures Extraction",
                    context="Error occurred during schema-level stored procedure extraction",
                    exc=e,
                )

    def _detect_rds_environment(self, conn: Connection) -> bool:
        """
        Detect if we're running in an RDS/managed environment vs on-premises.
        Returns True if RDS/managed, False if on-premises.
        """
        try:
            # Try to access system tables directly - this typically fails in RDS
            conn.execute("SELECT TOP 1 * FROM msdb.dbo.sysjobs")
            logger.debug(
                "Direct table access successful - likely on-premises environment"
            )
            return False
        except Exception:
            logger.debug("Direct table access failed - likely RDS/managed environment")
            return True

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

        for row in jobs_result:
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
                for step_row in steps_result:
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
        sql_config: SQLServerConfig,
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
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)
        with engine.connect() as conn:
            if self.config.database and self.config.database != "":
                inspector = inspect(conn)
                yield inspector
            else:
                databases = conn.execute(
                    "SELECT name FROM master.sys.databases WHERE name NOT IN \
                  ('master', 'model', 'msdb', 'tempdb', 'Resource', \
                       'distribution' , 'reportserver', 'reportservertempdb'); "
                )
                for db in databases:
                    if self.config.database_pattern.allowed(db["name"]):
                        url = self.config.get_sql_alchemy_url(current_db=db["name"])
                        with create_engine(
                            url, **self.config.options
                        ).connect() as conn:
                            inspector = inspect(conn)
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from super().get_workunits_internal()

        # This is done at the end so that we will have access to tables
        # from all databases in schema_resolver and discovered_tables
        for procedure in self.stored_procedures:
            with self.report.report_exc(
                message="Failed to parse stored procedure lineage",
                context=procedure.full_name,
                level=StructuredLogLevel.WARN,
            ):
                yield from auto_workunit(
                    generate_procedure_lineage(
                        schema_resolver=self.get_schema_resolver(),
                        procedure=procedure.to_base_procedure(),
                        procedure_job_urn=MSSQLDataJob(entity=procedure).urn,
                        is_temp_table=self.is_temp_table,
                        default_db=procedure.db,
                        default_schema=procedure.schema,
                    )
                )

    def is_temp_table(self, name: str) -> bool:
        if any(
            re.match(pattern, name, flags=re.IGNORECASE)
            for pattern in self.config.temporary_tables_pattern
        ):
            logger.debug(f"temp table matched by pattern {name}")
            return True

        try:
            parts = name.split(".")
            table_name = parts[-1]
            schema_name = parts[-2]
            db_name = parts[-3]

            if table_name.startswith("#"):
                return True

            # This is also a temp table if
            #   1. this name would be allowed by the dataset patterns, and
            #   2. we have a list of discovered tables, and
            #   3. it's not in the discovered tables list
            if (
                self.config.database_pattern.allowed(db_name)
                and self.config.schema_pattern.allowed(schema_name)
                and self.config.table_pattern.allowed(name)
                and self.standardize_identifier_case(name)
                not in self.discovered_datasets
            ):
                logger.debug(f"inferred as temp table {name}")
                return True

        except Exception:
            logger.warning(f"Error parsing table name {name} ")
        return False

    def standardize_identifier_case(self, table_ref_str: str) -> str:
        return (
            table_ref_str.lower()
            if self.config.convert_urns_to_lowercase
            else table_ref_str
        )
