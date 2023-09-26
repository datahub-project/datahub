import logging
import re
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pydantic
import sqlalchemy.dialects.mssql

# This import verifies that the dependencies are available.
import sqlalchemy_pytds  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError, ResourceClosedError

from datahub.configuration.common import AllowDenyPattern
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
    make_sqlalchemy_uri,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    StringTypeClass,
    UnionTypeClass,
)

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(sqlalchemy.dialects.mssql.BIT, BooleanTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SQL_VARIANT, UnionTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.UNIQUEIDENTIFIER, StringTypeClass)


class SQLServerConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port: str = Field(default="localhost:1433", description="MSSQL host URL.")
    scheme: str = Field(default="mssql+pytds", description="", hidden_from_docs=True)
    include_stored_procedures: bool = Field(
        default=True,
        description="Include ingest of stored procedures. Requires access to the 'sys' schema.",
    )
    include_stored_procedures_code: bool = Field(
        default=True, description="Include information about object code."
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
    def host(self):
        return self.platform_instance or self.host_port.split(":")[0]

    @property
    def db(self):
        return self.database_alias or self.database


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
    We have two options for the underlying library used to connect to SQL Server: (1) [python-tds](https://github.com/denisenkom/pytds) and (2) [pyodbc](https://github.com/mkleehammer/pyodbc). The TDS library is pure Python and hence easier to install, but only PyODBC supports encrypted connections.
    """

    def __init__(self, config: SQLServerConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "mssql")
        # Cache the table and column descriptions
        self.config: SQLServerConfig = config
        self.current_database = None
        self.table_descriptions: Dict[str, str] = {}
        self.column_descriptions: Dict[str, str] = {}
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
                self.report.report_failure(
                    "jobs",
                    f"Failed to list jobs due to error {e}",
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
                self.report.report_failure(
                    "jobs",
                    f"Failed to list jobs due to error {e}",
                )

    def _get_jobs(self, conn: Connection, db_name: str) -> Dict[str, Dict[str, Any]]:
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
        :return:
        """
        db_name = self.get_db_name(inspector)
        with inspector.engine.connect() as conn:
            jobs = self._get_jobs(conn, db_name)
            for job_name, job_steps in jobs.items():
                job = MSSQLJob(
                    name=job_name,
                    env=sql_config.env,
                    db=db_name,
                    platform_instance=sql_config.host,
                )
                data_flow = MSSQLDataFlow(entity=job)
                yield from self.construct_flow_workunits(data_flow=data_flow)
                yield from self.loop_job_steps(job, job_steps)

    def loop_job_steps(
        self, job: MSSQLJob, job_steps: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        for step_id, step_data in job_steps.items():
            step = JobStep(
                job_name=job.formatted_name,
                step_name=step_data["step_name"],
                flow=job,
            )
            data_job = MSSQLDataJob(entity=step)
            for data_name, data_value in step_data.items():
                data_job.add_property(name=data_name, value=str(data_value))
            yield from self.construct_job_workunits(data_job)

    def loop_stored_procedures(  # noqa: C901
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
            platform_instance=sql_config.host,
        )
        data_flow = MSSQLDataFlow(entity=mssql_default_job)
        with inspector.engine.connect() as conn:
            procedures_data_list = self._get_stored_procedures(conn, db_name, schema)
            procedures = [
                StoredProcedure(flow=mssql_default_job, **procedure_data)
                for procedure_data in procedures_data_list
            ]
            if procedures:
                yield from self.construct_flow_workunits(data_flow=data_flow)
            for procedure in procedures:
                upstream = self._get_procedure_upstream(conn, procedure)
                downstream = self._get_procedure_downstream(conn, procedure)
                data_job = MSSQLDataJob(
                    entity=procedure,
                )
                # TODO: because of this upstream and downstream are more dependencies,
                #  can't be used as DataJobInputOutput.
                #  Should be reorganized into lineage.
                data_job.add_property("procedure_depends_on", str(upstream.as_property))
                data_job.add_property(
                    "depending_on_procedure", str(downstream.as_property)
                )
                procedure_definition, procedure_code = self._get_procedure_code(
                    conn, procedure
                )
                if procedure_definition:
                    data_job.add_property("definition", procedure_definition)
                if sql_config.include_stored_procedures_code and procedure_code:
                    data_job.add_property("code", procedure_code)
                procedure_inputs = self._get_procedure_inputs(conn, procedure)
                properties = self._get_procedure_properties(conn, procedure)
                data_job.add_property(
                    "input parameters", str([param.name for param in procedure_inputs])
                )
                for param in procedure_inputs:
                    data_job.add_property(
                        f"parameter {param.name}", str(param.properties)
                    )
                for property_name, property_value in properties.items():
                    data_job.add_property(property_name, str(property_value))
                yield from self.construct_job_workunits(data_job)

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
        query = f"EXEC [{procedure.db}].dbo.sp_helptext '{procedure.full_name}'"
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
            definition = "\n".join(code_list[:code_slice_index])
            code = "\n".join(code_list[code_slice_index:])
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
            WHERE object_id = object_id('{procedure.full_name}')
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
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.as_datajob_info_aspect,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.as_datajob_input_output_aspect,
        ).as_workunit()
        # TODO: Add SubType when it appear

    def construct_flow_workunits(
        self,
        data_flow: MSSQLDataFlow,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_flow.urn,
            aspect=data_flow.as_dataflow_info_aspect,
        ).as_workunit()
        # TODO: Add SubType when it appear

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
            if self.config.database_alias:
                qualified_table_name = f"{self.config.database_alias}.{regular}"
            else:
                qualified_table_name = f"{self.config.database}.{regular}"
        if self.current_database:
            qualified_table_name = f"{self.current_database}.{regular}"
        return (
            qualified_table_name.lower()
            if self.config.convert_urns_to_lowercase
            else qualified_table_name
        )
