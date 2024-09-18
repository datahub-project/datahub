import logging
import re
import urllib.parse
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pydantic
import sqlalchemy.dialects.mssql

# This import verifies that the dependencies are available.
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ProgrammingError, ResourceClosedError

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
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
from datahub.ingestion.source.common.data_reader import DataReader
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
    SQLCommonConfig,
    make_sqlalchemy_uri,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import UpstreamLineage
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DatasetLineageTypeClass,
    NumberTypeClass,
    StringTypeClass,
    UnionTypeClass,
    UpstreamClass,
)

logger: logging.Logger = logging.getLogger(__name__)

register_custom_type(sqlalchemy.dialects.mssql.BIT, BooleanTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.MONEY, NumberTypeClass)
register_custom_type(sqlalchemy.dialects.mssql.SMALLMONEY, NumberTypeClass)
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
    mssql_lineage: bool = Field(
        default=False,
        description="Enable automatic lineage",
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

    def __init__(self, config: SQLServerConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "mssql")
        # Cache the table and column descriptions
        self.config: SQLServerConfig = config
        self.current_database = None
        self.table_descriptions: Dict[str, str] = {}
        self.column_descriptions: Dict[str, str] = {}
        self.full_lineage: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        self.procedures_dependencies: Dict[str, List[Dict[str, str]]] = defaultdict(
            list
        )
        if self.config.include_descriptions:
            for inspector in self.get_inspectors():
                db_name = self.get_db_name(inspector)
                with inspector.engine.connect() as conn:
                    if self.config.use_odbc:
                        self._add_output_converters(conn)
                    self._populate_table_descriptions(conn, db_name)
                    self._populate_column_descriptions(conn, db_name)
        if self.config.mssql_lineage:
            for inspector in self.get_inspectors():
                db_name = self.get_db_name(inspector)
                with inspector.engine.connect() as conn:
                    if self.config.use_odbc:
                        self._add_output_converters(conn)
                    try:
                        self._populate_object_links(conn, db_name)
                    except Exception as e:
                        self.warn(
                            logger, "_populate_object_links", f"The error occured: {e}"
                        )
                    try:
                        self._populate_stored_procedures_dependencies(conn)
                    except Exception as e:
                        self.warn(
                            logger,
                            "_populate_stored_procedures_dependencies",
                            f"The error occured: {e}",
                        )

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

    def get_upstreams(self, inspector: Inspector, key: str) -> List[UpstreamClass]:
        if not self.config.mssql_lineage:
            return []
        result = []
        for dest in self.full_lineage.get(key, []):
            dest_name = self.get_identifier(
                schema=dest.get("schema", ""),
                entity=dest.get("name", ""),
                inspector=inspector,
            )
            dest_urn = make_dataset_urn_with_platform_instance(
                self.platform,
                dest_name,
                self.config.platform_instance,
                self.config.env,
            )
            external_upstream_table = UpstreamClass(
                dataset=dest_urn,
                type=DatasetLineageTypeClass.COPY,
            )
            result.append(external_upstream_table)
        return result

    def _populate_stored_procedures_dependencies(self, conn: Connection) -> None:

        trans = conn.begin()

        conn.execute(
            """
            BEGIN;
            IF OBJECT_ID('tempdb.dbo.#ProceduresDependencies', 'U') IS NOT NULL
                DROP TABLE #ProceduresDependencies;

            CREATE TABLE #ProceduresDependencies(
                id                       INT NOT NULL IDENTITY(1,1) PRIMARY KEY
              , procedure_id             INT
              , current_db               NVARCHAR(128)
              , procedure_schema         NVARCHAR(128)
              , procedure_name           NVARCHAR(128)
              , type                     VARCHAR(2)
              , referenced_server_name   NVARCHAR(128)
              , referenced_database_name NVARCHAR(128)
              , referenced_schema_name   NVARCHAR(128)
              , referenced_entity_name   NVARCHAR(128)
              , referenced_id            INT
              , referenced_object_type   VARCHAR(2)
              , is_selected              INT
              , is_updated               INT
              , is_select_all            INT
              , is_all_columns_found     INT
                );

            BEGIN TRY
                    WITH dependencies
                         AS (
                             SELECT
                                    pro.object_id              AS procedure_id
                                  , db_name()                  AS current_db
                                  , schema_name(pro.schema_id) AS procedure_schema
                                  , pro.NAME                   AS procedure_name
                                  , pro.type                   AS type
                                  , p.referenced_id            AS referenced_id
                                  , CASE
                                          WHEN p.referenced_server_name IS NULL AND p.referenced_id IS NOT NULL
                                          THEN @@SERVERNAME
                                          ELSE p.referenced_server_name
                                    END                        AS referenced_server_name
                                  , CASE
                                          WHEN p.referenced_database_name IS NULL AND p.referenced_id IS NOT NULL
                                          THEN db_name()
                                          ELSE p.referenced_database_name
                                    END                        AS referenced_database_name
                                  , CASE
                                          WHEN p.referenced_schema_name IS NULL AND p.referenced_id IS NOT NULL
                                          THEN schema_name(ref_obj.schema_id)
                                          ELSE p.referenced_schema_name
                                    END                        AS referenced_schema_name
                                  , p.referenced_entity_name   AS referenced_entity_name
                                  , ref_obj.type               AS referenced_object_type
                                  , p.is_selected              AS is_selected
                                  , p.is_updated               AS is_updated
                                  , p.is_select_all            AS is_select_all
                                  , p.is_all_columns_found     AS is_all_columns_found
                             FROM   sys.procedures AS pro
                                    CROSS apply sys.dm_sql_referenced_entities(
                                                Concat(schema_name(pro.schema_id), '.', pro.NAME),
                                                'OBJECT') AS p
                                    LEFT JOIN sys.objects AS ref_obj
                                        ON p.referenced_id = ref_obj.object_id
                    )
                    INSERT INTO #ProceduresDependencies (
                                                 procedure_id
                                               , current_db
                                               , procedure_schema
                                               , procedure_name
                                               , type
                                               , referenced_server_name
                                               , referenced_database_name
                                               , referenced_schema_name
                                               , referenced_entity_name
                                               , referenced_id
                                               , referenced_object_type
                                               , is_selected
                                               , is_updated
                                               , is_select_all
                                               , is_all_columns_found)
                    SELECT DISTINCT
                        d.procedure_id
                      , d.current_db
                      , d.procedure_schema
                      , d.procedure_name
                      , d.type
                      , d.referenced_server_name
                      , d.referenced_database_name
                      , d.referenced_schema_name
                      , d.referenced_entity_name
                      , d.referenced_id
                      , d.referenced_object_type
                      , d.is_selected
                      , d.is_updated
                      , d.is_select_all
                      , d.is_all_columns_found
                    FROM dependencies AS d;
            END TRY
            BEGIN CATCH
                IF ERROR_NUMBER() = 2020 or ERROR_NUMBER() = 942
                    PRINT ERROR_MESSAGE()
                ELSE
                    THROW;
            END CATCH;


            DECLARE @id INT;
            DECLARE @referenced_server_name NVARCHAR(128);
            DECLARE @referenced_database_name NVARCHAR(128);
            DECLARE @referenced_id INT;
            DECLARE @referenced_object_type VARCHAR(2);
            DECLARE @referenced_info TABLE (
                id                INT
              , referenced_type   VARCHAR(2)
              , referenced_schema NVARCHAR(128)
              , referenced_name   NVARCHAR(128)
              );
            DECLARE @synonym_info TABLE (
                id              INT
              , referenced_name NVARCHAR(1035)
              , referenced_type VARCHAR(2)
              , referenced_object_id INT
              );
            DECLARE @SQL NVARCHAR(MAX);


            DECLARE proc_depend_cursor CURSOR FOR
            SELECT
                id
              , referenced_database_name
              , referenced_id
              , referenced_object_type
            FROM #ProceduresDependencies;

            OPEN proc_depend_cursor
            FETCH NEXT FROM proc_depend_cursor INTO
                @id
              , @referenced_database_name
              , @referenced_id
              , @referenced_object_type;
            WHILE @@FETCH_STATUS = 0
            BEGIN;

                IF @referenced_id IS NOT NULL
                    BEGIN
                        BEGIN TRY
                            SET @SQL = 'SELECT
                                        ' + CAST(@id AS NVARCHAR(10)) + '
                                          , o.type
                                          , s.name
                                          , o.name
                                        FROM ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.objects AS o
                                        INNER JOIN ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.schemas AS s
                                            ON o.schema_id = s.schema_id
                                        WHERE o.object_id = ' + CAST(@referenced_id AS NVARCHAR(10)) + ';';

                            INSERT INTO  @referenced_info(
                                id
                              , referenced_type
                              , referenced_schema
                              , referenced_name
                                )
                            EXEC sp_executesql @SQL;

                            UPDATE #ProceduresDependencies
                            SET
                              referenced_object_type = ri.referenced_type,
                              referenced_schema_name = ri.referenced_schema,
                              referenced_entity_name = ri.referenced_name
                            FROM #ProceduresDependencies pd
                            INNER JOIN @referenced_info ri
                                ON ri.id = pd.id
                            WHERE CURRENT OF proc_depend_cursor;

                            SET @referenced_object_type = (
                                SELECT
                                    referenced_type
                                FROM @referenced_info
                                WHERE id = @id
                                );
                        END TRY
                        BEGIN CATCH
                            PRINT ERROR_MESSAGE()
                        END CATCH;
                    END;

                IF @referenced_object_type = 'SN'
                  BEGIN
                      BEGIN TRY
                          SET @SQL  = 'SELECT
                                      ' + CAST(@id as NVARCHAR(10)) + '
                                        , syn_obj.name
                                        , syn_obj.type
                                        , syn_obj.object_id
                                      FROM ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.synonyms AS synon
                                      INNER JOIN ' + COALESCE(QUOTENAME(@referenced_database_name), '') + '.sys.objects  AS syn_obj
                                            ON OBJECT_ID(synon.base_object_name) = syn_obj.object_id
                                      WHERE synon.object_id = ' + CAST(@referenced_id AS NVARCHAR(10)) + ';';
                            PRINT @SQL;
                          INSERT INTO @synonym_info(
                              id
                            , referenced_name
                            , referenced_type
                            , referenced_object_id
                              )
                          EXEC sp_executesql @SQL;

                          UPDATE #ProceduresDependencies
                          SET
                              referenced_object_type = si.referenced_type,
                              referenced_entity_name = si.referenced_name,
                              referenced_id = si.referenced_object_id
                          FROM #ProceduresDependencies pd
                          INNER JOIN @synonym_info si
                              ON si.id = pd.id
                          WHERE CURRENT OF proc_depend_cursor;
                      END TRY
                      BEGIN CATCH
                          PRINT ERROR_MESSAGE()
                      END CATCH;
                  END;

                FETCH NEXT FROM proc_depend_cursor INTO
                    @id
                  , @referenced_database_name
                  , @referenced_id
                  , @referenced_object_type;
            END;
            CLOSE proc_depend_cursor;
            DEALLOCATE proc_depend_cursor;
            END;
            """
        )

        _dependencies = conn.execute(
            """
            SELECT DISTINCT
                *
            FROM #ProceduresDependencies
            WHERE referenced_object_type IN ('U ', 'V ', 'P ');
            """
        )

        for row in _dependencies:

            if row:
                _key = f"{row['current_db']}.{row['procedure_schema']}.{row['procedure_name']}"

                self.procedures_dependencies[_key].append(
                    {
                        "referenced_database_name": row["referenced_database_name"],
                        "referenced_schema_name": row["referenced_schema_name"],
                        "referenced_entity_name": row["referenced_entity_name"],
                        "referenced_object_type": row["referenced_object_type"],
                        "is_selected": row["is_selected"],
                        "is_select_all": row["is_select_all"],
                        "is_updated": row["is_updated"],
                    }
                )

        trans.commit()

    def _populate_object_links(self, conn: Connection, db_name: str) -> None:
        # see https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-sql-expression-dependencies-transact-sql
        _links = conn.execute(
            """
            SELECT
              DB_NAME() AS cur_database_name,
              OBJECT_SCHEMA_NAME(referencing_id) AS dst_schema_name,
              OBJECT_NAME(referencing_id) AS dst_object_name,
              so_dst.type AS dst_object_type,
              referenced_server_name AS src_server_name,
              referenced_database_name AS src_database_name,
              COALESCE(OBJECT_SCHEMA_NAME(referenced_id), referenced_schema_name) AS src_schema_name,
              COALESCE(OBJECT_NAME(referenced_id), referenced_entity_name) AS src_object_name,
              so_src.type AS src_object_type
            FROM sys.sql_expression_dependencies
              LEFT JOIN sys.objects AS so_dst ON so_dst.object_id = referencing_id
              LEFT JOIN sys.objects AS so_src ON so_src.object_id = referenced_id
            WHERE so_dst.type in ('U ', 'V ', 'P ')
              AND so_src.type in ('U ', 'V ', 'P ');
            """
        )
        # {"U ": "USER_TABLE", "V ": "VIEW", "P ": "SQL_STORED_PROCEDURE"}
        for row in _links:
            _key = f"{db_name}.{row['dst_schema_name']}.{row['dst_object_name']}"
            self.full_lineage[_key].append(
                {
                    "schema": row["src_schema_name"] or row["dst_schema_name"],
                    "name": row["src_object_name"],
                    "type": row["src_object_type"],
                }
            )

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

    def _process_table(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        table: str,
        sql_config: SQLCommonConfig,
        data_reader: Optional[DataReader],
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_table(
            dataset_name=dataset_name,
            inspector=inspector,
            schema=schema,
            table=table,
            sql_config=sql_config,
            data_reader=data_reader,
        )

        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )

        db_name = self.get_db_name(inspector)
        _key = f"{db_name}.{schema}.{table}"

        upstreams = self.get_upstreams(inspector=inspector, key=_key)
        if upstreams:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(upstreams=upstreams),
            ).as_workunit()

    def _process_view(
        self,
        dataset_name: str,
        inspector: Inspector,
        schema: str,
        view: str,
        sql_config: SQLCommonConfig,
    ) -> Iterable[Union[SqlWorkUnit, MetadataWorkUnit]]:
        yield from super()._process_view(
            dataset_name=dataset_name,
            inspector=inspector,
            schema=schema,
            view=view,
            sql_config=sql_config,
        )

        dataset_urn = make_dataset_urn_with_platform_instance(
            self.platform,
            dataset_name,
            self.config.platform_instance,
            self.config.env,
        )
        db_name = self.get_db_name(inspector)
        _key = f"{db_name}.{schema}.{view}"

        upstreams = self.get_upstreams(inspector=inspector, key=_key)
        if upstreams:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(upstreams=upstreams),
            ).as_workunit()

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
                    platform_instance=sql_config.platform_instance,
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
            platform_instance=sql_config.platform_instance,
            schema=schema,
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
                data_job = MSSQLDataJob(
                    entity=procedure,
                )

                if self.config.mssql_lineage:
                    dependency = self._extract_procedure_dependency(procedure)
                    data_job.incoming = dependency.get_input_datasets
                    data_job.input_jobs = dependency.get_input_datajobs
                    data_job.outgoing = dependency.get_output_datasets

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

    def _extract_procedure_dependency(
        self, procedure: StoredProcedure
    ) -> ProcedureLineageStream:

        upstream_dependencies = []
        for dependency in self.procedures_dependencies.get(
            ".".join([procedure.db, procedure.schema, procedure.name]), []
        ):
            upstream_dependencies.append(
                ProcedureDependency(
                    flow_id=f"{dependency['referenced_database_name']}.{dependency['referenced_schema_name']}.stored_procedures",
                    db=dependency["referenced_database_name"],
                    schema=dependency["referenced_schema_name"],
                    name=dependency["referenced_entity_name"],
                    type=dependency["referenced_object_type"],
                    incoming=int(dependency["is_selected"])
                    or int(dependency["is_select_all"]),
                    outgoing=int(dependency["is_updated"]),
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
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.get_datajob_info_aspect,
        ).as_workunit()

        yield MetadataChangeProposalWrapper(
            entityUrn=data_job.urn,
            aspect=data_job.get_datajob_input_output_aspect,
        ).as_workunit()
        # TODO: Add SubType when it appear

    def construct_flow_workunits(
        self,
        data_flow: MSSQLDataFlow,
    ) -> Iterable[MetadataWorkUnit]:
        yield MetadataChangeProposalWrapper(
            entityUrn=data_flow.urn,
            aspect=data_flow.get_dataflow_info_aspect,
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
            qualified_table_name = f"{self.config.database}.{regular}"
        if self.current_database:
            qualified_table_name = f"{self.current_database}.{regular}"
        return (
            qualified_table_name.lower()
            if self.config.convert_urns_to_lowercase
            else qualified_table_name
        )
