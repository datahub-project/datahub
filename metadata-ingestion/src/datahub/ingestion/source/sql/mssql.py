import logging
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pydantic

# This import verifies that the dependencies are available.
import sqlalchemy_pytds  # noqa: F401
from pydantic.fields import Field
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Connection
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.result import ResultProxy, RowProxy

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_uri,
)

logger: logging.Logger = logging.getLogger(__name__)


class SQLServerConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port: str = Field(default="localhost:1433", description="MSSQL host URL.")
    scheme: str = Field(default="mssql+pytds", description="", hidden_from_schema=True)
    use_odbc: bool = Field(
        default=False,
        description="See https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc.",
    )
    uri_args: Dict[str, str] = Field(
        default={},
        desscription="Arguments to URL-encode when connecting. See https://docs.microsoft.com/en-us/sql/connect/odbc/dsn-connection-string-attribute?view=sql-server-ver15.",
    )
    database_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for databases to filter in ingestion.",
    )
    database: Optional[str] = Field(
        default=None,
        description="database (catalog). If set to Null, all databases will be considered for ingestion.",
    )

    database_alias: Optional[str] = Field(
        default=None,
        description="Alias to apply to database when ingesting. Ignored when `database` is not set.",
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


@platform_name("Microsoft SQL Server", id="mssql")
@config_class(SQLServerConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Not provided by this module, use `bigquery-usage` for that.",
    supported=False,
)
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
        for inspector in self.get_inspectors():
            db_name: str = self.get_db_name(inspector)
            with inspector.engine.connect() as conn:
                self._populate_table_descriptions(conn, db_name)
                self._populate_column_descriptions(conn, db_name)

    def _populate_table_descriptions(self, conn: Connection, db_name: str) -> None:
        # see https://stackoverflow.com/questions/5953330/how-do-i-map-the-id-in-sys-extended-properties-to-an-object-name
        # also see https://www.mssqltips.com/sqlservertip/5384/working-with-sql-server-extended-properties/
        table_metadata: ResultProxy = conn.execute(
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
        for row in table_metadata:  # type: RowProxy
            self.table_descriptions[
                f"{db_name}.{row['schema_name']}.{row['table_name']}"
            ] = row["table_description"]

    def _populate_column_descriptions(self, conn: Connection, db_name: str) -> None:
        column_metadata: RowProxy = conn.execute(
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
        for row in column_metadata:  # type: RowProxy
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
        )  # type:Tuple[Optional[str], Dict[str, str], Optional[str]]
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
                        inspector = inspect(
                            create_engine(url, **self.config.options).connect()
                        )
                        self.current_database = db["name"]
                        yield inspector

    def get_identifier(
        self, *, schema: str, entity: str, inspector: Inspector, **kwargs: Any
    ) -> str:
        regular = f"{schema}.{entity}"
        if self.config.database:
            if self.config.database_alias:
                return f"{self.config.database_alias}.{regular}"
            return f"{self.config.database}.{regular}"
        if self.current_database:
            return f"{self.current_database}.{regular}"
        return regular
