import logging
from typing import Any, Iterable, List, NoReturn, Optional, Tuple, cast
from unittest.mock import patch

# This import verifies that the dependencies are available.
import cx_Oracle
import pydantic
from pydantic.fields import Field
from sqlalchemy import event, sql
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemySource,
    make_sqlalchemy_type,
)
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig

logger = logging.getLogger(__name__)

extra_oracle_types = {
    make_sqlalchemy_type("SDO_GEOMETRY"),
    make_sqlalchemy_type("SDO_POINT_TYPE"),
    make_sqlalchemy_type("SDO_ELEM_INFO_ARRAY"),
    make_sqlalchemy_type("SDO_ORDINATE_ARRAY"),
}
assert OracleDialect.ischema_names


def _raise_err(exc: Exception) -> NoReturn:
    raise exc


def output_type_handler(cursor, name, defaultType, size, precision, scale):
    """Add CLOB and BLOB support to Oracle connection."""

    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    elif defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    cursor.outputtypehandler = output_type_handler


class OracleConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme: str = Field(
        default="oracle+cx_oracle",
        description="Will be set automatically to default value.",
    )

    service_name: Optional[str] = Field(
        default=None, description="Oracle service name. If using, omit `database`."
    )
    database: Optional[str] = Field(
        default=None, description="If using, omit `service_name`."
    )
    add_database_name_to_urn: Optional[bool] = Field(
        default=False,
        description="Add oracle database name to urn, default urn is schema.table",
    )

    @pydantic.validator("service_name")
    def check_service_name(cls, v, values):
        if values.get("database") and v:
            raise ValueError(
                "specify one of 'database' and 'service_name', but not both"
            )
        return v

    def get_sql_alchemy_url(self):
        url = super().get_sql_alchemy_url()
        if self.service_name:
            assert not self.database
            url = f"{url}/?service_name={self.service_name}"
        return url

    def get_identifier(self, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.add_database_name_to_urn:
            if self.database_alias:
                return f"{self.database_alias}.{regular}"
            if self.database:
                return f"{self.database}.{regular}"
            return regular
        else:
            return regular


class OracleInspectorObjectWrapper:
    """
    Inspector class wrapper, which queries DBA_TABLES instead of ALL_TABLES
    """

    def __init__(self, inspector_instance: Inspector):
        self._inspector_instance = inspector_instance
        self.log = logging.getLogger(__name__)
        # tables that we don't want to ingest into the DataHub
        self.exclude_tablespaces: Tuple[str, str] = ("SYSTEM", "SYSAUX")

    def get_schema_names(self) -> List[str]:
        logger.debug("OracleInspectorObjectWrapper is in used")
        s = "SELECT username FROM dba_users ORDER BY username"
        cursor = self._inspector_instance.bind.execute(s)
        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid schema name: {row[0]}"))
            for row in cursor
        ]

    def get_table_names(
        self, schema: Optional[str] = None, order_by: Optional[str] = None
    ) -> List[str]:
        """
        skip order_by, we are not using order_by
        """
        logger.debug("OracleInspectorObjectWrapper is in used")
        schema = self._inspector_instance.dialect.denormalize_name(
            schema or self.default_schema_name
        )

        if schema is None:
            schema = self._inspector_instance.dialect.default_schema_name

        sql_str = "SELECT table_name FROM dba_tables WHERE "
        if self.exclude_tablespaces:
            tablespace_str = ", ".join([f"'{ts}'" for ts in self.exclude_tablespaces])
            sql_str += (
                f"nvl(tablespace_name, 'no tablespace') NOT IN ({tablespace_str}) AND "
            )

        sql_str += "OWNER = :owner AND IOT_NAME IS NULL "
        logger.debug(f"SQL = {sql_str}")
        cursor = self._inspector_instance.bind.execute(sql.text(sql_str), owner=schema)

        return [
            self._inspector_instance.dialect.normalize_name(row[0])
            or _raise_err(ValueError(f"Invalid table name: {row[0]}"))
            for row in cursor
        ]

    def __getattr__(self, item: str) -> Any:
        # Map method call to wrapper class
        if item in self.__dict__:
            return getattr(self, item)
        # Map method call to original class
        return getattr(self._inspector_instance, item)


@platform_name("Oracle")
@config_class(OracleConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.DOMAINS, "Enabled by default")
class OracleSource(SQLAlchemySource):
    """
    This plugin extracts the following:

    - Metadata for databases, schemas, and tables
    - Column types associated with each table
    - Table, row, and column statistics via optional SQL profiling

    Using the Oracle source requires that you've also installed the correct drivers; see the [cx_Oracle docs](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html). The easiest one is the [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html).

    """

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "oracle")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        for inspector in super().get_inspectors():
            event.listen(
                inspector.engine, "before_cursor_execute", before_cursor_execute
            )
            # To silent the mypy lint error
            yield cast(Inspector, OracleInspectorObjectWrapper(inspector))

    def get_workunits(self):
        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            return super().get_workunits()
