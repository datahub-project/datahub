from typing import Iterable, Optional
from unittest.mock import patch

# This import verifies that the dependencies are available.
import cx_Oracle
import pydantic
from pydantic.fields import Field
from sqlalchemy import event
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
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_type,
)

extra_oracle_types = {
    make_sqlalchemy_type("SDO_GEOMETRY"),
    make_sqlalchemy_type("SDO_POINT_TYPE"),
    make_sqlalchemy_type("SDO_ELEM_INFO_ARRAY"),
    make_sqlalchemy_type("SDO_ORDINATE_ARRAY"),
}
assert OracleDialect.ischema_names


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
            yield inspector

    def get_workunits(self):
        with patch.dict(
            "sqlalchemy.dialects.oracle.base.OracleDialect.ischema_names",
            {klass.__name__: klass for klass in extra_oracle_types},
            clear=False,
        ):
            return super().get_workunits()
