from typing import Iterable, Optional
from unittest.mock import patch

# This import verifies that the dependencies are available.
import cx_Oracle  # noqa: F401
import pydantic
from sqlalchemy import event
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.reflection import Inspector

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
    scheme = "oracle+cx_oracle"

    service_name: Optional[str]

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


class OracleSource(SQLAlchemySource):
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
