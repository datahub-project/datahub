from email.policy import default
import re
from textwrap import dedent
from pydantic.class_validators import validator

from sqlalchemy import sql, util
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.sqltypes import String
from sqla_vertica_python.vertica_python import VerticaDialect

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)
from datahub.utilities import config_clean


@reflection.cache
def get_view_definition(self, connection, view_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {
            'schema': schema.lower()
        }
    else:
        schema_condition = "1"

    view_def = connection.scalar(
        sql.text(
            dedent(
                """
                SELECT VIEW_DEFINITION
                FROM V_CATALOG.VIEWS
                WHERE table_name='%(view_name)s' AND %(schema_condition)s
                """ % {
                    'view_name': view_name,
                    'schema_condition': schema_condition
                }
            )
        )
    )

    return view_def


@reflection.cache
def get_columns(self, connection, table_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {
            'schema': schema.lower()
        }
    else:
        schema_condition = "1"

    sql_pk_column = sql.text(
        dedent(
            """
            SELECT column_name
            FROM v_catalog.primary_keys
            WHERE table_name = '%(table_name)s'
            AND constraint_type = 'p'
            AND %(schema_condition)s
            """ % {
                'table_name': table_name,
                'schema_condition': schema_condition
            }
        )
    )
    primary_key_columns = tuple(row[0] for row in connection.execute(sql_pk_column))

    sql_get_column = sql.text(
        dedent(
            """
            SELECT column_name, data_type, column_default, is_nullable
            FROM v_catalog.columns
            WHERE table_name = '%(table_name)s'
            AND %(schema_condition)s
            UNION
            SELECT column_name, data_type, '' as column_default, true as is_nullable
            FROM v_catalog.view_columns
            WHERE table_name = '%(table_name)s'
            AND %(schema_condition)s
            """ % {
                'table_name': table_name,
                'schema_condition': schema_condition
            }
        )
    )

    columns = []
    for row in list(connection.execute(sql_get_column)):
        name = row.column_name
        primary_key = name in primary_key_columns
        type = row.data_type.lower()
        default = row.column_default
        nullable = row.is_nullable

        column_info = _get_column_info(name, type, default, nullable, schema)
        column_info.update({'primary_key': primary_key})
        columns.append(column_info)
    
    return columns


def _get_column_info(self, name, data_type, default, is_nullable, schema):
    attype = re.sub(r"\(.*\)", "", data_type)

    charlen = re.search(r"\(([\d,]+)\)", data_type)
    if charlen:
        charlen = charlen.group(1)
    args = re.search(r"\((.*)\)", data_type)
    if args and args.group(1):
        args = tuple(re.split(r"\s*,\s*", args.group(1)))
    else:
        args = ()
    kwargs = {}

    if attype == "numeric":
        if charlen:
            prec, scale = charlen.split(",")
            args = (int(prec), int(scale))
        else:
            args = ()
    elif attype == "integer":
        args = ()
    elif attype in ("timestamptz", "timetz"):
        kwargs["timezone"] = True
        if charlen:
            kwargs["precision"] = int(charlen)
        args = ()
    elif attype in (
        "timestamp",
        "time",
    ):
        kwargs["timezone"] = False
        if charlen:
            kwargs["precision"] = int(charlen)
        args = ()
    elif attype.startswith("interval"):
        field_match = re.match(r"interval (.+)", attype, re.I)
        if charlen:
            kwargs["precision"] = int(charlen)
        if field_match:
            kwargs["fields"] = field_match.group(1)
        attype = "interval"
        args = ()
    elif charlen:
        args = (int(charlen),)
    self.ischema_names["UUID"] = UUID
    if attype.upper() in self.ischema_names:
        coltype = self.ischema_names[attype.upper()]
    else:
        coltype = None

    if coltype:
        coltype = coltype(*args, **kwargs)
    else:
        util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
        coltype = sqltypes.NULLTYPE
    # adjust the default value
    autoincrement = False
    if default is not None:
        match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
        if match is not None:
            if issubclass(coltype._type_affinity, sqltypes.Integer):
                autoincrement = True
            # the default is related to a Sequence
            sch = schema
            if "." not in match.group(2) and sch is not None:
                # unconditionally quote the schema name.  this could
                # later be enhanced to obey quoting rules /
                # "quote schema"
                default = (
                    match.group(1)
                    + ('"%s"' % sch)
                    + "."
                    + match.group(2)
                    + match.group(3)
                )

    column_info = dict(
        name=name,
        type=coltype,
        nullable=is_nullable,
        default=default,
        autoincrement=autoincrement,
    )
    return column_info

class UUID(String):

    """The SQL UUID type."""

    __visit_name__ = "UUID"


VerticaDialect.get_view_definition = get_view_definition
VerticaDialect.get_columns = get_columns
VerticaDialect._get_column_info = _get_column_info


class VerticaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "vertica+vertica_python"
    # The ingestion of views is set to false by default due to the beta version.
    include_views = False

    @validator("host_port")
    def clean_host_port(cls, v):
        return config_clean.remove_protocol(v)


class VerticaSource(SQLAlchemySource):

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "vertica")
    
    @classmethod
    def create(cls, config_dict, ctx):
        config = VerticaConfig.parse_obj(config_dict)
        return cls(config, ctx)
