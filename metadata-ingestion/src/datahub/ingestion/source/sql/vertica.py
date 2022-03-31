from textwrap import dedent
from pydantic.class_validators import validator

from sqlalchemy import sql
from sqlalchemy.engine import reflection
from sqla_vertica_python.vertica_python import VerticaDialect

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)
from datahub.utilities import config_clean

@reflection.cache
def get_view_definition(self, connection, view_name, schema=None, **kw):
    if schema is not None:
        schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
    else:
        schema_condition = "1"

    view_def = connection.scalar(
        sql.text(dedent("""
            SELECT VIEW_DEFINITION
            FROM V_CATALOG.VIEWS
            WHERE table_name='%(view_name)s' AND %(schema_condition)s
        """ % {'view_name': view_name, 'schema_condition': schema_condition}))
    )

    return view_def

VerticaDialect.get_view_definition = get_view_definition


class VerticaConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "vertica+vertica_python"

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
