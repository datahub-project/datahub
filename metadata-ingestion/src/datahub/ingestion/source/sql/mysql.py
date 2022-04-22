# This import verifies that the dependencies are available.

import pymysql  # noqa: F401
from sqlalchemy.dialects.mysql import base

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_type,
    register_custom_type,
)

GEOMETRY = make_sqlalchemy_type("GEOMETRY")
POINT = make_sqlalchemy_type("POINT")
LINESTRING = make_sqlalchemy_type("LINESTRING")
POLYGON = make_sqlalchemy_type("POLYGON")
DECIMAL128 = make_sqlalchemy_type("DECIMAL128")

register_custom_type(GEOMETRY)
register_custom_type(POINT)
register_custom_type(LINESTRING)
register_custom_type(POLYGON)
register_custom_type(DECIMAL128)

base.ischema_names["geometry"] = GEOMETRY
base.ischema_names["point"] = POINT
base.ischema_names["linestring"] = LINESTRING
base.ischema_names["polygon"] = POLYGON
base.ischema_names["decimal128"] = DECIMAL128


class MySQLConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:3306"
    scheme = "mysql+pymysql"

    def get_identifier(self, *, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{table}"
        else:
            return regular


class MySQLSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, self.get_platform())

    def get_platform(self):
        return "mysql"

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)
