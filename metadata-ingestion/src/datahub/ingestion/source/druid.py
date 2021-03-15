# This import verifies that the dependencies are available.
import pydruid # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource

class DruidConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "druid"

    def get_sql_alchemy_url(self):
        return f"{BasicSQLAlchemyConfig.get_sql_alchemy_url(self)}/druid/v2/sql/"


class DruidSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "druid")

    @classmethod
    def create(cls, config_dict, ctx):
        config = DruidConfig.parse_obj(config_dict)
        return cls(config, ctx)