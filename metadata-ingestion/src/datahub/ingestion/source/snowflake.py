# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class SnowflakeConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "snowflake"


class SnowflakeSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)
