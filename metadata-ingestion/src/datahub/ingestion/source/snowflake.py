# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class SnowflakeConfig(BasicSQLAlchemyConfig):
    scheme = "snowflake"

    database: str  # database is required

    def get_identifier(self, schema: str, table: str) -> str:
        regular = super().get_identifier(schema, table)
        return f"{self.database}.{regular}"


class SnowflakeSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)
