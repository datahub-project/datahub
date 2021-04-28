from typing import Optional

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import custom_types

from .sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
    TimeTypeClass,
    register_custom_type,
)

register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_NTZ, TimeTypeClass)


class SnowflakeConfig(BasicSQLAlchemyConfig):
    scheme = "snowflake"

    database: str  # database is required
    warehouse: Optional[str]
    role: Optional[str]

    def get_sql_alchemy_url(self):
        connect_string = super().get_sql_alchemy_url()
        options = {
            "warehouse": self.warehouse,
            "role": self.role,
        }
        params = "&".join(f"{key}={value}" for (key, value) in options.items() if value)
        if params:
            connect_string = f"{connect_string}?{params}"
        return connect_string

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
