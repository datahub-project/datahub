import logging
from typing import Optional

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import custom_types

from datahub.configuration.common import ConfigModel

from .sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    TimeTypeClass,
    make_sqlalchemy_uri,
    register_custom_type,
)

register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_NTZ, TimeTypeClass)

logger: logging.Logger = logging.getLogger(__name__)


class BaseSnowflakeConfig(ConfigModel):
    # Note: this config model is also used by the snowflake-usage source.

    scheme = "snowflake"

    username: Optional[str] = None
    password: Optional[str] = None
    host_port: str
    warehouse: Optional[str]
    role: Optional[str]

    def get_sql_alchemy_url(self, database=None):
        return make_sqlalchemy_uri(
            self.scheme,
            self.username,
            self.password,
            self.host_port,
            database,
            uri_opts={
                # Drop the options if value is None.
                key: value
                for (key, value) in {
                    "warehouse": self.warehouse,
                    "role": self.role,
                }.items()
                if value
            },
        )


class SnowflakeConfig(BaseSnowflakeConfig, SQLAlchemyConfig):
    database: str

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(self.database)

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
