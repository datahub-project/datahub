import logging
from typing import Iterable, Optional

import pydantic

# This import verifies that the dependencies are available.
import snowflake.sqlalchemy  # noqa: F401
from snowflake.sqlalchemy import custom_types
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql import text
from sqlalchemy.sql.elements import quoted_name

from datahub.configuration.common import AllowDenyPattern, ConfigModel

from .sql_common import (
    RecordTypeClass,
    SQLAlchemyConfig,
    SQLAlchemySource,
    TimeTypeClass,
    make_sqlalchemy_uri,
    register_custom_type,
)

register_custom_type(custom_types.TIMESTAMP_TZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_LTZ, TimeTypeClass)
register_custom_type(custom_types.TIMESTAMP_NTZ, TimeTypeClass)
register_custom_type(custom_types.VARIANT, RecordTypeClass)

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
    database_pattern: AllowDenyPattern = AllowDenyPattern(
        deny=[
            r"^UTIL_DB$",
            r"^SNOWFLAKE$",
            r"^SNOWFLAKE_SAMPLE_DATA$",
        ]
    )

    database: str = ".*"  # deprecated

    @pydantic.validator("database")
    def note_database_opt_deprecation(cls, v, values, **kwargs):
        logger.warn(
            "snowflake's `database` option has been deprecated; use database_pattern instead"
        )
        values["database_pattern"].allow = f"^{v}$"
        return None

    def get_sql_alchemy_url(self):
        return super().get_sql_alchemy_url(self.database)

    def get_identifier(self, schema: str, table: str) -> str:
        regular = super().get_identifier(schema, table)
        return f"{self.database}.{regular}"


class SnowflakeSource(SQLAlchemySource):
    config: SnowflakeConfig

    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_inspectors(self) -> Iterable[Inspector]:
        url = self.config.get_sql_alchemy_url()
        logger.debug(f"sql_alchemy_url={url}")
        engine = create_engine(url, **self.config.options)

        for db_row in engine.execute(text("SHOW DATABASES")):
            with engine.connect() as conn:
                db = db_row.name
                if self.config.database_pattern.allowed(db):
                    # TRICKY: As we iterate through this loop, we modify the value of
                    # self.config.database so that the get_identifier method can function
                    # as intended.
                    self.config.database = db
                    conn.execute((f'USE DATABASE "{quoted_name(db, True)}"'))
                    inspector = inspect(conn)
                    yield inspector
                else:
                    self.report.report_dropped(db)
