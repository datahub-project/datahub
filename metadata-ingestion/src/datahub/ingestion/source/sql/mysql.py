# This import verifies that the dependencies are available.
import pymysql  # noqa: F401

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)


class MySQLConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:3306"
    scheme = "mysql+pymysql"
    underlying_platform = "mysql"


class MySQLSource(SQLAlchemySource):
    def __init__(self, config, ctx):

        super().__init__(config, ctx, config.underlying_platform)

    def validate_underlying_platform(self, config):
        if config.underlying_platform not in ["mysql", "mariadb"]:
            raise ValueError(
                f"Invalid {config.underlying_platform} for underlying_platform"
            )

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)
