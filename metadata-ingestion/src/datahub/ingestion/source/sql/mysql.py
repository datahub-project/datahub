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
        self.underlying_platform = config.underlying_platform
        self.validate_underlying_platform()
        super().__init__(config, ctx, self.underlying_platform)

    def validate_underlying_platform(self):
        if self.underlying_platform not in ["mysql", "mariadb"]:
            raise ValueError(
                f"Invalid {self.underlying_platform} for underlying_platform"
            )

    def get_underlying_platform(self):
        return self.underlying_platform

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)
