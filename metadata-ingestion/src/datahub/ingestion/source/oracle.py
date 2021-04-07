# This import verifies that the dependencies are available.
import cx_Oracle  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class OracleConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "oracle+cx_oracle"


class OracleSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "oracle")

    @classmethod
    def create(cls, config_dict, ctx):
        config = OracleConfig.parse_obj(config_dict)
        return cls(config, ctx)
