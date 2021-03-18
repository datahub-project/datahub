# This import verifies that the dependencies are available.
import sqlalchemy_pytds  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class SQLServerConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:1433"
    scheme = "mssql+pytds"

    def get_identifier(self, schema: str, table: str):
        regular = f"{schema}.{table}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class SQLServerSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "mssql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)
