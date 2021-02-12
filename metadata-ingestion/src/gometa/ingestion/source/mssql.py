from .sql_common import SQLAlchemySource, SQLAlchemyConfig


class SQLServerConfig(SQLAlchemyConfig):
    # defaults
    host_port = "localhost:1433"
    scheme = "mssql+pytds"


class SQLServerSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "mssql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)
