from .sql_common import SQLAlchemySource, SQLAlchemyConfig

class MySQLConfig(SQLAlchemyConfig):
    #defaults
    host_port = "localhost:3306"
    scheme = "mysql+pymysql"

class MySQLSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "mysql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = MySQLConfig.parse_obj(config_dict)
        return cls(config, ctx)
