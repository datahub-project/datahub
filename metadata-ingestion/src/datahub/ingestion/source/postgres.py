from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


class PostgresConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "postgresql+psycopg2"


class PostgresSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "postgresql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.parse_obj(config_dict)
        return cls(config, ctx)
