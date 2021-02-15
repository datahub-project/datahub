from .sql_common import SQLAlchemyConfig, SQLAlchemySource


class PostgresConfig(SQLAlchemyConfig):
    # defaults
    scheme = "postgresql+psycopg2"


class PostgresSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "postgresql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = PostgresConfig.parse_obj(config_dict)
        return cls(config, ctx)
