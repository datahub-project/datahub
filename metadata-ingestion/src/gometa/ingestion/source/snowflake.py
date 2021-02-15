from .sql_common import SQLAlchemyConfig, SQLAlchemySource


class SnowflakeConfig(SQLAlchemyConfig):
    # defaults
    scheme = "snowflake"


class SnowflakeSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "snowflake")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SnowflakeConfig.parse_obj(config_dict)
        return cls(config, ctx)
