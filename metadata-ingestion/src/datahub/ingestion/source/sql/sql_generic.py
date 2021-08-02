from datahub.ingestion.api.common import PipelineContext

from .sql_common import SQLAlchemyConfig, SQLAlchemySource


class SQLAlchemyGenericConfig(SQLAlchemyConfig):
    platform: str
    connect_uri: str

    def get_sql_alchemy_url(self):
        return self.connect_uri


class SQLAlchemyGenericSource(SQLAlchemySource):
    def __init__(self, config: SQLAlchemyGenericConfig, ctx: PipelineContext):
        super().__init__(config, ctx, config.platform)

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLAlchemyGenericConfig.parse_obj(config_dict)
        return cls(config, ctx)
