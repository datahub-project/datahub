from typing import Optional

from .sql_common import SQLAlchemyConfig, SQLAlchemySource


class BigQueryConfig(SQLAlchemyConfig):
    scheme = "bigquery"
    project_id: Optional[str]

    def get_sql_alchemy_url(self):
        return f"{self.scheme}://{self.project_id}"


class BigQuerySource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)
