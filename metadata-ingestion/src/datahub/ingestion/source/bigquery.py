from typing import Optional, Tuple

from .sql_common import SQLAlchemyConfig, SQLAlchemySource


class BigQueryConfig(SQLAlchemyConfig):
    scheme = "bigquery"
    project_id: Optional[str] = None

    def get_sql_alchemy_url(self):
        if self.project_id:
            return f"{self.scheme}://{self.project_id}"
        # When project_id is not set, we will attempt to detect the project ID
        # based on the credentials or environment variables.
        # See https://github.com/mxmzdlv/pybigquery#authentication.
        return f"{self.scheme}://"

    def get_identifier(self, schema: str, table: str) -> str:
        if self.project_id:
            return f"{self.project_id}.{schema}.{table}"
        return f"{schema}.{table}"

    def mangle_schema_table_names(self, schema: str, table: str) -> Tuple[str, str]:
        segments = table.split(".")
        if len(segments) != 2:
            raise ValueError(f"expected table to contain schema name already {table}")
        if segments[0] != schema:
            raise ValueError(f"schema {schema} does not match table {table}")
        return segments[0], segments[1]


class BigQuerySource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)
