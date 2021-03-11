from typing import Optional, Tuple

# This import verifies that the dependencies are available.
import pybigquery  # noqa: F401

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

    def standardize_schema_table_names(
        self, schema: str, table: str
    ) -> Tuple[str, str]:
        # The get_table_names() method of the BigQuery driver returns table names
        # formatted as "<schema>.<table>" as the table name. Since later calls
        # pass both schema and table, schema essentially is passed in twice. As
        # such, one of the schema names is incorrectly interpreted as the
        # project ID. By removing the schema from the table name, we avoid this
        # issue.
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
