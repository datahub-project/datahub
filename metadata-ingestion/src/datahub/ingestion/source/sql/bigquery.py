import functools
from typing import Any, Optional, Tuple
from unittest.mock import patch

# This import verifies that the dependencies are available.
import pybigquery  # noqa: F401
import pybigquery.sqlalchemy_bigquery
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.source.sql.sql_common import (
    SQLAlchemyConfig,
    SQLAlchemySource,
    make_sqlalchemy_type,
    register_custom_type,
)

# The existing implementation of this method can be found here:
# https://github.com/googleapis/python-bigquery-sqlalchemy/blob/e0f1496c99dd627e0ed04a0c4e89ca5b14611be2/pybigquery/sqlalchemy_bigquery.py#L967-L974.
# The existing implementation does not use the schema parameter and hence
# does not properly resolve the view definitions. As such, we must monkey
# patch the implementation.


def get_view_definition(self, connection, view_name, schema=None, **kw):
    view = self._get_table(connection, view_name, schema)
    return view.view_query


pybigquery.sqlalchemy_bigquery.BigQueryDialect.get_view_definition = get_view_definition

# Handle the GEOGRAPHY type. We will temporarily patch the _type_map
# in the get_workunits method of the source.
GEOGRAPHY = make_sqlalchemy_type("GEOGRAPHY")
register_custom_type(GEOGRAPHY)
assert pybigquery.sqlalchemy_bigquery._type_map


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


class BigQuerySource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "bigquery")

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self):
        with patch.dict(
            "pybigquery.sqlalchemy_bigquery._type_map",
            {"GEOGRAPHY": GEOGRAPHY},
            clear=False,
        ):
            return super().get_workunits()

    def prepare_profiler_args(self, schema: str, table: str) -> dict:
        self.config: BigQueryConfig
        return dict(
            schema=self.config.project_id,
            table=f"{schema}.{table}",
            limit=self.config.profiling.limit,
            offset=self.config.profiling.offset,
        )

    @staticmethod
    @functools.lru_cache()
    def _get_project_id(inspector: Inspector) -> str:
        with inspector.bind.connect() as connection:
            project_id = connection.connection._client.project
            return project_id

    def get_identifier(
        self,
        *,
        schema: str,
        entity: str,
        inspector: Inspector,
        **kwargs: Any,
    ) -> str:
        assert inspector
        project_id = self._get_project_id(inspector)
        return f"{project_id}.{schema}.{entity}"

    def standardize_schema_table_names(
        self, schema: str, entity: str
    ) -> Tuple[str, str]:
        # The get_table_names() method of the BigQuery driver returns table names
        # formatted as "<schema>.<table>" as the table name. Since later calls
        # pass both schema and table, schema essentially is passed in twice. As
        # such, one of the schema names is incorrectly interpreted as the
        # project ID. By removing the schema from the table name, we avoid this
        # issue.
        segments = entity.split(".")
        if len(segments) != 2:
            raise ValueError(f"expected table to contain schema name already {entity}")
        if segments[0] != schema:
            raise ValueError(f"schema {schema} does not match table {entity}")
        return segments[0], segments[1]
