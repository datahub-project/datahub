import sys
from textwrap import dedent

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)

if sys.version_info >= (3, 7):
    # This import verifies that the dependencies are available.
    import sqlalchemy_trino  # noqa: F401
    from sqlalchemy import exc, sql
    from sqlalchemy.engine import reflection
    from sqlalchemy_trino.dialect import TrinoDialect

    # Read only table names and skip view names, as view names will also be returned
    # from get_view_names
    @reflection.cache  # type: ignore
    def get_table_names(self, connection, schema: str = None, **kw):  # type: ignore
        schema = schema or self._get_default_schema_name(connection)
        if schema is None:
            raise exc.NoSuchTableError("schema is required")
        query = dedent(
            """
            SELECT "table_name"
            FROM "information_schema"."tables"
            WHERE "table_schema" = :schema and "table_type" != 'VIEW'
        """
        ).strip()
        res = connection.execute(sql.text(query), schema=schema)
        return [row.table_name for row in res]

    TrinoDialect.get_table_names = get_table_names

else:
    raise ModuleNotFoundError("The trino plugin requires Python 3.7 or newer.")


class TrinoConfig(BasicSQLAlchemyConfig):
    # defaults
    scheme = "trino"

    def get_identifier(self: BasicSQLAlchemyConfig, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class TrinoSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "trino")

    @classmethod
    def create(cls, config_dict, ctx):
        config = TrinoConfig.parse_obj(config_dict)
        return cls(config, ctx)
