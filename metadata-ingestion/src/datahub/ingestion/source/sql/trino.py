import sys
from textwrap import dedent

from trino.exceptions import TrinoQueryError  # noqa

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)

if sys.version_info >= (3, 7):
    # This import verifies that the dependencies are available.
    import sqlalchemy_trino  # noqa: F401
    from sqlalchemy import exc, sql
    from sqlalchemy.engine import reflection
    from sqlalchemy_trino import datatype, error
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

    # Include all table properties instead of only "comment" property
    @reflection.cache  # type: ignore
    def get_table_comment(self, connection, table_name: str, schema: str = None, **kw):  # type: ignore
        try:
            properties_table = self._get_full_table(f"{table_name}$properties", schema)
            query = f"SELECT * FROM {properties_table}"
            row = connection.execute(sql.text(query)).fetchone()

            # Generate properties dictionary.
            properties = {}
            for col_name, col_value in row.items():
                properties[col_name] = col_value

            return {"text": properties.get("comment", None), "properties": properties}
        except TrinoQueryError as e:
            if e.error_name in (error.TABLE_NOT_FOUND):
                return dict(text=None)
            raise

    # Include column comment
    @reflection.cache  # type: ignore
    def _get_columns(self, connection, table_name, schema: str = None, **kw):  # type: ignore
        schema = schema or self._get_default_schema_name(connection)
        query = dedent(
            """
            SELECT
                "column_name",
                "data_type",
                "column_default",
                UPPER("is_nullable") AS "is_nullable",
                "comment"
            FROM "information_schema"."columns"
            WHERE "table_schema" = :schema
              AND "table_name" = :table
            ORDER BY "ordinal_position" ASC
        """
        ).strip()
        res = connection.execute(sql.text(query), schema=schema, table=table_name)
        columns = []
        for record in res:
            column = dict(
                name=record.column_name,
                type=datatype.parse_sqltype(record.data_type),
                nullable=record.is_nullable == "YES",
                default=record.column_default,
                comment=record.comment,
            )
            columns.append(column)
        return columns

    TrinoDialect.get_table_names = get_table_names
    TrinoDialect.get_table_comment = get_table_comment
    TrinoDialect._get_columns = _get_columns

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
