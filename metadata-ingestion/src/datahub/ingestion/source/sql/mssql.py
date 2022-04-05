import urllib.parse
from typing import Dict, List, Optional, Tuple

import pydantic

# This import verifies that the dependencies are available.
import sqlalchemy_pytds  # noqa: F401
from sqlalchemy.engine.reflection import Inspector

from datahub.ingestion.source.sql.sql_common import (
    BasicSQLAlchemyConfig,
    SQLAlchemySource,
)


class SQLServerConfig(BasicSQLAlchemyConfig):
    # defaults
    host_port = "localhost:1433"
    scheme = "mssql+pytds"

    use_odbc: bool = False
    uri_args: Dict[str, str] = {}

    @pydantic.validator("uri_args")
    def passwords_match(cls, v, values, **kwargs):
        if values["use_odbc"] and "driver" not in v:
            raise ValueError("uri_args must contain a 'driver' option")
        elif not values["use_odbc"] and v:
            raise ValueError("uri_args is not supported when ODBC is disabled")
        return v

    def get_sql_alchemy_url(self):
        if self.use_odbc:
            # Ensure that the import is available.
            import pyodbc  # noqa: F401

            self.scheme = "mssql+pyodbc"

        uri = super().get_sql_alchemy_url(uri_opts=None)
        if self.use_odbc:
            uri = f"{uri}?{urllib.parse.urlencode(self.uri_args)}"
        return uri

    def get_identifier(self, schema: str, table: str) -> str:
        regular = f"{schema}.{table}"
        if self.database_alias:
            return f"{self.database_alias}.{regular}"
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class SQLServerSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "mssql")

        self.table_descriptions = {}
        self.column_descriptions = {}

        for inspector in self.get_inspectors():

            db_name = self.get_db_name(inspector)

            with inspector.engine.connect() as conn:
                # see https://stackoverflow.com/questions/5953330/how-do-i-map-the-id-in-sys-extended-properties-to-an-object-name
                # also see https://www.mssqltips.com/sqlservertip/5384/working-with-sql-server-extended-properties/

                table_metadata = conn.execute(
                    """
                    SELECT SCHEMA_NAME(T.SCHEMA_ID), T.NAME, EP.VALUE
                    FROM SYS.TABLES AS T
                        INNER JOIN SYS.EXTENDED_PROPERTIES AS EP
                            ON EP.MAJOR_ID = T.[OBJECT_ID]
                            AND EP.MINOR_ID = 0
                            AND EP.NAME = 'MS_Description'
                            AND EP.CLASS = 1
                    """
                )

                for schema_name, table_name, table_description in table_metadata:
                    self.table_descriptions[
                        f"{db_name}.{schema_name}.{table_name}"
                    ] = table_description

                column_metadata = conn.execute(
                    """
                    SELECT SCHEMA_NAME(T.SCHEMA_ID), T.NAME, C.NAME, EP.VALUE
                    FROM SYS.TABLES AS T
                        INNER JOIN SYS.ALL_COLUMNS AS C ON C.OBJECT_ID = T.[OBJECT_ID]
                        INNER JOIN SYS.EXTENDED_PROPERTIES AS EP
                            ON EP.MAJOR_ID = T.[OBJECT_ID]
                            AND EP.MINOR_ID = C.COLUMN_ID
                            AND EP.NAME = 'MS_Description'
                            AND EP.CLASS = 1
                    """
                )

                for (
                    schema_name,
                    table_name,
                    column_name,
                    column_description,
                ) in column_metadata:
                    self.column_descriptions[
                        f"{db_name}.{schema_name}.{table_name}.{column_name}"
                    ] = column_description

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)

    # override to get table descriptions
    def get_table_properties(
        self, inspector: Inspector, schema: str, table: str
    ) -> Tuple[Optional[str], Optional[Dict[str, str]], Optional[str]]:

        description, properties, location_urn = super().get_table_properties(
            inspector, schema, table
        )

        db_name = self.get_db_name(inspector)

        description = self.table_descriptions.get(
            f"{db_name}.{schema}.{table}", description
        )

        return description, properties, location_urn

    # override to get column descriptions
    def _get_columns(
        self, dataset_name: str, inspector: Inspector, schema: str, table: str
    ) -> List[dict]:
        columns = super()._get_columns(dataset_name, inspector, schema, table)

        db_name = self.get_db_name(inspector)

        columns = [
            {
                **column,
                "comment": self.column_descriptions.get(
                    f"{db_name}.{schema}.{table}.{column['name']}",
                ),
            }
            for column in columns
        ]
        return columns
