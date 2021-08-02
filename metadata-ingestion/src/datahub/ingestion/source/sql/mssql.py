import urllib.parse
from typing import Dict

import pydantic

# This import verifies that the dependencies are available.
import sqlalchemy_pytds  # noqa: F401

from .sql_common import BasicSQLAlchemyConfig, SQLAlchemySource


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
        if self.database:
            return f"{self.database}.{regular}"
        return regular


class SQLServerSource(SQLAlchemySource):
    def __init__(self, config, ctx):
        super().__init__(config, ctx, "mssql")

    @classmethod
    def create(cls, config_dict, ctx):
        config = SQLServerConfig.parse_obj(config_dict)
        return cls(config, ctx)
