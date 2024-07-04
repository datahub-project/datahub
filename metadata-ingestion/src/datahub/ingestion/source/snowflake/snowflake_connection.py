import logging
from typing import Any

from snowflake.connector import SnowflakeConnection as NativeSnowflakeConnection
from snowflake.connector.cursor import DictCursor

from datahub.configuration.common import MetaError

logger = logging.getLogger(__name__)


class SnowflakeConnection:
    _connection: NativeSnowflakeConnection

    def __init__(self, connection: NativeSnowflakeConnection):
        self._connection = connection

    def native_connection(self) -> NativeSnowflakeConnection:
        return self._connection

    def query(self, query: str) -> Any:
        try:
            logger.info(f"Query: {query}", stacklevel=2)
            resp = self._connection.cursor(DictCursor).execute(query)
            return resp

        except Exception as e:
            if _is_permission_error(e):
                raise SnowflakePermissionError(e) from e
            raise

    def is_closed(self) -> bool:
        return self._connection.is_closed()

    def close(self):
        self._connection.close()


def _is_permission_error(e: Exception) -> bool:
    msg = str(e)
    # 002003 (02000): SQL compilation error: Database/SCHEMA 'XXXX' does not exist or not authorized.
    # Insufficient privileges to operate on database 'XXXX'
    return "Insufficient privileges" in msg or "not authorized" in msg


class SnowflakePermissionError(MetaError):
    """A permission error has happened"""
