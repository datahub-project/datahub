import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from snowflake.connector.cursor import DictCursor

from datahub.ingestion.source.fivetran.config import SnowflakeDestinationConfig
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakePermissionError,
    is_permission_error,
)

logger: logging.Logger = logging.getLogger(__name__)


class LogDestination(ABC):
    @abstractmethod
    def get_database(self) -> str:
        pass

    @abstractmethod
    def get_schema(self) -> str:
        pass

    @abstractmethod
    def query(self, query: str) -> List[Dict]:
        pass


class SnowflakeDestination(LogDestination):
    def __init__(self, config: SnowflakeDestinationConfig) -> None:
        self._config: SnowflakeDestinationConfig = config
        self._connection = self._config.get_connection()

    def get_database(self) -> str:
        return self._config.database

    def get_schema(self) -> str:
        return self._config.log_schema

    def query(self, query: str) -> List[Dict]:
        try:
            logger.debug("Query : {}".format(query))
            resp = self._connection.cursor(DictCursor).execute(query)
            return [row for row in resp]

        except Exception as e:
            if is_permission_error(e):
                raise SnowflakePermissionError(e) from e
            raise
