from abc import ABCMeta, abstractmethod
from typing import List


class SqlParserException(Exception):
    """Raised when sql parser fails"""

    pass


class SQLParser(metaclass=ABCMeta):
    def __init__(self, sql_query: str) -> None:
        self._sql_query = sql_query

    @abstractmethod
    def get_tables(self) -> List[str]:
        pass

    @abstractmethod
    def get_columns(self) -> List[str]:
        pass
