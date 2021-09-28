from abc import ABCMeta, abstractmethod
from typing import List

try:
    from sql_metadata import Parser as MetadataSQLParser
except ImportError:
    pass


class SQLParser(metaclass=ABCMeta):
    def __init__(self, sql_query: str) -> None:
        self._sql_query = sql_query

    @abstractmethod
    def get_tables(self) -> List[str]:
        pass


class DefaultSQLParser(SQLParser):
    def __init__(self, sql_query: str) -> None:
        # MetadataSQLParser makes mistakes on lateral flatten queries, use the prefix
        if "lateral flatten" in sql_query:
            sql_query = sql_query[: sql_query.find("lateral flatten")]
        self._parser = MetadataSQLParser(sql_query)

    def get_tables(self) -> List[str]:
        return self._parser.tables
