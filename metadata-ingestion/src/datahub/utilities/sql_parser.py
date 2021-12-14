import logging
import re
from abc import ABCMeta, abstractmethod
from typing import List

try:
    from sql_metadata import Parser as MetadataSQLParser
except ImportError:
    pass

logger = logging.getLogger(__name__)


class SQLParser(metaclass=ABCMeta):
    def __init__(self, sql_query: str) -> None:
        self._sql_query = sql_query

    @abstractmethod
    def get_tables(self) -> List[str]:
        pass

    @abstractmethod
    def get_columns(self) -> List[str]:
        pass


class DefaultSQLParser(SQLParser):
    _DATE_SWAP_TOKEN = "__d_a_t_e"

    def __init__(self, sql_query: str) -> None:
        super().__init__(sql_query)

        original_sql_query = sql_query

        # MetadataSQLParser makes mistakes on lateral flatten queries, use the prefix
        if "lateral flatten" in sql_query:
            sql_query = sql_query[: sql_query.find("lateral flatten")]

        # MetadataSQLParser also makes mistakes on columns called "date", rename them
        sql_query = re.sub(r"\sdate\s", f" {self._DATE_SWAP_TOKEN} ", sql_query)

        # MetadataSQLParser does not handle "encode" directives well. Remove them
        sql_query = re.sub(r"\sencode [a-zA-Z]*", "", sql_query)

        if sql_query != original_sql_query:
            logger.debug(f"rewrote original query {original_sql_query} as {sql_query}")

        self._parser = MetadataSQLParser(sql_query)

    def get_tables(self) -> List[str]:
        return self._parser.tables

    def get_columns(self) -> List[str]:

        columns_dict = self._parser.columns_dict
        # don't attempt to parse columns if there are joins involved
        if columns_dict.get("join", {}) != {}:
            return []

        columns_alias_dict = self._parser.columns_aliases_dict
        filtered_cols = [
            c
            for c in columns_dict.get("select", {})
            if c != "NULL" and not isinstance(c, list)
        ]
        if columns_alias_dict is not None:
            for col_alias in columns_alias_dict.get("select", []):
                if col_alias in self._parser.columns_aliases:
                    col_name = self._parser.columns_aliases[col_alias]
                    filtered_cols = [
                        col_alias if c == col_name else c for c in filtered_cols
                    ]
        # swap back renamed date column
        return ["date" if c == self._DATE_SWAP_TOKEN else c for c in filtered_cols]
