import contextlib
import logging
import multiprocessing
import re
import sys
import traceback
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Queue
from typing import List, Optional, Tuple, Type

from datahub.utilities.sql_lineage_parser_impl import SqlLineageSQLParserImpl

with contextlib.suppress(ImportError):
    from sql_metadata import Parser as MetadataSQLParser
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


class MetadataSQLSQLParser(SQLParser):
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
        result = self._parser.tables
        # Sort tables to make the list deterministic
        result.sort()
        return result

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


def sql_lineage_parser_impl_func_wrapper(
    queue: multiprocessing.Queue,
    sql_query: str,
) -> None:
    """
    The wrapper function that computes the tables and columns using the SqlLineageSQLParserImpl
    and puts the results on the shared IPC queue. This is used to isolate SqlLineageSQLParserImpl
    functionality in a separate process, and hence protect our sources from memory leaks originating in
    the sqllineage module.
    :param queue: The shared IPC queue on to which the results will be put.
    :param sql_query: The SQL query to extract the tables & columns from.
    :return: None.
    """
    exception_details: Optional[Tuple[Optional[Type[BaseException]], str]] = None
    tables: List[str] = []
    columns: List[str] = []
    try:
        parser = SqlLineageSQLParserImpl(sql_query)
        tables = parser.get_tables()
        columns = parser.get_columns()
    except BaseException:
        exc_info = sys.exc_info()
        exc_msg: str = str(exc_info[1]) + "".join(traceback.format_tb(exc_info[2]))
        exception_details = (exc_info[0], exc_msg)
        logger.error(exc_msg)
    finally:
        queue.put((tables, columns, exception_details))


class SqlLineageSQLParser(SQLParser):
    def __init__(self, sql_query: str) -> None:
        super().__init__(sql_query)
        self.tables, self.columns = self._get_tables_columns_process_wrapped(sql_query)

    @staticmethod
    def _get_tables_columns_process_wrapped(
        sql_query: str,
    ) -> Tuple[List[str], List[str]]:
        # Invoke sql_lineage_parser_impl_func_wrapper in a separate process to avoid
        # memory leaks from sqllineage module used by SqlLineageSQLParserImpl. This will help
        # shield our sources like lookml & redash, that need to parse a large number of SQL statements,
        # from causing significant memory leaks in the datahub cli during ingestion.
        queue: multiprocessing.Queue = Queue()
        process: multiprocessing.Process = Process(
            target=sql_lineage_parser_impl_func_wrapper,
            args=(
                queue,
                sql_query,
            ),
        )
        process.start()
        tables, columns, exception_details = queue.get(block=True)
        if exception_details is not None:
            raise exception_details[0](f"Sub-process exception: {exception_details[1]}")
        return tables, columns

    def get_tables(self) -> List[str]:
        return self.tables

    def get_columns(self) -> List[str]:
        return self.columns


class DefaultSQLParser(SQLParser):
    parser: SQLParser

    def __init__(self, sql_query: str) -> None:
        super().__init__(sql_query)
        self.parser = SqlLineageSQLParser(sql_query)

    def get_tables(self) -> List[str]:
        return self.parser.get_tables()

    def get_columns(self) -> List[str]:
        return self.parser.get_columns()
